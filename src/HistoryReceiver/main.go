package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type receiverConfig struct {
	Listen               string
	APIKey               string
	MaxBodyBytes         int64
	WriteTimeout         time.Duration
	Inbox                string
	Archive              string
	Staging              string
	Rejected             string
	Logs                 string
	ArchiveRetentionDays int
	LogRetentionDays     int

	PostgresEnabled   bool
	SynchronousCommit bool
	PostgresURL       string
	PostgresTimezone  string
	PostgresLocation  *time.Location
	ImportInterval    time.Duration
	ImportTimeout     time.Duration
	ImportBatchSize   int
	MaxBatchesPerPass int
}

type receiverServer struct {
	config   receiverConfig
	logger   *log.Logger
	dbPool   *pgxpool.Pool
	importer *batchImporter
	commitMu sync.Mutex
}

type batchHeaders struct {
	BatchID     string
	CollectorID string
	Mode        string
	Server      string
	Start       string
	End         string
	Rows        int
	SHA256      string
}

type ackResponse struct {
	OK           bool   `json:"ok"`
	Committed    bool   `json:"committed"`
	CommitLevel  string `json:"commit_level"`
	BatchID      string `json:"batch_id"`
	SHA256       string `json:"sha256"`
	ReceivedRows int    `json:"received_rows"`
}

type errorResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

func durationMilliseconds(value time.Duration) int64 {
	return int64(value / time.Millisecond)
}

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,180}$`)

func main() {
	configArg := flag.String("config", "receiver.ini", "receiver configuration file")
	importOnce := flag.Bool("import-once", false, "import current inbox batches and exit")
	flag.Parse()

	exePath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	baseDir := filepath.Dir(exePath)
	configPath := resolvePath(baseDir, *configArg)

	config, err := loadReceiverConfig(configPath, baseDir)
	if err != nil {
		log.Fatal(err)
	}
	if config.APIKey == "CHANGE_ME_BEFORE_USE" {
		log.Fatal("change [Server] ApiKey in receiver.ini before starting the Receiver")
	}

	for _, directory := range []string{config.Inbox, config.Archive, config.Staging, config.Rejected, config.Logs} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			log.Fatal(err)
		}
	}

	logPath := filepath.Join(config.Logs, "receiver_"+time.Now().Format("20060102")+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	logger := log.New(io.MultiWriter(os.Stdout, logFile), "", log.Ldate|log.Ltime|log.Lmicroseconds)
	server := &receiverServer{config: config, logger: logger}
	if err := server.recoverStaging(); err != nil {
		logger.Fatal(err)
	}

	ctx := context.Background()
	go runReceiverMaintenance(ctx, config, logger)
	if config.PostgresEnabled {
		pool, err := pgxpool.New(ctx, config.PostgresURL)
		if err != nil {
			logger.Fatal(err)
		}
		defer pool.Close()
		server.dbPool = pool

		importer, err := newBatchImporter(config, pool, logger)
		if err != nil {
			logger.Fatal(err)
		}
		if *importOnce {
			imported, failed, err := importer.importOnce(ctx)
			if err != nil {
				logger.Fatal(err)
			}
			logger.Printf("import-once completed imported=%d failed=%d", imported, failed)
			if failed > 0 {
				os.Exit(2)
			}
			return
		}
		server.importer = importer
		if config.SynchronousCommit {
			imported, failed, scanErr := importer.importOnce(ctx)
			if scanErr != nil {
				logger.Printf("startup inbox import scan failed: %v", scanErr)
			} else if imported > 0 || failed > 0 {
				logger.Printf("startup inbox import imported=%d failed=%d", imported, failed)
			}
		} else {
			go importer.run(ctx)
		}
	} else if *importOnce {
		logger.Fatal("cannot use --import-once while [PostgreSQL] Enabled=false")
	}

	httpServer := &http.Server{
		Addr:              config.Listen,
		Handler:           server.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       60 * time.Second,
	}

	logger.Printf("HistoryReceiver listening on %s", config.Listen)
	logger.Fatal(httpServer.ListenAndServe())
}

func (s *receiverServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/api/history/batch", s.handleBatch)
	return mux
}

func (s *receiverServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	inboxBatches := -1
	if entries, err := os.ReadDir(s.config.Inbox); err == nil {
		inboxBatches = 0
		for _, entry := range entries {
			if entry.IsDir() {
				inboxBatches++
			}
		}
	}
	databaseOK := !s.config.PostgresEnabled
	if s.dbPool != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		databaseOK = s.dbPool.Ping(ctx) == nil
		cancel()
	}
	status := http.StatusOK
	serviceOK := true
	if s.config.PostgresEnabled && !databaseOK {
		status = http.StatusServiceUnavailable
		serviceOK = false
	}
	writeJSON(w, status, map[string]interface{}{
		"ok": serviceOK, "service": "HistoryReceiver",
		"database_ok": databaseOK, "inbox_batches": inboxBatches,
	})
}

func (s *receiverServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !validBearer(r.Header.Get("Authorization"), s.config.APIKey) {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	headers, err := parseBatchHeaders(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if r.ContentLength > s.config.MaxBodyBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "request body is too large")
		return
	}

	if !s.config.SynchronousCommit {
		if ack, found, err := s.findExisting(headers); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		} else if found {
			receiveStarted := time.Now()
			actualHash, bodyBytes, err := hashBody(r.Body, s.config.MaxBodyBytes)
			receiveElapsed := time.Since(receiveStarted)
			if errors.Is(err, errBodyTooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, err.Error())
				return
			}
			if err != nil || bodyBytes == 0 || !strings.EqualFold(actualHash, headers.SHA256) {
				writeError(w, http.StatusBadRequest, "retry body does not match existing batch")
				return
			}
			s.logBatchTiming(headers, bodyBytes, receiveElapsed, 0, nil, time.Since(started))
			s.logger.Printf("idempotent ACK batch=%s rows=%d", headers.BatchID, headers.Rows)
			writeJSON(w, http.StatusOK, ack)
			return
		}
	}

	tempDir := filepath.Join(
		s.config.Staging,
		headers.BatchID+".tmp."+strconv.FormatInt(time.Now().UnixNano(), 10))
	if err := os.Mkdir(tempDir, 0750); err != nil {
		writeError(w, http.StatusInternalServerError, "cannot create staging batch")
		return
	}
	keepTemp := false
	defer func() {
		if !keepTemp {
			_ = os.RemoveAll(tempDir)
		}
	}()

	dataPath := filepath.Join(tempDir, "data.csv")
	staged, err := stageAndParseBody(
		dataPath,
		r.Body,
		s.config.MaxBodyBytes,
		headers.CollectorID,
		s.config.PostgresLocation)
	if err != nil {
		s.logBatchTiming(
			headers,
			staged.BodyBytes,
			staged.Receive,
			0,
			staged.timings(),
			time.Since(started))
		if errors.Is(err, errBodyTooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		if staged.ActualHash == "" && !errors.Is(err, errInvalidBatch) {
			writeError(w, http.StatusInternalServerError, "cannot stage batch body")
			return
		}
		if !strings.EqualFold(staged.ActualHash, headers.SHA256) {
			s.reject(tempDir, headers.BatchID, "sha256")
			keepTemp = true
			writeError(w, http.StatusBadRequest, "SHA-256 mismatch")
			return
		}
		if errors.Is(err, errInvalidBatch) {
			s.reject(tempDir, headers.BatchID, "csv")
			keepTemp = true
			writeError(w, http.StatusBadRequest, "invalid CSV: "+err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "cannot stage batch body")
		return
	}
	actualHash := staged.ActualHash
	bodyBytes := staged.BodyBytes
	if !strings.EqualFold(actualHash, headers.SHA256) {
		s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, staged.timings(), time.Since(started))
		s.reject(tempDir, headers.BatchID, "sha256")
		keepTemp = true
		writeError(w, http.StatusBadRequest, "SHA-256 mismatch")
		return
	}

	rows := len(staged.Rows)
	if rows != headers.Rows {
		s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, staged.timings(), time.Since(started))
		s.reject(tempDir, headers.BatchID, "rows")
		keepTemp = true
		writeError(w, http.StatusBadRequest, fmt.Sprintf("row count mismatch: expected %d, got %d", headers.Rows, rows))
		return
	}

	if err := writeReceiverMeta(filepath.Join(tempDir, "meta.ini"), headers, bodyBytes); err != nil {
		s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, staged.timings(), time.Since(started))
		writeError(w, http.StatusInternalServerError, "cannot write batch metadata")
		return
	}

	if s.config.SynchronousCommit {
		if s.importer == nil {
			s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, staged.timings(), time.Since(started))
			writeError(w, http.StatusServiceUnavailable, "database importer is unavailable")
			return
		}
		batch := importBatch{
			BatchID:     headers.BatchID,
			CollectorID: headers.CollectorID,
			SHA256:      actualHash,
			Rows:        rows,
			Data:        staged.Rows,
			Timings:     staged.timings(),
		}
		s.commitMu.Lock()
		batch, importErr := s.importer.importPreparedBatch(r.Context(), batch)
		if importErr == nil {
			importErr = s.importer.moveToArchive(tempDir, headers.BatchID)
		}
		s.commitMu.Unlock()
		if importErr != nil {
			s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, batch.Timings, time.Since(started))
			s.logger.Printf("synchronous import failed batch=%s error=%v", headers.BatchID, importErr)
			if errors.Is(importErr, errInvalidBatch) {
				s.reject(tempDir, headers.BatchID, "invalid")
				keepTemp = true
				writeError(w, http.StatusBadRequest, "invalid batch payload")
				return
			}
			if errors.Is(importErr, errBatchConflict) {
				writeError(w, http.StatusConflict, importErr.Error())
				return
			}
			writeError(w, http.StatusServiceUnavailable, "PostgreSQL commit failed")
			return
		}
		keepTemp = true
		s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, batch.Timings, time.Since(started))
		s.logger.Printf(
			"committed PostgreSQL batch=%s collector=%s rows=%d bytes=%d elapsed=%s",
			batch.BatchID, headers.CollectorID, rows, bodyBytes,
			time.Since(started).Round(time.Millisecond))
		writeJSON(w, http.StatusOK, makeAck(headers, "database"))
		return
	}

	ack, err := s.commit(tempDir, headers)
	if err != nil {
		s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, staged.timings(), time.Since(started))
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	keepTemp = true
	s.logBatchTiming(headers, bodyBytes, staged.Receive, 0, staged.timings(), time.Since(started))
	s.logger.Printf(
		"committed batch=%s collector=%s rows=%d bytes=%d elapsed=%s",
		headers.BatchID,
		headers.CollectorID,
		rows,
		bodyBytes,
		time.Since(started).Round(time.Millisecond))
	writeJSON(w, http.StatusOK, ack)
}

func (s *receiverServer) logBatchTiming(
	headers batchHeaders,
	bodyBytes int64,
	receive time.Duration,
	validate time.Duration,
	timings *importTimings,
	total time.Duration,
) {
	if s.logger == nil {
		return
	}
	parseMs := int64(0)
	copyMs := int64(0)
	upsertMs := int64(0)
	commitMs := int64(0)
	if timings != nil {
		parseMs = durationMilliseconds(timings.Parse)
		copyMs = durationMilliseconds(timings.Copy)
		upsertMs = durationMilliseconds(timings.Upsert)
		commitMs = durationMilliseconds(timings.Commit)
	}
	totalMs := durationMilliseconds(total)
	s.logger.Printf(
		"batch timing batch_id=%s rows=%d bytes=%d elapsed=%dms ReceiveMs=%d ValidateMs=%d ParseMs=%d CopyMs=%d UpsertMs=%d CommitMs=%d TotalMs=%d",
		headers.BatchID,
		headers.Rows,
		bodyBytes,
		totalMs,
		durationMilliseconds(receive),
		durationMilliseconds(validate),
		parseMs,
		copyMs,
		upsertMs,
		commitMs,
		totalMs)
}

func (s *receiverServer) commit(tempDir string, headers batchHeaders) (ackResponse, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	if ack, found, err := s.findExistingUnlocked(headers); found || err != nil {
		return ack, err
	}
	finalDir := filepath.Join(s.config.Inbox, headers.BatchID)
	if err := os.Rename(tempDir, finalDir); err != nil {
		return ackResponse{}, fmt.Errorf("cannot commit batch: %w", err)
	}
	return makeAck(headers, "inbox"), nil
}

func (s *receiverServer) findExisting(headers batchHeaders) (ackResponse, bool, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	return s.findExistingUnlocked(headers)
}

func (s *receiverServer) findExistingUnlocked(headers batchHeaders) (ackResponse, bool, error) {
	var metaPath string
	for _, root := range []string{s.config.Inbox, s.config.Archive} {
		candidate := filepath.Join(root, headers.BatchID, "meta.ini")
		if _, err := os.Stat(candidate); err == nil {
			metaPath = candidate
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return ackResponse{}, false, err
		}
	}
	if metaPath == "" {
		return ackResponse{}, false, nil
	}
	values, err := readINI(metaPath)
	if err != nil {
		return ackResponse{}, true, fmt.Errorf("existing batch metadata cannot be read: %w", err)
	}
	if !strings.EqualFold(values["Batch.Sha256"], headers.SHA256) ||
		values["Batch.Rows"] != strconv.Itoa(headers.Rows) {
		return ackResponse{}, true, errors.New("batch_id already exists with different content")
	}
	return makeAck(headers, s.commitLevel()), true, nil
}

func (s *receiverServer) commitLevel() string {
	if s.config.SynchronousCommit {
		return "database"
	}
	return "inbox"
}

func makeAck(headers batchHeaders, commitLevel string) ackResponse {
	return ackResponse{
		OK:           true,
		Committed:    true,
		CommitLevel:  commitLevel,
		BatchID:      headers.BatchID,
		SHA256:       strings.ToLower(headers.SHA256),
		ReceivedRows: headers.Rows,
	}
}

func parseBatchHeaders(r *http.Request) (batchHeaders, error) {
	rows, err := strconv.Atoi(r.Header.Get("X-Row-Count"))
	if err != nil || rows < 0 {
		return batchHeaders{}, errors.New("invalid X-Row-Count")
	}
	h := batchHeaders{
		BatchID:     r.Header.Get("X-Batch-Id"),
		CollectorID: r.Header.Get("X-Collector-Id"),
		Mode:        r.Header.Get("X-Batch-Mode"),
		Server:      r.Header.Get("X-Historian-Server"),
		Start:       r.Header.Get("X-Range-Start"),
		End:         r.Header.Get("X-Range-End"),
		Rows:        rows,
		SHA256:      strings.ToLower(r.Header.Get("X-Content-SHA256")),
	}
	if !safeID.MatchString(h.BatchID) || !safeID.MatchString(h.CollectorID) {
		return batchHeaders{}, errors.New("invalid batch or collector ID")
	}
	if h.Mode != "sync" && h.Mode != "init" && h.Mode != "backfill" {
		return batchHeaders{}, errors.New("invalid batch mode")
	}
	if h.Server == "" || h.Start == "" || h.End == "" ||
		len(h.Server) > 200 || len(h.Start) > 100 || len(h.End) > 100 {
		return batchHeaders{}, errors.New("missing or invalid batch metadata")
	}
	decodedHash, err := hex.DecodeString(h.SHA256)
	if err != nil || len(decodedHash) != sha256.Size {
		return batchHeaders{}, errors.New("invalid X-Content-SHA256")
	}
	return h, nil
}

var errBodyTooLarge = errors.New("request body is too large")

type stagedBatch struct {
	ActualHash string
	BodyBytes  int64
	Rows       []importRow
	Receive    time.Duration
	Parse      time.Duration
}

func (b stagedBatch) timings() *importTimings {
	return &importTimings{Parse: b.Parse}
}

type stagingReader struct {
	source      io.Reader
	sink        io.Writer
	Bytes       int64
	ReadElapsed time.Duration
	SourceError error
	SinkError   error
}

func (r *stagingReader) Read(buffer []byte) (int, error) {
	started := time.Now()
	count, readErr := r.source.Read(buffer)
	r.ReadElapsed += time.Since(started)
	if count > 0 {
		written, writeErr := r.sink.Write(buffer[:count])
		r.Bytes += int64(written)
		if writeErr != nil {
			r.SinkError = writeErr
			return written, writeErr
		}
		if written != count {
			r.SinkError = io.ErrShortWrite
			return written, io.ErrShortWrite
		}
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		r.SourceError = readErr
	}
	return count, readErr
}

func stageAndParseBody(
	path string,
	body io.Reader,
	maximum int64,
	collectorID string,
	timezone *time.Location,
) (stagedBatch, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return stagedBatch{}, err
	}
	digest := sha256.New()
	reader := &stagingReader{
		source: io.LimitReader(body, maximum+1),
		sink:   io.MultiWriter(file, digest),
	}
	parseStarted := time.Now()
	rows, parseErr := parseImportRows(reader, collectorID, timezone)
	if parseErr != nil {
		_, drainErr := io.Copy(io.Discard, reader)
		if drainErr != nil && reader.SourceError == nil && reader.SinkError == nil {
			parseErr = drainErr
		}
	}
	parseElapsed := time.Since(parseStarted)
	if reader.SourceError != nil {
		parseErr = reader.SourceError
	}
	if reader.SinkError != nil {
		parseErr = reader.SinkError
	}
	if parseErr == nil && reader.Bytes > maximum {
		parseErr = errBodyTooLarge
	}
	if syncErr := file.Sync(); parseErr == nil && syncErr != nil {
		parseErr = syncErr
	}
	if closeErr := file.Close(); parseErr == nil && closeErr != nil {
		parseErr = closeErr
	}
	if reader.Bytes > maximum {
		parseErr = errBodyTooLarge
	}
	return stagedBatch{
		ActualHash: hex.EncodeToString(digest.Sum(nil)),
		BodyBytes:  reader.Bytes,
		Rows:       rows,
		Receive:    reader.ReadElapsed,
		Parse:      parseElapsed,
	}, parseErr
}

func hashBody(body io.Reader, maximum int64) (string, int64, error) {
	digest := sha256.New()
	written, err := io.Copy(digest, io.LimitReader(body, maximum+1))
	if err == nil && written > maximum {
		err = errBodyTooLarge
	}
	return hex.EncodeToString(digest.Sum(nil)), written, err
}

func writeReceiverMeta(path string, h batchHeaders, bodyBytes int64) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	_, err = fmt.Fprintf(writer,
		"[Batch]\nBatchId=%s\nCollectorId=%s\nMode=%s\nServer=%s\nStart=%s\nEnd=%s\nRows=%d\nSha256=%s\nBytes=%d\nReceivedAt=%s\n",
		h.BatchID, h.CollectorID, h.Mode, h.Server, h.Start, h.End, h.Rows,
		strings.ToLower(h.SHA256), bodyBytes, time.Now().UTC().Format(time.RFC3339Nano))
	if err == nil {
		err = writer.Flush()
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *receiverServer) reject(tempDir, batchID, reason string) {
	destination := filepath.Join(
		s.config.Rejected,
		batchID+"_"+reason+"_"+strconv.FormatInt(time.Now().UnixNano(), 10))
	if err := os.Rename(tempDir, destination); err != nil {
		s.logger.Printf("cannot preserve rejected batch %s: %v", batchID, err)
	}
}

func (s *receiverServer) recoverStaging() error {
	entries, err := os.ReadDir(s.config.Staging)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.Contains(entry.Name(), ".tmp.") {
			continue
		}
		source := filepath.Join(s.config.Staging, entry.Name())
		destination := filepath.Join(s.config.Rejected, entry.Name()+"_recovered")
		if err := os.Rename(source, destination); err != nil {
			return err
		}
		s.logger.Printf("recovered incomplete staging batch=%s", entry.Name())
	}
	return nil
}

func validBearer(header, expected string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) || expected == "" {
		return false
	}
	actual := []byte(strings.TrimPrefix(header, prefix))
	wanted := []byte(expected)
	if len(actual) != len(wanted) {
		return false
	}
	return subtle.ConstantTimeCompare(actual, wanted) == 1
}

func loadReceiverConfig(path, baseDir string) (receiverConfig, error) {
	values, err := readINI(path)
	if err != nil {
		return receiverConfig{}, err
	}
	maximum, err := strconv.ParseInt(valueOr(values, "Server.MaxBodyBytes", "20971520"), 10, 64)
	if err != nil || maximum <= 0 {
		return receiverConfig{}, errors.New("invalid [Server] MaxBodyBytes")
	}
	writeTimeoutSeconds, err := strconv.Atoi(valueOr(values, "Server.WriteTimeoutSeconds", "60"))
	if err != nil || writeTimeoutSeconds <= 0 {
		return receiverConfig{}, errors.New("invalid [Server] WriteTimeoutSeconds")
	}
	postgresEnabled, err := parseBool(valueOr(values, "PostgreSQL.Enabled", "false"))
	if err != nil {
		return receiverConfig{}, fmt.Errorf("invalid [PostgreSQL] Enabled: %w", err)
	}
	synchronousCommit, err := parseBool(valueOr(values, "PostgreSQL.SynchronousCommit", "false"))
	if err != nil {
		return receiverConfig{}, fmt.Errorf("invalid [PostgreSQL] SynchronousCommit: %w", err)
	}
	if synchronousCommit && !postgresEnabled {
		return receiverConfig{}, errors.New("SynchronousCommit requires PostgreSQL Enabled=true")
	}
	postgresTimezone := valueOr(values, "PostgreSQL.Timezone", "Asia/Shanghai")
	postgresLocation, err := time.LoadLocation(postgresTimezone)
	if err != nil {
		return receiverConfig{}, fmt.Errorf("invalid PostgreSQL timezone %q: %w", postgresTimezone, err)
	}
	intervalSeconds, err := strconv.Atoi(valueOr(values, "PostgreSQL.ImportIntervalSeconds", "30"))
	if err != nil || intervalSeconds <= 0 {
		return receiverConfig{}, errors.New("invalid [PostgreSQL] ImportIntervalSeconds")
	}
	maxBatches, err := strconv.Atoi(valueOr(values, "PostgreSQL.MaxBatchesPerPass", "20"))
	if err != nil || maxBatches <= 0 {
		return receiverConfig{}, errors.New("invalid [PostgreSQL] MaxBatchesPerPass")
	}
	archiveRetentionDays, err := strconv.Atoi(valueOr(values, "Maintenance.ArchiveRetentionDays", "30"))
	if err != nil || archiveRetentionDays <= 0 {
		return receiverConfig{}, errors.New("invalid [Maintenance] ArchiveRetentionDays")
	}
	logRetentionDays, err := strconv.Atoi(valueOr(values, "Maintenance.LogRetentionDays", "30"))
	if err != nil || logRetentionDays <= 0 {
		return receiverConfig{}, errors.New("invalid [Maintenance] LogRetentionDays")
	}
	importTimeoutSeconds, err := strconv.Atoi(valueOr(values, "PostgreSQL.ImportTimeoutSeconds", "45"))
	if err != nil || importTimeoutSeconds <= 0 {
		return receiverConfig{}, errors.New("invalid [PostgreSQL] ImportTimeoutSeconds")
	}
	if synchronousCommit && importTimeoutSeconds >= writeTimeoutSeconds {
		return receiverConfig{}, errors.New("[PostgreSQL] ImportTimeoutSeconds must be less than [Server] WriteTimeoutSeconds")
	}
	importBatchSize, err := strconv.Atoi(valueOr(values, "PostgreSQL.ImportBatchSize", "500"))
	if err != nil || importBatchSize <= 0 || importBatchSize > 5000 {
		return receiverConfig{}, errors.New("invalid [PostgreSQL] ImportBatchSize")
	}
	config := receiverConfig{
		Listen:               valueOr(values, "Server.Listen", "0.0.0.0:8080"),
		APIKey:               values["Server.ApiKey"],
		MaxBodyBytes:         maximum,
		WriteTimeout:         time.Duration(writeTimeoutSeconds) * time.Second,
		Inbox:                resolvePath(baseDir, valueOr(values, "Files.Inbox", "inbox")),
		Archive:              resolvePath(baseDir, valueOr(values, "Files.Archive", "archive")),
		Staging:              resolvePath(baseDir, valueOr(values, "Files.Staging", "staging")),
		Rejected:             resolvePath(baseDir, valueOr(values, "Files.Rejected", "rejected")),
		Logs:                 resolvePath(baseDir, valueOr(values, "Files.Logs", "logs")),
		ArchiveRetentionDays: archiveRetentionDays,
		LogRetentionDays:     logRetentionDays,
		PostgresEnabled:      postgresEnabled,
		SynchronousCommit:    synchronousCommit,
		PostgresTimezone:     postgresTimezone,
		PostgresLocation:     postgresLocation,
		ImportInterval:       time.Duration(intervalSeconds) * time.Second,
		ImportTimeout:        time.Duration(importTimeoutSeconds) * time.Second,
		ImportBatchSize:      importBatchSize,
		MaxBatchesPerPass:    maxBatches,
	}
	if config.APIKey == "" {
		return receiverConfig{}, errors.New("[Server] ApiKey is required")
	}
	if config.PostgresEnabled {
		host := valueOr(values, "PostgreSQL.Host", "127.0.0.1")
		port, err := strconv.Atoi(valueOr(values, "PostgreSQL.Port", "5432"))
		if err != nil || port <= 0 || port > 65535 {
			return receiverConfig{}, errors.New("invalid [PostgreSQL] Port")
		}
		database := values["PostgreSQL.Database"]
		user := values["PostgreSQL.User"]
		password := values["PostgreSQL.Password"]
		if database == "" || user == "" || password == "" || password == "CHANGE_ME_BEFORE_USE" {
			return receiverConfig{}, errors.New("configure PostgreSQL Database, User, and Password before enabling import")
		}
		connectionURL := &url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(user, password),
			Host:   net.JoinHostPort(host, strconv.Itoa(port)),
			Path:   database,
		}
		query := connectionURL.Query()
		query.Set("sslmode", valueOr(values, "PostgreSQL.SSLMode", "disable"))
		connectionURL.RawQuery = query.Encode()
		config.PostgresURL = connectionURL.String()
	}
	return config, nil
}

func parseBool(text string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	default:
		return false, errors.New("expected true or false")
	}
}

func readINI(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	values := make(map[string]string)
	section := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "\ufeff"))
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			values[section+"."+strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return values, scanner.Err()
}

func resolvePath(baseDir, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(baseDir, path)
}

func valueOr(values map[string]string, key, fallback string) string {
	if value := values[key]; value != "" {
		return value
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{OK: false, Error: message})
}
