using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

namespace DeltaVHistoryCLI
{
    class BatchSendTimings
    {
        public long SendMilliseconds;
        public long AckWaitMilliseconds;
        public long TotalMilliseconds;
    }

    class BatchSendException : Exception
    {
        public readonly int StatusCode;
        public readonly bool Permanent;
        public readonly bool AuthenticationFailure;
        public readonly BatchSendTimings Timings;

        public BatchSendException(
            string message,
            int statusCode,
            bool permanent,
            bool authenticationFailure,
            BatchSendTimings timings) : base(message)
        {
            StatusCode = statusCode;
            Permanent = permanent;
            AuthenticationFailure = authenticationFailure;
            Timings = timings;
        }
    }

    class BatchReceipt
    {
        public string BatchId;
        public string Mode;
        public string CommitLevel;
        public DateTime RangeStart;
        public DateTime RangeEnd;
        public int Rows;
        public long PayloadBytes;
        public BatchSendTimings Timings;
    }

    delegate void BatchAcknowledged(BatchReceipt receipt);

    class BatchSender
    {
        private class PendingBatch
        {
            public string Directory;
            public bool HasRangeStart;
            public DateTime RangeStart;
        }

        private string _url;
        private string _apiKey;
        private int _timeoutMilliseconds;
        private long _backlogDrainMilliseconds;
        private string _ackMode;
        private string _spoolDirectory;
        private SyncLogger _log;
        private BatchSendTimings _lastTimings;

        public BatchSender(
            IniConfig config,
            string spoolDirectory,
            SyncLogger log)
        {
            _url = config.Get("Receiver", "Url", "");
            _apiKey = config.Get("Receiver", "ApiKey", "");
            _timeoutMilliseconds = checked(config.GetInt("Receiver", "TimeoutSeconds", 105) * 1000);
            int drainSeconds = config.GetInt("Receiver", "BacklogDrainSeconds", 60);
            _backlogDrainMilliseconds = checked((long)drainSeconds * 1000L);
            _ackMode = config.Get("Receiver", "AckMode", "inbox").ToLowerInvariant();
            _spoolDirectory = spoolDirectory;
            _log = log;

            if (_url.Length == 0)
                throw new Exception("[Receiver] Url is required when Sender is enabled.");
            if (_apiKey.Length == 0)
                throw new Exception("[Receiver] ApiKey is required when Sender is enabled.");
            if (_ackMode != "inbox" && _ackMode != "database")
                throw new Exception("[Receiver] AckMode must be inbox or database.");
            if (_timeoutMilliseconds <= 0 || drainSeconds <= 0)
                throw new Exception("Receiver timeout and BacklogDrainSeconds must be positive.");
        }

        public BatchSendTimings LastTimings
        {
            get { return _lastTimings; }
        }

        public BatchReceipt Send(HistoryBatch batch, BatchPayload payload)
        {
            if (batch == null)
                throw new ArgumentNullException("batch");
            if (payload == null || payload.Buffer == null)
                throw new ArgumentNullException("payload");
            if (payload.Length < 0 || payload.Length > payload.Buffer.Length)
                throw new ArgumentException("Batch payload length is invalid.", "payload");
            string actualHash = payload.Sha256;
            if (String.IsNullOrEmpty(actualHash))
                actualHash = batch.Sha256;
            if (String.IsNullOrEmpty(actualHash))
                actualHash = BatchEncoder.ComputeSha256(payload.Buffer, payload.Length);
            ValidateSha256(actualHash);
            payload.Sha256 = actualHash;
            batch.Sha256 = actualHash;
            using (MemoryStream input = new MemoryStream(
                payload.Buffer,
                0,
                payload.Length,
                false,
                true))
            {
                return SendPayload(
                    batch.BatchId,
                    batch.CollectorId,
                    batch.Mode,
                    batch.Server,
                    FormatTime(batch.RangeStart),
                    FormatTime(batch.RangeEnd),
                    batch.Samples.Count,
                    actualHash,
                    input,
                    payload.Length);
            }
        }

        public BatchReceipt Send(HistoryBatch batch, byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException("data");
            BatchPayload payload = new BatchPayload();
            payload.Buffer = data;
            payload.Length = data.Length;
            payload.Sha256 = batch == null ? null : batch.Sha256;
            return Send(batch, payload);
        }

        public int SendPending()
        {
            return SendPending(null);
        }

        public int SendPending(BatchAcknowledged acknowledged)
        {
            string pendingRoot = Path.Combine(_spoolDirectory, "pending");
            string failedRoot = Path.Combine(_spoolDirectory, "failed");
            string quarantineRoot = Path.Combine(_spoolDirectory, "quarantine");
            Directory.CreateDirectory(pendingRoot);
            Directory.CreateDirectory(failedRoot);
            Directory.CreateDirectory(quarantineRoot);

            List<PendingBatch> batches = GetPendingBatches(pendingRoot);

            Stopwatch drainClock = Stopwatch.StartNew();
            int sent = 0;
            while (batches.Count > 0)
            {
                if (sent > 0 && drainClock.ElapsedMilliseconds >= _backlogDrainMilliseconds)
                {
                    _log.Write(
                        "Sender drain time limit reached seconds=" +
                        (_backlogDrainMilliseconds / 1000L).ToString(CultureInfo.InvariantCulture));
                    break;
                }

                string batchDirectory = batches[0].Directory;
                string batchName = Path.GetFileName(batchDirectory);
                try
                {
                    BatchReceipt receipt = SendOne(batchDirectory);
                    if (acknowledged != null)
                        acknowledged(receipt);
                    Directory.Delete(batchDirectory, true);
                    sent++;
                    _log.Write(
                        "Removed acknowledged outbox batch=" + batchName +
                        " rows=" + receipt.Rows.ToString(CultureInfo.InvariantCulture) +
                        " bytes=" + receipt.PayloadBytes.ToString(CultureInfo.InvariantCulture) +
                        " SendMs=" + receipt.Timings.SendMilliseconds.ToString(CultureInfo.InvariantCulture) +
                        " AckWaitMs=" + receipt.Timings.AckWaitMilliseconds.ToString(CultureInfo.InvariantCulture) +
                        " TotalMs=" + receipt.Timings.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
                    batches.RemoveAt(0);
                }
                catch (InvalidDataException ex)
                {
                    string failedDirectory = Path.Combine(
                        quarantineRoot,
                        batchName + "_invalid_" + Guid.NewGuid().ToString("N"));
                    Directory.Move(batchDirectory, failedDirectory);
                    _log.Write("Moved invalid local batch to quarantine=" + batchName + " error=" + ex.Message);
                    _log.Write("Send stopped after quarantining invalid batch=" + batchName);
                    return 41;
                }
                catch (BatchSendException ex)
                {
                    if (ex.Permanent)
                    {
                        string failedDirectory = Path.Combine(
                            failedRoot,
                            batchName + "_http" + ex.StatusCode.ToString() + "_" + Guid.NewGuid().ToString("N"));
                        Directory.Move(batchDirectory, failedDirectory);
                        _log.Write("Receiver permanently rejected batch=" + batchName + " error=" + ex.Message);
                        _log.Write("Send stopped after permanent rejection batch=" + batchName);
                        return 41;
                    }
                    _log.Write("Send stopped; pending retained batch=" + batchName + " error=" + ex.Message);
                    return ex.AuthenticationFailure ? 42 : 40;
                }
                catch (Exception ex)
                {
                    _log.Write("Send stopped; pending retained batch=" + batchName + " error=" + ex.Message);
                    return 40;
                }
            }

            PendingStats remaining = new SpoolStore(_spoolDirectory).GetPendingStats();
            _log.Write(
                "Sender completed drain=true sent=" +
                sent.ToString(CultureInfo.InvariantCulture) +
                " pendingBatches=" + remaining.Batches.ToString(CultureInfo.InvariantCulture) +
                " pendingBytes=" + remaining.Bytes.ToString(CultureInfo.InvariantCulture) +
                " elapsedMs=" + drainClock.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture));
            return 0;
        }

        private static List<PendingBatch> GetPendingBatches(string pendingRoot)
        {
            string[] directories = Directory.GetDirectories(pendingRoot);
            List<PendingBatch> result = new List<PendingBatch>();
            int i;
            for (i = 0; i < directories.Length; i++)
            {
                PendingBatch item = new PendingBatch();
                item.Directory = directories[i];
                item.HasRangeStart = TryReadRangeStart(directories[i], out item.RangeStart);
                result.Add(item);
            }
            result.Sort(delegate(PendingBatch a, PendingBatch b)
            {
                if (a.HasRangeStart != b.HasRangeStart)
                    return a.HasRangeStart ? 1 : -1;
                if (a.HasRangeStart)
                {
                    int byTime = a.RangeStart.CompareTo(b.RangeStart);
                    if (byTime != 0)
                        return byTime;
                }
                return String.Compare(
                    a.Directory,
                    b.Directory,
                    StringComparison.OrdinalIgnoreCase);
            });
            return result;
        }

        private static bool TryReadRangeStart(string batchDirectory, out DateTime value)
        {
            value = DateTime.MinValue;
            try
            {
                string metaPath = Path.Combine(batchDirectory, "meta.ini");
                if (!File.Exists(metaPath))
                    return false;
                IniConfig meta = IniConfig.Load(metaPath);
                return DateTime.TryParseExact(
                    meta.Get("Batch", "Start", ""),
                    "yyyy-MM-dd HH:mm:ss.fffffff",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out value);
            }
            catch
            {
                value = DateTime.MinValue;
                return false;
            }
        }

        private BatchReceipt SendOne(string batchDirectory)
        {
            string metaPath = Path.Combine(batchDirectory, "meta.ini");
            string dataPath = Path.Combine(batchDirectory, "data.csv");
            if (!File.Exists(metaPath) || !File.Exists(dataPath))
                throw new InvalidDataException("Batch must contain meta.ini and data.csv.");

            IniConfig meta = IniConfig.Load(metaPath);
            string batchId = Required(meta, "BatchId");
            string collectorId = Required(meta, "CollectorId");
            string mode = Required(meta, "Mode");
            string server = Required(meta, "Server");
            string start = Required(meta, "Start");
            string end = Required(meta, "End");
            string expectedHash = Required(meta, "Sha256").ToLowerInvariant();
            int expectedRows = ParseNonNegativeInt(Required(meta, "Rows"), "Rows");
            long expectedBytes = ParseNonNegativeLong(Required(meta, "Bytes"), "Bytes");
            ValidateSha256(expectedHash);

            if (!String.Equals(batchId, Path.GetFileName(batchDirectory), StringComparison.Ordinal))
                throw new InvalidDataException("BatchId does not match its directory name.");

            ValidateHeader(batchId);
            ValidateHeader(collectorId);
            ValidateHeader(mode);
            ValidateHeader(server);
            ValidateHeader(start);
            ValidateHeader(end);

            using (FileStream input = new FileStream(
                dataPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                65536,
                FileOptions.SequentialScan))
            {
                long payloadBytes = input.Length;
                if (payloadBytes != expectedBytes)
                    throw new InvalidDataException("Local data.csv length does not match meta.ini.");
                return SendPayload(
                    batchId,
                    collectorId,
                    mode,
                    server,
                    start,
                    end,
                    expectedRows,
                    expectedHash,
                    input,
                    payloadBytes);
            }
        }

        private BatchReceipt SendPayload(
            string batchId,
            string collectorId,
            string mode,
            string server,
            string start,
            string end,
            int expectedRows,
            string actualHash,
            Stream data,
            long payloadBytes)
        {
            if (data == null)
                throw new ArgumentNullException("data");
            if (payloadBytes < 0)
                throw new ArgumentOutOfRangeException("payloadBytes");
            BatchSendTimings timings = new BatchSendTimings();
            Stopwatch totalClock = Stopwatch.StartNew();
            try
            {
                ValidateHeader(batchId);
                ValidateHeader(collectorId);
                ValidateHeader(mode);
                ValidateHeader(server);
                ValidateHeader(start);
                ValidateHeader(end);

                _log.Write("Sending batch=" + batchId + " rows=" + expectedRows.ToString(CultureInfo.InvariantCulture));

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(_url);
                request.Method = "POST";
                request.ContentType = "text/csv; charset=utf-8";
                request.Timeout = _timeoutMilliseconds;
                request.ReadWriteTimeout = _timeoutMilliseconds;
                request.KeepAlive = true;
                request.ServicePoint.Expect100Continue = false;
                request.Headers[HttpRequestHeader.Authorization] = "Bearer " + _apiKey;
                request.Headers["X-Collector-Id"] = collectorId;
                request.Headers["X-Batch-Id"] = batchId;
                request.Headers["X-Batch-Mode"] = mode;
                request.Headers["X-Historian-Server"] = server;
                request.Headers["X-Range-Start"] = start;
                request.Headers["X-Range-End"] = end;
                request.Headers["X-Row-Count"] = expectedRows.ToString(CultureInfo.InvariantCulture);
                request.Headers["X-Content-SHA256"] = actualHash;
                request.ContentLength = payloadBytes;

                Stopwatch sendClock = Stopwatch.StartNew();
                try
                {
                    using (Stream output = request.GetRequestStream())
                    {
                        CopyPayload(data, output, payloadBytes);
                    }
                }
                finally
                {
                    sendClock.Stop();
                    timings.SendMilliseconds = sendClock.ElapsedMilliseconds;
                }

                string responseText;
                HttpStatusCode status;
                Stopwatch ackClock = Stopwatch.StartNew();
                try
                {
                    try
                    {
                        using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                        {
                            status = response.StatusCode;
                            using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                                responseText = reader.ReadToEnd();
                        }
                    }
                    catch (WebException ex)
                    {
                        HttpWebResponse response = ex.Response as HttpWebResponse;
                        if (response == null)
                            throw new Exception("Receiver connection failed: " + ex.Message);
                        using (response)
                        using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                            responseText = reader.ReadToEnd();
                        int statusCode = (int)response.StatusCode;
                        bool permanent = statusCode == 400 || statusCode == 409 || statusCode == 413;
                        bool authentication = statusCode == 401 || statusCode == 403;
                        throw new BatchSendException(
                            "Receiver HTTP " + statusCode.ToString(CultureInfo.InvariantCulture) + ": " + responseText,
                            statusCode,
                            permanent,
                            authentication,
                            timings);
                    }

                    if (status != HttpStatusCode.OK)
                        throw new Exception("Unexpected Receiver HTTP status: " + ((int)status).ToString(CultureInfo.InvariantCulture));

                    string commitLevel = ValidateAck(responseText, batchId, actualHash, expectedRows);
                    ackClock.Stop();
                    timings.AckWaitMilliseconds = ackClock.ElapsedMilliseconds;
                    timings.TotalMilliseconds = totalClock.ElapsedMilliseconds;
                    BatchReceipt receipt = new BatchReceipt();
                    receipt.BatchId = batchId;
                    receipt.Mode = mode;
                    receipt.CommitLevel = commitLevel;
                    receipt.RangeStart = ParseTime(start);
                    receipt.RangeEnd = ParseTime(end);
                    receipt.Rows = expectedRows;
                    receipt.PayloadBytes = payloadBytes;
                    receipt.Timings = timings;
                    _log.Write(
                        "ACK batch=" + batchId +
                        " rows=" + expectedRows.ToString(CultureInfo.InvariantCulture) +
                        " SendMs=" + timings.SendMilliseconds.ToString(CultureInfo.InvariantCulture) +
                        " AckWaitMs=" + timings.AckWaitMilliseconds.ToString(CultureInfo.InvariantCulture) +
                        " TotalMs=" + timings.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
                    return receipt;
                }
                finally
                {
                    ackClock.Stop();
                    timings.AckWaitMilliseconds = ackClock.ElapsedMilliseconds;
                }
            }
            finally
            {
                totalClock.Stop();
                timings.TotalMilliseconds = totalClock.ElapsedMilliseconds;
                _lastTimings = timings;
            }
        }

        private static string FormatTime(DateTime value)
        {
            return value.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }

        private static DateTime ParseTime(string value)
        {
            DateTime parsed;
            if (!DateTime.TryParseExact(
                value,
                "yyyy-MM-dd HH:mm:ss.fffffff",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out parsed))
                throw new InvalidDataException("Invalid batch range time: " + value);
            return parsed;
        }

        private string ValidateAck(
            string json,
            string batchId,
            string hash,
            int rows)
        {
            if (!JsonBool(json, "ok") || !JsonBool(json, "committed"))
                throw new Exception("Receiver ACK is not committed: " + json);
            string commitLevel = JsonString(json, "commit_level").ToLowerInvariant();
            if (_ackMode == "database" && commitLevel != "database")
                throw new Exception("Receiver ACK does not prove PostgreSQL commit: " + json);
            if (_ackMode == "inbox" && commitLevel != "inbox" && commitLevel != "database")
                throw new Exception("Receiver ACK has an unknown commit_level: " + json);
            if (!String.Equals(JsonString(json, "batch_id"), batchId, StringComparison.Ordinal))
                throw new Exception("Receiver ACK batch_id mismatch.");
            if (!String.Equals(JsonString(json, "sha256"), hash, StringComparison.OrdinalIgnoreCase))
                throw new Exception("Receiver ACK SHA-256 mismatch.");
            if (JsonInt(json, "received_rows") != rows)
                throw new Exception("Receiver ACK row count mismatch.");
            return commitLevel;
        }

        private static bool JsonBool(string json, string name)
        {
            Match match = Regex.Match(
                json,
                "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*(true|false)",
                RegexOptions.IgnoreCase);
            return match.Success && String.Equals(match.Groups[1].Value, "true", StringComparison.OrdinalIgnoreCase);
        }

        private static string JsonString(string json, string name)
        {
            Match match = Regex.Match(
                json,
                "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"",
                RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new Exception("Receiver ACK is missing " + name + ".");
            return match.Groups[1].Value;
        }

        private static int JsonInt(string json, string name)
        {
            Match match = Regex.Match(
                json,
                "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*([0-9]+)",
                RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new Exception("Receiver ACK is missing " + name + ".");
            return ParseNonNegativeInt(match.Groups[1].Value, name);
        }

        private static string Required(IniConfig meta, string key)
        {
            string value = meta.Get("Batch", key, "");
            if (value.Length == 0)
                throw new InvalidDataException("meta.ini is missing [Batch] " + key + ".");
            return value;
        }

        private static int ParseNonNegativeInt(string text, string name)
        {
            int value;
            if (!Int32.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) || value < 0)
                throw new InvalidDataException("Invalid " + name + ": " + text);
            return value;
        }

        private static long ParseNonNegativeLong(string text, string name)
        {
            long value;
            if (!Int64.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) || value < 0)
                throw new InvalidDataException("Invalid " + name + ": " + text);
            return value;
        }

        private static void ValidateHeader(string value)
        {
            if (value.IndexOf('\r') >= 0 || value.IndexOf('\n') >= 0)
                throw new InvalidDataException("Batch metadata contains an invalid HTTP header value.");
        }

        private static void ValidateSha256(string value)
        {
            if (!Regex.IsMatch(value, "^[0-9a-fA-F]{64}$"))
                throw new InvalidDataException("Batch SHA-256 must contain 64 hexadecimal characters.");
        }

        private static void CopyPayload(Stream input, Stream output, long payloadBytes)
        {
            byte[] buffer = new byte[65536];
            long remaining = payloadBytes;
            while (remaining > 0)
            {
                int requested = remaining > buffer.Length
                    ? buffer.Length
                    : (int)remaining;
                int read = input.Read(buffer, 0, requested);
                if (read <= 0)
                    throw new InvalidDataException("Payload stream ended before Content-Length.");
                output.Write(buffer, 0, read);
                remaining -= read;
            }
            if (input.Read(buffer, 0, 1) != 0)
                throw new InvalidDataException("Payload stream is longer than Content-Length.");
            output.Flush();
        }
    }
}
