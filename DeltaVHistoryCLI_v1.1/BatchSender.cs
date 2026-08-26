using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace DeltaVHistoryCLI
{
    class BatchSendException : Exception
    {
        public readonly int StatusCode;
        public readonly bool Permanent;
        public readonly bool AuthenticationFailure;

        public BatchSendException(
            string message,
            int statusCode,
            bool permanent,
            bool authenticationFailure) : base(message)
        {
            StatusCode = statusCode;
            Permanent = permanent;
            AuthenticationFailure = authenticationFailure;
        }
    }

    class BatchReceipt
    {
        public string BatchId;
        public string Mode;
        public string CommitLevel;
        public DateTime RangeStart;
        public DateTime RangeEnd;
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
        private int _maxBatches;
        private string _ackMode;
        private string _spoolDirectory;
        private SyncLogger _log;

        public BatchSender(
            IniConfig config,
            string spoolDirectory,
            SyncLogger log)
        {
            _url = config.Get("Receiver", "Url", "");
            _apiKey = config.Get("Receiver", "ApiKey", "");
            _timeoutMilliseconds = config.GetInt("Receiver", "TimeoutSeconds", 15) * 1000;
            _maxBatches = config.GetInt("Receiver", "MaxBatchesPerRun", 20);
            _ackMode = config.Get("Receiver", "AckMode", "inbox").ToLowerInvariant();
            _spoolDirectory = spoolDirectory;
            _log = log;

            if (_url.Length == 0)
                throw new Exception("[Receiver] Url is required when Sender is enabled.");
            if (_apiKey.Length == 0)
                throw new Exception("[Receiver] ApiKey is required when Sender is enabled.");
            if (_ackMode != "inbox" && _ackMode != "database")
                throw new Exception("[Receiver] AckMode must be inbox or database.");
            if (_timeoutMilliseconds <= 0 || _maxBatches <= 0)
                throw new Exception("Receiver timeout and MaxBatchesPerRun must be positive.");
        }

        public BatchReceipt Send(HistoryBatch batch, byte[] data)
        {
            if (batch == null)
                throw new ArgumentNullException("batch");
            if (data == null)
                throw new ArgumentNullException("data");
            string actualHash = BatchEncoder.ComputeSha256(data);
            if (!String.IsNullOrEmpty(batch.Sha256) &&
                !String.Equals(batch.Sha256, actualHash, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException("In-memory batch SHA-256 mismatch.");
            batch.Sha256 = actualHash;
            return SendPayload(
                batch.BatchId,
                batch.CollectorId,
                batch.Mode,
                batch.Server,
                FormatTime(batch.RangeStart),
                FormatTime(batch.RangeEnd),
                batch.Samples.Count,
                actualHash,
                data);
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

            int limit = batches.Count < _maxBatches ? batches.Count : _maxBatches;
            int sent = 0;
            int i;
            for (i = 0; i < limit; i++)
            {
                string batchDirectory = batches[i].Directory;
                string batchName = Path.GetFileName(batchDirectory);
                try
                {
                    BatchReceipt receipt = SendOne(batchDirectory);
                    if (acknowledged != null)
                        acknowledged(receipt);
                    Directory.Delete(batchDirectory, true);
                    sent++;
                    _log.Write("Removed acknowledged outbox batch=" + batchName);
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

            _log.Write("Sender completed sent=" + sent.ToString() + " remaining=" +
                (Directory.GetDirectories(pendingRoot).Length).ToString());
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

            if (!String.Equals(batchId, Path.GetFileName(batchDirectory), StringComparison.Ordinal))
                throw new InvalidDataException("BatchId does not match its directory name.");

            string actualHash = ComputeSha256(dataPath);
            if (!String.Equals(expectedHash, actualHash, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException("Local data.csv SHA-256 does not match meta.ini.");

            ValidateHeader(batchId);
            ValidateHeader(collectorId);
            ValidateHeader(mode);
            ValidateHeader(server);
            ValidateHeader(start);
            ValidateHeader(end);

            byte[] data = File.ReadAllBytes(dataPath);
            return SendPayload(
                batchId,
                collectorId,
                mode,
                server,
                start,
                end,
                expectedRows,
                actualHash,
                data);
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
            byte[] data)
        {
            ValidateHeader(batchId);
            ValidateHeader(collectorId);
            ValidateHeader(mode);
            ValidateHeader(server);
            ValidateHeader(start);
            ValidateHeader(end);

            _log.Write("Sending batch=" + batchId + " rows=" + expectedRows.ToString());

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(_url);
            request.Method = "POST";
            request.ContentType = "text/csv; charset=utf-8";
            request.Timeout = _timeoutMilliseconds;
            request.ReadWriteTimeout = _timeoutMilliseconds;
            request.KeepAlive = false;
            request.Headers[HttpRequestHeader.Authorization] = "Bearer " + _apiKey;
            request.Headers["X-Collector-Id"] = collectorId;
            request.Headers["X-Batch-Id"] = batchId;
            request.Headers["X-Batch-Mode"] = mode;
            request.Headers["X-Historian-Server"] = server;
            request.Headers["X-Range-Start"] = start;
            request.Headers["X-Range-End"] = end;
            request.Headers["X-Row-Count"] = expectedRows.ToString(CultureInfo.InvariantCulture);
            request.Headers["X-Content-SHA256"] = actualHash;

            request.ContentLength = data.Length;

            using (Stream output = request.GetRequestStream())
            {
                output.Write(data, 0, data.Length);
                output.Flush();
            }

            string responseText;
            HttpStatusCode status;
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
                    "Receiver HTTP " + statusCode.ToString() + ": " + responseText,
                    statusCode,
                    permanent,
                    authentication);
            }

            if (status != HttpStatusCode.OK)
                throw new Exception("Unexpected Receiver HTTP status: " + ((int)status).ToString());

            string commitLevel = ValidateAck(responseText, batchId, actualHash, expectedRows);
            _log.Write("ACK batch=" + batchId + " rows=" + expectedRows.ToString());
            BatchReceipt receipt = new BatchReceipt();
            receipt.BatchId = batchId;
            receipt.Mode = mode;
            receipt.CommitLevel = commitLevel;
            receipt.RangeStart = ParseTime(start);
            receipt.RangeEnd = ParseTime(end);
            return receipt;
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

        private static void ValidateHeader(string value)
        {
            if (value.IndexOf('\r') >= 0 || value.IndexOf('\n') >= 0)
                throw new InvalidDataException("Batch metadata contains an invalid HTTP header value.");
        }

        private static string ComputeSha256(string path)
        {
            using (SHA256 sha = SHA256.Create())
            using (FileStream stream = File.OpenRead(path))
            {
                byte[] hash = sha.ComputeHash(stream);
                StringBuilder text = new StringBuilder(hash.Length * 2);
                int i;
                for (i = 0; i < hash.Length; i++)
                    text.Append(hash[i].ToString("x2", CultureInfo.InvariantCulture));
                return text.ToString();
            }
        }
    }
}
