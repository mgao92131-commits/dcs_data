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
    class BatchSender
    {
        private string _url;
        private string _apiKey;
        private int _timeoutMilliseconds;
        private int _maxBatches;
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
            _spoolDirectory = spoolDirectory;
            _log = log;

            if (_url.Length == 0)
                throw new Exception("[Receiver] Url is required when Sender is enabled.");
            if (_apiKey.Length == 0)
                throw new Exception("[Receiver] ApiKey is required when Sender is enabled.");
            if (_timeoutMilliseconds <= 0 || _maxBatches <= 0)
                throw new Exception("Receiver timeout and MaxBatchesPerRun must be positive.");
        }

        public int SendPending()
        {
            string pendingRoot = Path.Combine(_spoolDirectory, "pending");
            string archiveRoot = Path.Combine(_spoolDirectory, "archive");
            string failedRoot = Path.Combine(_spoolDirectory, "failed");
            Directory.CreateDirectory(pendingRoot);
            Directory.CreateDirectory(archiveRoot);
            Directory.CreateDirectory(failedRoot);

            string[] batches = Directory.GetDirectories(pendingRoot);
            Array.Sort(batches, StringComparer.OrdinalIgnoreCase);

            int limit = batches.Length < _maxBatches ? batches.Length : _maxBatches;
            int sent = 0;
            int invalid = 0;
            int i;
            for (i = 0; i < limit; i++)
            {
                string batchDirectory = batches[i];
                string batchName = Path.GetFileName(batchDirectory);
                try
                {
                    SendOne(batchDirectory);
                    string archiveDirectory = Path.Combine(archiveRoot, batchName);
                    if (Directory.Exists(archiveDirectory))
                        archiveDirectory += "_duplicate_" + Guid.NewGuid().ToString("N");
                    Directory.Move(batchDirectory, archiveDirectory);
                    sent++;
                    _log.Write("Archived acknowledged batch=" + batchName);
                }
                catch (InvalidDataException ex)
                {
                    string failedDirectory = Path.Combine(
                        failedRoot,
                        batchName + "_invalid_" + Guid.NewGuid().ToString("N"));
                    Directory.Move(batchDirectory, failedDirectory);
                    _log.Write("Moved invalid local batch to failed=" + batchName + " error=" + ex.Message);
                    invalid++;
                }
                catch (Exception ex)
                {
                    _log.Write("Send stopped; pending retained batch=" + batchName + " error=" + ex.Message);
                    return 40;
                }
            }

            _log.Write("Sender completed sent=" + sent.ToString() + " remaining=" +
                (Directory.GetDirectories(pendingRoot).Length).ToString());
            return invalid == 0 ? 0 : 41;
        }

        private void SendOne(string batchDirectory)
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

            FileInfo info = new FileInfo(dataPath);
            request.ContentLength = info.Length;

            using (FileStream input = File.OpenRead(dataPath))
            using (Stream output = request.GetRequestStream())
            {
                byte[] buffer = new byte[65536];
                int count;
                while ((count = input.Read(buffer, 0, buffer.Length)) > 0)
                    output.Write(buffer, 0, count);
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
                throw new Exception("Receiver HTTP " + ((int)response.StatusCode).ToString() + ": " + responseText);
            }

            if (status != HttpStatusCode.OK)
                throw new Exception("Unexpected Receiver HTTP status: " + ((int)status).ToString());

            ValidateAck(responseText, batchId, actualHash, expectedRows);
            _log.Write("ACK batch=" + batchId + " rows=" + expectedRows.ToString());
        }

        private static void ValidateAck(
            string json,
            string batchId,
            string hash,
            int rows)
        {
            if (!JsonBool(json, "ok") || !JsonBool(json, "committed"))
                throw new Exception("Receiver ACK is not committed: " + json);
            if (!String.Equals(JsonString(json, "batch_id"), batchId, StringComparison.Ordinal))
                throw new Exception("Receiver ACK batch_id mismatch.");
            if (!String.Equals(JsonString(json, "sha256"), hash, StringComparison.OrdinalIgnoreCase))
                throw new Exception("Receiver ACK SHA-256 mismatch.");
            if (JsonInt(json, "received_rows") != rows)
                throw new Exception("Receiver ACK row count mismatch.");
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
