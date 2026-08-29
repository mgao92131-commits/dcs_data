using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace DeltaVHistoryCLI
{
    class BatchSendTimings
    {
        public long SendMilliseconds;
        public long AckWaitMilliseconds;
        public long TotalMilliseconds;
        public int Attempts;
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

    class BatchSender
    {
        private readonly string _url;
        private readonly string _apiKey;
        private readonly int _timeoutMilliseconds;
        private readonly int _sendRetryMilliseconds;
        private readonly string _ackMode;
        private readonly SyncLogger _log;

        public BatchSender(IniConfig config, SyncLogger log)
        {
            _url = config.Get("Receiver", "Url", "");
            _apiKey = config.Get("Receiver", "ApiKey", "");
            int timeoutSeconds = config.GetInt("Receiver", "TimeoutSeconds", 135);
            int retrySeconds = config.GetInt("Receiver", "SendRetrySeconds", 30);
            _ackMode = config.Get("Receiver", "AckMode", "database").ToLowerInvariant();
            _log = log;

            if (_url.Length == 0)
                throw new Exception("[Receiver] Url is required.");
            if (_apiKey.Length == 0)
                throw new Exception("[Receiver] ApiKey is required.");
            if (_ackMode != "database")
                throw new Exception("[Receiver] AckMode must be database.");
            if (timeoutSeconds <= 0 || retrySeconds <= 0)
                throw new Exception("[Receiver] TimeoutSeconds and SendRetrySeconds must be positive.");
            _timeoutMilliseconds = checked(timeoutSeconds * 1000);
            _sendRetryMilliseconds = checked(retrySeconds * 1000);
        }

        public BatchReceipt SendWithRetry(
            HistoryBatch batch,
            BatchPayload payload,
            WaitHandle stopHandle)
        {
            return SendWithRetryCore(
                batch,
                payload,
                stopHandle == null ? null : new WaitHandle[] { stopHandle });
        }

        internal BatchReceipt SendWithRetryAny(
            HistoryBatch batch,
            BatchPayload payload,
            WaitHandle[] stopHandles)
        {
            return SendWithRetryCore(batch, payload, stopHandles);
        }

        private BatchReceipt SendWithRetryCore(
            HistoryBatch batch,
            BatchPayload payload,
            WaitHandle[] stopHandles)
        {
            if (batch == null)
                throw new ArgumentNullException("batch");
            if (payload == null)
                throw new ArgumentNullException("payload");

            int attempt = 0;
            while (true)
            {
                ThrowIfStopRequested(stopHandles);
                attempt++;
                try
                {
                    BatchReceipt receipt = Send(batch, payload);
                    if (receipt.Timings != null)
                        receipt.Timings.Attempts = attempt;
                    return receipt;
                }
                catch (BatchSendException ex)
                {
                    if (ex.Timings != null)
                        ex.Timings.Attempts = attempt;
                    if (ex.Permanent)
                        throw;

                    _log.Write(
                        "Transient Receiver failure; retrying same batch=" + batch.BatchId +
                        " attempt=" + attempt.ToString(CultureInfo.InvariantCulture) +
                        " afterSeconds=" +
                        (_sendRetryMilliseconds / 1000).ToString(CultureInfo.InvariantCulture) +
                        " error=" + ex.Message);
                    WaitBeforeRetry(batch.BatchId, stopHandles);
                }
            }
        }

        public BatchReceipt SendWithRetry(HistoryBatch batch, BatchPayload payload)
        {
            return SendWithRetryCore(batch, payload, null);
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
                false))
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

                _log.Write(
                    "Sending batch=" + batchId +
                    " rows=" + expectedRows.ToString(CultureInfo.InvariantCulture));

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
                        CopyPayload(data, output, payloadBytes);
                }
                catch (WebException ex)
                {
                    throw CreateTransientException(
                        "Receiver connection failed while sending batch: " + ex.Message,
                        timings);
                }
                catch (IOException ex)
                {
                    throw CreateTransientException(
                        "Receiver connection failed while sending batch: " + ex.Message,
                        timings);
                }
                finally
                {
                    sendClock.Stop();
                    timings.SendMilliseconds = sendClock.ElapsedMilliseconds;
                }

                string responseText;
                HttpStatusCode status;
                BatchReceipt receipt = null;
                Stopwatch ackClock = Stopwatch.StartNew();
                try
                {
                    try
                    {
                        using (HttpWebResponse response =
                            (HttpWebResponse)request.GetResponse())
                        {
                            status = response.StatusCode;
                            responseText = ReadResponseBody(response);
                        }
                    }
                    catch (WebException ex)
                    {
                        throw CreateWebException(ex, timings);
                    }
                    catch (IOException ex)
                    {
                        throw CreateTransientException(
                            "Receiver ACK read failed: " + ex.Message,
                            timings);
                    }

                    if (status != HttpStatusCode.OK)
                        throw CreateHttpException((int)status, responseText, timings);

                    string commitLevel = ValidateAck(
                        responseText,
                        batchId,
                        actualHash,
                        expectedRows);
                    receipt = new BatchReceipt();
                    receipt.BatchId = batchId;
                    receipt.Mode = mode;
                    receipt.CommitLevel = commitLevel;
                    receipt.RangeStart = ParseTime(start);
                    receipt.RangeEnd = ParseTime(end);
                    receipt.Rows = expectedRows;
                    receipt.PayloadBytes = payloadBytes;
                    receipt.Timings = timings;
                }
                finally
                {
                    ackClock.Stop();
                    timings.AckWaitMilliseconds = ackClock.ElapsedMilliseconds;
                }
                totalClock.Stop();
                timings.TotalMilliseconds = totalClock.ElapsedMilliseconds;
                _log.Write(
                    "ACK database batch=" + batchId +
                    " rows=" + expectedRows.ToString(CultureInfo.InvariantCulture) +
                    " SendMs=" + timings.SendMilliseconds.ToString(CultureInfo.InvariantCulture) +
                    " AckWaitMs=" + timings.AckWaitMilliseconds.ToString(CultureInfo.InvariantCulture) +
                    " TotalMs=" + timings.TotalMilliseconds.ToString(CultureInfo.InvariantCulture));
                return receipt;
            }
            finally
            {
                totalClock.Stop();
                timings.TotalMilliseconds = totalClock.ElapsedMilliseconds;
            }
        }

        private void WaitBeforeRetry(string batchId, WaitHandle[] stopHandles)
        {
            if (stopHandles == null || stopHandles.Length == 0)
            {
                Thread.Sleep(_sendRetryMilliseconds);
                return;
            }
            if (WaitHandle.WaitAny(stopHandles, _sendRetryMilliseconds, false) !=
                WaitHandle.WaitTimeout)
                throw new SyncStopRequestedException(
                    "Stop requested while waiting to retry batch " + batchId + ".");
        }

        private static void ThrowIfStopRequested(WaitHandle[] stopHandles)
        {
            if (stopHandles != null &&
                stopHandles.Length > 0 &&
                WaitHandle.WaitAny(stopHandles, 0, false) != WaitHandle.WaitTimeout)
                throw new SyncStopRequestedException("Stop requested.");
        }

        private static string ReadResponseBody(HttpWebResponse response)
        {
            if (response == null || response.GetResponseStream() == null)
                return "";
            using (StreamReader reader =
                new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                return reader.ReadToEnd();
        }

        private static BatchSendException CreateWebException(
            WebException exception,
            BatchSendTimings timings)
        {
            HttpWebResponse response = exception.Response as HttpWebResponse;
            if (response == null)
                return CreateTransientException(
                    "Receiver connection failed: " + exception.Message,
                    timings);

            int statusCode = (int)response.StatusCode;
            string body;
            try
            {
                using (response)
                    body = ReadResponseBody(response);
            }
            catch (Exception readException)
            {
                return CreateTransientException(
                    "Receiver response read failed HTTP " +
                    statusCode.ToString(CultureInfo.InvariantCulture) +
                    ": " + readException.Message,
                    timings);
            }
            return CreateHttpException(statusCode, body, timings);
        }

        private static BatchSendException CreateHttpException(
            int statusCode,
            string responseText,
            BatchSendTimings timings)
        {
            bool authentication = statusCode == 401 || statusCode == 403;
            bool transient = statusCode == 408 || statusCode == 429 ||
                statusCode >= 500 && statusCode <= 599;
            return new BatchSendException(
                "Receiver HTTP " + statusCode.ToString(CultureInfo.InvariantCulture) +
                ": " + responseText,
                statusCode,
                !transient,
                authentication,
                timings);
        }

        private static BatchSendException CreateTransientException(
            string message,
            BatchSendTimings timings)
        {
            return new BatchSendException(message, 0, false, false, timings);
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

        private static string ValidateAck(
            string json,
            string batchId,
            string hash,
            int rows)
        {
            try
            {
                if (!JsonBool(json, "ok") || !JsonBool(json, "committed"))
                    throw new Exception("Receiver ACK is not committed.");
                string commitLevel = JsonString(json, "commit_level").ToLowerInvariant();
                if (commitLevel != "database")
                    throw new Exception(
                        "Receiver ACK does not prove PostgreSQL commit: " + json);
                if (!String.Equals(
                    JsonString(json, "batch_id"),
                    batchId,
                    StringComparison.Ordinal))
                    throw new Exception("Receiver ACK batch_id mismatch.");
                if (!String.Equals(
                    JsonString(json, "sha256"),
                    hash,
                    StringComparison.OrdinalIgnoreCase))
                    throw new Exception("Receiver ACK SHA-256 mismatch.");
                if (JsonInt(json, "received_rows") != rows)
                    throw new Exception("Receiver ACK row count mismatch.");
                return commitLevel;
            }
            catch (BatchSendException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new BatchSendException(
                    "Invalid database ACK: " + ex.Message + " " + json,
                    200,
                    true,
                    false,
                    null);
            }
        }

        private static bool JsonBool(string json, string name)
        {
            Match match = Regex.Match(
                json == null ? "" : json,
                "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*(true|false)",
                RegexOptions.IgnoreCase);
            return match.Success &&
                String.Equals(
                    match.Groups[1].Value,
                    "true",
                    StringComparison.OrdinalIgnoreCase);
        }

        private static string JsonString(string json, string name)
        {
            Match match = Regex.Match(
                json == null ? "" : json,
                "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"",
                RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new Exception("Receiver ACK is missing " + name + ".");
            return match.Groups[1].Value;
        }

        private static int JsonInt(string json, string name)
        {
            Match match = Regex.Match(
                json == null ? "" : json,
                "\\\"" + Regex.Escape(name) + "\\\"\\s*:\\s*([0-9]+)",
                RegexOptions.IgnoreCase);
            if (!match.Success)
                throw new Exception("Receiver ACK is missing " + name + ".");
            int value;
            if (!Int32.TryParse(
                match.Groups[1].Value,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value))
                throw new Exception("Receiver ACK has an invalid " + name + ".");
            return value;
        }

        private static void ValidateHeader(string value)
        {
            if (String.IsNullOrEmpty(value) ||
                value.IndexOf('\r') >= 0 ||
                value.IndexOf('\n') >= 0)
                throw new InvalidDataException("Batch metadata contains an invalid HTTP header value.");
        }

        private static void ValidateSha256(string value)
        {
            if (!Regex.IsMatch(value, "^[0-9a-fA-F]{64}$"))
                throw new InvalidDataException(
                    "Batch SHA-256 must contain 64 hexadecimal characters.");
        }

        private static void CopyPayload(
            Stream input,
            Stream output,
            long payloadBytes)
        {
            CopyPayloadCore(input, output, payloadBytes, null);
        }

        private static string CopyPayloadAndHash(
            Stream input,
            Stream output,
            long payloadBytes,
            string expectedHash)
        {
            using (SHA256 sha = SHA256.Create())
            {
                CopyPayloadCore(input, output, payloadBytes, sha);
                byte[] hash = sha.Hash;
                StringBuilder text = new StringBuilder(hash.Length * 2);
                int i;
                for (i = 0; i < hash.Length; i++)
                    text.Append(hash[i].ToString("x2", CultureInfo.InvariantCulture));
                string actualHash = text.ToString();
                if (!String.Equals(
                    actualHash,
                    expectedHash,
                    StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException(
                        "Local payload SHA-256 does not match the expected value.");
                return actualHash;
            }
        }

        private static void CopyPayloadCore(
            Stream input,
            Stream output,
            long payloadBytes,
            HashAlgorithm hash)
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
                    throw new InvalidDataException(
                        "Payload stream ended before Content-Length.");
                if (hash != null)
                    hash.TransformBlock(buffer, 0, read, buffer, 0);
                output.Write(buffer, 0, read);
                remaining -= read;
            }
            if (input.Read(buffer, 0, 1) != 0)
                throw new InvalidDataException(
                    "Payload stream is longer than Content-Length.");
            if (hash != null)
                hash.TransformFinalBlock(new byte[0], 0, 0);
            output.Flush();
        }
    }
}
