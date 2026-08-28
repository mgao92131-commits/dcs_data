using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;

namespace DeltaVHistoryCLI
{
    class ProcessedSyncSelfTest
    {
        static int Main()
        {
            try
            {
                Assert(typeof(HistorianClient).GetMethod("ReadRaw") == null,
                    "Raw API must not be exposed");
                Assert(typeof(HistorianClient).GetMethod("ReadProcessed") != null,
                    "Processed API must be exposed");
                Assert(typeof(HistorianClient).GetMethod("ReadProcessedBatch") != null,
                    "Processed batch API must be exposed");

                DateTime input = new DateTime(2026, 8, 28, 10, 5, 27);
                DateTime aligned = (DateTime)InvokePrivate(
                    typeof(SyncProgram),
                    "AlignDown",
                    new object[] { input, 10 });
                Assert(aligned == new DateTime(2026, 8, 28, 10, 5, 20),
                    "10-second grid alignment");

                int futureWait = (int)InvokePrivate(
                    typeof(SyncProgram),
                    "CalculateWaitMilliseconds",
                    new object[] {
                        new DateTime(2026, 8, 28, 10, 5, 0),
                        new DateTime(2026, 8, 28, 10, 0, 0)
                    });
                int overdueWait = (int)InvokePrivate(
                    typeof(SyncProgram),
                    "CalculateWaitMilliseconds",
                    new object[] {
                        new DateTime(2026, 8, 28, 10, 0, 0),
                        new DateTime(2026, 8, 28, 10, 5, 0)
                    });
                Assert(futureWait == 300000 && overdueWait == 0,
                    "fixed start-to-start schedule delay");

                string timingRoot = Path.Combine(
                    Path.GetTempPath(),
                    "HistorySyncTimingSelfTest_" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(timingRoot);
                try
                {
                    string timingConfig = Path.Combine(timingRoot, "config.ini");
                    File.WriteAllText(
                        timingConfig,
                        "[Sync]\nIntervalMinutes=5\n[Receiver]\nPendingRetrySeconds=30\n");
                    int pendingRetryMilliseconds = (int)InvokePrivate(
                        typeof(SyncProgram),
                        "ReadPendingRetryMilliseconds",
                        new object[] {
                            new string[] { "run", "--config", timingConfig },
                            timingRoot
                        });
                    Assert(pendingRetryMilliseconds == 30000,
                        "paused pending retry schedule");
                }
                finally
                {
                    try { Directory.Delete(timingRoot, true); }
                    catch { }
                }

                TimeSpan slice = (TimeSpan)InvokePrivate(
                    typeof(SyncProgram),
                    "CalculateEffectiveSlice",
                    new object[] { 827, 50000, 10, TimeSpan.FromMinutes(30) });
                Assert(slice == TimeSpan.FromMinutes(10),
                    "row-capacity pre-split");

                DateTime utc = new DateTime(
                    2026, 8, 27, 2, 0, 0, DateTimeKind.Utc);
                DateTime local = (DateTime)InvokePrivate(
                    typeof(HistorianClient),
                    "ToCollectorLocalTime",
                    new object[] { utc });
                Assert(local == utc.ToLocalTime(),
                    "UTC Historian timestamp conversion");

                string sequence = (string)InvokePrivate(
                    typeof(HistorianClient),
                    "BuildProcessedSequence",
                    new object[] { 10 });
                Assert(sequence == "P:InterpolatedValue:10",
                    "stable Processed identity");

                FakeHistorianReadInterface fakeRead = new FakeHistorianReadInterface();
                FakeHistorianConnection fakeConnection = new FakeHistorianConnection();
                HistorianClient fakeClient = new HistorianClient("", null);
                SetPrivateField(fakeClient, "_readInterface", fakeRead);
                SetPrivateField(fakeClient, "_connection", fakeConnection);
                SetPrivateField(
                    fakeClient,
                    "_readProcessed",
                    typeof(FakeHistorianConnection).GetMethod("readProcessed"));
                SetPrivateField(fakeClient, "_processedSampleType", typeof(object));
                SetPrivateField(fakeClient, "_interpolatedAggregate", "InterpolatedValue");
                SetPrivateField(fakeClient, "_connectionHandle", 1);
                List<TagResult> fakeTags = new List<TagResult>();
                fakeTags.Add(new TagResult { Name = "TAG/A", Handle = 101, Status = 1 });
                fakeTags.Add(new TagResult { Name = "TAG/B", Handle = 102, Status = 1 });
                fakeTags.Add(new TagResult { Name = "TAG/INVALID", Handle = -1, Status = 0 });
                List<ProcessedTagResult> fakeResults = fakeClient.ReadProcessedBatch(
                    fakeTags,
                    new DateTime(2026, 8, 28, 10, 0, 0),
                    new DateTime(2026, 8, 28, 10, 5, 0),
                    10);
                Assert(fakeRead.CreateCount == 1 && fakeRead.ReleaseCount == 1,
                    "one shared Historian TimeSpan per window");
                Assert(fakeConnection.ReadCount == 2 && fakeResults.Count == 3 &&
                    fakeResults[0].Result != null && fakeResults[1].Result != null &&
                    fakeResults[2].Error != null,
                    "serial Processed reads preserve per-tag results");
                fakeClient.Dispose();

                byte[] streamingInput = new byte[150000];
                int streamingIndex;
                for (streamingIndex = 0; streamingIndex < streamingInput.Length; streamingIndex++)
                    streamingInput[streamingIndex] = (byte)(streamingIndex % 251);
                MemoryStream streamingSource = new MemoryStream(streamingInput, false);
                MemoryStream streamingDestination = new MemoryStream();
                InvokePrivate(
                    typeof(BatchSender),
                    "CopyPayload",
                    new object[] { streamingSource, streamingDestination, (long)streamingInput.Length });
                byte[] streamingOutput = streamingDestination.ToArray();
                Assert(streamingOutput.Length == streamingInput.Length,
                    "streaming sender payload length");
                for (streamingIndex = 0; streamingIndex < streamingInput.Length; streamingIndex++)
                    Assert(streamingOutput[streamingIndex] == streamingInput[streamingIndex],
                        "streaming sender payload copy");

                RunPendingDrainSelfTest();

                string temporaryRoot = Path.Combine(
                    Path.GetTempPath(),
                    "HistorySyncSelfTest_" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(temporaryRoot);
                try
                {
                    string statePath = Path.Combine(temporaryRoot, "state.ini");
                    SyncState initial = new SyncState();
                    initial.LastCollectedEnd = new DateTime(2026, 8, 28, 10, 0, 0);
                    initial.LastAcceptedEnd = initial.LastCollectedEnd;
                    initial.LastCommittedEnd = initial.LastCollectedEnd;
                    initial.CollectionPaused = true;
                    initial.PauseReason = "pending capacity reached";
                    SyncStateStore stateStore = new SyncStateStore(statePath);
                    stateStore.Save(initial);
                    SyncState loaded = stateStore.LoadOrCreate(initial);
                    Assert(loaded.CollectionPaused &&
                        loaded.PauseReason == initial.PauseReason,
                        "persisted CollectionPaused state");

                    HistoryBatch batch = new HistoryBatch();
                    batch.BatchId = "selftest_pending";
                    batch.CollectorId = "DCS-SELFTEST";
                    batch.Mode = "sync";
                    batch.Sampling = "InterpolatedValue";
                    batch.SamplingIntervalSeconds = 10;
                    batch.Server = "APP";
                    batch.RangeStart = initial.LastCollectedEnd;
                    batch.RangeEnd = initial.LastCollectedEnd.AddMinutes(5);
                    byte[] payload = Encoding.UTF8.GetBytes("payload");
                    batch.Sha256 = BatchEncoder.ComputeSha256(payload);
                    SpoolStore spool = new SpoolStore(Path.Combine(temporaryRoot, "spool"));
                    spool.SavePending(batch, payload);
                    PendingStats stats = spool.GetPendingStats();
                    Assert(stats.Batches == 1 && stats.Bytes == payload.Length,
                        "pending spool statistics");
                    bool capacityRejected = false;
                    try
                    {
                        spool.EnsurePendingCapacity(2, payload.Length);
                    }
                    catch (PendingCapacityException)
                    {
                        capacityRejected = true;
                    }
                    Assert(capacityRejected,
                        "pending byte capacity stops at the exact limit");
                    bool incomingRejected = false;
                    try
                    {
                        spool.EnsurePendingCapacity(2, payload.Length + 1, 2);
                    }
                    catch (PendingCapacityException)
                    {
                        incomingRejected = true;
                    }
                    Assert(incomingRejected,
                        "pending capacity includes incoming batch");
                }
                finally
                {
                    if (Directory.Exists(temporaryRoot))
                        Directory.Delete(temporaryRoot, true);
                }

                Console.WriteLine("PROCESSED SYNC SELF TEST OK");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("PROCESSED SYNC SELF TEST FAILED: " + ex);
                return 1;
            }
        }

        private static object InvokePrivate(Type type, string name, object[] args)
        {
            MethodInfo method = type.GetMethod(
                name,
                BindingFlags.NonPublic | BindingFlags.Static);
            if (method == null)
                throw new Exception("Method not found: " + type.FullName + "." + name);
            return method.Invoke(null, args);
        }

        private static void SetPrivateField(object instance, string name, object value)
        {
            FieldInfo field = instance.GetType().GetField(
                name,
                BindingFlags.NonPublic | BindingFlags.Instance);
            if (field == null)
                throw new Exception("Field not found: " + name);
            field.SetValue(instance, value);
        }

        private static void RunPendingDrainSelfTest()
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                "HistorySyncDrainSelfTest_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            string spool = Path.Combine(root, "spool");
            string logs = Path.Combine(root, "logs");
            Directory.CreateDirectory(spool);
            Directory.CreateDirectory(logs);

            TcpListener listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            FakePendingReceiverResult receiverResult = new FakePendingReceiverResult();
            Thread receiverThread = new Thread(delegate()
            {
                FakePendingReceiver.Run(listener, receiverResult);
            });
            receiverThread.IsBackground = true;
            receiverThread.Start();
            try
            {
                int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                string configPath = Path.Combine(root, "config.ini");
                File.WriteAllText(
                    configPath,
                    "[Receiver]\n" +
                    "Url=http://127.0.0.1:" + port.ToString() + "/api/history/batch\n" +
                    "ApiKey=selftest\n" +
                    "TimeoutSeconds=5\n" +
                    "BacklogDrainSeconds=60\n" +
                    "AckMode=inbox\n",
                    Encoding.UTF8);
                IniConfig config = IniConfig.Load(configPath);
                SpoolStore store = new SpoolStore(spool);
                int batchIndex;
                for (batchIndex = 0; batchIndex < 2; batchIndex++)
                {
                    HistoryBatch batch = new HistoryBatch();
                    batch.BatchId = "selftest_drain_" + batchIndex.ToString();
                    batch.CollectorId = "DCS-SELFTEST";
                    batch.Mode = "sync";
                    batch.Sampling = "InterpolatedValue";
                    batch.SamplingIntervalSeconds = 10;
                    batch.Server = "APP";
                    batch.RangeStart = new DateTime(2026, 8, 28, 10, batchIndex, 0);
                    batch.RangeEnd = batch.RangeStart.AddMinutes(1);
                    batch.Samples.Add(new HistorySample {
                        Tag = "TAG/SELFTEST",
                        Timestamp = batch.RangeStart,
                        Value = batchIndex.ToString(),
                        DataType = "Float",
                        Flags = "",
                        SequenceNo = "P:InterpolatedValue:10",
                        ArchiveStatus = "Current"
                    });
                    byte[] data = BatchEncoder.EncodeCsv(batch);
                    batch.Sha256 = BatchEncoder.ComputeSha256(data);
                    store.SavePending(batch, data);
                }

                using (SyncLogger log = new SyncLogger(logs))
                {
                    BatchSender sender = new BatchSender(config, spool, log);
                    int code = sender.SendPending();
                    PendingStats remaining = store.GetPendingStats();
                    Assert(code == 0 && remaining.Batches == 0 &&
                        receiverResult.Requests == 2 && receiverResult.Error == null,
                        "pending drain sends all batches over one streaming path");
                }
            }
            finally
            {
                listener.Stop();
                if (receiverResult.Client != null)
                    receiverResult.Client.Close();
                if (!receiverThread.Join(5000))
                    throw new Exception("Fake pending Receiver did not stop.");
                if (Directory.Exists(root))
                    Directory.Delete(root, true);
            }
        }

        private static void Assert(bool condition, string name)
        {
            if (!condition)
                throw new Exception("Assertion failed: " + name);
        }
    }

    class FakeHistorianReadInterface
    {
        public int CreateCount;
        public int ReleaseCount;
        private readonly FakeHistorianTimeSpan _timeSpan = new FakeHistorianTimeSpan();

        public int createTimeSpan()
        {
            CreateCount++;
            return 7;
        }

        public FakeHistorianTimeSpan getTimeSpan(int handle)
        {
            if (handle != 7)
                throw new Exception("unexpected fake TimeSpan handle");
            return _timeSpan;
        }

        public void releaseTimeSpan(int handle)
        {
            if (handle != 7)
                throw new Exception("unexpected fake TimeSpan release");
            ReleaseCount++;
        }

        public void closeConnection(int handle) { }
    }

    class FakeHistorianTimeSpan
    {
        public DateTime Start;
        public DateTime End;
        public TimeSpan Interval;

        public void setAbsoluteStartTime(DateTime value) { Start = value; }
        public void setAbsoluteEndTime(DateTime value) { End = value; }
        public void setResampleInterval(TimeSpan value) { Interval = value; }
    }

    class FakeHistorianConnection
    {
        public int ReadCount;

        public FakeHistorianProcessed readProcessed(
            int timeSpanHandle,
            int tagHandle,
            object sampleType,
            ArrayList aggregates)
        {
            if (timeSpanHandle != 7 || sampleType == null || aggregates.Count != 1)
                throw new Exception("shared Processed read arguments were not configured");
            ReadCount++;
            FakeHistorianProcessed processed = new FakeHistorianProcessed();
            processed.nSamples = 1;
            ArrayList samples = new ArrayList();
            samples.Add(new FakeHistorianPoint {
                timestamp = new DateTime(2026, 8, 28, 10, 0, ReadCount),
                value = tagHandle,
                dataType = "Float",
                archiveStatus = 0
            });
            processed.dataSamples.Add(samples);
            return processed;
        }
    }

    class FakeHistorianProcessed
    {
        public int nSamples;
        public ArrayList dataSamples = new ArrayList();
    }

    class FakeHistorianPoint
    {
        public DateTime timestamp;
        public object value;
        public object dataType;
        public int archiveStatus;
    }

    class FakePendingReceiverResult
    {
        public int Requests;
        public Exception Error;
        public TcpClient Client;
    }

    class FakePendingReceiver
    {
        public static void Run(TcpListener listener, FakePendingReceiverResult result)
        {
            try
            {
                using (TcpClient client = listener.AcceptTcpClient())
                {
                    result.Client = client;
                    client.ReceiveTimeout = 5000;
                    client.SendTimeout = 5000;
                    using (NetworkStream stream = client.GetStream())
                    {
                        while (result.Requests < 2)
                        {
                            string headers = ReadHeaders(stream);
                            if (headers == null)
                                throw new Exception("Sender closed before all pending requests arrived.");
                            int contentLength = Int32.Parse(
                                HeaderValue(headers, "Content-Length"),
                                CultureInfo.InvariantCulture);
                            byte[] body = ReadBody(stream, contentLength);
                            string batchId = HeaderValue(headers, "X-Batch-Id");
                            string hash = BatchEncoder.ComputeSha256(body);
                            string rows = HeaderValue(headers, "X-Row-Count");
                            string responseText =
                                "{\"ok\":true,\"committed\":true,\"commit_level\":\"inbox\",\"batch_id\":\"" +
                                batchId + "\",\"sha256\":\"" + hash + "\",\"received_rows\":" + rows + "}";
                            byte[] responseBody = Encoding.UTF8.GetBytes(responseText);
                            string responseHeaders =
                                "HTTP/1.1 200 OK\r\n" +
                                "Content-Type: application/json\r\n" +
                                "Content-Length: " + responseBody.Length.ToString(CultureInfo.InvariantCulture) + "\r\n" +
                                "Connection: keep-alive\r\n\r\n";
                            byte[] response = Encoding.ASCII.GetBytes(responseHeaders);
                            stream.Write(response, 0, response.Length);
                            stream.Write(responseBody, 0, responseBody.Length);
                            stream.Flush();
                            result.Requests++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                result.Error = ex;
            }
        }

        private static string ReadHeaders(Stream stream)
        {
            StringBuilder text = new StringBuilder();
            while (text.Length < 65536)
            {
                int value = stream.ReadByte();
                if (value < 0)
                    return null;
                text.Append((char)value);
                int length = text.Length;
                if (length >= 4 && text[length - 4] == '\r' && text[length - 3] == '\n' &&
                    text[length - 2] == '\r' && text[length - 1] == '\n')
                    return text.ToString();
            }
            throw new Exception("HTTP request headers are too large.");
        }

        private static byte[] ReadBody(Stream stream, int length)
        {
            byte[] body = new byte[length];
            int offset = 0;
            while (offset < length)
            {
                int read = stream.Read(body, offset, length - offset);
                if (read <= 0)
                    throw new Exception("HTTP request body ended early.");
                offset += read;
            }
            return body;
        }

        private static string HeaderValue(string headers, string name)
        {
            string[] lines = headers.Split(
                new string[] { "\r\n" },
                StringSplitOptions.None);
            int i;
            for (i = 1; i < lines.Length; i++)
            {
                int separator = lines[i].IndexOf(':');
                if (separator <= 0)
                    continue;
                string key = lines[i].Substring(0, separator).Trim();
                if (String.Equals(key, name, StringComparison.OrdinalIgnoreCase))
                    return lines[i].Substring(separator + 1).Trim();
            }
            throw new Exception("Missing HTTP header: " + name);
        }
    }
}
