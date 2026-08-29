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
                Assert(
                    typeof(HistorianClient).GetMethod("ReadRaw") == null,
                    "Raw API must not be exposed");
                Assert(
                    typeof(HistorianClient).GetMethod("ReadProcessed") != null,
                    "Processed API must be exposed");
                Assert(
                    typeof(HistorianClient).GetMethod("ReadProcessedBatch") != null,
                    "Processed batch API must be exposed");
                Assert(
                    HasMethod(typeof(BatchSender), "SendWithRetry"),
                    "database ACK retry API must be exposed");
                Assert(
                    typeof(BatchSender).GetMethod("SendPending") == null,
                    "pending sender API must be removed");
                Assert(
                    typeof(SyncState).GetField("CheckpointEnd") != null,
                    "single CheckpointEnd state field");
                Assert(
                    typeof(SyncState).GetField("LastCollectedEnd") == null &&
                    typeof(SyncState).GetField("LastAcceptedEnd") == null &&
                    typeof(SyncState).GetField("LastCommittedEnd") == null &&
                    typeof(SyncState).GetField("CollectionPaused") == null &&
                    typeof(SyncState).GetField("PauseReason") == null,
                    "legacy state fields must be removed");

                RunPureFunctionSelfTest();
                RunHistorianCoreSelfTest();
                RunStateSelfTest();
                RunBatchBackpressureSelfTest();
                RunAckLossSelfTest();
                RunPermanentStatusSelfTest(400, false);
                RunPermanentStatusSelfTest(401, true);
                RunPermanentStatusSelfTest(413, false);
                RunStopSelfTest();

                Console.WriteLine("PROCESSED SYNC SELF TEST OK");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("PROCESSED SYNC SELF TEST FAILED: " + ex);
                return 1;
            }
        }

        private static void RunPureFunctionSelfTest()
        {
            DateTime input = new DateTime(2026, 8, 28, 10, 5, 27);
            DateTime aligned = (DateTime)InvokePrivate(
                typeof(SyncProgram),
                "AlignDown",
                new object[] { input, 10 });
            Assert(
                aligned == new DateTime(2026, 8, 28, 10, 5, 20),
                "10-second grid alignment");

            int futureWait = (int)InvokePrivate(
                typeof(SyncProgram),
                "CalculateWaitMilliseconds",
                new object[]
                {
                    new DateTime(2026, 8, 28, 10, 5, 0),
                    new DateTime(2026, 8, 28, 10, 0, 0)
                });
            int overdueWait = (int)InvokePrivate(
                typeof(SyncProgram),
                "CalculateWaitMilliseconds",
                new object[]
                {
                    new DateTime(2026, 8, 28, 10, 0, 0),
                    new DateTime(2026, 8, 28, 10, 5, 0)
                });
            Assert(
                futureWait == 300000 && overdueWait == 0,
                "continuous schedule catches up with zero overdue wait");

            TimeSpan rowSlice = (TimeSpan)InvokePrivate(
                typeof(SyncProgram),
                "CalculateByteAwareSlice",
                new object[]
                {
                    827,
                    50000,
                    25000,
                    20971520L,
                    10485760L,
                    256.0,
                    10,
                    TimeSpan.FromMinutes(30)
                });
            Assert(
                rowSlice == TimeSpan.FromMinutes(5),
                "row/byte-aware normal window capacity");

            TimeSpan byteLimitedSlice = (TimeSpan)InvokePrivate(
                typeof(SyncProgram),
                "CalculateByteAwareSlice",
                new object[]
                {
                    100,
                    50000,
                    25000,
                    20971520L,
                    10240L,
                    100.0,
                    10,
                    TimeSpan.FromMinutes(30)
                });
            Assert(
                byteLimitedSlice == TimeSpan.FromSeconds(10),
                "byte-aware pre-split");

            HistoryBatch batch = BuildBatch(
                "selftest_encoder",
                new DateTime(2026, 8, 28, 10, 0, 0),
                new DateTime(2026, 8, 28, 10, 5, 0));
            BatchPayload payload = BatchEncoder.EncodePayload(batch, 4096);
            Assert(
                payload.Length > 0 &&
                payload.Length <= payload.Buffer.Length &&
                payload.Sha256 == BatchEncoder.ComputeSha256(
                    payload.Buffer,
                    payload.Length),
                "bounded batch payload buffer and SHA");

            byte[] inputBytes = new byte[150000];
            int index;
            for (index = 0; index < inputBytes.Length; index++)
                inputBytes[index] = (byte)(index % 251);
            string expectedHash = BatchEncoder.ComputeSha256(inputBytes);
            MemoryStream destination = new MemoryStream();
            string actualHash = (string)InvokePrivate(
                typeof(BatchSender),
                "CopyPayloadAndHash",
                new object[]
                {
                    new MemoryStream(inputBytes, false),
                    destination,
                    (long)inputBytes.Length,
                    expectedHash
                });
            Assert(
                actualHash == expectedHash &&
                ByteArraysEqual(destination.ToArray(), inputBytes),
                "streaming payload copy and SHA");
        }

        private static void RunHistorianCoreSelfTest()
        {
            DateTime utc = new DateTime(
                2026,
                8,
                27,
                2,
                0,
                0,
                DateTimeKind.Utc);
            DateTime local = (DateTime)InvokePrivate(
                typeof(HistorianClient),
                "ToCollectorLocalTime",
                new object[] { utc });
            Assert(
                local == utc.ToLocalTime(),
                "UTC Historian timestamp conversion");

            string sequence = (string)InvokePrivate(
                typeof(HistorianClient),
                "BuildProcessedSequence",
                new object[] { 10 });
            Assert(
                sequence == "P:InterpolatedValue:10",
                "stable Processed identity");

            FakeHistorianReadInterface fakeRead =
                new FakeHistorianReadInterface();
            FakeHistorianConnection fakeConnection =
                new FakeHistorianConnection();
            HistorianClient fakeClient = CreateFakeClient(
                fakeRead,
                fakeConnection);
            fakeConnection.PointsPerRead = 1000;
            List<TagResult> fakeTags = new List<TagResult>();
            fakeTags.Add(new TagResult { Name = "TAG/A", Handle = 101, Status = 1 });
            fakeTags.Add(new TagResult { Name = "TAG/B", Handle = 102, Status = 1 });
            fakeTags.Add(new TagResult {
                Name = "TAG/INVALID",
                Handle = -1,
                Status = 0
            });
            List<ProcessedTagResult> fakeResults =
                fakeClient.ReadProcessedBatch(
                    fakeTags,
                    new DateTime(2026, 8, 28, 10, 0, 0),
                    new DateTime(2026, 8, 28, 10, 5, 0),
                    10);
            Assert(
                fakeRead.CreateCount == 1 &&
                fakeRead.ReleaseCount == 1,
                "one shared Historian TimeSpan per window");
            Assert(
                fakeConnection.ReadCount == 2 &&
                fakeResults.Count == 3 &&
                fakeResults[0].Result != null &&
                fakeResults[1].Result != null &&
                fakeResults[2].Error != null,
                "serial Processed reads preserve per-tag results");
            Assert(
                fakeClient.ProcessedPointAccessorBuildCount == 1 &&
                fakeResults[0].Result.Samples.Count == 1000 &&
                fakeResults[1].Result.Samples.Count == 1000,
                "Processed point accessors are reused for one point type");
            Assert(
                fakeClient.LastPerformance.ReturnedSamples == 2000 &&
                fakeClient.LastPerformance.InvalidSamples == 0 &&
                fakeClient.LastPerformance.NormalizeFastPathTags == 2 &&
                fakeClient.LastPerformance.NormalizeFallbackTags == 0,
                "Processed hot-path performance counters");

            fakeConnection.UseAlternatePointType = true;
            List<TagResult> alternateTags = new List<TagResult>();
            alternateTags.Add(new TagResult {
                Name = "TAG/ALTERNATE",
                Handle = 103,
                Status = 1
            });
            List<ProcessedTagResult> alternateResults =
                fakeClient.ReadProcessedBatch(
                    alternateTags,
                    new DateTime(2026, 8, 28, 10, 0, 0),
                    new DateTime(2026, 8, 28, 10, 5, 0),
                    10);
            Assert(
                fakeClient.ProcessedPointAccessorBuildCount == 2 &&
                alternateResults[0].Result != null &&
                alternateResults[0].Result.Samples.Count == 1000 &&
                alternateResults[0].Result.Samples[0].Tag == "TAG/ALTERNATE",
                "Processed point accessors rebuild for a different point type");

            fakeConnection.UseAlternatePointType = false;
            List<TagResult> repeatedTags = new List<TagResult>();
            repeatedTags.Add(new TagResult {
                Name = "TAG/REPEATED",
                Handle = 104,
                Status = 1
            });
            List<ProcessedTagResult> repeatedResults =
                fakeClient.ReadProcessedBatch(
                    repeatedTags,
                    new DateTime(2026, 8, 28, 10, 0, 0),
                    new DateTime(2026, 8, 28, 10, 5, 0),
                    10);
            Assert(
                fakeClient.ProcessedPointAccessorBuildCount == 2 &&
                repeatedResults[0].Result != null &&
                repeatedResults[0].Result.Samples.Count == 1000,
                "Processed point accessors reuse an earlier runtime point type");

            List<HistorySample> unordered = new List<HistorySample>();
            unordered.Add(new HistorySample {
                Timestamp = new DateTime(2026, 8, 28, 10, 0, 20)
            });
            unordered.Add(new HistorySample {
                Timestamp = new DateTime(2026, 8, 28, 10, 0, 10)
            });
            bool normalizeFastPath;
            List<HistorySample> normalized =
                HistorySampleSet.NormalizeProcessed(
                    unordered,
                    out normalizeFastPath);
            Assert(
                !normalizeFastPath &&
                normalized[0].Timestamp < normalized[1].Timestamp,
                "Processed normalization fallback preserves ordering");
            fakeClient.Dispose();
        }

        private static void RunStateSelfTest()
        {
            string root = CreateTempRoot("HistorySyncStateSelfTest");
            try
            {
                string path = Path.Combine(root, "state.ini");
                DateTime checkpoint = new DateTime(2026, 8, 29, 7, 30, 0);
                SyncStateStore store = new SyncStateStore(path);
                SyncState initial = new SyncState();
                initial.CheckpointEnd = checkpoint;
                store.Save(initial);
                string text = File.ReadAllText(path, Encoding.UTF8);
                Assert(
                    text.IndexOf("[ContinuousSync]", StringComparison.Ordinal) >= 0 &&
                    text.IndexOf("CheckpointEnd=", StringComparison.Ordinal) >= 0 &&
                    text.IndexOf("LastCollectedEnd=", StringComparison.Ordinal) < 0,
                    "state.ini contains only CheckpointEnd");
                SyncState loaded = store.LoadOrCreate(initial);
                Assert(
                    loaded.CheckpointEnd == checkpoint,
                    "CheckpointEnd round trip");

                File.WriteAllText(
                    path,
                    "[ContinuousSync]\n" +
                    "LastCommittedEnd=2026-08-29 07:30:00.0000000\n",
                    Encoding.UTF8);
                bool legacyRejected = false;
                try
                {
                    store.LoadOrCreate(initial);
                }
                catch (InvalidDataException)
                {
                    legacyRejected = true;
                }
                Assert(legacyRejected, "legacy state is not compatibility-read");
            }
            finally
            {
                DeleteTempRoot(root);
            }
        }

        private static void RunBatchBackpressureSelfTest()
        {
            string root = CreateTempRoot("HistorySyncBackpressureSelfTest");
            ScriptedReceiver receiver = new ScriptedReceiver(
                new int[] { 503, 503, 503, 200, 200 },
                false);
            receiver.Start();
            ManualResetEvent pipelineDone = new ManualResetEvent(false);
            ManualResetEvent releaseAck = receiver.ReleaseResponse;
            Exception pipelineError = null;
            int firstResult = -1;
            int secondResult = -1;
            DateTime firstStart = new DateTime(2026, 8, 29, 10, 0, 0);
            DateTime firstEnd = firstStart.AddMinutes(5);
            DateTime secondEnd = firstEnd.AddMinutes(5);
            try
            {
                IniConfig config = LoadReceiverConfig(
                    root,
                    receiver.Port,
                    1);
                string logs = Path.Combine(root, "logs");
                Directory.CreateDirectory(logs);
                SyncOptions options = BuildTestOptions(
                    root,
                    "sync",
                    firstStart,
                    secondEnd);
                SyncState state = new SyncState();
                state.CheckpointEnd = firstStart;
                SyncStateStore stateStore = new SyncStateStore(
                    Path.Combine(root, "state.ini"));
                stateStore.Save(state);
                FakeHistorianReadInterface fakeRead =
                    new FakeHistorianReadInterface();
                FakeHistorianConnection fakeConnection =
                    new FakeHistorianConnection();
                HistorianClient fakeClient = CreateFakeClient(
                    fakeRead,
                    fakeConnection);
                List<TagResult> tags = new List<TagResult>();
                tags.Add(new TagResult {
                    Name = "TAG/BACKPRESSURE",
                    Handle = 101,
                    Status = 1
                });

                Thread pipeline = new Thread(delegate()
                {
                    try
                    {
                        using (SyncLogger log = new SyncLogger(logs))
                        {
                            BatchSender sender = new BatchSender(config, log);
                            int firstCreated = 0;
                            double estimate = 256.0;
                            object[] firstArgs = new object[]
                            {
                                options,
                                firstStart,
                                firstEnd,
                                log,
                                fakeClient,
                                tags,
                                sender,
                                state,
                                stateStore,
                                firstCreated,
                                estimate,
                                null
                            };
                            firstResult = (int)InvokePrivate(
                                typeof(SyncProgram),
                                "CollectWindow",
                                firstArgs);
                            int secondCreated = 0;
                            object[] secondArgs = new object[]
                            {
                                options,
                                firstEnd,
                                secondEnd,
                                log,
                                fakeClient,
                                tags,
                                sender,
                                state,
                                stateStore,
                                secondCreated,
                                firstArgs[10],
                                null
                            };
                            secondResult = (int)InvokePrivate(
                                typeof(SyncProgram),
                                "CollectWindow",
                                secondArgs);
                        }
                    }
                    catch (Exception ex)
                    {
                        pipelineError = ex;
                    }
                    finally
                    {
                        fakeClient.Dispose();
                        pipelineDone.Set();
                    }
                });
                pipeline.IsBackground = true;
                pipeline.Start();

                Assert(
                    receiver.ThirdTransientObserved.WaitOne(10000, false),
                    "Batch 1 reaches repeated transient failures");
                Assert(
                    fakeConnection.ReadCount == 1 &&
                    state.CheckpointEnd == firstStart,
                    "no next Historian read or checkpoint before Batch 1 ACK");
                releaseAck.Set();
                Assert(
                    pipelineDone.WaitOne(15000, false),
                    "strict backpressure pipeline completes");
                Assert(
                    pipelineError == null,
                    "strict backpressure pipeline has no error");
                Assert(
                    firstResult == 0 && secondResult == 0,
                    "ACKed batches return success");
                Assert(
                    fakeConnection.ReadCount == 2 &&
                    state.CheckpointEnd == secondEnd,
                    "Batch 2 starts only after Batch 1 checkpoint");
                Assert(
                    receiver.Requests == 5,
                    "three transient retries then two acknowledged batches");
                Assert(
                    ByteArraysEqual(receiver.Bodies[0], receiver.Bodies[1]) &&
                    ByteArraysEqual(receiver.Bodies[0], receiver.Bodies[2]) &&
                    ByteArraysEqual(receiver.Bodies[0], receiver.Bodies[3]),
                    "Batch 1 retries use identical payload bytes");
                Assert(
                    StringEqualsAll(receiver.BatchIds, 0, 3) &&
                    StringEqualsAll(receiver.Hashes, 0, 3),
                    "Batch 1 retries use identical BatchId and SHA");
                Assert(
                    !Directory.Exists(Path.Combine(root, "spool")),
                    "backpressure run creates no spool directory");
            }
            finally
            {
                releaseAck.Set();
                receiver.StopAndJoin();
                DeleteTempRoot(root);
            }
        }

        private static void RunAckLossSelfTest()
        {
            string root = CreateTempRoot("HistorySyncAckLossSelfTest");
            ScriptedReceiver receiver = new ScriptedReceiver(
                new int[] { 200, 200 },
                true);
            receiver.Start();
            try
            {
                IniConfig config = LoadReceiverConfig(root, receiver.Port, 1);
                using (SyncLogger log =
                    new SyncLogger(Path.Combine(root, "logs")))
                {
                    BatchSender sender = new BatchSender(config, log);
                    HistoryBatch batch = BuildBatch(
                        "selftest_ack_loss",
                        new DateTime(2026, 8, 29, 11, 0, 0),
                        new DateTime(2026, 8, 29, 11, 5, 0));
                    BatchPayload payload = BatchEncoder.EncodePayload(batch, 4096);
                    batch.Sha256 = payload.Sha256;
                    BatchReceipt receipt = sender.SendWithRetry(
                        batch,
                        payload,
                        null);
                    Assert(
                        receipt.CommitLevel == "database" &&
                        receipt.Timings.Attempts == 2,
                        "ACK loss retries until database ACK");
                }
                Assert(
                    receiver.Requests == 2 &&
                    ByteArraysEqual(receiver.Bodies[0], receiver.Bodies[1]) &&
                    receiver.BatchIds[0] == receiver.BatchIds[1] &&
                    receiver.Hashes[0] == receiver.Hashes[1],
                    "ACK loss resends the exact in-memory batch");
            }
            finally
            {
                receiver.StopAndJoin();
                DeleteTempRoot(root);
            }
        }

        private static void RunPermanentStatusSelfTest(
            int statusCode,
            bool authentication)
        {
            string root = CreateTempRoot(
                "HistorySyncHttp" + statusCode.ToString(
                    CultureInfo.InvariantCulture) + "SelfTest");
            ScriptedReceiver receiver = new ScriptedReceiver(
                new int[] { statusCode },
                false);
            receiver.Start();
            try
            {
                IniConfig config = LoadReceiverConfig(root, receiver.Port, 1);
                bool rejected = false;
                using (SyncLogger log =
                    new SyncLogger(Path.Combine(root, "logs")))
                {
                    BatchSender sender = new BatchSender(config, log);
                    HistoryBatch batch = BuildBatch(
                        "selftest_http_" + statusCode.ToString(
                            CultureInfo.InvariantCulture),
                        new DateTime(2026, 8, 29, 12, 0, 0),
                        new DateTime(2026, 8, 29, 12, 5, 0));
                    BatchPayload payload = BatchEncoder.EncodePayload(batch, 4096);
                    try
                    {
                        sender.SendWithRetry(batch, payload, null);
                    }
                    catch (BatchSendException ex)
                    {
                        rejected = ex.Permanent &&
                            ex.AuthenticationFailure == authentication &&
                            ex.StatusCode == statusCode;
                    }
                }
                Assert(
                    rejected && receiver.Requests == 1,
                    statusCode.ToString(CultureInfo.InvariantCulture) +
                    " is an immediate permanent/authentication stop");
            }
            finally
            {
                receiver.StopAndJoin();
                DeleteTempRoot(root);
            }
        }

        private static void RunStopSelfTest()
        {
            string root = CreateTempRoot("HistorySyncStopSelfTest");
            ScriptedReceiver receiver = new ScriptedReceiver(
                new int[] { 503 },
                false);
            receiver.Start();
            ManualResetEvent stop = new ManualResetEvent(false);
            ManualResetEvent done = new ManualResetEvent(false);
            Exception sendError = null;
            try
            {
                IniConfig config = LoadReceiverConfig(root, receiver.Port, 30);
                Thread worker = new Thread(delegate()
                {
                    try
                    {
                        using (SyncLogger log =
                            new SyncLogger(Path.Combine(root, "logs")))
                        {
                            BatchSender sender = new BatchSender(config, log);
                            HistoryBatch batch = BuildBatch(
                                "selftest_stop",
                                new DateTime(2026, 8, 29, 13, 0, 0),
                                new DateTime(2026, 8, 29, 13, 5, 0));
                            BatchPayload payload =
                                BatchEncoder.EncodePayload(batch, 4096);
                            sender.SendWithRetry(batch, payload, stop);
                        }
                    }
                    catch (Exception ex)
                    {
                        sendError = ex;
                    }
                    finally
                    {
                        done.Set();
                    }
                });
                worker.IsBackground = true;
                worker.Start();
                Assert(
                    receiver.RequestObserved.WaitOne(5000, false),
                    "stop test sends the first transient request");
                stop.Set();
                Assert(
                    done.WaitOne(2000, false),
                    "stop interrupts fixed retry wait");
                Assert(
                    sendError is SyncStopRequestedException,
                    "stop returns a stop-requested result");
            }
            finally
            {
                stop.Set();
                receiver.StopAndJoin();
                DeleteTempRoot(root);
            }
        }

        private static SyncOptions BuildTestOptions(
            string root,
            string command,
            DateTime start,
            DateTime end)
        {
            SyncOptions options = new SyncOptions();
            options.Command = command;
            options.Server = "APP";
            options.CollectorId = "DCS-SELFTEST";
            options.Start = start;
            options.End = end;
            options.Slice = TimeSpan.FromMinutes(5);
            options.SamplingIntervalSeconds = 10;
            options.MaxFailedTagsPerBatch = 5;
            options.MaxRows = 50000;
            options.TargetRows = 25000;
            options.MaxBytes = 20971520;
            options.TargetBytes = 10485760;
            options.MinWindowSeconds = 10;
            options.OverlapSeconds = 60;
            options.StatePath = Path.Combine(root, "state.ini");
            options.LogsDirectory = Path.Combine(root, "logs");
            return options;
        }

        private static IniConfig LoadReceiverConfig(
            string root,
            int port,
            int retrySeconds)
        {
            string path = Path.Combine(root, "receiver.ini");
            File.WriteAllText(
                path,
                "[Receiver]\n" +
                "Url=http://127.0.0.1:" +
                port.ToString(CultureInfo.InvariantCulture) +
                "/api/history/batch\n" +
                "ApiKey=selftest\n" +
                "TimeoutSeconds=5\n" +
                "SendRetrySeconds=" +
                retrySeconds.ToString(CultureInfo.InvariantCulture) +
                "\nAckMode=database\n",
                Encoding.UTF8);
            return IniConfig.Load(path);
        }

        private static HistoryBatch BuildBatch(
            string batchId,
            DateTime start,
            DateTime end)
        {
            HistoryBatch batch = new HistoryBatch();
            batch.BatchId = batchId;
            batch.CollectorId = "DCS-SELFTEST";
            batch.Mode = "sync";
            batch.Sampling = "InterpolatedValue";
            batch.SamplingIntervalSeconds = 10;
            batch.Server = "APP";
            batch.RangeStart = start;
            batch.RangeEnd = end;
            batch.Samples.Add(new HistorySample {
                Tag = "TAG/SELFTEST",
                Timestamp = start,
                Value = "1",
                DataType = "Float",
                Flags = "",
                SequenceNo = "P:InterpolatedValue:10",
                ArchiveStatus = "Current"
            });
            return batch;
        }

        private static HistorianClient CreateFakeClient(
            FakeHistorianReadInterface read,
            FakeHistorianConnection connection)
        {
            HistorianClient client = new HistorianClient("", null);
            SetPrivateField(client, "_readInterface", read);
            SetPrivateField(client, "_connection", connection);
            SetPrivateField(
                client,
                "_readProcessed",
                typeof(FakeHistorianConnection).GetMethod("readProcessed"));
            SetPrivateField(client, "_processedSampleType", typeof(object));
            SetPrivateField(client, "_interpolatedAggregate", "InterpolatedValue");
            SetPrivateField(client, "_connectionHandle", 1);
            return client;
        }

        private static object InvokePrivate(
            Type type,
            string name,
            object[] args)
        {
            MethodInfo method = type.GetMethod(
                name,
                BindingFlags.NonPublic | BindingFlags.Static);
            if (method == null)
                throw new Exception(
                    "Method not found: " + type.FullName + "." + name);
            try
            {
                return method.Invoke(null, args);
            }
            catch (TargetInvocationException ex)
            {
                if (ex.InnerException != null)
                    throw ex.InnerException;
                throw;
            }
        }

        private static bool HasMethod(Type type, string name)
        {
            MethodInfo[] methods = type.GetMethods(
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance |
                BindingFlags.Static);
            int i;
            for (i = 0; i < methods.Length; i++)
                if (String.Equals(
                    methods[i].Name,
                    name,
                    StringComparison.Ordinal))
                    return true;
            return false;
        }

        private static void SetPrivateField(
            object instance,
            string name,
            object value)
        {
            FieldInfo field = instance.GetType().GetField(
                name,
                BindingFlags.NonPublic | BindingFlags.Instance);
            if (field == null)
                throw new Exception("Field not found: " + name);
            field.SetValue(instance, value);
        }

        private static string CreateTempRoot(string prefix)
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                prefix + "_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            return root;
        }

        private static void DeleteTempRoot(string root)
        {
            if (String.IsNullOrEmpty(root))
                return;
            if (Directory.Exists(root))
            {
                try { Directory.Delete(root, true); }
                catch { }
            }
        }

        private static bool StringEqualsAll(
            List<string> values,
            int first,
            int last)
        {
            int i;
            for (i = first + 1; i <= last; i++)
                if (values[i] != values[first])
                    return false;
            return true;
        }

        private static bool ByteArraysEqual(byte[] left, byte[] right)
        {
            if (left == null || right == null || left.Length != right.Length)
                return false;
            int i;
            for (i = 0; i < left.Length; i++)
                if (left[i] != right[i])
                    return false;
            return true;
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
        private readonly FakeHistorianTimeSpan _timeSpan =
            new FakeHistorianTimeSpan();

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
        public int PointsPerRead = 1;
        public bool UseAlternatePointType;

        public FakeHistorianProcessed readProcessed(
            int timeSpanHandle,
            int tagHandle,
            object sampleType,
            ArrayList aggregates)
        {
            if (timeSpanHandle != 7 ||
                sampleType == null ||
                aggregates.Count != 1)
                throw new Exception(
                    "shared Processed read arguments were not configured");
            ReadCount++;
            FakeHistorianProcessed processed = new FakeHistorianProcessed();
            processed.nSamples = PointsPerRead;
            ArrayList samples = new ArrayList();
            int pointIndex;
            for (pointIndex = 0; pointIndex < PointsPerRead; pointIndex++)
            {
                DateTime timestamp = new DateTime(
                    2026,
                    8,
                    29,
                    10,
                    0,
                    0).AddMilliseconds(
                        (ReadCount - 1) * PointsPerRead + pointIndex);
                if (UseAlternatePointType)
                    samples.Add(new FakeHistorianPointAlternate {
                        timestamp = timestamp,
                        value = tagHandle,
                        dataType = "Float",
                        archiveStatus = 0
                    });
                else
                    samples.Add(new FakeHistorianPoint {
                        timestamp = timestamp,
                        value = tagHandle,
                        dataType = "Float",
                        archiveStatus = 0
                    });
            }
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

    class FakeHistorianPointAlternate
    {
        public DateTime timestamp;
        public object value;
        public object dataType;
        public int archiveStatus;
    }

    class ScriptedReceiver
    {
        private readonly int[] _statuses;
        private readonly bool _closeFirst;
        private readonly TcpListener _listener;
        private Thread _thread;
        private readonly object _sync = new object();

        public readonly ManualResetEvent ThirdTransientObserved =
            new ManualResetEvent(false);
        public readonly ManualResetEvent ReleaseResponse =
            new ManualResetEvent(false);
        public readonly ManualResetEvent RequestObserved =
            new ManualResetEvent(false);
        public readonly List<byte[]> Bodies = new List<byte[]>();
        public readonly List<string> BatchIds = new List<string>();
        public readonly List<string> Hashes = new List<string>();
        public int Requests;
        public Exception Error;

        public ScriptedReceiver(int[] statuses, bool closeFirst)
        {
            _statuses = statuses;
            _closeFirst = closeFirst;
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
        }

        public int Port
        {
            get { return ((IPEndPoint)_listener.LocalEndpoint).Port; }
        }

        public void Start()
        {
            _thread = new Thread(Run);
            _thread.IsBackground = true;
            _thread.Start();
        }

        public void StopAndJoin()
        {
            ReleaseResponse.Set();
            try { _listener.Stop(); }
            catch { }
            if (_thread != null && !_thread.Join(10000))
                throw new Exception("Scripted Receiver did not stop.");
            if (Error != null)
                throw new Exception("Scripted Receiver failed.", Error);
        }

        private void Run()
        {
            try
            {
                int index;
                for (index = 0; index < _statuses.Length; index++)
                {
                    using (TcpClient client = _listener.AcceptTcpClient())
                    {
                        client.ReceiveTimeout = 10000;
                        client.SendTimeout = 10000;
                        using (NetworkStream stream = client.GetStream())
                        {
                            string headers = ReadHeaders(stream);
                            if (headers == null)
                                throw new Exception("request headers ended early");
                            int contentLength = Int32.Parse(
                                HeaderValue(headers, "Content-Length"),
                                CultureInfo.InvariantCulture);
                            byte[] body = ReadBody(stream, contentLength);
                            string batchId = HeaderValue(headers, "X-Batch-Id");
                            string hash = HeaderValue(
                                headers,
                                "X-Content-SHA256");
                            lock (_sync)
                            {
                                Requests++;
                                Bodies.Add(body);
                                BatchIds.Add(batchId);
                                Hashes.Add(hash);
                            }
                            RequestObserved.Set();
                            if (index == 2)
                            {
                                ThirdTransientObserved.Set();
                                ReleaseResponse.WaitOne(10000, false);
                            }
                            if (_closeFirst && index == 0)
                                continue;

                            int statusCode = _statuses[index];
                            string responseText;
                            if (statusCode == 200)
                            {
                                responseText =
                                    "{\"ok\":true,\"committed\":true," +
                                    "\"commit_level\":\"database\"," +
                                    "\"batch_id\":\"" + batchId +
                                    "\",\"sha256\":\"" + hash +
                                    "\",\"received_rows\":" +
                                    HeaderValue(
                                        headers,
                                        "X-Row-Count") + "}";
                            }
                            else
                                responseText = "temporary";
                            byte[] responseBody = Encoding.UTF8.GetBytes(
                                responseText);
                            string reason = statusCode == 200
                                ? "OK"
                                : "Error";
                            string responseHeaders =
                                "HTTP/1.1 " +
                                statusCode.ToString(
                                    CultureInfo.InvariantCulture) +
                                " " + reason + "\r\n" +
                                "Content-Type: application/json\r\n" +
                                "Content-Length: " +
                                responseBody.Length.ToString(
                                    CultureInfo.InvariantCulture) +
                                "\r\nConnection: close\r\n\r\n";
                            byte[] headerBytes = Encoding.ASCII.GetBytes(
                                responseHeaders);
                            stream.Write(
                                headerBytes,
                                0,
                                headerBytes.Length);
                            stream.Write(
                                responseBody,
                                0,
                                responseBody.Length);
                            stream.Flush();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Error = ex;
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
                if (length >= 4 &&
                    text[length - 4] == '\r' &&
                    text[length - 3] == '\n' &&
                    text[length - 2] == '\r' &&
                    text[length - 1] == '\n')
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
                if (String.Equals(
                    key,
                    name,
                    StringComparison.OrdinalIgnoreCase))
                    return lines[i].Substring(separator + 1).Trim();
            }
            throw new Exception("Missing HTTP header: " + name);
        }
    }
}
