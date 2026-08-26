using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;

namespace DeltaVHistoryCLI
{
    class Phase1SelfTest
    {
        static int Main(string[] args)
        {
            string root = Path.Combine(
                Path.GetTempPath(),
                "DeltaVHistory_Phase1_Test_" + Guid.NewGuid().ToString("N"));

            try
            {
                Directory.CreateDirectory(root);
                TestIni(root);
                TestHistorySampleNormalization();
                TestMemoryBatchAndSpool(root);
                TestAtomicSyncState(root);
                TestOutboxCheckpointRecovery(root);
                TestInitialStateSkipsCorruptOutbox(root);
                TestPendingRangeOrder(root);
                TestCsvCombination(root);
                TestStagingRecovery(root);
                Console.WriteLine("PHASE 1 SELF-TEST PASSED");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("PHASE 1 SELF-TEST FAILED: " + ex.ToString());
                return 1;
            }
            finally
            {
                try
                {
                    if (Directory.Exists(root))
                        Directory.Delete(root, true);
                }
                catch { }
            }
        }

        private static void TestHistorySampleNormalization()
        {
            DateTime firstTime = new DateTime(2026, 8, 26, 9, 0, 0);
            HistorySample later = MakeSample("TAG/A", firstTime.AddSeconds(1), "2");
            HistorySample first = MakeSample("TAG/A", firstTime, "1");
            HistorySample duplicate = MakeSample("TAG/A", firstTime, "1");
            System.Collections.Generic.List<HistorySample> rows =
                new System.Collections.Generic.List<HistorySample>();
            rows.Add(later);
            rows.Add(first);
            rows.Add(duplicate);

            rows = HistorySampleSet.Normalize(rows);
            Assert(rows.Count == 2, "Historian Core deduplication");
            Assert(rows[0].Timestamp == firstTime, "Historian Core timestamp ordering");
            Assert(rows[1].Value == "2", "Historian Core sample preservation");
        }

        private static HistorySample MakeSample(string tag, DateTime timestamp, string value)
        {
            HistorySample sample = new HistorySample();
            sample.Tag = tag;
            sample.Timestamp = timestamp;
            sample.Value = value;
            sample.DataType = "Float";
            sample.Flags = "";
            sample.SequenceNo = "";
            sample.ArchiveStatus = "";
            return sample;
        }

        private static void TestMemoryBatchAndSpool(string root)
        {
            HistoryBatch batch = new HistoryBatch();
            batch.BatchId = "test_batch_memory";
            batch.CollectorId = "DCS-TEST";
            batch.Mode = "sync";
            batch.Server = "APP";
            batch.RangeStart = new DateTime(2026, 8, 26, 9, 0, 0);
            batch.RangeEnd = batch.RangeStart.AddMinutes(5);
            batch.Samples.Add(MakeSample("TAG/A", batch.RangeStart.AddSeconds(1), "1.25"));

            byte[] data = BatchEncoder.EncodeCsv(batch);
            batch.Sha256 = BatchEncoder.ComputeSha256(data);
            string csv = Encoding.UTF8.GetString(data);
            Assert(csv.IndexOf("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus") >= 0,
                "in-memory batch header");
            Assert(csv.IndexOf("\"TAG/A\"") >= 0, "in-memory batch row");
            Assert(batch.Sha256.Length == 64, "in-memory batch SHA-256");

            string spool = Path.Combine(root, "memory-spool");
            SpoolStore store = new SpoolStore(spool);
            store.SavePending(batch, data);
            string pending = Path.Combine(spool, "pending", batch.BatchId);
            Assert(File.Exists(Path.Combine(pending, "data.csv")), "outbox data persistence");
            Assert(File.Exists(Path.Combine(pending, "meta.ini")), "outbox metadata persistence");
            Assert(Directory.GetDirectories(Path.Combine(spool, "staging")).Length == 0,
                "outbox atomic staging cleanup");
            bool capacityBlocked = false;
            try { store.EnsurePendingCapacity(1, 1024 * 1024); }
            catch (IOException) { capacityBlocked = true; }
            Assert(capacityBlocked, "outbox batch capacity");
        }

        private static void TestAtomicSyncState(string root)
        {
            string path = Path.Combine(root, "state", "state.ini");
            DateTime baseline = new DateTime(2026, 8, 26, 9, 0, 0);
            SyncStateStore store = new SyncStateStore(path);
            SyncState state = store.LoadOrCreate(baseline);
            state.LastCollectedEnd = baseline.AddMinutes(5);
            state.LastAcceptedEnd = baseline.AddMinutes(5);
            store.Save(state);

            SyncState loaded = store.LoadOrCreate(baseline);
            Assert(loaded.LastCollectedEnd == baseline.AddMinutes(5), "state collected persistence");
            Assert(loaded.LastAcceptedEnd == baseline.AddMinutes(5), "state accepted persistence");
            Assert(loaded.LastCommittedEnd == baseline, "legacy ACK does not imply DB commit");
            Assert(Directory.GetFiles(Path.GetDirectoryName(path), "*.tmp.*").Length == 0,
                "state temporary cleanup");
        }

        private static void TestIni(string root)
        {
            string path = Path.Combine(root, "config.ini");
            using (StreamWriter writer = new StreamWriter(path, false, Encoding.UTF8))
            {
                writer.WriteLine("[Historian]");
                writer.WriteLine("Server=APP");
                writer.WriteLine("[Sync]");
                writer.WriteLine("MaxSamples=10000");
            }

            IniConfig config = IniConfig.Load(path);
            Assert(config.Get("Historian", "Server", "") == "APP", "INI string value");
            Assert(config.GetInt("Sync", "MaxSamples", 0) == 10000, "INI integer value");
        }

        private static void TestOutboxCheckpointRecovery(string root)
        {
            string spool = Path.Combine(root, "recovery-spool");
            string pending = Path.Combine(spool, "pending", "recovery_batch");
            Directory.CreateDirectory(pending);
            using (StreamWriter writer = new StreamWriter(
                Path.Combine(pending, "meta.ini"), false, Encoding.UTF8))
            {
                writer.WriteLine("[Batch]");
                writer.WriteLine("Mode=sync");
                writer.WriteLine("Start=2026-08-26 08:59:00.0000000");
                writer.WriteLine("End=2026-08-26 09:05:00.0000000");
            }

            string statePath = Path.Combine(root, "recovery-state", "state.ini");
            DateTime baseline = new DateTime(2026, 8, 26, 9, 0, 0);
            SyncStateStore store = new SyncStateStore(statePath);
            SyncState state = store.LoadOrCreate(baseline);
            using (SyncLogger logger = new SyncLogger(Path.Combine(root, "recovery-logs")))
            {
                MethodInfo reconcile = typeof(SyncProgram).GetMethod(
                    "ReconcileCollectedFromOutbox",
                    BindingFlags.NonPublic | BindingFlags.Static);
                if (reconcile == null)
                    throw new Exception("ReconcileCollectedFromOutbox was not found.");
                reconcile.Invoke(null, new object[] { spool, state, store, logger });
            }

            SyncState recovered = store.LoadOrCreate(baseline);
            Assert(
                recovered.LastCollectedEnd == new DateTime(2026, 8, 26, 9, 5, 0),
                "outbox checkpoint recovery");
            Assert(
                recovered.LastAcceptedEnd == baseline && recovered.LastCommittedEnd == baseline,
                "outbox recovery does not imply remote commit");
        }

        private static void TestPendingRangeOrder(string root)
        {
            string pending = Path.Combine(root, "ordered-pending");
            Directory.CreateDirectory(Path.Combine(pending, "z_later"));
            Directory.CreateDirectory(Path.Combine(pending, "a_earlier"));
            Directory.CreateDirectory(Path.Combine(pending, "m_invalid"));
            WritePendingMeta(
                Path.Combine(pending, "z_later", "meta.ini"),
                "2026-08-26 09:05:00.0000000");
            WritePendingMeta(
                Path.Combine(pending, "a_earlier", "meta.ini"),
                "2026-08-26 09:00:00.0000000");

            MethodInfo order = typeof(BatchSender).GetMethod(
                "GetPendingBatches",
                BindingFlags.NonPublic | BindingFlags.Static);
            if (order == null)
                throw new Exception("GetPendingBatches was not found.");
            System.Collections.IList batches = (System.Collections.IList)order.Invoke(
                null,
                new object[] { pending });
            Assert(batches.Count == 3, "pending order batch count");
            FieldInfo directory = batches[0].GetType().GetField("Directory");
            Assert(
                Path.GetFileName((string)directory.GetValue(batches[0])) == "m_invalid",
                "invalid pending metadata is processed before valid batches");
            Assert(
                Path.GetFileName((string)directory.GetValue(batches[1])) == "a_earlier",
                "pending batches are sorted by RangeStart");
            Assert(
                Path.GetFileName((string)directory.GetValue(batches[2])) == "z_later",
                "pending later range follows earlier range");
        }

        private static void TestInitialStateSkipsCorruptOutbox(string root)
        {
            string spool = Path.Combine(root, "corrupt-initial-spool");
            string pending = Path.Combine(spool, "pending", "corrupt_batch");
            Directory.CreateDirectory(pending);
            using (StreamWriter writer = new StreamWriter(
                Path.Combine(pending, "meta.ini"), false, Encoding.UTF8))
            {
                writer.WriteLine("[Batch]");
                writer.WriteLine("Mode=sync");
                writer.WriteLine("Start=not-a-time");
                writer.WriteLine("End=not-a-time");
            }

            SyncOptions options = new SyncOptions();
            options.SpoolDirectory = spool;
            options.Start = new DateTime(2026, 8, 26, 9, 0, 0);
            MethodInfo build = typeof(SyncProgram).GetMethod(
                "BuildInitialState",
                BindingFlags.NonPublic | BindingFlags.Static);
            if (build == null)
                throw new Exception("BuildInitialState was not found.");
            SyncState state = (SyncState)build.Invoke(null, new object[] { options });
            Assert(
                state.LastCollectedEnd == options.Start &&
                    state.LastAcceptedEnd == options.Start &&
                    state.LastCommittedEnd == options.Start,
                "corrupt outbox does not abort initial state creation");
        }

        private static void WritePendingMeta(string path, string start)
        {
            using (StreamWriter writer = new StreamWriter(path, false, Encoding.UTF8))
            {
                writer.WriteLine("[Batch]");
                writer.WriteLine("Start=" + start);
            }
        }

        private static void TestCsvCombination(string root)
        {
            string readerDirectory = Path.Combine(root, "reader");
            Directory.CreateDirectory(readerDirectory);
            WriteReaderCsv(Path.Combine(readerDirectory, "a.csv"), "TAG/A", "2026-08-26 09:00:00.1000000", "1.25");
            WriteReaderCsv(Path.Combine(readerDirectory, "b.csv"), "TAG/B", "2026-08-26 09:00:01.2000000", "2.50");

            string dataPath = Path.Combine(root, "data.csv");
            MethodInfo combine = typeof(SyncProgram).GetMethod(
                "CombineReaderFiles",
                BindingFlags.NonPublic | BindingFlags.Static);
            if (combine == null)
                throw new Exception("CombineReaderFiles was not found.");

            long rows = Convert.ToInt64(
                combine.Invoke(null, new object[] { readerDirectory, dataPath, 50000, 20971520L }),
                CultureInfo.InvariantCulture);
            Assert(rows == 2, "combined row count");

            string[] lines = File.ReadAllLines(dataPath, Encoding.UTF8);
            Assert(lines.Length == 3, "combined CSV line count");
            Assert(lines[1].StartsWith("\"TAG/A\","), "first combined Tag");
            Assert(lines[2].StartsWith("\"TAG/B\","), "second combined Tag");

            MethodInfo checksum = typeof(SyncProgram).GetMethod(
                "ComputeSha256",
                BindingFlags.NonPublic | BindingFlags.Static);
            string hash = (string)checksum.Invoke(null, new object[] { dataPath });
            Assert(hash.Length == 64, "SHA-256 length");
        }

        private static void TestStagingRecovery(string root)
        {
            string spool = Path.Combine(root, "spool");
            string staging = Path.Combine(spool, "staging");
            Directory.CreateDirectory(Path.Combine(staging, "unfinished.tmp"));

            string logs = Path.Combine(root, "logs");
            using (SyncLogger logger = new SyncLogger(logs))
            {
                MethodInfo prepare = typeof(SyncProgram).GetMethod(
                    "PrepareSpool",
                    BindingFlags.NonPublic | BindingFlags.Static);
                prepare.Invoke(null, new object[] { spool, logger });
            }

            Assert(Directory.GetDirectories(staging).Length == 0, "staging recovery source");
            Assert(Directory.GetDirectories(Path.Combine(spool, "quarantine")).Length == 1, "staging recovery destination");
        }

        private static void WriteReaderCsv(
            string path,
            string tag,
            string timestamp,
            string value)
        {
            using (StreamWriter writer = new StreamWriter(path, false, Encoding.UTF8))
            {
                writer.WriteLine("# DeltaV Historian Raw Export");
                writer.WriteLine("# Tag=" + tag);
                writer.WriteLine("Timestamp,Value,DataType,Flags");
                writer.WriteLine("\"" + timestamp + "\",\"" + value + "\",\"Float\",\"\"");
            }
        }

        private static void Assert(bool condition, string name)
        {
            if (!condition)
                throw new Exception("Assertion failed: " + name);
        }
    }
}
