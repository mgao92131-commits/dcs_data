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
                TestCsvCombination(root);
                TestStagingRecovery(root);
                Console.WriteLine("PHASE 1 SELF-TEST PASSED");
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("PHASE 1 SELF-TEST FAILED: " + ex.Message);
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
            Assert(Directory.GetDirectories(Path.Combine(spool, "failed")).Length == 1, "staging recovery destination");
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
