using System;
using System.Reflection;

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

                DateTime input = new DateTime(2026, 8, 28, 10, 5, 27);
                DateTime aligned = (DateTime)InvokePrivate(
                    typeof(SyncProgram),
                    "AlignDown",
                    new object[] { input, 10 });
                Assert(aligned == new DateTime(2026, 8, 28, 10, 5, 20),
                    "10-second grid alignment");

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

        private static void Assert(bool condition, string name)
        {
            if (!condition)
                throw new Exception("Assertion failed: " + name);
        }
    }
}
