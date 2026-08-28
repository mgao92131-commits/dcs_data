using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace DeltaVHistoryCLI
{
    class BatchLimitException : Exception
    {
        public BatchLimitException(string message) : base(message) { }
    }

    class PendingCapacityException : IOException
    {
        public readonly int PendingBatches;
        public readonly long PendingBytes;
        public readonly int MaxPendingBatches;
        public readonly long MaxPendingBytes;
        public readonly long AdditionalBytes;

        public PendingCapacityException(
            int pendingBatches,
            long pendingBytes,
            int maxPendingBatches,
            long maxPendingBytes,
            long additionalBytes)
            : base(
                "Pending outbox capacity reached. batches=" +
                pendingBatches.ToString(CultureInfo.InvariantCulture) +
                " bytes=" + pendingBytes.ToString(CultureInfo.InvariantCulture) +
                " maxBatches=" + maxPendingBatches.ToString(CultureInfo.InvariantCulture) +
                " maxBytes=" + maxPendingBytes.ToString(CultureInfo.InvariantCulture) +
                " additionalBytes=" + additionalBytes.ToString(CultureInfo.InvariantCulture))
        {
            PendingBatches = pendingBatches;
            PendingBytes = pendingBytes;
            MaxPendingBatches = maxPendingBatches;
            MaxPendingBytes = maxPendingBytes;
            AdditionalBytes = additionalBytes;
        }
    }

    class PendingStats
    {
        public int Batches;
        public long Bytes;
    }

    class HistoryBatch
    {
        public string BatchId;
        public string CollectorId;
        public string Mode;
        public string Sampling;
        public int SamplingIntervalSeconds;
        public int FailedTags;
        public int InvalidSlots;
        public string Server;
        public DateTime RangeStart;
        public DateTime RangeEnd;
        public List<HistorySample> Samples = new List<HistorySample>();
        public string Sha256;
    }

    static class BatchEncoder
    {
        public static byte[] EncodeCsv(HistoryBatch batch)
        {
            if (batch == null)
                throw new ArgumentNullException("batch");
            MemoryStream memory = new MemoryStream();
            using (StreamWriter writer = new StreamWriter(memory, new UTF8Encoding(true)))
            {
                writer.WriteLine("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus");
                int i;
                for (i = 0; i < batch.Samples.Count; i++)
                {
                    HistorySample sample = batch.Samples[i];
                    writer.Write(Csv(sample.Tag));
                    writer.Write(',');
                    writer.Write(Csv(sample.Timestamp.ToString(
                        "yyyy-MM-dd HH:mm:ss.fffffff",
                        CultureInfo.InvariantCulture)));
                    writer.Write(',');
                    writer.Write(Csv(sample.Value));
                    writer.Write(',');
                    writer.Write(Csv(sample.DataType));
                    writer.Write(',');
                    writer.Write(Csv(sample.Flags));
                    writer.Write(',');
                    writer.Write(Csv(sample.SequenceNo));
                    writer.Write(',');
                    writer.WriteLine(Csv(sample.ArchiveStatus));
                }
                writer.Flush();
                return memory.ToArray();
            }
        }

        public static string ComputeSha256(byte[] data)
        {
            using (SHA256 sha = SHA256.Create())
            {
                byte[] hash = sha.ComputeHash(data);
                StringBuilder text = new StringBuilder(hash.Length * 2);
                int i;
                for (i = 0; i < hash.Length; i++)
                    text.Append(hash[i].ToString("x2", CultureInfo.InvariantCulture));
                return text.ToString();
            }
        }

        private static string Csv(string value)
        {
            if (value == null)
                value = "";
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }
    }

    class SpoolStore
    {
        private readonly string _spoolDirectory;

        public SpoolStore(string spoolDirectory)
        {
            _spoolDirectory = spoolDirectory;
        }

        public void SavePending(HistoryBatch batch, byte[] data)
        {
            Save(batch, data, "pending", batch.BatchId);
        }

        public void SaveFailed(HistoryBatch batch, byte[] data, string reason)
        {
            Save(
                batch,
                data,
                "failed",
                batch.BatchId + "_" + SafeName(reason) + "_" + Guid.NewGuid().ToString("N"));
        }

        public PendingStats GetPendingStats()
        {
            string pendingRoot = Path.Combine(_spoolDirectory, "pending");
            Directory.CreateDirectory(pendingRoot);
            string[] directories = Directory.GetDirectories(pendingRoot);
            long bytes = 0;
            int i;
            for (i = 0; i < directories.Length; i++)
            {
                string dataPath = Path.Combine(directories[i], "data.csv");
                if (File.Exists(dataPath))
                {
                    long length = new FileInfo(dataPath).Length;
                    if (length > Int64.MaxValue - bytes)
                        bytes = Int64.MaxValue;
                    else
                        bytes += length;
                }
            }
            PendingStats stats = new PendingStats();
            stats.Batches = directories.Length;
            stats.Bytes = bytes;
            return stats;
        }

        public void EnsurePendingCapacity(int maxBatches, long maxBytes)
        {
            EnsurePendingCapacity(maxBatches, maxBytes, 0);
        }

        public void EnsurePendingCapacity(int maxBatches, long maxBytes, long additionalBytes)
        {
            if (maxBatches <= 0 || maxBytes <= 0 || additionalBytes < 0)
                throw new ArgumentOutOfRangeException("maxBatches");

            PendingStats stats = GetPendingStats();
            bool batchLimit = stats.Batches >= maxBatches;
            bool byteLimit = stats.Bytes >= maxBytes ||
                additionalBytes > maxBytes - stats.Bytes;
            if (batchLimit || byteLimit)
                throw new PendingCapacityException(
                    stats.Batches,
                    stats.Bytes,
                    maxBatches,
                    maxBytes,
                    additionalBytes);
        }

        private void Save(HistoryBatch batch, byte[] data, string area, string directoryName)
        {
            string stagingRoot = Path.Combine(_spoolDirectory, "staging");
            string destinationRoot = Path.Combine(_spoolDirectory, area);
            Directory.CreateDirectory(stagingRoot);
            Directory.CreateDirectory(destinationRoot);
            string temporary = Path.Combine(stagingRoot, batch.BatchId + ".tmp");
            string destination = Path.Combine(destinationRoot, directoryName);
            if (Directory.Exists(temporary) || Directory.Exists(destination))
                throw new IOException("Batch already exists in spool: " + batch.BatchId);

            Directory.CreateDirectory(temporary);
            bool committed = false;
            try
            {
                WriteBytes(Path.Combine(temporary, "data.csv"), data);
                WriteMetadata(Path.Combine(temporary, "meta.ini"), batch, data.Length);
                Directory.Move(temporary, destination);
                committed = true;
            }
            finally
            {
                if (!committed && Directory.Exists(temporary))
                {
                    try { Directory.Delete(temporary, true); }
                    catch { }
                }
            }
        }

        private static string SafeName(string value)
        {
            if (String.IsNullOrEmpty(value))
                return "failed";
            StringBuilder result = new StringBuilder(value.Length);
            int i;
            for (i = 0; i < value.Length; i++)
            {
                char c = value[i];
                result.Append(Char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_');
            }
            return result.ToString();
        }

        private static void WriteBytes(string path, byte[] data)
        {
            using (FileStream stream = new FileStream(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                4096,
                FileOptions.WriteThrough))
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
            }
        }

        private static void WriteMetadata(string path, HistoryBatch batch, int bytes)
        {
            using (FileStream stream = new FileStream(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                4096,
                FileOptions.WriteThrough))
            using (StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(true)))
            {
                writer.WriteLine("[Batch]");
                writer.WriteLine("BatchId=" + batch.BatchId);
                writer.WriteLine("CollectorId=" + batch.CollectorId);
                writer.WriteLine("Mode=" + batch.Mode);
                writer.WriteLine("Sampling=" + batch.Sampling);
                writer.WriteLine("SamplingIntervalSeconds=" +
                    batch.SamplingIntervalSeconds.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("FailedTags=" +
                    batch.FailedTags.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("InvalidSlots=" +
                    batch.InvalidSlots.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("Server=" + batch.Server);
                writer.WriteLine("Start=" + FormatTime(batch.RangeStart));
                writer.WriteLine("End=" + FormatTime(batch.RangeEnd));
                writer.WriteLine("Rows=" + batch.Samples.Count.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("CreatedAt=" + FormatTime(DateTime.Now));
                writer.WriteLine("DataFile=data.csv");
                writer.WriteLine("Sha256=" + batch.Sha256);
                writer.WriteLine("Bytes=" + bytes.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("ReaderStatus=success");
                writer.Flush();
                stream.Flush();
            }
        }

        private static string FormatTime(DateTime value)
        {
            return value.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }
    }
}
