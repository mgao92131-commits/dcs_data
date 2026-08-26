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

    class HistoryBatch
    {
        public string BatchId;
        public string CollectorId;
        public string Mode;
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
            string stagingRoot = Path.Combine(_spoolDirectory, "staging");
            string pendingRoot = Path.Combine(_spoolDirectory, "pending");
            Directory.CreateDirectory(stagingRoot);
            Directory.CreateDirectory(pendingRoot);
            string temporary = Path.Combine(stagingRoot, batch.BatchId + ".tmp");
            string pending = Path.Combine(pendingRoot, batch.BatchId);
            if (Directory.Exists(temporary) || Directory.Exists(pending))
                throw new IOException("Batch already exists in spool: " + batch.BatchId);

            Directory.CreateDirectory(temporary);
            bool committed = false;
            try
            {
                WriteBytes(Path.Combine(temporary, "data.csv"), data);
                WriteMetadata(Path.Combine(temporary, "meta.ini"), batch, data.Length);
                Directory.Move(temporary, pending);
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

        private static void WriteBytes(string path, byte[] data)
        {
            using (FileStream stream = new FileStream(
                path,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None))
            {
                stream.Write(data, 0, data.Length);
                stream.Flush();
            }
        }

        private static void WriteMetadata(string path, HistoryBatch batch, int bytes)
        {
            using (StreamWriter writer = new StreamWriter(path, false, new UTF8Encoding(true)))
            {
                writer.WriteLine("[Batch]");
                writer.WriteLine("BatchId=" + batch.BatchId);
                writer.WriteLine("CollectorId=" + batch.CollectorId);
                writer.WriteLine("Mode=" + batch.Mode);
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
            }
        }

        private static string FormatTime(DateTime value)
        {
            return value.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }
    }
}
