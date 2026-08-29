using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.IO;
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
        public string Sampling;
        public int SamplingIntervalSeconds;
        public int FailedTags;
        public int InvalidSlots;
        public string Server;
        public DateTime RangeStart;
        public DateTime RangeEnd;
        public List<HistorySample> Samples = new List<HistorySample>();
        public string Sha256;
        public long HistorianRpcMilliseconds;
        public long SampleConvertMilliseconds;
        public long NormalizeMilliseconds;
        public int ReturnedSamples;
        public int InvalidSamples;
        public int NormalizeFastPathTags;
        public int NormalizeFallbackTags;
    }

    class BatchPayload
    {
        public byte[] Buffer;
        public int Length;
        public string Sha256;
    }

    static class BatchEncoder
    {
        public static byte[] EncodeCsv(HistoryBatch batch)
        {
            BatchPayload payload = EncodePayload(batch, 0);
            byte[] result = new byte[payload.Length];
            Buffer.BlockCopy(payload.Buffer, 0, result, 0, payload.Length);
            return result;
        }

        public static BatchPayload EncodePayload(HistoryBatch batch, int estimatedCapacity)
        {
            if (batch == null)
                throw new ArgumentNullException("batch");
            if (estimatedCapacity < 0)
                throw new ArgumentOutOfRangeException("estimatedCapacity");

            MemoryStream memory = estimatedCapacity > 0
                ? new MemoryStream(estimatedCapacity)
                : new MemoryStream();
            try
            {
                StreamWriter writer = new StreamWriter(memory, new UTF8Encoding(true));
                try
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
                    int length = checked((int)memory.Length);
                    byte[] buffer = memory.GetBuffer();
                    BatchPayload payload = new BatchPayload();
                    payload.Buffer = buffer;
                    payload.Length = length;
                    payload.Sha256 = ComputeSha256(buffer, length);
                    return payload;
                }
                finally
                {
                    writer.Close();
                }
            }
            finally
            {
                memory.Close();
            }
        }

        public static string ComputeSha256(byte[] data)
        {
            if (data == null)
                throw new ArgumentNullException("data");
            return ComputeSha256(data, data.Length);
        }

        public static string ComputeSha256(byte[] data, int length)
        {
            if (data == null)
                throw new ArgumentNullException("data");
            if (length < 0 || length > data.Length)
                throw new ArgumentOutOfRangeException("length");
            using (SHA256 sha = SHA256.Create())
            {
                byte[] hash = sha.ComputeHash(data, 0, length);
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
}
