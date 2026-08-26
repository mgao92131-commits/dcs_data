using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace DeltaVHistoryCLI
{
    class IniConfig
    {
        private Dictionary<string, string> _values =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public static IniConfig Load(string path)
        {
            IniConfig config = new IniConfig();
            if (!File.Exists(path))
                throw new FileNotFoundException("Config file not found: " + path);

            string section = "";
            using (StreamReader reader = new StreamReader(path, Encoding.Default, true))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    line = line.Trim();
                    if (line.Length == 0 || line.StartsWith("#") || line.StartsWith(";"))
                        continue;

                    if (line.StartsWith("[") && line.EndsWith("]"))
                    {
                        section = line.Substring(1, line.Length - 2).Trim();
                        continue;
                    }

                    int equals = line.IndexOf('=');
                    if (equals <= 0)
                        continue;

                    string key = line.Substring(0, equals).Trim();
                    string value = line.Substring(equals + 1).Trim();
                    config._values[section + "." + key] = value;
                }
            }
            return config;
        }

        public string Get(string section, string key, string defaultValue)
        {
            string value;
            if (_values.TryGetValue(section + "." + key, out value))
                return value;
            return defaultValue;
        }

        public int GetInt(string section, string key, int defaultValue)
        {
            string text = Get(section, key, null);
            int value;
            if (text == null)
                return defaultValue;
            if (!Int32.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                throw new Exception("Invalid integer in config: [" + section + "] " + key + "=" + text);
            return value;
        }

        public bool GetBool(string section, string key, bool defaultValue)
        {
            string text = Get(section, key, null);
            if (text == null)
                return defaultValue;
            if (String.Equals(text, "true", StringComparison.OrdinalIgnoreCase) || text == "1" ||
                String.Equals(text, "yes", StringComparison.OrdinalIgnoreCase))
                return true;
            if (String.Equals(text, "false", StringComparison.OrdinalIgnoreCase) || text == "0" ||
                String.Equals(text, "no", StringComparison.OrdinalIgnoreCase))
                return false;
            throw new Exception("Invalid boolean in config: [" + section + "] " + key + "=" + text);
        }
    }

    class SyncOptions
    {
        public string Command;
        public string ConfigPath;
        public string Server;
        public string TagsFile;
        public string SingleTag;
        public string SpoolDirectory;
        public string CollectorId;
        public DateTime Start;
        public DateTime End;
        public TimeSpan Slice;
        public int MaxSamples;
        public int MaxBatchRows;
        public long MaxBatchBytes;
    }

    class SyncLogger : IDisposable
    {
        private StreamWriter _writer;

        public SyncLogger(string directory)
        {
            Directory.CreateDirectory(directory);
            string path = Path.Combine(
                directory,
                "sync_" + DateTime.Now.ToString("yyyyMMdd") + ".log");
            _writer = new StreamWriter(path, true, new UTF8Encoding(true));
            _writer.AutoFlush = true;
        }

        public void Write(string text)
        {
            string line = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + " " + text;
            Console.WriteLine(text);
            _writer.WriteLine(line);
        }

        public void Dispose()
        {
            if (_writer != null)
            {
                _writer.Flush();
                _writer.Close();
                _writer = null;
            }
        }
    }

    class SyncProgram
    {
        private const string Version = "2.0-refactor";

        static int Main(string[] args)
        {
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            Directory.SetCurrentDirectory(baseDirectory);

            bool created;
            using (Mutex mutex = new Mutex(false, "DeltaVHistorySync_Phase1", out created))
            {
                bool acquired = false;
                try
                {
                    acquired = mutex.WaitOne(0, false);
                }
                catch (AbandonedMutexException)
                {
                    acquired = true;
                }

                if (!acquired)
                {
                    Console.WriteLine("Another HistorySync instance is already running.");
                    return 30;
                }

                try
                {
                    return Run(args, baseDirectory);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("FATAL: " + ex.Message);
                    return 99;
                }
                finally
                {
                    try { mutex.ReleaseMutex(); }
                    catch { }
                }
            }
        }

        private static int Run(string[] args, string baseDirectory)
        {
            if (args.Length == 0 || HasArg(args, "--help") || HasArg(args, "-h") || HasArg(args, "/?"))
            {
                PrintHelp();
                return args.Length == 0 ? 1 : 0;
            }

            if (HasArg(args, "--version"))
            {
                Console.WriteLine("HistorySync " + Version);
                return 0;
            }

            string command = args[0].ToLowerInvariant();
            if (command != "sync" && command != "init" && command != "backfill" &&
                command != "validate" && command != "send")
                throw new Exception("Unknown command: " + args[0]);

            string configText = FindOption(args, "--config");
            string configPath = ResolvePath(baseDirectory, configText == null ? "config.ini" : configText);
            IniConfig config = IniConfig.Load(configPath);

            SyncOptions options = ParseOptions(args, command, config, configPath, baseDirectory);
            string logsDirectory = ResolvePath(baseDirectory, config.Get("Files", "Logs", "logs"));

            using (SyncLogger log = new SyncLogger(logsDirectory))
            {
                log.Write("HistorySync " + Version + " mode=" + command);
                log.Write("Server=" + options.Server + " Start=" + FormatTime(options.Start) + " End=" + FormatTime(options.End));

                if (command == "validate")
                    return RunValidate(options, log);

                PrepareSpool(options.SpoolDirectory, log);
                SpoolMaintenance.Run(config, options.SpoolDirectory, logsDirectory, log);
                if (command == "send")
                    return RunSender(config, options.SpoolDirectory, log);

                bool senderEnabled = config.GetBool("Receiver", "Enabled", false);
                bool sendRequested = senderEnabled && !HasArg(args, "--no-send");
                BatchSender sender = sendRequested
                    ? new BatchSender(config, options.SpoolDirectory, log)
                    : null;
                int senderCode = 0;
                bool directSendAllowed = false;
                if (sender != null)
                {
                    senderCode = sender.SendPending();
                    directSendAllowed = senderCode == 0 &&
                        Directory.GetDirectories(
                            Path.Combine(options.SpoolDirectory, "pending")).Length == 0;
                }

                int collectionCode = RunCollection(
                    options,
                    log,
                    sender,
                    directSendAllowed);
                if (collectionCode != 0 && collectionCode != 5)
                    return collectionCode;
                return senderCode == 0 ? collectionCode : senderCode;
            }
        }

        private static SyncOptions ParseOptions(
            string[] args,
            string command,
            IniConfig config,
            string configPath,
            string baseDirectory)
        {
            SyncOptions options = new SyncOptions();
            options.Command = command;
            options.ConfigPath = configPath;
            options.Server = OptionOrDefault(args, "--server", config.Get("Historian", "Server", "APP"));
            options.SingleTag = FindOption(args, "--tag");

            string tagsText = OptionOrDefault(args, "--tags", config.Get("Files", "Tags", "tags.txt"));
            options.TagsFile = ResolvePath(baseDirectory, tagsText);
            options.SpoolDirectory = ResolvePath(baseDirectory, config.Get("Files", "Spool", "spool"));
            options.CollectorId = config.Get("Collector", "Id", Environment.MachineName);
            options.MaxSamples = ParsePositiveInt(
                OptionOrDefault(args, "--max", config.Get("Sync", "MaxSamples", "10000")),
                "--max");
            options.MaxBatchRows = config.GetInt("Spool", "MaxBatchRows", 50000);
            options.MaxBatchBytes = ParsePositiveLong(
                config.Get("Spool", "MaxBatchBytes", "20971520"),
                "[Spool] MaxBatchBytes");
            if (options.MaxBatchRows <= 0)
                throw new Exception("[Spool] MaxBatchRows must be positive.");

            if (command == "validate" || command == "send")
            {
                options.Start = DateTime.Now.AddMinutes(-1);
                options.End = DateTime.Now;
                options.Slice = TimeSpan.FromMinutes(1);
                return options;
            }

            if (command == "sync")
            {
                int lookback = config.GetInt("Sync", "LookbackMinutes", 15);
                int endDelay = config.GetInt("Sync", "EndDelaySeconds", 30);
                if (lookback <= 0 || endDelay < 0)
                    throw new Exception("LookbackMinutes must be positive and EndDelaySeconds cannot be negative.");

                options.End = DateTime.Now.AddSeconds(-endDelay);
                options.Start = options.End.AddMinutes(-lookback);
                options.Slice = options.End.Subtract(options.Start);
                return options;
            }

            string startText = FindOption(args, "--start");
            string endText = FindOption(args, "--end");
            string lastText = FindOption(args, "--last");

            if (lastText != null)
            {
                options.End = DateTime.Now;
                options.Start = options.End.Subtract(ParseDuration(lastText));
            }
            else
            {
                if (startText == null || endText == null)
                    throw new Exception(command + " requires --start and --end, or --last.");
                options.Start = ParseDateTime(startText);
                options.End = ParseDateTime(endText);
            }

            if (options.End <= options.Start)
                throw new Exception("End time must be later than start time.");

            string defaultSlice = command == "init" ? "1d" : "6h";
            options.Slice = ParseDuration(OptionOrDefault(args, "--slice", defaultSlice));
            return options;
        }

        private static int RunValidate(SyncOptions options, SyncLogger log)
        {
            HistorianClient client = null;
            try
            {
                client = new HistorianClient(
                    @"C:\DeltaV",
                    delegate(string message) { log.Write("Historian " + message); });
                client.Connect(options.Server);
                List<TagResult> tags = client.ResolveTags(LoadSyncTags(options));
                int bad = 0;
                int i;
                for (i = 0; i < tags.Count; i++)
                {
                    log.Write("Tag " + tags[i].Name + " status=" + tags[i].Status.ToString());
                    if (tags[i].Status != 1)
                        bad++;
                }
                return bad == 0 ? 0 : 4;
            }
            finally
            {
                if (client != null)
                    client.Dispose();
            }
        }

        private static int RunSender(IniConfig config, string spoolDirectory, SyncLogger log)
        {
            BatchSender sender = new BatchSender(config, spoolDirectory, log);
            return sender.SendPending();
        }

        private static int RunCollection(
            SyncOptions options,
            SyncLogger log,
            BatchSender sender,
            bool directSendAllowed)
        {
            HistorianClient client = null;
            try
            {
                client = new HistorianClient(
                    @"C:\DeltaV",
                    delegate(string message) { log.Write("Historian " + message); });
                client.Connect(options.Server);
                List<string> tagNames = LoadSyncTags(options);
                List<TagResult> tags = client.ResolveTags(tagNames);
                int badTags = 0;
                int tagIndex;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                    if (tags[tagIndex].Status != 1)
                        badTags++;
                if (badTags == tags.Count)
                    throw new Exception("No valid Historian tags.");

                DateTime sliceStart = options.Start;
                int batches = 0;
                while (sliceStart < options.End)
                {
                    DateTime sliceEnd = sliceStart.Add(options.Slice);
                    if (sliceEnd > options.End)
                        sliceEnd = options.End;

                    int result = CreateBatch(
                        options,
                        sliceStart,
                        sliceEnd,
                        log,
                        client,
                        tags,
                        sender,
                        ref directSendAllowed);
                    if (result != 0 && result != 5)
                        return result;
                    batches++;
                    sliceStart = sliceEnd;
                }
                log.Write("Completed batches=" + batches.ToString() + " invalidTags=" + badTags.ToString());
                return badTags == 0 ? 0 : 5;
            }
            finally
            {
                if (client != null)
                    client.Dispose();
            }
        }

        private static int CreateBatch(
            SyncOptions options,
            DateTime start,
            DateTime end,
            SyncLogger log,
            HistorianClient client,
            List<TagResult> tags,
            BatchSender sender,
            ref bool directSendAllowed)
        {
            string batchId = BuildBatchId(options.CollectorId);
            log.Write("Collect batch=" + batchId + " range=" + FormatTime(start) + " .. " + FormatTime(end));

            try
            {
                HistoryBatch batch = new HistoryBatch();
                batch.BatchId = batchId;
                batch.CollectorId = options.CollectorId;
                batch.Mode = options.Command;
                batch.Server = options.Server;
                batch.RangeStart = start;
                batch.RangeEnd = end;

                int tagIndex;
                int invalidTags = 0;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                {
                    if (tags[tagIndex].Status != 1)
                    {
                        invalidTags++;
                        continue;
                    }
                    List<HistorySample> samples = client.ReadRaw(
                        tags[tagIndex],
                        start,
                        end,
                        options.MaxSamples,
                        true);
                    batch.Samples.AddRange(samples);
                    if (batch.Samples.Count > options.MaxBatchRows)
                        throw new BatchLimitException("Batch row limit exceeded.");
                }

                byte[] data = BatchEncoder.EncodeCsv(batch);
                if (data.Length > options.MaxBatchBytes)
                    throw new BatchLimitException("Batch size limit exceeded.");
                batch.Sha256 = BatchEncoder.ComputeSha256(data);

                if (sender != null && directSendAllowed)
                {
                    try
                    {
                        sender.Send(batch, data);
                        log.Write("Direct ACK batch=" + batchId + " rows=" + batch.Samples.Count.ToString());
                        return invalidTags == 0 ? 0 : 5;
                    }
                    catch (Exception ex)
                    {
                        directSendAllowed = false;
                        log.Write("Direct send failed; switching to outbox batch=" + batchId + " error=" + ex.Message);
                    }
                }

                SpoolStore spool = new SpoolStore(options.SpoolDirectory);
                spool.SavePending(batch, data);
                log.Write("Pending batch=" + batchId + " rows=" + batch.Samples.Count.ToString() + " sha256=" + batch.Sha256);
                return invalidTags == 0 ? 0 : 5;
            }
            catch (Exception ex)
            {
                log.Write("Batch failed=" + batchId + " error=" + ex.Message);
                return 20;
            }
        }

        private static long CombineReaderFiles(
            string readerDirectory,
            string dataPath,
            int maxRows,
            long maxBytes)
        {
            string[] files = Directory.GetFiles(readerDirectory, "*.csv");
            Array.Sort(files, StringComparer.OrdinalIgnoreCase);
            long rows = 0;

            using (StreamWriter writer = new StreamWriter(dataPath, false, new UTF8Encoding(true)))
            {
                writer.WriteLine("Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus");

                int i;
                for (i = 0; i < files.Length; i++)
                {
                    string tag = null;
                    bool inData = false;
                    using (StreamReader reader = new StreamReader(files[i], Encoding.UTF8, true))
                    {
                        string line;
                        while ((line = reader.ReadLine()) != null)
                        {
                            if (line.StartsWith("# Tag="))
                                tag = line.Substring(6);
                            else if (line == "Timestamp,Value,DataType,Flags")
                                inData = true;
                            else if (inData && line.Length > 0)
                            {
                                if (tag == null)
                                    throw new Exception("CSV metadata does not contain a Tag: " + files[i]);
                                string outputLine = Csv(tag) + "," + line + ",\"\",\"\"";
                                if (rows >= maxRows)
                                    throw new Exception(
                                        "Batch row limit exceeded (" + maxRows.ToString() +
                                        "). Use a smaller --slice.");
                                writer.WriteLine(outputLine);
                                rows++;
                                if ((rows % 1000) == 0)
                                {
                                    writer.Flush();
                                    if (new FileInfo(dataPath).Length > maxBytes)
                                        throw new Exception(
                                            "Batch size limit exceeded (" + maxBytes.ToString() +
                                            " bytes). Use a smaller --slice.");
                                }
                            }
                        }
                    }
                }
                writer.Flush();
            }

            if (new FileInfo(dataPath).Length > maxBytes)
                throw new Exception(
                    "Batch size limit exceeded (" + maxBytes.ToString() +
                    " bytes). Use a smaller --slice.");

            return rows;
        }

        private static List<string> LoadSyncTags(SyncOptions options)
        {
            List<string> result = new List<string>();
            Dictionary<string, bool> seen = new Dictionary<string, bool>(
                StringComparer.OrdinalIgnoreCase);
            if (!String.IsNullOrEmpty(options.SingleTag))
            {
                result.Add(options.SingleTag);
                return result;
            }
            if (!File.Exists(options.TagsFile))
                throw new FileNotFoundException("Tags file not found: " + options.TagsFile);
            using (StreamReader reader = new StreamReader(options.TagsFile, Encoding.Default, true))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    line = line.Trim();
                    if (line.Length == 0 || line.StartsWith("#") || line.StartsWith(";"))
                        continue;
                    if (!seen.ContainsKey(line))
                    {
                        seen.Add(line, true);
                        result.Add(line);
                    }
                }
            }
            if (result.Count == 0)
                throw new Exception("No tags were found in " + options.TagsFile);
            return result;
        }

        private static void WriteBatchMetadata(
            string path,
            string batchId,
            SyncOptions options,
            DateTime start,
            DateTime end,
            long rows,
            string checksum,
            string readerStatus)
        {
            using (StreamWriter writer = new StreamWriter(path, false, new UTF8Encoding(true)))
            {
                writer.WriteLine("[Batch]");
                writer.WriteLine("BatchId=" + batchId);
                writer.WriteLine("CollectorId=" + options.CollectorId);
                writer.WriteLine("Mode=" + options.Command);
                writer.WriteLine("Server=" + options.Server);
                writer.WriteLine("Start=" + FormatTime(start));
                writer.WriteLine("End=" + FormatTime(end));
                writer.WriteLine("Rows=" + rows.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("CreatedAt=" + FormatTime(DateTime.Now));
                writer.WriteLine("DataFile=data.csv");
                writer.WriteLine("Sha256=" + checksum);
                writer.WriteLine("ReaderStatus=" + readerStatus);
                writer.Flush();
            }
        }

        private static void PrepareSpool(string spoolDirectory, SyncLogger log)
        {
            string staging = Path.Combine(spoolDirectory, "staging");
            string pending = Path.Combine(spoolDirectory, "pending");
            string failed = Path.Combine(spoolDirectory, "failed");
            string archive = Path.Combine(spoolDirectory, "archive");
            Directory.CreateDirectory(staging);
            Directory.CreateDirectory(pending);
            Directory.CreateDirectory(failed);
            Directory.CreateDirectory(archive);

            string[] leftovers = Directory.GetDirectories(staging, "*.tmp");
            int i;
            for (i = 0; i < leftovers.Length; i++)
            {
                string name = Path.GetFileName(leftovers[i]);
                string destination = Path.Combine(failed, name + "_recovered_" + Guid.NewGuid().ToString("N"));
                Directory.Move(leftovers[i], destination);
                log.Write("Recovered incomplete staging batch to failed: " + name);
            }
        }

        private static void AddTagArguments(List<string> args, SyncOptions options)
        {
            if (!String.IsNullOrEmpty(options.SingleTag))
            {
                args.Add("--tag");
                args.Add(options.SingleTag);
            }
            else
            {
                args.Add("--tags");
                args.Add(options.TagsFile);
            }
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

        private static string BuildBatchId(string collectorId)
        {
            return SafeName(collectorId) + "_" + DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") + "_" + Guid.NewGuid().ToString("N");
        }

        private static string SafeName(string text)
        {
            if (String.IsNullOrEmpty(text))
                return "collector";
            char[] invalid = Path.GetInvalidFileNameChars();
            StringBuilder result = new StringBuilder(text.Length);
            int i;
            for (i = 0; i < text.Length; i++)
            {
                char c = text[i];
                bool bad = false;
                int j;
                for (j = 0; j < invalid.Length; j++)
                {
                    if (c == invalid[j]) { bad = true; break; }
                }
                result.Append(bad ? '_' : c);
            }
            return result.ToString();
        }

        private static string ResolvePath(string baseDirectory, string path)
        {
            if (Path.IsPathRooted(path))
                return Path.GetFullPath(path);
            return Path.GetFullPath(Path.Combine(baseDirectory, path));
        }

        private static string FindOption(string[] args, string name)
        {
            int i;
            for (i = 1; i < args.Length; i++)
            {
                if (String.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
                {
                    if (i + 1 >= args.Length)
                        throw new Exception("Missing value for " + name);
                    return args[i + 1];
                }
            }
            return null;
        }

        private static string OptionOrDefault(string[] args, string name, string defaultValue)
        {
            string value = FindOption(args, name);
            return value == null ? defaultValue : value;
        }

        private static bool HasArg(string[] args, string name)
        {
            int i;
            for (i = 0; i < args.Length; i++)
            {
                if (String.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        private static int ParsePositiveInt(string text, string name)
        {
            int value;
            if (!Int32.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) || value <= 0)
                throw new Exception("Invalid " + name + " value: " + text);
            return value;
        }

        private static long ParsePositiveLong(string text, string name)
        {
            long value;
            if (!Int64.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value) || value <= 0)
                throw new Exception("Invalid " + name + " value: " + text);
            return value;
        }

        private static DateTime ParseDateTime(string text)
        {
            string[] formats = new string[]
            {
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm",
                "yyyy-MM-ddTHH:mm:ss",
                "yyyy/MM/dd HH:mm:ss",
                "yyyy/MM/dd HH:mm",
                "yyyy-MM-dd",
                "yyyy/MM/dd"
            };
            DateTime value;
            if (DateTime.TryParseExact(text, formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out value))
                return value;
            throw new Exception("Invalid date/time: " + text + ". Use yyyy-MM-dd HH:mm:ss");
        }

        private static TimeSpan ParseDuration(string text)
        {
            if (String.IsNullOrEmpty(text))
                throw new Exception("Duration cannot be empty.");
            text = text.Trim().ToLowerInvariant();
            char unit = text[text.Length - 1];
            string number = text.Substring(0, text.Length - 1);
            if (unit != 'm' && unit != 'h' && unit != 'd')
                throw new Exception("Invalid duration: " + text + ". Use m, h or d.");
            double value;
            if (!Double.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out value) || value <= 0)
                throw new Exception("Invalid duration: " + text);
            if (unit == 'd') return TimeSpan.FromDays(value);
            if (unit == 'h') return TimeSpan.FromHours(value);
            return TimeSpan.FromMinutes(value);
        }

        private static string FormatTime(DateTime value)
        {
            return value.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }

        private static string FormatReaderTime(DateTime value)
        {
            return value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }

        private static string Csv(string text)
        {
            if (text == null) text = "";
            return "\"" + text.Replace("\"", "\"\"") + "\"";
        }

        private static void PrintHelp()
        {
            Console.WriteLine("HistorySync " + Version);
            Console.WriteLine();
            Console.WriteLine("  HistorySync.exe sync");
            Console.WriteLine("  HistorySync.exe init --start \"2026-07-01 00:00:00\" --end \"2026-08-01 00:00:00\" --slice 1d");
            Console.WriteLine("  HistorySync.exe backfill --last 1d --slice 6h");
            Console.WriteLine("  HistorySync.exe backfill --tag \"TI-021007/AI1/PV.CV\" --last 2d --slice 6h");
            Console.WriteLine("  HistorySync.exe validate --tags tags.txt");
            Console.WriteLine("  HistorySync.exe send");
            Console.WriteLine();
            Console.WriteLine("Options: --config --server --tag --tags --start --end --last --slice --max --no-send");
        }
    }
}
