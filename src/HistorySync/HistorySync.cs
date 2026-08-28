using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Diagnostics;

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
        public string StatePath;
        public string LogsDirectory;
        public string CollectorId;
        public string AckMode;
        public DateTime Start;
        public DateTime End;
        public TimeSpan Slice;
        public int OverlapSeconds;
        public int MinWindowSeconds;
        public int MaxPendingBatches;
        public long MaxPendingBytes;
        public int ConnectRetries;
        public int RetrySeconds;
        public int SamplingIntervalSeconds;
        public int MaxFailedTagsPerBatch;
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
        private const string Version = "3.0-processed";
        private const string MutexName = "Global\\DeltaVHistorySync";
        private const string ContinuousStopEventName =
            "Local\\DcsDataHistorySyncStop";

        static int Main(string[] args)
        {
            if (args.Length == 1 && String.Equals(
                args[0],
                "stop",
                StringComparison.OrdinalIgnoreCase))
                return SignalContinuousStop();
            return Execute(args);
        }

        private static int SignalContinuousStop()
        {
            try
            {
                using (EventWaitHandle stop =
                    EventWaitHandle.OpenExisting(ContinuousStopEventName))
                {
                    stop.Set();
                }
                Console.WriteLine("HistorySync stop requested; waiting for the current cycle.");
            }
            catch (WaitHandleCannotBeOpenedException)
            {
                Console.WriteLine("No HistorySync continuous host is running.");
                return 31;
            }

            int attempt;
            for (attempt = 0; attempt < 600; attempt++)
            {
                Thread.Sleep(100);
                try
                {
                    using (EventWaitHandle probe =
                        EventWaitHandle.OpenExisting(ContinuousStopEventName))
                    {
                    }
                }
                catch (WaitHandleCannotBeOpenedException)
                {
                    Console.WriteLine("HistorySync continuous host stopped.");
                    return 0;
                }
            }
            Console.WriteLine("HistorySync stop timed out after 60 seconds.");
            return 32;
        }

        internal static int Execute(string[] args)
        {
            return ExecuteWithProcessLock(args);
        }

        private static int ExecuteWithProcessLock(string[] args)
        {
            string baseDirectory = AppDomain.CurrentDomain.BaseDirectory;
            Directory.SetCurrentDirectory(baseDirectory);

            bool created;
            using (Mutex mutex = new Mutex(false, MutexName, out created))
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
                    if (args.Length > 0 && String.Equals(
                        args[0],
                        "run",
                        StringComparison.OrdinalIgnoreCase))
                        return RunContinuous(args, baseDirectory);
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

        private static int RunContinuous(string[] args, string baseDirectory)
        {
            string[] syncArgs = new string[args.Length];
            Array.Copy(args, syncArgs, args.Length);
            syncArgs[0] = "sync";

            bool eventCreated;
            using (ManualResetEvent consoleStop = new ManualResetEvent(false))
            using (EventWaitHandle externalStop = new EventWaitHandle(
                false,
                EventResetMode.ManualReset,
                ContinuousStopEventName,
                out eventCreated))
            {
                externalStop.Reset();
                WaitHandle[] stopHandles = new WaitHandle[] { consoleStop, externalStop };
                ConsoleCancelEventHandler handler = delegate(object sender, ConsoleCancelEventArgs e)
                {
                    e.Cancel = true;
                    consoleStop.Set();
                };
                Console.CancelKeyPress += handler;
                try
                {
                    ReadRunIntervalMilliseconds(args, baseDirectory);
                    Console.WriteLine("HistorySync continuous host started. Press Ctrl+C to stop.");
                    while (WaitHandle.WaitAny(stopHandles, 0, false) == WaitHandle.WaitTimeout)
                    {
                        int result;
                        try
                        {
                            result = Run(syncArgs, baseDirectory);
                        }
                        catch (Exception ex)
                        {
                            result = 99;
                            Console.WriteLine("Sync cycle failed: " + ex.Message);
                        }
                        Console.WriteLine(
                            "Sync cycle exit code=" +
                            result.ToString(CultureInfo.InvariantCulture));
                        if (WaitHandle.WaitAny(
                            stopHandles,
                            ReadRunIntervalMilliseconds(args, baseDirectory),
                            false) != WaitHandle.WaitTimeout)
                            break;
                    }
                    Console.WriteLine("HistorySync continuous host stopped.");
                    return 0;
                }
                finally
                {
                    Console.CancelKeyPress -= handler;
                }
            }
        }

        private static int ReadRunIntervalMilliseconds(string[] args, string baseDirectory)
        {
            string configText = FindOption(args, "--config");
            string configPath = ResolvePath(
                baseDirectory,
                configText == null ? "config.ini" : configText);
            IniConfig config = IniConfig.Load(configPath);
            int minutes = ParsePositiveInt(
                config.Get("Sync", "IntervalMinutes", null),
                "[Sync] IntervalMinutes");
            if (minutes > 1440)
                throw new Exception("[Sync] IntervalMinutes cannot exceed 1440.");
            return checked(minutes * 60 * 1000);
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
                command != "validate" && command != "send" && command != "status")
                throw new Exception("Unknown command: " + args[0]);

            string configText = FindOption(args, "--config");
            string configPath = ResolvePath(baseDirectory, configText == null ? "config.ini" : configText);
            IniConfig config = IniConfig.Load(configPath);

            SyncOptions options = ParseOptions(args, command, config, configPath, baseDirectory);
            string logsDirectory = ResolvePath(baseDirectory, config.Get("Files", "Logs", "logs"));

            using (SyncLogger log = new SyncLogger(logsDirectory))
            {
                log.Write("HistorySync " + Version + " mode=" + command);

                if (command == "validate")
                    return RunValidate(options, log);
                if (command == "status")
                    return RunStatus(options, config);

                PrepareSpool(options.SpoolDirectory, log);
                SpoolMaintenance.Run(config, options.SpoolDirectory, logsDirectory, log);

                SyncStateStore stateStore = null;
                SyncState state = null;
                if (command == "sync")
                {
                    stateStore = new SyncStateStore(options.StatePath);
                    state = stateStore.LoadOrCreate(BuildInitialState(options));
                    ReconcileCollectedFromOutbox(
                        options.SpoolDirectory,
                        state,
                        stateStore,
                        log);
                    options.Start = state.LastCollectedEnd.AddSeconds(-options.OverlapSeconds);
                    log.Write(
                        "Checkpoint collected=" + FormatTime(state.LastCollectedEnd) +
                        " accepted=" + FormatTime(state.LastAcceptedEnd) +
                        " committed=" + FormatTime(state.LastCommittedEnd));
                }
                else if (command == "send" && File.Exists(options.StatePath))
                {
                    stateStore = new SyncStateStore(options.StatePath);
                    state = stateStore.LoadOrCreate(DateTime.Now);
                }

                log.Write("Server=" + options.Server + " Start=" + FormatTime(options.Start) + " End=" + FormatTime(options.End));
                BatchAcknowledged acknowledged = CreateAcknowledgedHandler(
                    options,
                    state,
                    stateStore,
                    log);
                if (command == "send")
                {
                    BatchSender sendOnly = new BatchSender(config, options.SpoolDirectory, log);
                    return sendOnly.SendPending(acknowledged);
                }

                bool senderEnabled = config.GetBool("Receiver", "Enabled", false);
                bool sendRequested = senderEnabled && !HasArg(args, "--no-send");
                BatchSender sender = sendRequested
                    ? new BatchSender(config, options.SpoolDirectory, log)
                    : null;
                int senderCode = 0;
                bool directSendAllowed = false;
                if (sender != null)
                {
                    senderCode = sender.SendPending(acknowledged);
                    directSendAllowed = senderCode == 0 &&
                        Directory.GetDirectories(
                            Path.Combine(options.SpoolDirectory, "pending")).Length == 0;
                    if (senderCode == 42)
                    {
                        log.Write("Collection paused because Receiver authentication failed.");
                        return 42;
                    }
                }

                if (command != "send")
                {
                    try
                    {
                        SpoolMaintenance.EnsureFreeSpace(
                            options.SpoolDirectory,
                            config.GetInt("Maintenance", "MinFreeSpaceMB", 2048));
                    }
                    catch (IOException ex)
                    {
                        log.Write("Collection paused: " + ex.Message);
                        return 43;
                    }
                }

                if (command == "sync" && HasBlockingContinuousFailures(options.SpoolDirectory))
                {
                    log.Write("Collection paused because failed or quarantined continuous batches require attention.");
                    return 44;
                }

                try
                {
                    new SpoolStore(options.SpoolDirectory).EnsurePendingCapacity(
                        options.MaxPendingBatches,
                        options.MaxPendingBytes);
                }
                catch (IOException ex)
                {
                    log.Write("Collection paused: " + ex.Message);
                    return 43;
                }

                int collectionCode = RunCollection(
                    options,
                    log,
                    sender,
                    directSendAllowed,
                    state,
                    stateStore);
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
            options.StatePath = ResolvePath(baseDirectory, config.Get("Files", "State", "state.ini"));
            options.LogsDirectory = ResolvePath(baseDirectory, config.Get("Files", "Logs", "logs"));
            options.CollectorId = config.Get("Collector", "Id", Environment.MachineName);
            options.ConnectRetries = config.GetInt("Historian", "ConnectRetries", 3);
            options.RetrySeconds = config.GetInt("Historian", "RetrySeconds", 10);
            if (options.ConnectRetries <= 0 || options.RetrySeconds < 0)
                throw new Exception("Invalid Historian reconnect configuration.");
            options.AckMode = config.Get("Receiver", "AckMode", "inbox").ToLowerInvariant();
            if (options.AckMode != "inbox" && options.AckMode != "database")
                throw new Exception("[Receiver] AckMode must be inbox or database.");
            options.SamplingIntervalSeconds = ParsePositiveInt(
                config.Get("Sampling", "IntervalSeconds", null),
                "[Sampling] IntervalSeconds");
            options.MaxFailedTagsPerBatch = config.GetInt(
                "Sampling",
                "MaxFailedTagsPerBatch",
                5);
            options.MaxBatchRows = config.GetInt("Spool", "MaxBatchRows", 50000);
            options.MaxBatchBytes = ParsePositiveLong(
                config.Get("Spool", "MaxBatchBytes", "20971520"),
                "[Spool] MaxBatchBytes");
            options.MinWindowSeconds = config.GetInt("Sync", "MinWindowSeconds", 10);
            options.MaxPendingBatches = config.GetInt("Spool", "MaxPendingBatches", 200);
            options.MaxPendingBytes = ParsePositiveLong(
                config.Get("Spool", "MaxPendingBytes", "1073741824"),
                "[Spool] MaxPendingBytes");
            if (options.MaxBatchRows <= 0 || options.MinWindowSeconds <= 0 ||
                options.MaxPendingBatches <= 0 || options.MaxFailedTagsPerBatch < 0)
                throw new Exception("Invalid batch, failure, or dynamic window limits.");

            if (command == "validate" || command == "send" || command == "status")
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
                int overlap = config.GetInt("Sync", "OverlapSeconds", 60);
                int maxWindow = config.GetInt("Sync", "MaxWindowMinutes", 30);
                int minWindow = config.GetInt("Sync", "MinWindowSeconds", 10);
                if (lookback <= 0 || endDelay < 0 || overlap < 0 || maxWindow <= 0 || minWindow <= 0)
                    throw new Exception("Invalid continuous sync timing configuration.");

                options.End = DateTime.Now.AddSeconds(-endDelay);
                options.Start = options.End.AddMinutes(-lookback);
                options.Slice = TimeSpan.FromMinutes(maxWindow);
                options.OverlapSeconds = overlap;
                options.MinWindowSeconds = minWindow;
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
                client = ConnectHistorian(options, log);
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

        private static HistorianClient ConnectHistorian(SyncOptions options, SyncLogger log)
        {
            Exception last = null;
            int attempt;
            for (attempt = 1; attempt <= options.ConnectRetries; attempt++)
            {
                HistorianClient client = new HistorianClient(
                    @"C:\DeltaV",
                    delegate(string message) { log.Write("Historian " + message); });
                try
                {
                    client.Connect(options.Server);
                    if (attempt > 1)
                        log.Write("Historian reconnect succeeded attempt=" + attempt.ToString());
                    return client;
                }
                catch (Exception ex)
                {
                    last = ex;
                    client.Dispose();
                    log.Write(
                        "Historian connect failed attempt=" + attempt.ToString() +
                        "/" + options.ConnectRetries.ToString() + " error=" + ex.Message);
                    if (attempt < options.ConnectRetries && options.RetrySeconds > 0)
                        Thread.Sleep(options.RetrySeconds * 1000);
                }
            }
            throw new Exception("Historian connection failed after retries.", last);
        }

        private static int RunStatus(SyncOptions options, IniConfig config)
        {
            Console.WriteLine("Historian       Not checked");
            Console.WriteLine("Receiver        " + (ReceiverOnline(config) ? "Online" : "Offline"));
            if (File.Exists(options.StatePath))
            {
                SyncState state = new SyncStateStore(options.StatePath).LoadOrCreate(DateTime.Now);
                Console.WriteLine("LastCollected   " + FormatTime(state.LastCollectedEnd));
                Console.WriteLine("LastAccepted    " + FormatTime(state.LastAcceptedEnd));
                Console.WriteLine("LastCommitted   " + FormatTime(state.LastCommittedEnd));
            }
            else
            {
                Console.WriteLine("LastCollected   Not initialized");
                Console.WriteLine("LastAccepted    Not initialized");
                Console.WriteLine("LastCommitted   Not initialized");
            }
            Console.WriteLine("PendingBatches  " + CountDirectories(options.SpoolDirectory, "pending").ToString());
            Console.WriteLine("FailedBatches   " + CountDirectories(options.SpoolDirectory, "failed").ToString());
            Console.WriteLine("Quarantined     " + CountDirectories(options.SpoolDirectory, "quarantine").ToString());
            Console.WriteLine("AckMode         " + options.AckMode);
            Console.WriteLine("LastError       " + FindLastError(options.LogsDirectory));
            return 0;
        }

        private static bool ReceiverOnline(IniConfig config)
        {
            try
            {
                Uri batchUri = new Uri(config.Get("Receiver", "Url", ""));
                Uri healthUri = new Uri(batchUri, "/healthz");
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(healthUri);
                request.Method = "GET";
                request.Timeout = 3000;
                request.KeepAlive = false;
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                    return response.StatusCode == HttpStatusCode.OK;
            }
            catch
            {
                return false;
            }
        }

        private static int CountDirectories(string spoolDirectory, string name)
        {
            string path = Path.Combine(spoolDirectory, name);
            return Directory.Exists(path) ? Directory.GetDirectories(path).Length : 0;
        }

        private static bool HasBlockingContinuousFailures(string spoolDirectory)
        {
            string[] areas = new string[] { "failed", "quarantine" };
            int areaIndex;
            for (areaIndex = 0; areaIndex < areas.Length; areaIndex++)
            {
                string root = Path.Combine(spoolDirectory, areas[areaIndex]);
                if (!Directory.Exists(root))
                    continue;
                string[] directories = Directory.GetDirectories(root);
                int i;
                for (i = 0; i < directories.Length; i++)
                {
                    string metaPath = Path.Combine(directories[i], "meta.ini");
                    if (!File.Exists(metaPath))
                        return true;
                    try
                    {
                        IniConfig meta = IniConfig.Load(metaPath);
                        if (String.Equals(
                            meta.Get("Batch", "Mode", "sync"),
                            "sync",
                            StringComparison.OrdinalIgnoreCase))
                            return true;
                    }
                    catch
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        private static string FindLastError(string logsDirectory)
        {
            if (!Directory.Exists(logsDirectory))
                return "None";
            string[] files = Directory.GetFiles(logsDirectory, "sync_*.log");
            Array.Sort(files, StringComparer.OrdinalIgnoreCase);
            int fileIndex;
            for (fileIndex = files.Length - 1; fileIndex >= 0; fileIndex--)
            {
                string[] lines;
                try { lines = File.ReadAllLines(files[fileIndex], Encoding.UTF8); }
                catch { continue; }
                int lineIndex;
                for (lineIndex = lines.Length - 1; lineIndex >= 0; lineIndex--)
                {
                    string lower = lines[lineIndex].ToLowerInvariant();
                    if (lower.IndexOf("error=") >= 0 ||
                        lower.IndexOf("fatal") >= 0 ||
                        lower.IndexOf(" failed") >= 0)
                        return lines[lineIndex];
                }
            }
            return "None";
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
            bool directSendAllowed,
            SyncState state,
            SyncStateStore stateStore)
        {
            HistorianClient client = null;
            try
            {
                client = ConnectHistorian(options, log);
                List<string> tagNames = LoadSyncTags(options);
                List<TagResult> tags = client.ResolveTags(tagNames);
                int badTags = 0;
                int tagIndex;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                    if (tags[tagIndex].Status != 1)
                        badTags++;
                int validTags = tags.Count - badTags;
                if (validTags <= 0)
                    throw new Exception("No valid Historian tags.");

                DateTime collectionStart = AlignDown(
                    options.Start,
                    options.SamplingIntervalSeconds);
                DateTime collectionEnd = AlignDown(
                    options.End,
                    options.SamplingIntervalSeconds);
                if (collectionEnd <= collectionStart)
                {
                    log.Write("No completed processed interval is available.");
                    return badTags == 0 ? 0 : 5;
                }

                TimeSpan effectiveSlice = CalculateEffectiveSlice(
                    validTags,
                    options.MaxBatchRows,
                    options.SamplingIntervalSeconds,
                    options.Slice);

                log.Write(
                    "Sampling=InterpolatedValue intervalSeconds=" +
                    options.SamplingIntervalSeconds.ToString(CultureInfo.InvariantCulture) +
                    " validTags=" + validTags.ToString(CultureInfo.InvariantCulture) +
                    " maxWindowMinutes=" + options.Slice.TotalMinutes.ToString(
                        "0.###", CultureInfo.InvariantCulture) +
                    " effectiveWindowMinutes=" + effectiveSlice.TotalMinutes.ToString(
                        "0.###", CultureInfo.InvariantCulture));

                DateTime sliceStart = collectionStart;
                int batches = 0;
                while (sliceStart < collectionEnd)
                {
                    DateTime sliceEnd = sliceStart.Add(effectiveSlice);
                    if (sliceEnd > collectionEnd)
                        sliceEnd = collectionEnd;
                    sliceEnd = AlignDown(sliceEnd, options.SamplingIntervalSeconds);
                    if (sliceEnd <= sliceStart)
                        throw new Exception("Processed collection window could not be aligned.");

                    int created = 0;
                    int result = CollectWindow(
                        options,
                        sliceStart,
                        sliceEnd,
                        log,
                        client,
                        tags,
                        sender,
                        ref directSendAllowed,
                        state,
                        stateStore,
                        ref created);
                    if (result != 0 && result != 5)
                        return result;
                    batches += created;
                    sliceStart = sliceEnd;
                }
                log.Write(
                    "Completed batches=" + batches.ToString(CultureInfo.InvariantCulture) +
                    " invalidTags=" + badTags.ToString(CultureInfo.InvariantCulture));
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
            ref bool directSendAllowed,
            SyncState state,
            SyncStateStore stateStore)
        {
            string batchId = BuildBatchId(options.CollectorId);
            log.Write("Collect batch=" + batchId + " range=" + FormatTime(start) + " .. " + FormatTime(end));
            Stopwatch stopwatch = Stopwatch.StartNew();

            try
            {
                HistoryBatch batch = new HistoryBatch();
                batch.BatchId = batchId;
                batch.CollectorId = options.CollectorId;
                batch.Mode = options.Command;
                batch.Sampling = "InterpolatedValue";
                batch.SamplingIntervalSeconds = options.SamplingIntervalSeconds;
                batch.Server = options.Server;
                batch.RangeStart = start;
                batch.RangeEnd = end;

                int tagIndex;
                int invalidTags = 0;
                int successfulTags = 0;
                int failedTags = 0;
                int invalidSlots = 0;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                {
                    if (tags[tagIndex].Status != 1)
                    {
                        invalidTags++;
                        continue;
                    }
                    try
                    {
                        ProcessedHistoryResult result = client.ReadProcessed(
                            tags[tagIndex],
                            start,
                            end,
                            options.SamplingIntervalSeconds);
                        successfulTags++;
                        invalidSlots += result.InvalidSlots;

                        Dictionary<long, bool> seenTimestamps =
                            new Dictionary<long, bool>();
                        int sampleIndex;
                        for (sampleIndex = 0; sampleIndex < result.Samples.Count; sampleIndex++)
                        {
                            HistorySample sample = result.Samples[sampleIndex];
                            if (sample.Timestamp < start || sample.Timestamp >= end)
                                continue;
                            if (seenTimestamps.ContainsKey(sample.Timestamp.Ticks))
                                continue;
                            seenTimestamps.Add(sample.Timestamp.Ticks, true);
                            batch.Samples.Add(sample);
                        }
                    }
                    catch (Exception ex)
                    {
                        failedTags++;
                        log.Write(
                            "Processed tag failed tag=" + tags[tagIndex].Name +
                            " start=" + FormatTime(start) +
                            " end=" + FormatTime(end) +
                            " intervalSeconds=" +
                            options.SamplingIntervalSeconds.ToString(CultureInfo.InvariantCulture) +
                            " error=" + ex.Message);
                        continue;
                    }
                    if (batch.Samples.Count > options.MaxBatchRows)
                        throw new BatchLimitException("Batch row limit exceeded.");
                }

                if (successfulTags == 0)
                    throw new Exception("All valid Historian tag reads failed.");
                if (failedTags > options.MaxFailedTagsPerBatch)
                    throw new Exception(
                        "Failed Historian tag count " +
                        failedTags.ToString(CultureInfo.InvariantCulture) +
                        " exceeds [Sampling] MaxFailedTagsPerBatch=" +
                        options.MaxFailedTagsPerBatch.ToString(CultureInfo.InvariantCulture) +
                        ". The partial batch was rejected.");

                batch.FailedTags = failedTags;
                batch.InvalidSlots = invalidSlots;

                stopwatch.Stop();
                log.Write(
                    "Batch collection completed batch=" + batchId +
                    " tags=" + tags.Count.ToString(CultureInfo.InvariantCulture) +
                    " successTags=" + successfulTags.ToString(CultureInfo.InvariantCulture) +
                    " failedTags=" + failedTags.ToString(CultureInfo.InvariantCulture) +
                    " invalidTags=" + invalidTags.ToString(CultureInfo.InvariantCulture) +
                    " rows=" + batch.Samples.Count.ToString(CultureInfo.InvariantCulture) +
                    " invalidSlots=" + invalidSlots.ToString(CultureInfo.InvariantCulture) +
                    " elapsedMs=" + stopwatch.ElapsedMilliseconds.ToString(
                        CultureInfo.InvariantCulture));

                byte[] data = BatchEncoder.EncodeCsv(batch);
                if (data.Length > options.MaxBatchBytes)
                    throw new BatchLimitException("Batch size limit exceeded.");
                batch.Sha256 = BatchEncoder.ComputeSha256(data);

                if (sender != null && directSendAllowed)
                {
                    try
                    {
                        BatchReceipt receipt = sender.Send(batch, data);
                        AdvanceAfterCollection(
                            options,
                            state,
                            stateStore,
                            end,
                            true,
                            String.Equals(receipt.CommitLevel, "database", StringComparison.OrdinalIgnoreCase));
                        log.Write("Direct ACK batch=" + batchId + " rows=" + batch.Samples.Count.ToString());
                        return invalidTags == 0 ? 0 : 5;
                    }
                    catch (BatchSendException ex)
                    {
                        directSendAllowed = false;
                        SpoolStore rejectedStore = new SpoolStore(options.SpoolDirectory);
                        if (ex.Permanent)
                        {
                            rejectedStore.SaveFailed(
                                batch,
                                data,
                                "http" + ex.StatusCode.ToString(CultureInfo.InvariantCulture));
                            AdvanceAfterCollection(options, state, stateStore, end, false, false);
                            log.Write("Receiver permanently rejected batch=" + batchId + " error=" + ex.Message);
                            return 41;
                        }
                        rejectedStore.EnsurePendingCapacity(
                            options.MaxPendingBatches,
                            options.MaxPendingBytes);
                        rejectedStore.SavePending(batch, data);
                        AdvanceAfterCollection(options, state, stateStore, end, false, false);
                        log.Write("Direct send failed; saved to outbox batch=" + batchId + " error=" + ex.Message);
                        return ex.AuthenticationFailure ? 42 : 0;
                    }
                    catch (Exception ex)
                    {
                        directSendAllowed = false;
                        log.Write("Direct send failed; switching to outbox batch=" + batchId + " error=" + ex.Message);
                    }
                }

                SpoolStore spool = new SpoolStore(options.SpoolDirectory);
                spool.EnsurePendingCapacity(
                    options.MaxPendingBatches,
                    options.MaxPendingBytes);
                spool.SavePending(batch, data);
                AdvanceAfterCollection(
                    options,
                    state,
                    stateStore,
                    end,
                    false,
                    false);
                log.Write("Pending batch=" + batchId + " rows=" + batch.Samples.Count.ToString() + " sha256=" + batch.Sha256);
                return invalidTags == 0 ? 0 : 5;
            }
            catch (BatchLimitException ex)
            {
                log.Write("Batch limit batch=" + batchId + " error=" + ex.Message);
                return 21;
            }
            catch (IOException ex)
            {
                log.Write("Outbox unavailable batch=" + batchId + " error=" + ex.Message);
                return 43;
            }
            catch (Exception ex)
            {
                log.Write("Batch failed=" + batchId + " error=" + ex.Message);
                return 20;
            }
        }

        private static int CollectWindow(
            SyncOptions options,
            DateTime start,
            DateTime end,
            SyncLogger log,
            HistorianClient client,
            List<TagResult> tags,
            BatchSender sender,
            ref bool directSendAllowed,
            SyncState state,
            SyncStateStore stateStore,
            ref int created)
        {
            int result = CreateBatch(
                options,
                start,
                end,
                log,
                client,
                tags,
                sender,
                ref directSendAllowed,
                state,
                stateStore);
            if (result != 21)
            {
                if (result == 0 || result == 5)
                    created++;
                return result;
            }

            if (end.Subtract(start).TotalSeconds <= options.MinWindowSeconds)
            {
                log.Write("Minimum dynamic window reached; batch still exceeds limits.");
                return 20;
            }

            DateTime middle = AlignDown(
                start.AddTicks((end.Ticks - start.Ticks) / 2),
                options.SamplingIntervalSeconds);
            if (middle <= start)
                middle = start.AddSeconds(options.SamplingIntervalSeconds);
            if (middle >= end)
            {
                log.Write("Aligned dynamic split cannot reduce the collection window.");
                return 20;
            }
            log.Write(
                "Dynamic split " + FormatTime(start) + " .. " + FormatTime(end) +
                " at " + FormatTime(middle));
            int first = CollectWindow(
                options, start, middle, log, client, tags, sender,
                ref directSendAllowed, state, stateStore, ref created);
            if (first != 0 && first != 5)
                return first;
            int second = CollectWindow(
                options, middle, end, log, client, tags, sender,
                ref directSendAllowed, state, stateStore, ref created);
            if (second != 0 && second != 5)
                return second;
            return first == 5 || second == 5 ? 5 : 0;
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

        private static SyncState BuildInitialState(SyncOptions options)
        {
            SyncState state = new SyncState();
            DateTime baseline = options.Start;
            DateTime collected = baseline;
            string[] areas = new string[] { "pending", "failed" };
            int areaIndex;
            for (areaIndex = 0; areaIndex < areas.Length; areaIndex++)
            {
                string root = Path.Combine(options.SpoolDirectory, areas[areaIndex]);
                string[] directories = Directory.Exists(root)
                    ? Directory.GetDirectories(root)
                    : new string[0];
                int i;
                for (i = 0; i < directories.Length; i++)
                {
                    string metaPath = Path.Combine(directories[i], "meta.ini");
                    if (!File.Exists(metaPath))
                        continue;
                    try
                    {
                        IniConfig meta = IniConfig.Load(metaPath);
                        if (!String.Equals(
                            meta.Get("Batch", "Mode", ""),
                            "sync",
                            StringComparison.OrdinalIgnoreCase))
                            continue;
                        DateTime start = ParseCheckpointTime(meta.Get("Batch", "Start", ""));
                        DateTime end = ParseCheckpointTime(meta.Get("Batch", "End", ""));
                        if (start < baseline)
                            baseline = start;
                        if (end > collected)
                            collected = end;
                    }
                    catch
                    {
                        // Sender will quarantine malformed durable batches after startup.
                    }
                }
            }
            state.LastCollectedEnd = collected;
            state.LastAcceptedEnd = baseline;
            state.LastCommittedEnd = baseline;
            return state;
        }

        private static void ReconcileCollectedFromOutbox(
            string spoolDirectory,
            SyncState state,
            SyncStateStore stateStore,
            SyncLogger log)
        {
            DateTime recovered = state.LastCollectedEnd;
            string[] areas = new string[] { "pending", "failed" };
            int areaIndex;
            for (areaIndex = 0; areaIndex < areas.Length; areaIndex++)
            {
                string root = Path.Combine(spoolDirectory, areas[areaIndex]);
                if (!Directory.Exists(root))
                    continue;
                string[] directories = Directory.GetDirectories(root);
                int i;
                for (i = 0; i < directories.Length; i++)
                {
                    string metaPath = Path.Combine(directories[i], "meta.ini");
                    if (!File.Exists(metaPath))
                        continue;
                    try
                    {
                        IniConfig meta = IniConfig.Load(metaPath);
                        if (!String.Equals(
                            meta.Get("Batch", "Mode", ""),
                            "sync",
                            StringComparison.OrdinalIgnoreCase))
                            continue;
                        DateTime end = ParseCheckpointTime(meta.Get("Batch", "End", ""));
                        if (end > recovered)
                            recovered = end;
                    }
                    catch (Exception ex)
                    {
                        log.Write(
                            "Cannot reconcile outbox checkpoint directory=" +
                            directories[i] + " error=" + ex.Message);
                    }
                }
            }

            if (recovered <= state.LastCollectedEnd)
                return;

            SyncState before = state.Copy();
            try
            {
                state.LastCollectedEnd = recovered;
                stateStore.Save(state);
                log.Write("Recovered LastCollectedEnd from durable outbox=" + FormatTime(recovered));
            }
            catch
            {
                state.LastCollectedEnd = before.LastCollectedEnd;
                state.LastAcceptedEnd = before.LastAcceptedEnd;
                state.LastCommittedEnd = before.LastCommittedEnd;
                throw;
            }
        }

        private static BatchAcknowledged CreateAcknowledgedHandler(
            SyncOptions options,
            SyncState state,
            SyncStateStore stateStore,
            SyncLogger log)
        {
            if (state == null || stateStore == null)
                return null;
            return delegate(BatchReceipt receipt)
            {
                if (!String.Equals(receipt.Mode, "sync", StringComparison.OrdinalIgnoreCase))
                    return;
                if (receipt.RangeEnd <= state.LastAcceptedEnd)
                    return;
                if (receipt.RangeStart > state.LastAcceptedEnd)
                    throw new InvalidDataException(
                        "Outbox gap detected before batch " + receipt.BatchId +
                        ": expected start at or before " + FormatTime(state.LastAcceptedEnd));
                if (receipt.RangeEnd > state.LastCollectedEnd)
                    throw new InvalidDataException(
                        "Acknowledged batch exceeds LastCollectedEnd: " + receipt.BatchId);

                SyncState before = state.Copy();
                try
                {
                    state.LastAcceptedEnd = receipt.RangeEnd;
                    if (String.Equals(receipt.CommitLevel, "database", StringComparison.OrdinalIgnoreCase))
                        state.LastCommittedEnd = receipt.RangeEnd;
                    stateStore.Save(state);
                    log.Write(
                        "Checkpoint ACK accepted=" + FormatTime(state.LastAcceptedEnd) +
                        " committed=" + FormatTime(state.LastCommittedEnd));
                }
                catch
                {
                    state.LastCollectedEnd = before.LastCollectedEnd;
                    state.LastAcceptedEnd = before.LastAcceptedEnd;
                    state.LastCommittedEnd = before.LastCommittedEnd;
                    throw;
                }
            };
        }

        private static void AdvanceAfterCollection(
            SyncOptions options,
            SyncState state,
            SyncStateStore stateStore,
            DateTime end,
            bool acknowledged,
            bool databaseCommitted)
        {
            if (state == null || stateStore == null || options.Command != "sync")
                return;
            SyncState before = state.Copy();
            try
            {
                if (end > state.LastCollectedEnd)
                    state.LastCollectedEnd = end;
                if (acknowledged && end > state.LastAcceptedEnd)
                    state.LastAcceptedEnd = end;
                if (acknowledged && databaseCommitted && end > state.LastCommittedEnd)
                    state.LastCommittedEnd = end;
                stateStore.Save(state);
            }
            catch
            {
                state.LastCollectedEnd = before.LastCollectedEnd;
                state.LastAcceptedEnd = before.LastAcceptedEnd;
                state.LastCommittedEnd = before.LastCommittedEnd;
                throw;
            }
        }

        private static DateTime ParseCheckpointTime(string text)
        {
            DateTime value;
            if (!DateTime.TryParseExact(
                text,
                "yyyy-MM-dd HH:mm:ss.fffffff",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out value))
                throw new InvalidDataException("Invalid batch checkpoint time: " + text);
            return value;
        }

        private static void PrepareSpool(string spoolDirectory, SyncLogger log)
        {
            string staging = Path.Combine(spoolDirectory, "staging");
            string pending = Path.Combine(spoolDirectory, "pending");
            string failed = Path.Combine(spoolDirectory, "failed");
            string quarantine = Path.Combine(spoolDirectory, "quarantine");
            Directory.CreateDirectory(staging);
            Directory.CreateDirectory(pending);
            Directory.CreateDirectory(failed);
            Directory.CreateDirectory(quarantine);

            string[] leftovers = Directory.GetDirectories(staging, "*.tmp");
            int i;
            for (i = 0; i < leftovers.Length; i++)
            {
                string name = Path.GetFileName(leftovers[i]);
                string destination = Path.Combine(quarantine, name + "_recovered_" + Guid.NewGuid().ToString("N"));
                Directory.Move(leftovers[i], destination);
                log.Write("Recovered incomplete staging batch to quarantine: " + name);
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

        private static DateTime AlignDown(DateTime value, int intervalSeconds)
        {
            long intervalTicks = TimeSpan.FromSeconds(intervalSeconds).Ticks;
            long alignedTicks = value.Ticks - (value.Ticks % intervalTicks);
            return new DateTime(alignedTicks, value.Kind);
        }

        private static TimeSpan CalculateEffectiveSlice(
            int validTags,
            int maxBatchRows,
            int intervalSeconds,
            TimeSpan requestedSlice)
        {
            int maximumSlots = maxBatchRows / validTags;
            if (maximumSlots <= 0)
                throw new Exception(
                    "MaxBatchRows is smaller than the number of valid Historian tags.");
            TimeSpan capacitySlice = TimeSpan.FromSeconds(
                (double)maximumSlots * intervalSeconds);
            TimeSpan result = requestedSlice < capacitySlice
                ? requestedSlice
                : capacitySlice;
            TimeSpan minimum = TimeSpan.FromSeconds(intervalSeconds);
            return result < minimum ? minimum : result;
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

        private static void PrintHelp()
        {
            Console.WriteLine("HistorySync " + Version);
            Console.WriteLine();
            Console.WriteLine("  HistorySync.exe run");
            Console.WriteLine("  HistorySync.exe stop");
            Console.WriteLine("  HistorySync.exe sync");
            Console.WriteLine("  HistorySync.exe init --start \"2026-07-01 00:00:00\" --end \"2026-08-01 00:00:00\" --slice 1d");
            Console.WriteLine("  HistorySync.exe backfill --last 1d --slice 6h");
            Console.WriteLine("  HistorySync.exe backfill --tag \"TI-021007/AI1/PV.CV\" --last 2d --slice 6h");
            Console.WriteLine("  HistorySync.exe validate --tags tags.txt");
            Console.WriteLine("  HistorySync.exe send");
            Console.WriteLine("Options: --config --server --tag --tags --start --end --last --slice --no-send");
        }
    }
}
