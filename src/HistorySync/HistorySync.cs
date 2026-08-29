using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

namespace DeltaVHistoryCLI
{
    class IniConfig
    {
        private readonly Dictionary<string, string> _values =
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

        public string Required(string section, string key)
        {
            string value = Get(section, key, null);
            if (String.IsNullOrEmpty(value))
                throw new Exception("Missing required config value: [" + section + "] " + key);
            return value;
        }

        public int GetInt(string section, string key, int defaultValue)
        {
            string text = Get(section, key, null);
            int value;
            if (text == null)
                return defaultValue;
            if (!Int32.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
                throw new Exception(
                    "Invalid integer in config: [" + section + "] " + key + "=" + text);
            return value;
        }

        public bool GetBool(string section, string key, bool defaultValue)
        {
            string text = Get(section, key, null);
            if (text == null)
                return defaultValue;
            if (String.Equals(text, "true", StringComparison.OrdinalIgnoreCase) ||
                text == "1" ||
                String.Equals(text, "yes", StringComparison.OrdinalIgnoreCase))
                return true;
            if (String.Equals(text, "false", StringComparison.OrdinalIgnoreCase) ||
                text == "0" ||
                String.Equals(text, "no", StringComparison.OrdinalIgnoreCase))
                return false;
            throw new Exception(
                "Invalid boolean in config: [" + section + "] " + key + "=" + text);
        }
    }

    class SyncOptions
    {
        public string Command;
        public string ConfigPath;
        public string Server;
        public string TagsFile;
        public string SingleTag;
        public string StatePath;
        public string LogsDirectory;
        public string CollectorId;
        public string AckMode;
        public DateTime Start;
        public DateTime End;
        public TimeSpan Slice;
        public int OverlapSeconds;
        public int MinWindowSeconds;
        public int ConnectRetries;
        public int RetrySeconds;
        public int SamplingIntervalSeconds;
        public int MaxFailedTagsPerBatch;
        public int MaxRows;
        public long MaxBytes;
        public int TargetRows;
        public long TargetBytes;
    }

    class CycleMetrics
    {
        public long ConnectMilliseconds;
        public long ResolveTagsMilliseconds;
        public int ValidTags;
        public int InvalidTags;
        public double EstimatedBytesPerRow;
        public TimeSpan EffectiveWindow;
    }

    class SyncLogger : IDisposable
    {
        private readonly object _writeLock = new object();
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
            lock (_writeLock)
            {
                if (_writer == null)
                    return;
                string line = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + " " + text;
                Console.WriteLine(text);
                _writer.WriteLine(line);
            }
        }

        public void Dispose()
        {
            lock (_writeLock)
            {
                if (_writer != null)
                {
                    _writer.Flush();
                    _writer.Close();
                    _writer = null;
                }
            }
        }
    }

    class SyncStopRequestedException : Exception
    {
        public SyncStopRequestedException(string message) : base(message) { }
    }

    class SyncProgram
    {
        private const string Version = "3.4.1";
        private const int InitialCheckpointMinutes = 15;
        private const int CheckpointRetrySeconds = 5;
        private const string MutexName = "Global\\DeltaVHistorySync";
        private const string ContinuousStopEventName =
            "Local\\DcsDataHistorySyncStop";

        private sealed class ContinuousHistorianSession : IDisposable
        {
            private HistorianClient _client;
            private List<TagResult> _tags;
            private string _server;
            private string _singleTag;
            private string _tagsPath;
            private DateTime _tagsLastWriteTimeUtc;
            private long _tagsLength;
            private bool _hasTagsFileSignature;
            private SyncLogger _log;
            private double _bytesPerRowEstimate = 256.0;

            public HistorianClient Client
            {
                get { return _client; }
            }

            public List<TagResult> Tags
            {
                get { return _tags; }
            }

            public double BytesPerRowEstimate
            {
                get { return _bytesPerRowEstimate; }
                set { _bytesPerRowEstimate = value; }
            }

            public bool RequiresResolve(SyncOptions options)
            {
                if (_client == null || _tags == null)
                    return true;
                if (!String.Equals(
                    _server,
                    options.Server,
                    StringComparison.OrdinalIgnoreCase))
                    return true;
                if (!String.Equals(_singleTag, options.SingleTag, StringComparison.Ordinal))
                    return true;
                if (!String.IsNullOrEmpty(options.SingleTag))
                    return false;

                FileInfo tagsFile;
                try
                {
                    if (!File.Exists(options.TagsFile))
                        return true;
                    tagsFile = new FileInfo(options.TagsFile);
                }
                catch
                {
                    return true;
                }
                return !_hasTagsFileSignature ||
                    !String.Equals(
                        _tagsPath,
                        options.TagsFile,
                        StringComparison.OrdinalIgnoreCase) ||
                    _tagsLastWriteTimeUtc != tagsFile.LastWriteTimeUtc ||
                    _tagsLength != tagsFile.Length;
            }

            public void Prepare(
                SyncOptions options,
                List<string> tagNames,
                SyncLogger log,
                WaitHandle stopHandle,
                out long connectMilliseconds,
                out long resolveMilliseconds)
            {
                connectMilliseconds = 0;
                resolveMilliseconds = 0;
                _log = log;

                bool reconnect = _client == null ||
                    !String.Equals(
                        _server,
                        options.Server,
                        StringComparison.OrdinalIgnoreCase);
                if (reconnect)
                {
                    DisposeClient();
                    Stopwatch connectClock = Stopwatch.StartNew();
                    _client = ConnectHistorian(
                        options,
                        delegate(string message)
                        {
                            if (_log != null)
                                _log.Write("Historian " + message);
                        },
                        log,
                        stopHandle);
                    connectClock.Stop();
                    connectMilliseconds = connectClock.ElapsedMilliseconds;
                    _server = options.Server;
                    _tags = null;
                    _hasTagsFileSignature = false;
                }

                if (!RequiresResolve(options))
                    return;
                if (tagNames == null)
                    tagNames = LoadSyncTags(options);

                Stopwatch resolveClock = Stopwatch.StartNew();
                try
                {
                    _tags = _client.ResolveTags(tagNames);
                    CaptureTagsFileSignature(options);
                }
                catch
                {
                    Invalidate();
                    throw;
                }
                finally
                {
                    resolveClock.Stop();
                    resolveMilliseconds = resolveClock.ElapsedMilliseconds;
                }
            }

            public void Invalidate()
            {
                DisposeClient();
                _tags = null;
                _hasTagsFileSignature = false;
            }

            public void Dispose()
            {
                Invalidate();
                _log = null;
            }

            private void CaptureTagsFileSignature(SyncOptions options)
            {
                _singleTag = options.SingleTag;
                _tagsPath = options.TagsFile;
                _hasTagsFileSignature = false;
                if (!String.IsNullOrEmpty(options.SingleTag))
                    return;
                try
                {
                    if (!File.Exists(options.TagsFile))
                        return;
                    FileInfo tagsFile = new FileInfo(options.TagsFile);
                    _tagsLastWriteTimeUtc = tagsFile.LastWriteTimeUtc;
                    _tagsLength = tagsFile.Length;
                    _hasTagsFileSignature = true;
                }
                catch
                {
                }
            }

            private void DisposeClient()
            {
                if (_client != null)
                {
                    try { _client.Dispose(); }
                    catch { }
                    _client = null;
                }
            }
        }

        static int Main(string[] args)
        {
            if (args.Length > 0 &&
                String.Equals(args[0], "stop", StringComparison.OrdinalIgnoreCase))
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
                Console.WriteLine(
                    "HistorySync stop requested; waiting for the current cycle.");
            }
            catch (WaitHandleCannotBeOpenedException)
            {
                Console.WriteLine("No HistorySync continuous host is running.");
                return 31;
            }

            int attempt;
            for (attempt = 0; attempt < 1800; attempt++)
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
            Console.WriteLine("HistorySync stop timed out after 180 seconds.");
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
                    if (args.Length > 0 &&
                        String.Equals(args[0], "run", StringComparison.OrdinalIgnoreCase))
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
            ContinuousHistorianSession historianSession =
                new ContinuousHistorianSession();

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
                ConsoleCancelEventHandler handler =
                    delegate(object sender, ConsoleCancelEventArgs e)
                    {
                        e.Cancel = true;
                        consoleStop.Set();
                        externalStop.Set();
                    };
                Console.CancelKeyPress += handler;
                try
                {
                    int intervalMilliseconds =
                        ReadRunIntervalMilliseconds(args, baseDirectory);
                    DateTime nextStart = DateTime.Now;
                    Console.WriteLine(
                        "HistorySync continuous host started. Press Ctrl+C to stop.");
                    while (WaitHandle.WaitAny(stopHandles, 0, false) ==
                        WaitHandle.WaitTimeout)
                    {
                        int result;
                        try
                        {
                            result = Run(
                                syncArgs,
                                baseDirectory,
                                historianSession,
                                externalStop);
                        }
                        catch (SyncStopRequestedException)
                        {
                            result = 0;
                        }
                        catch (Exception ex)
                        {
                            historianSession.Invalidate();
                            result = 99;
                            Console.WriteLine("Sync cycle failed: " + ex.Message);
                        }
                        Console.WriteLine(
                            "Sync cycle exit code=" +
                            result.ToString(CultureInfo.InvariantCulture));

                        if (result == 41 || result == 42)
                        {
                            Console.WriteLine(
                                "HistorySync continuous host stopped after a permanent Receiver error.");
                            return result;
                        }

                        if (WaitHandle.WaitAny(stopHandles, 0, false) !=
                            WaitHandle.WaitTimeout)
                            break;

                        nextStart = nextStart.AddMilliseconds(intervalMilliseconds);
                        intervalMilliseconds =
                            ReadRunIntervalMilliseconds(args, baseDirectory);
                        if (WaitHandle.WaitAny(
                            stopHandles,
                            CalculateWaitMilliseconds(nextStart, DateTime.Now),
                            false) != WaitHandle.WaitTimeout)
                            break;
                    }
                    Console.WriteLine("HistorySync continuous host stopped.");
                    return 0;
                }
                finally
                {
                    historianSession.Dispose();
                    Console.CancelKeyPress -= handler;
                }
            }
        }

        private static int ReadRunIntervalMilliseconds(
            string[] args,
            string baseDirectory)
        {
            IniConfig config = LoadContinuousConfig(args, baseDirectory);
            int minutes = ParsePositiveInt(
                config.Required("Sync", "IntervalMinutes"),
                "[Sync] IntervalMinutes");
            if (minutes > 1440)
                throw new Exception("[Sync] IntervalMinutes cannot exceed 1440.");
            return checked(minutes * 60 * 1000);
        }

        private static IniConfig LoadContinuousConfig(
            string[] args,
            string baseDirectory)
        {
            string configText = FindOption(args, "--config");
            string configPath = ResolvePath(
                baseDirectory,
                configText == null ? "config.ini" : configText);
            return IniConfig.Load(configPath);
        }

        private static int CalculateWaitMilliseconds(DateTime nextStart, DateTime now)
        {
            double milliseconds = (nextStart - now).TotalMilliseconds;
            if (milliseconds <= 0)
                return 0;
            if (milliseconds >= Int32.MaxValue)
                return Int32.MaxValue;
            return (int)Math.Ceiling(milliseconds);
        }

        private static int Run(string[] args, string baseDirectory)
        {
            return Run(args, baseDirectory, null, null);
        }

        private static int Run(
            string[] args,
            string baseDirectory,
            ContinuousHistorianSession historianSession)
        {
            return Run(args, baseDirectory, historianSession, null);
        }

        private static int Run(
            string[] args,
            string baseDirectory,
            ContinuousHistorianSession historianSession,
            WaitHandle stopHandle)
        {
            if (args.Length == 0 ||
                HasArg(args, "--help") ||
                HasArg(args, "-h") ||
                HasArg(args, "/?"))
            {
                PrintHelp();
                return args.Length == 0 ? 1 : 0;
            }

            if (HasArg(args, "--version"))
            {
                Console.WriteLine("HistorySync " + Version);
                return 0;
            }

            if (HasArg(args, "--no-send"))
                throw new Exception(
                    "--no-send was removed; sync commands always send and wait for database ACK.");

            string command = args[0].ToLowerInvariant();
            if (command != "sync" &&
                command != "init" &&
                command != "backfill" &&
                command != "validate" &&
                command != "status")
                throw new Exception("Unknown command: " + args[0]);

            string configText = FindOption(args, "--config");
            string configPath = ResolvePath(
                baseDirectory,
                configText == null ? "config.ini" : configText);
            IniConfig config = IniConfig.Load(configPath);
            SyncOptions options = ParseOptions(
                args,
                command,
                config,
                configPath,
                baseDirectory);

            using (SyncLogger log = new SyncLogger(options.LogsDirectory))
            {
                log.Write("HistorySync " + Version + " mode=" + command);

                if (command == "validate")
                    return RunValidate(options, log);
                if (command == "status")
                    return RunStatus(options, config);

                if (!config.GetBool("Receiver", "Enabled", true))
                    throw new Exception(
                        "[Receiver] Enabled must be true for " +
                        command +
                        "; collection without sending is no longer supported.");

                SyncStateStore stateStore = null;
                SyncState state = null;
                if (command == "sync")
                {
                    stateStore = new SyncStateStore(options.StatePath);
                    state = stateStore.LoadOrCreate(BuildInitialState(options));
                    try
                    {
                        options.Start = state.CheckpointEnd.AddSeconds(
                            -options.OverlapSeconds);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        throw new InvalidDataException(
                            "CheckpointEnd is too close to DateTime.MinValue for the configured overlap.");
                    }
                    log.Write(
                        "CheckpointEnd=" + FormatTime(state.CheckpointEnd) +
                        " Start=" + FormatTime(options.Start));
                }

                BatchSender sender = new BatchSender(config, log);
                log.Write(
                    "Server=" + options.Server +
                    " Start=" + FormatTime(options.Start) +
                    " End=" + FormatTime(options.End));
                return RunCollection(
                    options,
                    log,
                    sender,
                    state,
                    stateStore,
                    historianSession,
                    stopHandle);
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
            options.Server = OptionOrDefault(
                args,
                "--server",
                config.Required("Historian", "Server"));
            options.SingleTag = FindOption(args, "--tag");
            options.TagsFile = ResolvePath(
                baseDirectory,
                OptionOrDefault(
                    args,
                    "--tags",
                    config.Required("Files", "Tags")));
            options.StatePath = ResolvePath(
                baseDirectory,
                config.Required("Files", "State"));
            options.LogsDirectory = ResolvePath(
                baseDirectory,
                config.Required("Files", "Logs"));
            options.CollectorId = config.Required("Collector", "Id");
            options.ConnectRetries = ParsePositiveInt(
                config.Required("Historian", "ConnectRetries"),
                "[Historian] ConnectRetries");
            options.RetrySeconds = ParseNonNegativeInt(
                config.Required("Historian", "RetrySeconds"),
                "[Historian] RetrySeconds");
            options.AckMode = config.Required("Receiver", "AckMode").ToLowerInvariant();
            if (options.AckMode != "database")
                throw new Exception("[Receiver] AckMode must be database.");

            options.SamplingIntervalSeconds = ParsePositiveInt(
                config.Required("Sampling", "IntervalSeconds"),
                "[Sampling] IntervalSeconds");
            options.MaxFailedTagsPerBatch = config.GetInt(
                "Sampling",
                "MaxFailedTagsPerBatch",
                -1);
            options.MaxRows = config.GetInt("Batch", "MaxRows", -1);
            options.MaxBytes = ParsePositiveLong(
                config.Get("Batch", "MaxBytes", null),
                "[Batch] MaxBytes");
            options.TargetRows = config.GetInt("Batch", "TargetRows", -1);
            options.TargetBytes = ParsePositiveLong(
                config.Get("Batch", "TargetBytes", null),
                "[Batch] TargetBytes");
            options.MinWindowSeconds = config.GetInt("Sync", "MinWindowSeconds", -1);
            if (options.MaxRows <= 0 ||
                options.TargetRows <= 0 ||
                options.TargetRows > options.MaxRows ||
                options.TargetBytes > options.MaxBytes ||
                options.MinWindowSeconds <= 0 ||
                options.MaxFailedTagsPerBatch < 0)
                throw new Exception("Invalid batch, failure, or dynamic window limits.");

            if (command == "validate" || command == "status")
            {
                options.Start = DateTime.Now.AddMinutes(-1);
                options.End = DateTime.Now;
                options.Slice = TimeSpan.FromMinutes(1);
                return options;
            }

            if (command == "sync")
            {
                int endDelay = config.GetInt("Sync", "EndDelaySeconds", -1);
                int overlap = config.GetInt("Sync", "OverlapSeconds", -1);
                int maxWindow = config.GetInt("Sync", "MaxWindowMinutes", -1);
                int intervalMinutes = config.GetInt("Sync", "IntervalMinutes", -1);
                if (endDelay < 0 ||
                    overlap < 0 ||
                    maxWindow <= 0 ||
                    intervalMinutes <= 0)
                    throw new Exception("Invalid continuous sync timing configuration.");

                options.End = DateTime.Now.AddSeconds(-endDelay);
                options.Start = options.End.AddMinutes(
                    -InitialCheckpointMinutes);
                options.Slice = TimeSpan.FromMinutes(maxWindow);
                options.OverlapSeconds = overlap;
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
                    throw new Exception(
                        command + " requires --start and --end, or --last.");
                options.Start = ParseDateTime(startText);
                options.End = ParseDateTime(endText);
            }

            if (options.End <= options.Start)
                throw new Exception("End time must be later than start time.");
            string defaultSlice = command == "init" ? "1d" : "6h";
            options.Slice = ParseDuration(
                OptionOrDefault(args, "--slice", defaultSlice));
            options.OverlapSeconds = 0;
            return options;
        }

        private static int RunValidate(SyncOptions options, SyncLogger log)
        {
            HistorianClient client = null;
            try
            {
                client = ConnectHistorian(options, log, null);
                List<TagResult> tags = client.ResolveTags(LoadSyncTags(options));
                int bad = 0;
                int i;
                for (i = 0; i < tags.Count; i++)
                {
                    log.Write(
                        "Tag " + tags[i].Name +
                        " status=" + tags[i].Status.ToString(
                            CultureInfo.InvariantCulture));
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

        private static HistorianClient ConnectHistorian(
            SyncOptions options,
            SyncLogger log,
            WaitHandle stopHandle)
        {
            return ConnectHistorian(
                options,
                delegate(string message) { log.Write("Historian " + message); },
                log,
                stopHandle);
        }

        private static HistorianClient ConnectHistorian(
            SyncOptions options,
            HistorianLog historianLog,
            SyncLogger log,
            WaitHandle stopHandle)
        {
            Exception last = null;
            int attempt;
            for (attempt = 1; attempt <= options.ConnectRetries; attempt++)
            {
                ThrowIfStopRequested(stopHandle);
                HistorianClient client = new HistorianClient(
                    @"C:\DeltaV",
                    historianLog);
                try
                {
                    client.Connect(options.Server);
                    if (attempt > 1)
                        log.Write(
                            "Historian reconnect succeeded attempt=" +
                            attempt.ToString(CultureInfo.InvariantCulture));
                    return client;
                }
                catch (Exception ex)
                {
                    last = ex;
                    client.Dispose();
                    log.Write(
                        "Historian connect failed attempt=" +
                        attempt.ToString(CultureInfo.InvariantCulture) +
                        "/" +
                        options.ConnectRetries.ToString(CultureInfo.InvariantCulture) +
                        " error=" + ex.Message);
                    if (attempt < options.ConnectRetries && options.RetrySeconds > 0)
                        WaitForStop(
                            stopHandle,
                            checked(options.RetrySeconds * 1000),
                            "Historian reconnect");
                }
            }
            throw new Exception("Historian connection failed after retries.", last);
        }

        private static int RunStatus(SyncOptions options, IniConfig config)
        {
            Console.WriteLine("Historian       " + options.Server);
            bool receiverEnabled = config.GetBool("Receiver", "Enabled", true);
            Console.WriteLine(
                "Receiver        " +
                (receiverEnabled
                    ? (ReceiverOnline(config) ? "Online" : "Offline")
                    : "Disabled"));

            SyncState statusState = null;
            if (File.Exists(options.StatePath))
            {
                statusState = new SyncStateStore(options.StatePath).LoadOrCreate(
                    DateTime.Now.AddMinutes(-InitialCheckpointMinutes));
                Console.WriteLine(
                    "CheckpointEnd   " + FormatTime(statusState.CheckpointEnd));
                DateTime syncEnd = DateTime.Now.AddSeconds(
                    -config.GetInt("Sync", "EndDelaySeconds", 0));
                double lagSeconds = (syncEnd - statusState.CheckpointEnd).TotalSeconds;
                Console.WriteLine("SyncLag         " + FormatLag(lagSeconds));
            }
            else
            {
                Console.WriteLine("CheckpointEnd   Not initialized");
                Console.WriteLine("SyncLag         Not initialized");
            }
            Console.WriteLine("AckMode         database");
            Console.WriteLine("LastError       " + FindLastError(options.LogsDirectory));
            return 0;
        }

        private static bool ReceiverOnline(IniConfig config)
        {
            try
            {
                Uri batchUri = new Uri(config.Required("Receiver", "Url"));
                Uri healthUri = new Uri(batchUri, "/healthz");
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(healthUri);
                request.Method = "GET";
                request.Timeout = 3000;
                request.ReadWriteTimeout = 3000;
                request.KeepAlive = false;
                using (HttpWebResponse response =
                    (HttpWebResponse)request.GetResponse())
                    return response.StatusCode == HttpStatusCode.OK;
            }
            catch
            {
                return false;
            }
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
                try
                {
                    lines = File.ReadAllLines(files[fileIndex], Encoding.UTF8);
                }
                catch
                {
                    continue;
                }
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

        private static int RunCollection(
            SyncOptions options,
            SyncLogger log,
            BatchSender sender,
            SyncState state,
            SyncStateStore stateStore,
            ContinuousHistorianSession historianSession,
            WaitHandle stopHandle)
        {
            if (sender == null)
                throw new ArgumentNullException("sender");

            HistorianClient client = null;
            bool ownsHistorian = historianSession == null;
            BatchPipeline pipeline = null;
            CycleMetrics cycleMetrics = new CycleMetrics();
            double bytesPerRowEstimate = historianSession == null
                ? 256.0
                : historianSession.BytesPerRowEstimate;
            try
            {
                List<TagResult> tags;
                if (historianSession == null)
                {
                    Stopwatch connectClock = Stopwatch.StartNew();
                    client = ConnectHistorian(options, log, stopHandle);
                    connectClock.Stop();
                    cycleMetrics.ConnectMilliseconds =
                        connectClock.ElapsedMilliseconds;

                    List<string> tagNames = LoadSyncTags(options);
                    Stopwatch resolveClock = Stopwatch.StartNew();
                    tags = client.ResolveTags(tagNames);
                    resolveClock.Stop();
                    cycleMetrics.ResolveTagsMilliseconds =
                        resolveClock.ElapsedMilliseconds;
                }
                else
                {
                    List<string> tagNames = historianSession.RequiresResolve(options)
                        ? LoadSyncTags(options)
                        : null;
                    historianSession.Prepare(
                        options,
                        tagNames,
                        log,
                        stopHandle,
                        out cycleMetrics.ConnectMilliseconds,
                        out cycleMetrics.ResolveTagsMilliseconds);
                    client = historianSession.Client;
                    tags = historianSession.Tags;
                }

                int badTags = 0;
                int tagIndex;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                    if (tags[tagIndex].Status != 1)
                        badTags++;
                int validTags = tags.Count - badTags;
                if (validTags <= 0)
                    throw new Exception("No valid Historian tags.");
                cycleMetrics.ValidTags = validTags;
                cycleMetrics.InvalidTags = badTags;

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

                TimeSpan effectiveSlice = CalculateByteAwareSlice(
                    validTags,
                    options.MaxRows,
                    options.TargetRows,
                    options.MaxBytes,
                    options.TargetBytes,
                    bytesPerRowEstimate,
                    options.SamplingIntervalSeconds,
                    options.Slice);
                cycleMetrics.EstimatedBytesPerRow = bytesPerRowEstimate;
                cycleMetrics.EffectiveWindow = effectiveSlice;
                LogCycleMetrics(
                    options,
                    log,
                    collectionStart,
                    collectionEnd,
                    cycleMetrics);

                pipeline = new BatchPipeline(
                    options,
                    sender,
                    state,
                    stateStore,
                    log,
                    stopHandle);
                DateTime sliceStart = collectionStart;
                int batches = 0;
                while (sliceStart < collectionEnd)
                {
                    ThrowIfStopRequested(stopHandle);
                    DateTime sliceEnd = sliceStart.Add(effectiveSlice);
                    if (sliceEnd > collectionEnd)
                        sliceEnd = collectionEnd;
                    sliceEnd = AlignDown(
                        sliceEnd,
                        options.SamplingIntervalSeconds);
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
                        pipeline,
                        ref created,
                        ref bytesPerRowEstimate,
                        stopHandle);
                    if (result != 0 && result != 5)
                        return result;
                    batches += created;
                    sliceStart = sliceEnd;
                }
                pipeline.WaitForAll();
                log.Write(
                    "Completed batches=" +
                    batches.ToString(CultureInfo.InvariantCulture) +
                    " invalidTags=" +
                    badTags.ToString(CultureInfo.InvariantCulture));
                return badTags == 0 ? 0 : 5;
            }
            catch (SyncStopRequestedException ex)
            {
                log.Write("Pipeline stopped: " + ex.Message);
                return 0;
            }
            catch (BatchSendException ex)
            {
                log.Write(
                    (ex.AuthenticationFailure
                        ? "Receiver authentication failed"
                        : "Receiver permanently rejected batch") +
                    " status=" +
                    ex.StatusCode.ToString(CultureInfo.InvariantCulture) +
                    " error=" + ex.Message);
                return ex.AuthenticationFailure ? 42 : 41;
            }
            catch (InvalidDataException ex)
            {
                log.Write("Permanent batch data/protocol error: " + ex.Message);
                return 41;
            }
            catch (BatchLimitException ex)
            {
                log.Write("Batch limit failure: " + ex.Message);
                return 20;
            }
            catch (Exception ex)
            {
                log.Write("Collection pipeline failed: " + ex.Message);
                return 20;
            }
            finally
            {
                if (pipeline != null)
                    pipeline.Dispose();
                if (historianSession != null)
                    historianSession.BytesPerRowEstimate = bytesPerRowEstimate;
                if (historianSession != null &&
                    client != null &&
                    client.LastReadHadErrors)
                    historianSession.Invalidate();
                if (ownsHistorian && client != null)
                    client.Dispose();
            }
        }

        private static PreparedBatch PrepareBatch(
            SyncOptions options,
            DateTime start,
            DateTime end,
            SyncLogger log,
            HistorianClient client,
            List<TagResult> tags,
            ref double bytesPerRowEstimate,
            WaitHandle stopHandle)
        {
            string batchId = BuildBatchId(options.CollectorId);
            log.Write(
                "Prepare batch=" + batchId +
                " range=" + FormatTime(start) +
                " .. " + FormatTime(end));
            Stopwatch totalClock = Stopwatch.StartNew();
            Stopwatch historianClock = Stopwatch.StartNew();
            ThrowIfStopRequested(stopHandle);

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
            List<ProcessedTagResult> processedResults =
                client.ReadProcessedBatch(
                    tags,
                    start,
                    end,
                    options.SamplingIntervalSeconds);
            for (tagIndex = 0; tagIndex < processedResults.Count; tagIndex++)
            {
                ThrowIfStopRequested(stopHandle);
                ProcessedTagResult processedTag = processedResults[tagIndex];
                if (processedTag.Tag == null || processedTag.Tag.Status != 1)
                {
                    invalidTags++;
                    continue;
                }
                if (processedTag.Error != null)
                {
                    failedTags++;
                    log.Write(
                        "Processed tag failed tag=" + processedTag.Tag.Name +
                        " start=" + FormatTime(start) +
                        " end=" + FormatTime(end) +
                        " intervalSeconds=" +
                        options.SamplingIntervalSeconds.ToString(
                            CultureInfo.InvariantCulture) +
                        " error=" + processedTag.Error.Message);
                    continue;
                }

                ProcessedHistoryResult result = processedTag.Result;
                if (result == null)
                {
                    failedTags++;
                    log.Write(
                        "Processed tag returned no result tag=" +
                        processedTag.Tag.Name +
                        " start=" + FormatTime(start) +
                        " end=" + FormatTime(end) +
                        " intervalSeconds=" +
                        options.SamplingIntervalSeconds.ToString(
                            CultureInfo.InvariantCulture));
                    continue;
                }

                successfulTags++;
                invalidSlots += result.InvalidSlots;
                int sampleIndex;
                for (sampleIndex = 0; sampleIndex < result.Samples.Count; sampleIndex++)
                {
                    HistorySample sample = result.Samples[sampleIndex];
                    if (sample.Timestamp < start || sample.Timestamp >= end)
                        continue;
                    batch.Samples.Add(sample);
                }
                if (batch.Samples.Count > options.MaxRows)
                    throw new BatchLimitException("Batch row limit exceeded.");
            }

            historianClock.Stop();
            long historianReadMilliseconds = historianClock.ElapsedMilliseconds;
            HistorianPerformanceMetrics performance = client.LastPerformance;
            batch.HistorianRpcMilliseconds = performance.RpcMilliseconds;
            batch.SampleConvertMilliseconds =
                performance.SampleConvertMilliseconds;
            batch.NormalizeMilliseconds = performance.NormalizeMilliseconds;
            batch.ReturnedSamples = performance.ReturnedSamples;
            batch.InvalidSamples = performance.InvalidSamples;
            batch.NormalizeFastPathTags = performance.NormalizeFastPathTags;
            batch.NormalizeFallbackTags = performance.NormalizeFallbackTags;

            if (successfulTags == 0)
                throw new Exception("All valid Historian tag reads failed.");
            if (failedTags > options.MaxFailedTagsPerBatch)
                throw new Exception(
                    "Failed Historian tag count " +
                    failedTags.ToString(CultureInfo.InvariantCulture) +
                    " exceeds [Sampling] MaxFailedTagsPerBatch=" +
                    options.MaxFailedTagsPerBatch.ToString(
                        CultureInfo.InvariantCulture) +
                    ". The partial batch was rejected.");

            batch.FailedTags = failedTags;
            batch.InvalidSlots = invalidSlots;
            log.Write(
                "Batch prepared=" + batchId +
                " tags=" + tags.Count.ToString(CultureInfo.InvariantCulture) +
                " successTags=" +
                successfulTags.ToString(CultureInfo.InvariantCulture) +
                " failedTags=" +
                failedTags.ToString(CultureInfo.InvariantCulture) +
                " invalidTags=" +
                invalidTags.ToString(CultureInfo.InvariantCulture) +
                " rows=" +
                batch.Samples.Count.ToString(CultureInfo.InvariantCulture) +
                " invalidSlots=" +
                invalidSlots.ToString(CultureInfo.InvariantCulture) +
                " HistorianReadMs=" +
                historianReadMilliseconds.ToString(
                    CultureInfo.InvariantCulture));

            Stopwatch encodeClock = Stopwatch.StartNew();
            BatchPayload payload = BatchEncoder.EncodePayload(
                batch,
                EstimatePayloadCapacity(
                    batch.Samples.Count,
                    bytesPerRowEstimate));
            if (payload.Length > options.MaxBytes)
                throw new BatchLimitException("Batch size limit exceeded.");
            batch.Sha256 = payload.Sha256;
            if (batch.Samples.Count > 0)
            {
                double currentBytesPerRow =
                    (double)payload.Length / batch.Samples.Count;
                bytesPerRowEstimate =
                    bytesPerRowEstimate * 0.8 + currentBytesPerRow * 0.2;
            }
            encodeClock.Stop();

            PreparedBatch prepared = new PreparedBatch();
            prepared.Batch = batch;
            prepared.Payload = payload;
            prepared.RangeStart = start;
            prepared.RangeEnd = end;
            prepared.State = BatchWorkState.Prepared;
            prepared.ResultCode = invalidTags == 0 ? 0 : 5;
            prepared.HistorianReadMilliseconds = historianReadMilliseconds;
            prepared.EncodeMilliseconds = encodeClock.ElapsedMilliseconds;
            prepared.TotalClock = totalClock;
            return prepared;
        }

        internal static void SaveCheckpointWithRetry(
            SyncState state,
            SyncStateStore stateStore,
            DateTime checkpointEnd,
            SyncLogger log,
            WaitHandle stopHandle)
        {
            if (checkpointEnd < state.CheckpointEnd)
                throw new InvalidDataException(
                    "A database ACK moved backwards from the current CheckpointEnd.");
            if (checkpointEnd == state.CheckpointEnd)
                return;

            DateTime before = state.CheckpointEnd;
            while (true)
            {
                ThrowIfStopRequested(stopHandle);
                state.CheckpointEnd = checkpointEnd;
                try
                {
                    stateStore.Save(state);
                    log.Write(
                        "Checkpoint saved after database ACK CheckpointEnd=" +
                        FormatTime(checkpointEnd));
                    return;
                }
                catch (Exception ex)
                {
                    state.CheckpointEnd = before;
                    log.Write(
                        "Checkpoint save failed; retrying same acknowledged batch in " +
                        CheckpointRetrySeconds.ToString(
                            CultureInfo.InvariantCulture) +
                        "s error=" + ex.Message);
                    WaitForStop(
                        stopHandle,
                        CheckpointRetrySeconds * 1000,
                        "checkpoint save");
                }
            }
        }

        internal static void LogBatchMetrics(
            SyncOptions options,
            SyncState state,
            SyncLogger log,
            HistoryBatch batch,
            int bytes,
            Stopwatch totalClock,
            long historianReadMilliseconds,
            long encodeMilliseconds,
            BatchSendTimings sendTimings,
            long sequence,
            int pipelineDepth,
            int inFlight)
        {
            long sendMilliseconds = sendTimings == null
                ? 0
                : sendTimings.SendMilliseconds;
            long ackWaitMilliseconds = sendTimings == null
                ? 0
                : sendTimings.AckWaitMilliseconds;
            int attempts = sendTimings == null ? 0 : sendTimings.Attempts;
            long workingSetBytes = Process.GetCurrentProcess().WorkingSet64;
            long gcMemoryBytes = GC.GetTotalMemory(false);
            double rowsPerSecond = totalClock.Elapsed.TotalSeconds > 0
                ? batch.Samples.Count / totalClock.Elapsed.TotalSeconds
                : 0;
            long syncLagSeconds = 0;
            if (options.Command == "sync" && state != null)
            {
                double lag = (options.End - state.CheckpointEnd).TotalSeconds;
                if (lag > 0)
                    syncLagSeconds = (long)lag;
            }
            log.Write(
                "BatchMetrics batch_id=" + batch.BatchId +
                " rows=" +
                batch.Samples.Count.ToString(CultureInfo.InvariantCulture) +
                " bytes=" + bytes.ToString(CultureInfo.InvariantCulture) +
                " elapsed=" +
                totalClock.ElapsedMilliseconds.ToString(
                    CultureInfo.InvariantCulture) + "ms" +
                " HistorianReadMs=" +
                historianReadMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " EncodeMs=" +
                encodeMilliseconds.ToString(CultureInfo.InvariantCulture) +
                " SendMs=" +
                sendMilliseconds.ToString(CultureInfo.InvariantCulture) +
                " AckWaitMs=" +
                ackWaitMilliseconds.ToString(CultureInfo.InvariantCulture) +
                " TotalMs=" +
                totalClock.ElapsedMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " Attempts=" + attempts.ToString(CultureInfo.InvariantCulture) +
                " Sequence=" + sequence.ToString(CultureInfo.InvariantCulture) +
                " PipelineDepth=" + pipelineDepth.ToString(CultureInfo.InvariantCulture) +
                " InFlight=" + inFlight.ToString(CultureInfo.InvariantCulture) +
                " CheckpointEnd=" +
                (state == null ? "None" : FormatTime(state.CheckpointEnd)) +
                " SyncLagSeconds=" +
                syncLagSeconds.ToString(CultureInfo.InvariantCulture));
            log.Write(
                "Performance batch_id=" + batch.BatchId +
                " RpcMs=" +
                batch.HistorianRpcMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " ConvertMs=" +
                batch.SampleConvertMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " NormalizeMs=" +
                batch.NormalizeMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " ReturnedSamples=" +
                batch.ReturnedSamples.ToString(CultureInfo.InvariantCulture) +
                " InvalidSamples=" +
                batch.InvalidSamples.ToString(CultureInfo.InvariantCulture) +
                " NormalizeFastPathTags=" +
                batch.NormalizeFastPathTags.ToString(
                    CultureInfo.InvariantCulture) +
                " NormalizeFallbackTags=" +
                batch.NormalizeFallbackTags.ToString(
                    CultureInfo.InvariantCulture) +
                " RowsPerSec=" +
                rowsPerSecond.ToString("0.###", CultureInfo.InvariantCulture) +
                " WorkingSetBytes=" +
                workingSetBytes.ToString(CultureInfo.InvariantCulture) +
                " GCMemoryBytes=" +
                gcMemoryBytes.ToString(CultureInfo.InvariantCulture));
        }

        private static void LogCycleMetrics(
            SyncOptions options,
            SyncLogger log,
            DateTime start,
            DateTime end,
            CycleMetrics metrics)
        {
            log.Write(
                "CycleMetrics range=" + FormatTime(start) +
                " .. " + FormatTime(end) +
                " Sampling=InterpolatedValue" +
                " IntervalSeconds=" +
                options.SamplingIntervalSeconds.ToString(
                    CultureInfo.InvariantCulture) +
                " ConnectMs=" +
                metrics.ConnectMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " ResolveTagsMs=" +
                metrics.ResolveTagsMilliseconds.ToString(
                    CultureInfo.InvariantCulture) +
                " ValidTags=" +
                metrics.ValidTags.ToString(CultureInfo.InvariantCulture) +
                " InvalidTags=" +
                metrics.InvalidTags.ToString(CultureInfo.InvariantCulture) +
                " MaxWindowMinutes=" +
                options.Slice.TotalMinutes.ToString(
                    "0.###",
                    CultureInfo.InvariantCulture) +
                " TargetRows=" +
                options.TargetRows.ToString(CultureInfo.InvariantCulture) +
                " TargetBytes=" +
                options.TargetBytes.ToString(
                    CultureInfo.InvariantCulture) +
                " EstimatedBytesPerRow=" +
                metrics.EstimatedBytesPerRow.ToString(
                    "0.###",
                    CultureInfo.InvariantCulture) +
                " EffectiveWindowSeconds=" +
                metrics.EffectiveWindow.TotalSeconds.ToString(
                    "0.###",
                    CultureInfo.InvariantCulture));
        }

        private static int CollectWindow(
            SyncOptions options,
            DateTime start,
            DateTime end,
            SyncLogger log,
            HistorianClient client,
            List<TagResult> tags,
            BatchPipeline pipeline,
            ref int created,
            ref double bytesPerRowEstimate,
            WaitHandle stopHandle)
        {
            pipeline.WaitForCapacity();
            PreparedBatch prepared = null;
            try
            {
                prepared = PrepareBatch(
                    options,
                    start,
                    end,
                    log,
                    client,
                    tags,
                    ref bytesPerRowEstimate,
                    stopHandle);
            }
            catch (BatchLimitException ex)
            {
                log.Write(
                    "Batch limit range=" + FormatTime(start) +
                    " .. " + FormatTime(end) +
                    " error=" + ex.Message);
            }

            if (prepared != null)
            {
                pipeline.Submit(prepared);
                created++;
                return prepared.ResultCode;
            }

            if (end.Subtract(start).TotalSeconds <= options.MinWindowSeconds)
            {
                log.Write(
                    "Minimum dynamic window reached; batch still exceeds limits.");
                return 20;
            }

            DateTime middle = AlignDown(
                start.AddTicks((end.Ticks - start.Ticks) / 2),
                options.SamplingIntervalSeconds);
            if (middle <= start)
                middle = start.AddSeconds(options.SamplingIntervalSeconds);
            if (middle >= end)
            {
                log.Write(
                    "Aligned dynamic split cannot reduce the collection window.");
                return 20;
            }
            log.Write(
                "Dynamic split " + FormatTime(start) +
                " .. " + FormatTime(end) +
                " at " + FormatTime(middle));
            int first = CollectWindow(
                options,
                start,
                middle,
                log,
                client,
                tags,
                pipeline,
                ref created,
                ref bytesPerRowEstimate,
                stopHandle);
            if (first != 0 && first != 5)
                return first;
            int second = CollectWindow(
                options,
                middle,
                end,
                log,
                client,
                tags,
                pipeline,
                ref created,
                ref bytesPerRowEstimate,
                stopHandle);
            if (second != 0 && second != 5)
                return second;
            return first == 5 || second == 5 ? 5 : 0;
        }

        private static List<string> LoadSyncTags(SyncOptions options)
        {
            List<string> result = new List<string>();
            Dictionary<string, bool> seen =
                new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            if (!String.IsNullOrEmpty(options.SingleTag))
            {
                result.Add(options.SingleTag);
                return result;
            }
            if (!File.Exists(options.TagsFile))
                throw new FileNotFoundException(
                    "Tags file not found: " + options.TagsFile);
            using (StreamReader reader =
                new StreamReader(options.TagsFile, Encoding.Default, true))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    line = line.Trim();
                    if (line.Length == 0 ||
                        line.StartsWith("#") ||
                        line.StartsWith(";"))
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
            state.CheckpointEnd = AlignDown(
                options.End.AddMinutes(-InitialCheckpointMinutes),
                options.SamplingIntervalSeconds);
            return state;
        }

        private static void ThrowIfStopRequested(WaitHandle stopHandle)
        {
            if (stopHandle != null && stopHandle.WaitOne(0, false))
                throw new SyncStopRequestedException("Stop requested.");
        }

        private static void WaitForStop(
            WaitHandle stopHandle,
            int milliseconds,
            string operation)
        {
            if (stopHandle == null)
            {
                Thread.Sleep(milliseconds);
                return;
            }
            if (stopHandle.WaitOne(milliseconds, false))
                throw new SyncStopRequestedException(
                    "Stop requested during " + operation + ".");
        }

        private static string BuildBatchId(string collectorId)
        {
            return SafeName(collectorId) + "_" +
                DateTime.UtcNow.ToString("yyyyMMdd_HHmmss_fff") +
                "_" + Guid.NewGuid().ToString("N");
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
                    if (c == invalid[j])
                    {
                        bad = true;
                        break;
                    }
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

        private static string OptionOrDefault(
            string[] args,
            string name,
            string defaultValue)
        {
            string value = FindOption(args, name);
            return value == null ? defaultValue : value;
        }

        private static bool HasArg(string[] args, string name)
        {
            int i;
            for (i = 0; i < args.Length; i++)
                if (String.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
                    return true;
            return false;
        }

        private static int ParsePositiveInt(string text, string name)
        {
            int value;
            if (!Int32.TryParse(
                text,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value) ||
                value <= 0)
                throw new Exception("Invalid " + name + " value: " + text);
            return value;
        }

        private static int ParseNonNegativeInt(string text, string name)
        {
            int value;
            if (!Int32.TryParse(
                text,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value) ||
                value < 0)
                throw new Exception("Invalid " + name + " value: " + text);
            return value;
        }

        private static long ParsePositiveLong(string text, string name)
        {
            long value;
            if (!Int64.TryParse(
                text,
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value) ||
                value <= 0)
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
            if (DateTime.TryParseExact(
                text,
                formats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces,
                out value))
                return value;
            throw new Exception(
                "Invalid date/time: " + text +
                ". Use yyyy-MM-dd HH:mm:ss");
        }

        private static DateTime AlignDown(DateTime value, int intervalSeconds)
        {
            long intervalTicks = TimeSpan.FromSeconds(intervalSeconds).Ticks;
            long alignedTicks = value.Ticks - value.Ticks % intervalTicks;
            return new DateTime(alignedTicks, value.Kind);
        }

        private static TimeSpan CalculateByteAwareSlice(
            int validTags,
            int maxBatchRows,
            int targetBatchRows,
            long maxBatchBytes,
            long targetBatchBytes,
            double estimatedBytesPerRow,
            int intervalSeconds,
            TimeSpan requestedSlice)
        {
            if (validTags <= 0)
                throw new ArgumentOutOfRangeException("validTags");
            if (maxBatchRows <= 0 ||
                targetBatchRows <= 0 ||
                maxBatchBytes <= 0 ||
                targetBatchBytes <= 0 ||
                intervalSeconds <= 0 ||
                estimatedBytesPerRow <= 0)
                throw new ArgumentOutOfRangeException("batch limits");
            if (targetBatchRows > maxBatchRows ||
                targetBatchBytes > maxBatchBytes)
                throw new ArgumentException(
                    "Target batch limits cannot exceed hard batch limits.");
            if (maxBatchRows < validTags)
                throw new Exception(
                    "[Batch] MaxRows is smaller than the number of valid Historian tags.");

            int rowCapacity = Math.Min(maxBatchRows, targetBatchRows);
            double byteRowsDouble = Math.Floor(
                targetBatchBytes / estimatedBytesPerRow);
            long byteRows = byteRowsDouble >= Int64.MaxValue
                ? Int64.MaxValue
                : (long)byteRowsDouble;
            if (byteRows < rowCapacity)
                rowCapacity = byteRows > Int32.MaxValue
                    ? Int32.MaxValue
                    : (int)byteRows;
            if (rowCapacity < validTags)
                rowCapacity = validTags;

            int maximumSlots = rowCapacity / validTags;
            if (maximumSlots <= 0)
                throw new Exception(
                    "[Batch] MaxRows is smaller than the number of valid Historian tags.");
            TimeSpan capacitySlice = TimeSpan.FromSeconds(
                (double)maximumSlots * intervalSeconds);
            TimeSpan result = requestedSlice < capacitySlice
                ? requestedSlice
                : capacitySlice;
            TimeSpan minimum = TimeSpan.FromSeconds(intervalSeconds);
            return result < minimum ? minimum : result;
        }

        private static int EstimatePayloadCapacity(
            int rows,
            double estimatedBytesPerRow)
        {
            if (rows <= 0 || estimatedBytesPerRow <= 0)
                return 0;
            double estimated = rows * estimatedBytesPerRow;
            if (estimated <= 4096)
                return 4096;
            const int maximumInitialCapacity = 16 * 1024 * 1024;
            if (estimated >= maximumInitialCapacity)
                return maximumInitialCapacity;
            return (int)Math.Ceiling(estimated);
        }

        private static TimeSpan ParseDuration(string text)
        {
            if (String.IsNullOrEmpty(text))
                throw new Exception("Duration cannot be empty.");
            text = text.Trim().ToLowerInvariant();
            char unit = text[text.Length - 1];
            string number = text.Substring(0, text.Length - 1);
            if (unit != 'm' && unit != 'h' && unit != 'd')
                throw new Exception(
                    "Invalid duration: " + text + ". Use m, h or d.");
            double value;
            if (!Double.TryParse(
                number,
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out value) ||
                value <= 0)
                throw new Exception("Invalid duration: " + text);
            if (unit == 'd')
                return TimeSpan.FromDays(value);
            if (unit == 'h')
                return TimeSpan.FromHours(value);
            return TimeSpan.FromMinutes(value);
        }

        internal static string FormatTime(DateTime value)
        {
            return value.ToString(
                "yyyy-MM-dd HH:mm:ss.fffffff",
                CultureInfo.InvariantCulture);
        }

        private static string FormatLag(double seconds)
        {
            if (seconds <= 0)
                return "00:00:00";
            long totalSeconds = (long)Math.Floor(seconds);
            long hours = totalSeconds / 3600;
            long minutes = totalSeconds / 60 % 60;
            long remainingSeconds = totalSeconds % 60;
            return hours.ToString("00", CultureInfo.InvariantCulture) +
                ":" + minutes.ToString("00", CultureInfo.InvariantCulture) +
                ":" + remainingSeconds.ToString(
                    "00",
                    CultureInfo.InvariantCulture);
        }

        private static void PrintHelp()
        {
            Console.WriteLine("HistorySync " + Version);
            Console.WriteLine();
            Console.WriteLine("  HistorySync.exe run");
            Console.WriteLine("  HistorySync.exe stop");
            Console.WriteLine("  HistorySync.exe sync");
            Console.WriteLine(
                "  HistorySync.exe init --start \"2026-07-01 00:00:00\" --end \"2026-08-01 00:00:00\" --slice 1d");
            Console.WriteLine(
                "  HistorySync.exe backfill --last 1d --slice 6h");
            Console.WriteLine(
                "  HistorySync.exe backfill --tag \"TI-021007/AI1/PV.CV\" --last 2d --slice 6h");
            Console.WriteLine("  HistorySync.exe validate --tags tags.txt");
            Console.WriteLine("  HistorySync.exe status");
            Console.WriteLine();
            Console.WriteLine(
                "Options: --config --server --tag --tags --start --end --last --slice");
        }
    }
}
