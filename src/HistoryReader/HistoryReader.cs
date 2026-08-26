using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;

namespace DeltaVHistoryCLI
{
    class Options
    {
        public string Server = "APP";
        public string Tag = null;
        public string TagsFile = null;

        public DateTime Start;
        public DateTime End;

        public int MaxSamples = 10000;
        public string OutputDirectory = "export";

        public bool ValidateOnly = false;
        public bool AutoSplit = true;
    }

    class TagInfo
    {
        public string Name;
        public int Handle;
        public int Status;
    }

    class SampleRow
    {
        public DateTime Timestamp;
        public string Value;
        public string DataType;
        public string Flags;
    }

    class RawSegment
    {
        public List<SampleRow> Rows = new List<SampleRow>();
        public bool Truncated = false;
    }

    class Program
    {
        private const string Version = "1.1";

        private static string _deltaVRoot = @"C:\DeltaV";
        private static string _dvchAssemblyPath = null;
        private static string _assemblyDir = null;
        private static StreamWriter _log = null;

        static int Main(string[] args)
        {
            return Execute(args);
        }

        internal static int Execute(string[] args)
        {
            try
            {
                _log = new StreamWriter(
                    "HistoryReader.log",
                    true,
                    Encoding.UTF8);

                _log.AutoFlush = true;

                Log("============================================================");
                Log("DeltaV History CLI v" + Version +
                    " started " +
                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                AppDomain.CurrentDomain.AssemblyResolve +=
                    new ResolveEventHandler(CurrentDomain_AssemblyResolve);

                if (args.Length == 0)
                {
                    PrintHelp();
                    return 1;
                }

                if (HasArg(args, "--help") ||
                    HasArg(args, "-h") ||
                    HasArg(args, "/?"))
                {
                    PrintHelp();
                    return 0;
                }

                if (HasArg(args, "--version"))
                {
                    Console.WriteLine(
                        "DeltaV History CLI v" + Version);
                    return 0;
                }

                if (args.Length == 1 &&
                    String.Equals(
                        args[0],
                        "--probe",
                        StringComparison.OrdinalIgnoreCase))
                {
                    return Probe();
                }

                Options opt = ParseOptions(args);
                return Run(opt);
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    "FATAL: " + ex.Message);

                LogException("FATAL", ex);
                return 99;
            }
            finally
            {
                if (_log != null)
                {
                    _log.Flush();
                    _log.Close();
                }
            }
        }

        private static Options ParseOptions(string[] args)
        {
            Options opt = new Options();

            int i = 0;

            if (String.Equals(
                args[0],
                "export",
                StringComparison.OrdinalIgnoreCase))
            {
                i = 1;
            }
            else if (String.Equals(
                args[0],
                "validate",
                StringComparison.OrdinalIgnoreCase))
            {
                opt.ValidateOnly = true;
                i = 1;
            }

            string startText = null;
            string endText = null;
            string lastText = null;

            while (i < args.Length)
            {
                string a = args[i];

                if (String.Equals(
                    a,
                    "--server",
                    StringComparison.OrdinalIgnoreCase))
                {
                    opt.Server =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--tag",
                    StringComparison.OrdinalIgnoreCase))
                {
                    opt.Tag =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--tags",
                    StringComparison.OrdinalIgnoreCase))
                {
                    opt.TagsFile =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--start",
                    StringComparison.OrdinalIgnoreCase))
                {
                    startText =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--end",
                    StringComparison.OrdinalIgnoreCase))
                {
                    endText =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--last",
                    StringComparison.OrdinalIgnoreCase))
                {
                    lastText =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--out-dir",
                    StringComparison.OrdinalIgnoreCase))
                {
                    opt.OutputDirectory =
                        RequireValue(args, ref i, a);
                }
                else if (String.Equals(
                    a,
                    "--max",
                    StringComparison.OrdinalIgnoreCase))
                {
                    string s =
                        RequireValue(args, ref i, a);

                    if (!Int32.TryParse(
                        s,
                        out opt.MaxSamples) ||
                        opt.MaxSamples <= 0)
                    {
                        throw new Exception(
                            "--max must be a positive integer.");
                    }
                }
                else if (String.Equals(
                    a,
                    "--no-auto-split",
                    StringComparison.OrdinalIgnoreCase))
                {
                    opt.AutoSplit = false;
                }
                else
                {
                    throw new Exception(
                        "Unknown argument: " + a);
                }

                i++;
            }

            if (String.IsNullOrEmpty(opt.Tag) &&
                String.IsNullOrEmpty(opt.TagsFile))
            {
                throw new Exception(
                    "Specify --tag or --tags.");
            }

            if (!String.IsNullOrEmpty(lastText))
            {
                if (!String.IsNullOrEmpty(startText) ||
                    !String.IsNullOrEmpty(endText))
                {
                    throw new Exception(
                        "--last cannot be combined with --start/--end.");
                }

                TimeSpan duration =
                    ParseDuration(lastText);

                opt.End = DateTime.Now;
                opt.Start =
                    opt.End.Subtract(duration);
            }
            else if (!String.IsNullOrEmpty(startText) ||
                     !String.IsNullOrEmpty(endText))
            {
                if (String.IsNullOrEmpty(startText) ||
                    String.IsNullOrEmpty(endText))
                {
                    throw new Exception(
                        "--start and --end must be used together.");
                }

                opt.Start =
                    ParseDateTime(startText);

                opt.End =
                    ParseDateTime(endText);
            }
            else
            {
                opt.End = DateTime.Now;
                opt.Start =
                    opt.End.AddHours(-1);
            }

            if (opt.Start >= opt.End)
            {
                throw new Exception(
                    "Start time must be earlier than end time.");
            }

            return opt;
        }

        private static int Run(Options opt)
        {
            List<string> tagNames =
                LoadTags(opt.Tag, opt.TagsFile);

            Console.WriteLine(
                "DeltaV History CLI v" + Version);

            Console.WriteLine(
                "Server      : " + opt.Server);

            Console.WriteLine(
                "Tags        : " +
                tagNames.Count.ToString());

            Console.WriteLine(
                "Start       : " +
                opt.Start.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            Console.WriteLine(
                "End         : " +
                opt.End.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            Console.WriteLine(
                "Output dir  : " +
                Path.GetFullPath(
                    opt.OutputDirectory));

            Console.WriteLine(
                "Max/read    : " +
                opt.MaxSamples.ToString());

            Console.WriteLine(
                "Auto split  : " +
                opt.AutoSplit.ToString());

            HistorianClient client = null;

            try
            {
                client = new HistorianClient(
                    _deltaVRoot,
                    delegate(string message) { Log(message); });
                client.Connect(opt.Server);

                Console.WriteLine(
                    "Connection  : OK (handle " +
                    client.ConnectionHandle.ToString() + ")");

                List<TagResult> coreTags = client.ResolveTags(tagNames);
                List<TagInfo> tags = new List<TagInfo>();
                int coreIndex;
                for (coreIndex = 0; coreIndex < coreTags.Count; coreIndex++)
                {
                    TagInfo legacyTag = new TagInfo();
                    legacyTag.Name = coreTags[coreIndex].Name;
                    legacyTag.Handle = coreTags[coreIndex].Handle;
                    legacyTag.Status = coreTags[coreIndex].Status;
                    tags.Add(legacyTag);
                }

                int ok = 0;
                int bad = 0;
                int t;

                Console.WriteLine();

                for (t = 0; t < tags.Count; t++)
                {
                    TagInfo ti = tags[t];

                    Console.WriteLine(
                        "[" +
                        TagStatusText(ti.Status) +
                        "] " +
                        ti.Name);

                    if (ti.Status == 1)
                        ok++;
                    else
                        bad++;
                }

                Console.WriteLine();

                Console.WriteLine(
                    "Resolved    : " +
                    ok.ToString() +
                    " OK, " +
                    bad.ToString() +
                    " invalid/ambiguous");

                if (opt.ValidateOnly)
                {
                    return bad == 0 ? 0 : 4;
                }

                if (ok == 0)
                {
                    throw new Exception(
                        "No valid Historian tags.");
                }

                if (!Directory.Exists(
                    opt.OutputDirectory))
                {
                    Directory.CreateDirectory(
                        opt.OutputDirectory);
                }

                long totalRows = 0;
                int exportedFiles = 0;

                for (t = 0; t < tags.Count; t++)
                {
                    TagInfo ti = tags[t];

                    if (ti.Status != 1)
                        continue;

                    Console.WriteLine();
                    Console.WriteLine(
                        "Reading     : " + ti.Name);

                    List<HistorySample> coreRows = client.ReadRaw(
                        coreTags[t],
                        opt.Start,
                        opt.End,
                        opt.MaxSamples,
                        opt.AutoSplit);
                    List<SampleRow> rows = new List<SampleRow>();
                    int rowIndex;
                    for (rowIndex = 0; rowIndex < coreRows.Count; rowIndex++)
                    {
                        SampleRow row = new SampleRow();
                        row.Timestamp = coreRows[rowIndex].Timestamp;
                        row.Value = coreRows[rowIndex].Value;
                        row.DataType = coreRows[rowIndex].DataType;
                        row.Flags = coreRows[rowIndex].Flags;
                        rows.Add(row);
                    }

                    string fileName =
                        BuildOutputFileName(
                            ti.Name,
                            opt.Start,
                            opt.End);

                    string outputPath =
                        Path.Combine(
                            opt.OutputDirectory,
                            fileName);

                    WriteTagCsv(
                        outputPath,
                        ti.Name,
                        opt.Server,
                        opt.Start,
                        opt.End,
                        rows);

                    totalRows += rows.Count;
                    exportedFiles++;

                    Console.WriteLine(
                        "Rows        : " +
                        rows.Count.ToString());

                    Console.WriteLine(
                        "File        : " +
                        Path.GetFullPath(
                            outputPath));
                }

                WriteMetadata(
                    opt,
                    tags,
                    exportedFiles,
                    totalRows);

                Console.WriteLine();
                Console.WriteLine(
                    "Files       : " +
                    exportedFiles.ToString());

                Console.WriteLine(
                    "Total rows  : " +
                    totalRows.ToString());

                Console.WriteLine(
                    "Metadata    : " +
                    Path.GetFullPath(
                        Path.Combine(
                            opt.OutputDirectory,
                            "export.meta.txt")));

                Console.WriteLine(
                    "Log         : " +
                    Path.GetFullPath(
                        "HistoryReader.log"));

                return bad == 0 ? 0 : 5;
            }
            catch (TargetInvocationException tie)
            {
                Exception inner =
                    tie.InnerException != null
                    ? tie.InnerException
                    : tie;

                Console.WriteLine(
                    "ERROR: " +
                    inner.Message);

                LogException(
                    "TargetInvocationException",
                    inner);

                return 10;
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    "ERROR: " +
                    ex.Message);

                LogException(
                    "Run",
                    ex);

                return 11;
            }
            finally
            {
                if (client != null)
                    client.Dispose();
            }
        }

        private static List<TagInfo> ResolveTags(
            object connection,
            List<string> names)
        {
            ArrayList list =
                new ArrayList();

            int i;

            for (i = 0; i < names.Count; i++)
            {
                list.Add(names[i]);
            }

            object[] args =
                new object[]
                {
                    list,
                    null,
                    null
                };

            MethodInfo getHandles =
                FindCompatibleMethod(
                    connection.GetType(),
                    "getServerTagHandles",
                    3);

            if (getHandles == null)
            {
                throw new Exception(
                    "getServerTagHandles() was not found.");
            }

            getHandles.Invoke(
                connection,
                args);

            int[] handles =
                args[1] as int[];

            int[] status =
                args[2] as int[];

            if (handles == null ||
                status == null)
            {
                throw new Exception(
                    "Historian did not return tag handles/status.");
            }

            List<TagInfo> result =
                new List<TagInfo>();

            for (i = 0; i < names.Count; i++)
            {
                TagInfo ti =
                    new TagInfo();

                ti.Name =
                    names[i];

                ti.Handle =
                    i < handles.Length
                    ? handles[i]
                    : -1;

                ti.Status =
                    i < status.Length
                    ? status[i]
                    : -1;

                result.Add(ti);
            }

            return result;
        }

        private static void ReadRangeRecursive(
            object readInterface,
            object connection,
            MethodInfo readRaw,
            int tagHandle,
            DateTime start,
            DateTime end,
            int maxSamples,
            bool autoSplit,
            int depth,
            List<SampleRow> output)
        {
            RawSegment segment =
                ReadRawSegment(
                    readInterface,
                    connection,
                    readRaw,
                    tagHandle,
                    start,
                    end,
                    maxSamples);

            if (segment.Truncated &&
                autoSplit &&
                depth < 20 &&
                end.Subtract(start).TotalSeconds > 2.0)
            {
                DateTime middle =
                    start.AddTicks(
                        (end.Ticks -
                         start.Ticks) / 2);

                Console.WriteLine(
                    "  Truncated  : split " +
                    start.ToString(
                        "MM-dd HH:mm:ss") +
                    " .. " +
                    end.ToString(
                        "MM-dd HH:mm:ss"));

                Log(
                    "Truncated; split depth " +
                    depth.ToString() +
                    ": " +
                    start.ToString(
                        "yyyy-MM-dd HH:mm:ss") +
                    " -> " +
                    end.ToString(
                        "yyyy-MM-dd HH:mm:ss"));

                ReadRangeRecursive(
                    readInterface,
                    connection,
                    readRaw,
                    tagHandle,
                    start,
                    middle,
                    maxSamples,
                    true,
                    depth + 1,
                    output);

                ReadRangeRecursive(
                    readInterface,
                    connection,
                    readRaw,
                    tagHandle,
                    middle,
                    end,
                    maxSamples,
                    true,
                    depth + 1,
                    output);

                return;
            }

            if (segment.Truncated)
            {
                throw new Exception(
                    "Historian result is still truncated after automatic splitting. " +
                    "The incomplete segment was rejected: " +
                    start.ToString("yyyy-MM-dd HH:mm:ss.fffffff") +
                    " -> " +
                    end.ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
            }

            output.AddRange(
                segment.Rows);
        }

        private static RawSegment ReadRawSegment(
            object readInterface,
            object connection,
            MethodInfo readRaw,
            int tagHandle,
            DateTime start,
            DateTime end,
            int maxSamples)
        {
            int timeSpanHandle = -1;

            try
            {
                object tsResult =
                    InvokeNamed(
                        readInterface,
                        "createTimeSpan",
                        new object[0]);

                timeSpanHandle =
                    Convert.ToInt32(
                        tsResult,
                        CultureInfo.InvariantCulture);

                object timeSpan =
                    InvokeNamed(
                        readInterface,
                        "getTimeSpan",
                        new object[]
                        {
                            timeSpanHandle
                        });

                SetHistorianAbsoluteTime(
                    timeSpan,
                    "setAbsoluteStartTime",
                    start);

                SetHistorianAbsoluteTime(
                    timeSpan,
                    "setAbsoluteEndTime",
                    end);

                ParameterInfo[] rp =
                    readRaw.GetParameters();

                object inclusion =
                    EnumOrNumber(
                        rp[2].ParameterType,
                        "AllSamples",
                        3);

                object boundaryStart =
                    EnumOrNumber(
                        rp[3].ParameterType,
                        "None",
                        0);

                object boundaryEnd =
                    EnumOrNumber(
                        rp[4].ParameterType,
                        "None",
                        0);

                object rawResult =
                    readRaw.Invoke(
                        connection,
                        new object[]
                        {
                            timeSpanHandle,
                            tagHandle,
                            inclusion,
                            boundaryStart,
                            boundaryEnd,
                            maxSamples
                        });

                if (rawResult == null)
                {
                    throw new Exception(
                        "readRaw() returned null.");
                }

                RawSegment result =
                    new RawSegment();

                result.Truncated =
                    ToBool(
                        GetMemberValue(
                            rawResult,
                            "dataTruncated"),
                        false);

                object samplesObject =
                    GetMemberValue(
                        rawResult,
                        "dataSamples");

                IEnumerable samples =
                    samplesObject as IEnumerable;

                if (samples == null)
                {
                    throw new Exception(
                        "RawHistorySamples.dataSamples is not enumerable.");
                }

                foreach (object point in samples)
                {
                    if (point == null)
                        continue;

                    SampleRow row =
                        new SampleRow();

                    object ts =
                        GetMemberValue(
                            point,
                            "timestamp");

                    if (ts is DateTime)
                    {
                        row.Timestamp =
                            (DateTime)ts;
                    }
                    else
                    {
                        DateTime parsed;

                        if (!DateTime.TryParse(
                            ts == null
                            ? ""
                            : ts.ToString(),
                            out parsed))
                        {
                            continue;
                        }

                        row.Timestamp =
                            parsed;
                    }

                    row.Value =
                        FormatValue(
                            GetMemberValue(
                                point,
                                "value"));

                    object dt =
                        GetMemberValue(
                            point,
                            "dataType");

                    row.DataType =
                        dt == null
                        ? ""
                        : dt.ToString();

                    row.Flags =
                        BuildFlags(point);

                    result.Rows.Add(row);
                }

                return result;
            }
            finally
            {
                if (timeSpanHandle >= 0)
                {
                    TryInvoke(
                        readInterface,
                        "releaseTimeSpan",
                        new object[]
                        {
                            timeSpanHandle
                        });
                }
            }
        }

        private static List<SampleRow> Deduplicate(
            List<SampleRow> rows)
        {
            Dictionary<string, bool> seen =
                new Dictionary<string, bool>(
                    StringComparer.Ordinal);

            List<SampleRow> result =
                new List<SampleRow>();

            int i;

            for (i = 0; i < rows.Count; i++)
            {
                SampleRow row =
                    rows[i];

                string key =
                    row.Timestamp.Ticks.ToString(
                        CultureInfo.InvariantCulture) +
                    "|" +
                    row.Value +
                    "|" +
                    row.DataType +
                    "|" +
                    row.Flags;

                if (!seen.ContainsKey(key))
                {
                    seen.Add(
                        key,
                        true);

                    result.Add(row);
                }
            }

            return result;
        }

        private static List<string> LoadTags(
            string singleTag,
            string tagsFile)
        {
            List<string> result =
                new List<string>();

            Dictionary<string, bool> seen =
                new Dictionary<string, bool>(
                    StringComparer.OrdinalIgnoreCase);

            if (!String.IsNullOrEmpty(singleTag))
            {
                AddTag(
                    result,
                    seen,
                    singleTag);
            }

            if (!String.IsNullOrEmpty(tagsFile))
            {
                if (!File.Exists(tagsFile))
                {
                    throw new FileNotFoundException(
                        "Tag file not found: " +
                        tagsFile);
                }

                StreamReader sr =
                    new StreamReader(
                        tagsFile,
                        Encoding.Default,
                        true);

                string line;

                while ((line = sr.ReadLine()) != null)
                {
                    line =
                        line.Trim();

                    if (line.Length == 0)
                        continue;

                    if (line.StartsWith("#"))
                        continue;

                    AddTag(
                        result,
                        seen,
                        line);
                }

                sr.Close();
            }

            if (result.Count == 0)
            {
                throw new Exception(
                    "No tags were loaded.");
            }

            return result;
        }

        private static void AddTag(
            List<string> result,
            Dictionary<string, bool> seen,
            string tag)
        {
            tag = tag.Trim();

            if (tag.Length == 0)
                return;

            if (!seen.ContainsKey(tag))
            {
                seen.Add(
                    tag,
                    true);

                result.Add(tag);
            }
        }

        private static string BuildOutputFileName(
            string tag,
            DateTime start,
            DateTime end)
        {
            return
                SafeFileName(tag) +
                "_" +
                start.ToString(
                    "yyyyMMdd_HHmmss") +
                "_" +
                end.ToString(
                    "yyyyMMdd_HHmmss") +
                ".csv";
        }

        private static void WriteTagCsv(
            string path,
            string tag,
            string server,
            DateTime start,
            DateTime end,
            List<SampleRow> rows)
        {
            StreamWriter sw =
                new StreamWriter(
                    path,
                    false,
                    new UTF8Encoding(true));

            sw.WriteLine(
                "# DeltaV Historian Raw Export");

            sw.WriteLine(
                "# Server=" + server);

            sw.WriteLine(
                "# Tag=" + tag);

            sw.WriteLine(
                "# Start=" +
                start.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            sw.WriteLine(
                "# End=" +
                end.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            sw.WriteLine(
                "# Rows=" +
                rows.Count.ToString());

            sw.WriteLine(
                "Timestamp,Value,DataType,Flags");

            int i;

            for (i = 0; i < rows.Count; i++)
            {
                SampleRow r =
                    rows[i];

                sw.WriteLine(
                    Csv(
                        r.Timestamp.ToString(
                            "yyyy-MM-dd HH:mm:ss.fffffff")) +
                    "," +
                    Csv(r.Value) +
                    "," +
                    Csv(r.DataType) +
                    "," +
                    Csv(r.Flags));
            }

            sw.Flush();
            sw.Close();
        }

        private static void WriteMetadata(
            Options opt,
            List<TagInfo> tags,
            int exportedFiles,
            long totalRows)
        {
            string path =
                Path.Combine(
                    opt.OutputDirectory,
                    "export.meta.txt");

            StreamWriter sw =
                new StreamWriter(
                    path,
                    false,
                    Encoding.UTF8);

            sw.WriteLine(
                "DeltaV History CLI v" +
                Version);

            sw.WriteLine(
                "ExportedAt=" +
                DateTime.Now.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            sw.WriteLine(
                "Server=" +
                opt.Server);

            sw.WriteLine(
                "Start=" +
                opt.Start.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            sw.WriteLine(
                "End=" +
                opt.End.ToString(
                    "yyyy-MM-dd HH:mm:ss"));

            sw.WriteLine(
                "OutputDirectory=" +
                Path.GetFullPath(
                    opt.OutputDirectory));

            sw.WriteLine(
                "ExportedFiles=" +
                exportedFiles.ToString());

            sw.WriteLine(
                "TotalRows=" +
                totalRows.ToString());

            sw.WriteLine(
                "MaxSamplesPerRead=" +
                opt.MaxSamples.ToString());

            sw.WriteLine(
                "AutoSplit=" +
                opt.AutoSplit.ToString());

            sw.WriteLine();
            sw.WriteLine("[Tags]");

            int i;

            for (i = 0; i < tags.Count; i++)
            {
                sw.WriteLine(
                    TagStatusText(
                        tags[i].Status) +
                    "\t" +
                    tags[i].Name);
            }

            sw.Flush();
            sw.Close();
        }

        private static string SafeFileName(
            string tag)
        {
            string s = tag;

            char[] invalid =
                Path.GetInvalidFileNameChars();

            int i;

            for (i = 0; i < invalid.Length; i++)
            {
                s =
                    s.Replace(
                        invalid[i],
                        '_');
            }

            s =
                s.Replace('/', '_');

            s =
                s.Replace('\\', '_');

            s =
                s.Replace(':', '_');

            if (s.Length > 120)
            {
                s =
                    s.Substring(
                        0,
                        120);
            }

            return s;
        }

        private static DateTime ParseDateTime(
            string text)
        {
            string[] formats =
                new string[]
                {
                    "yyyy-MM-dd HH:mm:ss",
                    "yyyy-MM-dd HH:mm:ss.fffffff",
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
            {
                return value;
            }

            throw new Exception(
                "Invalid date/time: " +
                text +
                ". Use yyyy-MM-dd HH:mm:ss");
        }

        private static TimeSpan ParseDuration(
            string text)
        {
            if (String.IsNullOrEmpty(text))
            {
                throw new Exception(
                    "Invalid --last value.");
            }

            text =
                text.Trim().ToLowerInvariant();

            char unit =
                text[text.Length - 1];

            string numberText =
                text;

            if (unit == 'm' ||
                unit == 'h' ||
                unit == 'd')
            {
                numberText =
                    text.Substring(
                        0,
                        text.Length - 1);
            }
            else
            {
                unit = 'm';
            }

            double n;

            if (!Double.TryParse(
                numberText,
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out n) ||
                n <= 0)
            {
                throw new Exception(
                    "Invalid --last value: " +
                    text);
            }

            if (unit == 'd')
                return TimeSpan.FromDays(n);

            if (unit == 'h')
                return TimeSpan.FromHours(n);

            return TimeSpan.FromMinutes(n);
        }

        private static string RequireValue(
            string[] args,
            ref int index,
            string name)
        {
            if (index + 1 >= args.Length)
            {
                throw new Exception(
                    name +
                    " requires a value.");
            }

            index++;
            return args[index];
        }

        private static bool HasArg(
            string[] args,
            string value)
        {
            int i;

            for (i = 0; i < args.Length; i++)
            {
                if (String.Equals(
                    args[i],
                    value,
                    StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private static Assembly LoadDvCHAssembly()
        {
            string fileName =
                "DeltaV.Historian.DvCHDataAccess.dll";

            string local =
                Path.Combine(
                    AppDomain.CurrentDomain.BaseDirectory,
                    fileName);

            if (File.Exists(local))
            {
                _dvchAssemblyPath =
                    local;
            }

            if (_dvchAssemblyPath == null)
            {
                string[] preferred =
                    new string[]
                    {
                        Path.Combine(
                            _deltaVRoot,
                            "bin\\" + fileName),

                        Path.Combine(
                            _deltaVRoot,
                            fileName),

                        Path.Combine(
                            _deltaVRoot,
                            "Historian\\" + fileName),

                        Path.Combine(
                            _deltaVRoot,
                            "Excel\\" + fileName)
                    };

                int i;

                for (i = 0; i < preferred.Length; i++)
                {
                    if (File.Exists(preferred[i]))
                    {
                        _dvchAssemblyPath =
                            preferred[i];

                        break;
                    }
                }
            }

            if (_dvchAssemblyPath == null &&
                Directory.Exists(_deltaVRoot))
            {
                _dvchAssemblyPath =
                    FindFileSafe(
                        _deltaVRoot,
                        fileName);
            }

            if (_dvchAssemblyPath == null)
            {
                throw new FileNotFoundException(
                    fileName +
                    " was not found under C:\\DeltaV.");
            }

            _assemblyDir =
                Path.GetDirectoryName(
                    _dvchAssemblyPath);

            Console.WriteLine(
                "DvCH DLL    : " +
                _dvchAssemblyPath);

            Log(
                "Loading DvCH: " +
                _dvchAssemblyPath);

            return Assembly.LoadFrom(
                _dvchAssemblyPath);
        }

        private static Assembly CurrentDomain_AssemblyResolve(
            object sender,
            ResolveEventArgs args)
        {
            try
            {
                string shortName =
                    new AssemblyName(
                        args.Name).Name +
                    ".dll";

                if (!String.IsNullOrEmpty(
                    _assemblyDir))
                {
                    string p =
                        Path.Combine(
                            _assemblyDir,
                            shortName);

                    if (File.Exists(p))
                    {
                        Log(
                            "AssemblyResolve: " +
                            p);

                        return Assembly.LoadFrom(p);
                    }
                }

                if (Directory.Exists(
                    _deltaVRoot))
                {
                    string found =
                        FindFileSafe(
                            _deltaVRoot,
                            shortName);

                    if (!String.IsNullOrEmpty(
                        found))
                    {
                        Log(
                            "AssemblyResolve search: " +
                            found);

                        return Assembly.LoadFrom(
                            found);
                    }
                }
            }
            catch (Exception ex)
            {
                LogException(
                    "AssemblyResolve",
                    ex);
            }

            return null;
        }

        private static string FindFileSafe(
            string root,
            string fileName)
        {
            try
            {
                string[] files =
                    Directory.GetFiles(
                        root,
                        fileName);

                if (files != null &&
                    files.Length > 0)
                {
                    return files[0];
                }
            }
            catch
            {
            }

            string[] dirs = null;

            try
            {
                dirs =
                    Directory.GetDirectories(
                        root);
            }
            catch
            {
                return null;
            }

            int i;

            for (i = 0; i < dirs.Length; i++)
            {
                string result =
                    FindFileSafe(
                        dirs[i],
                        fileName);

                if (!String.IsNullOrEmpty(
                    result))
                {
                    return result;
                }
            }

            return null;
        }

        private static Type FindTypeBySimpleName(
            Assembly asm,
            string name)
        {
            Type[] types =
                asm.GetTypes();

            int i;

            for (i = 0; i < types.Length; i++)
            {
                if (types[i].Name == name)
                    return types[i];
            }

            return null;
        }

        private static MethodInfo FindMethod(
            Type type,
            string name,
            bool isStatic,
            int parameterCount)
        {
            BindingFlags flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                (isStatic
                    ? BindingFlags.Static
                    : BindingFlags.Instance);

            MethodInfo[] methods =
                type.GetMethods(flags);

            int i;

            for (i = 0; i < methods.Length; i++)
            {
                if (String.Equals(
                    methods[i].Name,
                    name,
                    StringComparison.OrdinalIgnoreCase) &&
                    methods[i].GetParameters().Length ==
                    parameterCount)
                {
                    return methods[i];
                }
            }

            return null;
        }

        private static MethodInfo FindCompatibleMethod(
            Type type,
            string name,
            int parameterCount)
        {
            BindingFlags flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance;

            MethodInfo[] methods =
                type.GetMethods(flags);

            int i;

            for (i = 0; i < methods.Length; i++)
            {
                if (String.Equals(
                    methods[i].Name,
                    name,
                    StringComparison.OrdinalIgnoreCase) &&
                    methods[i].GetParameters().Length ==
                    parameterCount)
                {
                    return methods[i];
                }
            }

            return null;
        }

        private static MethodInfo FindReadRaw6(
            Type type)
        {
            BindingFlags flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance;

            MethodInfo[] methods =
                type.GetMethods(flags);

            int i;

            for (i = 0; i < methods.Length; i++)
            {
                if (String.Equals(
                    methods[i].Name,
                    "readRaw",
                    StringComparison.OrdinalIgnoreCase) &&
                    methods[i].GetParameters().Length == 6)
                {
                    return methods[i];
                }
            }

            return null;
        }

        private static object InvokeNamed(
            object target,
            string methodName,
            object[] args)
        {
            if (target == null)
                throw new ArgumentNullException(
                    "target");

            MethodInfo m =
                FindCompatibleMethod(
                    target.GetType(),
                    methodName,
                    args.Length);

            if (m == null)
            {
                DumpMethods(
                    target.GetType(),
                    methodName);

                throw new MissingMethodException(
                    target.GetType().FullName,
                    methodName);
            }

            Log(
                "Invoke: " +
                MethodSignature(m));

            return m.Invoke(
                target,
                args);
        }

        private static void TryInvoke(
            object target,
            string methodName,
            object[] args)
        {
            try
            {
                MethodInfo m =
                    FindCompatibleMethod(
                        target.GetType(),
                        methodName,
                        args.Length);

                if (m != null)
                {
                    m.Invoke(
                        target,
                        args);
                }
            }
            catch (Exception ex)
            {
                Log(
                    "Cleanup " +
                    methodName +
                    " failed: " +
                    ex.Message);
            }
        }

        private static object GetStaticMember(
            Type type,
            string name)
        {
            BindingFlags flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Static;

            PropertyInfo p =
                type.GetProperty(
                    name,
                    flags);

            if (p != null)
            {
                return p.GetValue(
                    null,
                    null);
            }

            MethodInfo getter =
                type.GetMethod(
                    "get_" + name,
                    flags);

            if (getter != null)
            {
                return getter.Invoke(
                    null,
                    null);
            }

            FieldInfo f =
                type.GetField(
                    name,
                    flags);

            if (f != null)
            {
                return f.GetValue(null);
            }

            return null;
        }

        private static object GetMemberValue(
            object obj,
            string name)
        {
            if (obj == null)
                return null;

            BindingFlags flags =
                BindingFlags.Public |
                BindingFlags.NonPublic |
                BindingFlags.Instance;

            Type t =
                obj.GetType();

            PropertyInfo p =
                t.GetProperty(
                    name,
                    flags);

            if (p != null)
            {
                try
                {
                    return p.GetValue(
                        obj,
                        null);
                }
                catch
                {
                }
            }

            FieldInfo f =
                t.GetField(
                    name,
                    flags);

            if (f != null)
            {
                try
                {
                    return f.GetValue(obj);
                }
                catch
                {
                }
            }

            MethodInfo m =
                t.GetMethod(
                    "get_" + name,
                    flags);

            if (m != null)
            {
                try
                {
                    return m.Invoke(
                        obj,
                        null);
                }
                catch
                {
                }
            }

            PropertyInfo[] ps =
                t.GetProperties(flags);

            int i;

            for (i = 0; i < ps.Length; i++)
            {
                if (String.Equals(
                    ps[i].Name,
                    name,
                    StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        return ps[i].GetValue(
                            obj,
                            null);
                    }
                    catch
                    {
                    }
                }
            }

            FieldInfo[] fs =
                t.GetFields(flags);

            for (i = 0; i < fs.Length; i++)
            {
                if (String.Equals(
                    fs[i].Name,
                    name,
                    StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        return fs[i].GetValue(
                            obj);
                    }
                    catch
                    {
                    }
                }
            }

            return null;
        }

        private static void SetHistorianAbsoluteTime(
            object timeSpan,
            string methodName,
            DateTime localTime)
        {
            MethodInfo m =
                FindCompatibleMethod(
                    timeSpan.GetType(),
                    methodName,
                    1);

            if (m == null)
            {
                throw new MissingMethodException(
                    timeSpan.GetType().FullName,
                    methodName);
            }

            ParameterInfo p =
                m.GetParameters()[0];

            Type parameterType =
                p.ParameterType;

            Type effectiveType =
                parameterType.IsByRef
                ? parameterType.GetElementType()
                : parameterType;

            object converted;

            if (effectiveType == typeof(DateTime))
            {
                converted =
                    localTime;
            }
            else if (effectiveType.FullName ==
                "System.Runtime.InteropServices.ComTypes.FILETIME")
            {
                long ft64 =
                    localTime
                    .ToUniversalTime()
                    .ToFileTimeUtc();

                object ft =
                    Activator.CreateInstance(
                        effectiveType);

                FieldInfo low =
                    effectiveType.GetField(
                        "dwLowDateTime",
                        BindingFlags.Public |
                        BindingFlags.Instance);

                FieldInfo high =
                    effectiveType.GetField(
                        "dwHighDateTime",
                        BindingFlags.Public |
                        BindingFlags.Instance);

                if (low == null ||
                    high == null)
                {
                    throw new Exception(
                        "FILETIME fields were not found.");
                }

                low.SetValue(
                    ft,
                    unchecked(
                        (int)
                        (ft64 & 0xFFFFFFFFL)));

                high.SetValue(
                    ft,
                    unchecked(
                        (int)
                        (ft64 >> 32)));

                converted =
                    ft;
            }
            else
            {
                throw new Exception(
                    methodName +
                    "() expects unsupported time type: " +
                    effectiveType.FullName);
            }

            m.Invoke(
                timeSpan,
                new object[]
                {
                    converted
                });
        }

        private static object EnumOrNumber(
            Type type,
            string preferredName,
            int fallbackValue)
        {
            Type effectiveType =
                type;

            if (effectiveType.IsByRef)
            {
                effectiveType =
                    effectiveType.GetElementType();
            }

            if (effectiveType.IsEnum)
            {
                try
                {
                    return Enum.Parse(
                        effectiveType,
                        preferredName,
                        true);
                }
                catch
                {
                    return Enum.ToObject(
                        effectiveType,
                        fallbackValue);
                }
            }

            return Convert.ChangeType(
                fallbackValue,
                effectiveType,
                CultureInfo.InvariantCulture);
        }

        private static int Probe()
        {
            try
            {
                Assembly asm =
                    LoadDvCHAssembly();

                Type[] types =
                    asm.GetTypes();

                Console.WriteLine(
                    "Assembly: " +
                    asm.FullName);

                Console.WriteLine();

                int i;

                for (i = 0; i < types.Length; i++)
                {
                    Type t =
                        types[i];

                    if (t.FullName.IndexOf(
                        "DvCH",
                        StringComparison.OrdinalIgnoreCase) >= 0 ||
                        t.FullName.IndexOf(
                        "RawHistory",
                        StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        Console.WriteLine(
                            "=== " +
                            t.FullName +
                            " ===");

                        MethodInfo[] ms =
                            t.GetMethods(
                                BindingFlags.Public |
                                BindingFlags.NonPublic |
                                BindingFlags.Instance |
                                BindingFlags.Static |
                                BindingFlags.DeclaredOnly);

                        int j;

                        for (j = 0; j < ms.Length; j++)
                        {
                            Console.WriteLine(
                                "  " +
                                MethodSignature(
                                    ms[j]));
                        }
                    }
                }

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    "PROBE ERROR: " +
                    ex.Message);

                LogException(
                    "Probe",
                    ex);

                return 20;
            }
        }

        private static void DumpMethods(
            Type type,
            string nameContains)
        {
            try
            {
                Log(
                    "Methods on " +
                    type.FullName +
                    " matching '" +
                    nameContains +
                    "':");

                MethodInfo[] ms =
                    type.GetMethods(
                        BindingFlags.Public |
                        BindingFlags.NonPublic |
                        BindingFlags.Instance |
                        BindingFlags.Static);

                int i;

                for (i = 0; i < ms.Length; i++)
                {
                    if (ms[i].Name.IndexOf(
                        nameContains,
                        StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        Log(
                            "  " +
                            MethodSignature(
                                ms[i]));
                    }
                }
            }
            catch
            {
            }
        }

        private static string MethodSignature(
            MethodInfo m)
        {
            try
            {
                ParameterInfo[] p =
                    m.GetParameters();

                string s =
                    m.ReturnType.FullName +
                    " " +
                    m.Name +
                    "(";

                int i;

                for (i = 0; i < p.Length; i++)
                {
                    if (i > 0)
                        s += ", ";

                    s +=
                        p[i].ParameterType.FullName +
                        " " +
                        p[i].Name;
                }

                return s + ")";
            }
            catch
            {
                return m.Name;
            }
        }

        private static string TagStatusText(
            int status)
        {
            if (status == 1)
                return "OK";

            if (status == 2)
                return "UNKNOWN";

            if (status == 3)
                return "AMBIGUOUS";

            return
                "STATUS " +
                status.ToString();
        }

        private static string FormatValue(
            object value)
        {
            if (value == null)
                return "";

            IFormattable f =
                value as IFormattable;

            if (f != null)
            {
                return f.ToString(
                    null,
                    CultureInfo.InvariantCulture);
            }

            return value.ToString();
        }

        private static string BuildFlags(
            object point)
        {
            string s = "";

            if (ToBool(
                GetMemberValue(
                    point,
                    "isHistoryHole"),
                false))
            {
                s += "HistoryHole;";
            }

            if (ToBool(
                GetMemberValue(
                    point,
                    "isCRHole"),
                false))
            {
                s += "CRHole;";
            }

            if (ToBool(
                GetMemberValue(
                    point,
                    "isManuallyDeleted"),
                false))
            {
                s += "ManuallyDeleted;";
            }

            if (ToBool(
                GetMemberValue(
                    point,
                    "isManuallyInserted"),
                false))
            {
                s += "ManuallyInserted;";
            }

            if (s.EndsWith(";"))
            {
                s =
                    s.Substring(
                        0,
                        s.Length - 1);
            }

            return s;
        }

        private static bool ToBool(
            object o,
            bool defaultValue)
        {
            if (o == null)
                return defaultValue;

            try
            {
                return Convert.ToBoolean(
                    o,
                    CultureInfo.InvariantCulture);
            }
            catch
            {
                return defaultValue;
            }
        }

        private static string Csv(
            string s)
        {
            if (s == null)
                s = "";

            return
                "\"" +
                s.Replace(
                    "\"",
                    "\"\"") +
                "\"";
        }

        private static void PrintHelp()
        {
            Console.WriteLine(
                "DeltaV History CLI v" +
                Version);

            Console.WriteLine();

            Console.WriteLine(
                "Read-only DeltaV Continuous Historian RAW exporter.");

            Console.WriteLine(
                "One Historian tag = one CSV file.");

            Console.WriteLine();

            Console.WriteLine(
                "Examples:");

            Console.WriteLine();

            Console.WriteLine(
                "  HistoryReader.exe export --tags tags.txt --last 1h");

            Console.WriteLine();

            Console.WriteLine(
                "  HistoryReader.exe export --tags tags.txt --last 1d --out-dir D:\\HistoryData");

            Console.WriteLine();

            Console.WriteLine(
                "  HistoryReader.exe export --tags tags.txt --start \"2026-08-25 08:00:00\" --end \"2026-08-25 14:00:00\"");

            Console.WriteLine();

            Console.WriteLine(
                "  HistoryReader.exe export --tag \"TI-021007/AI1/PV.CV\" --last 1h");

            Console.WriteLine();

            Console.WriteLine(
                "  HistoryReader.exe validate --tags tags.txt");

            Console.WriteLine();

            Console.WriteLine(
                "Options:");

            Console.WriteLine(
                "  --server NAME       Historian node, default APP");

            Console.WriteLine(
                "  --tag TAG           Export one tag");

            Console.WriteLine(
                "  --tags FILE         TXT, one tag per line");

            Console.WriteLine(
                "  --start TIME        yyyy-MM-dd HH:mm:ss");

            Console.WriteLine(
                "  --end TIME          yyyy-MM-dd HH:mm:ss");

            Console.WriteLine(
                "  --last N[m|h|d]     e.g. 30m, 6h, 1d");

            Console.WriteLine(
                "  --out-dir DIR       default .\\export");

            Console.WriteLine(
                "  --max N             samples/read, default 10000");

            Console.WriteLine(
                "  --no-auto-split     disable automatic split");

            Console.WriteLine(
                "  --probe             dump DvCH API signatures");
        }

        private static void Log(
            string text)
        {
            if (_log != null)
            {
                _log.WriteLine(
                    DateTime.Now.ToString(
                        "HH:mm:ss.fff") +
                    " " +
                    text);
            }
        }

        private static void LogException(
            string where,
            Exception ex)
        {
            Log(
                where +
                ": " +
                ex.GetType().FullName +
                ": " +
                ex.Message);

            Log(
                ex.StackTrace == null
                ? ""
                : ex.StackTrace);

            if (ex.InnerException != null)
            {
                Log(
                    "Inner: " +
                    ex.InnerException.GetType().FullName +
                    ": " +
                    ex.InnerException.Message);

                Log(
                    ex.InnerException.StackTrace == null
                    ? ""
                    : ex.InnerException.StackTrace);
            }
        }
    }
}
