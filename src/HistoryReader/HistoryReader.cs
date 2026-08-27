using System;
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
        public string Tag;
        public string TagsFile;
        public DateTime Start;
        public DateTime End;
        public int MaxSamples = 10000;
        public string OutputDirectory = "export";
        public bool ValidateOnly;
        public bool AutoSplit = true;
    }

    class SampleRow
    {
        public DateTime Timestamp;
        public string Value;
        public string DataType;
        public string Flags;
    }

    class Program
    {
        private const string Version = "1.1";
        private static string _deltaVRoot = @"C:\DeltaV";
        private static StreamWriter _log;

        static int Main(string[] args)
        {
            return Execute(args);
        }

        internal static int Execute(string[] args)
        {
            try
            {
                _log = new StreamWriter("HistoryReader.log", true, Encoding.UTF8);
                _log.AutoFlush = true;
                Log("============================================================");
                Log("DeltaV History CLI v" + Version + " started " +
                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));

                if (args.Length == 0)
                {
                    PrintHelp();
                    return 1;
                }
                if (HasArg(args, "--help") || HasArg(args, "-h") || HasArg(args, "/?"))
                {
                    PrintHelp();
                    return 0;
                }
                if (HasArg(args, "--version"))
                {
                    Console.WriteLine("DeltaV History CLI v" + Version);
                    return 0;
                }
                if (args.Length == 1 && String.Equals(
                    args[0], "--probe", StringComparison.OrdinalIgnoreCase))
                    return HistorianClient.Probe(_deltaVRoot,
                        delegate(string message) { Log(message); });

                Options options = ParseOptions(args);
                return Run(options);
            }
            catch (Exception ex)
            {
                Console.WriteLine("FATAL: " + ex.Message);
                LogException("FATAL", ex);
                return 99;
            }
            finally
            {
                if (_log != null)
                {
                    _log.Flush();
                    _log.Close();
                    _log = null;
                }
            }
        }

        private static Options ParseOptions(string[] args)
        {
            Options options = new Options();
            int index = 0;
            if (String.Equals(args[0], "export", StringComparison.OrdinalIgnoreCase))
                index = 1;
            else if (String.Equals(args[0], "validate", StringComparison.OrdinalIgnoreCase))
            {
                options.ValidateOnly = true;
                index = 1;
            }

            string startText = null;
            string endText = null;
            string lastText = null;
            while (index < args.Length)
            {
                string arg = args[index];
                if (String.Equals(arg, "--server", StringComparison.OrdinalIgnoreCase))
                    options.Server = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--tag", StringComparison.OrdinalIgnoreCase))
                    options.Tag = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--tags", StringComparison.OrdinalIgnoreCase))
                    options.TagsFile = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--start", StringComparison.OrdinalIgnoreCase))
                    startText = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--end", StringComparison.OrdinalIgnoreCase))
                    endText = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--last", StringComparison.OrdinalIgnoreCase))
                    lastText = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--out-dir", StringComparison.OrdinalIgnoreCase))
                    options.OutputDirectory = RequireValue(args, ref index, arg);
                else if (String.Equals(arg, "--max", StringComparison.OrdinalIgnoreCase))
                {
                    string maxText = RequireValue(args, ref index, arg);
                    if (!Int32.TryParse(maxText, NumberStyles.Integer,
                        CultureInfo.InvariantCulture, out options.MaxSamples) ||
                        options.MaxSamples <= 0)
                        throw new Exception("--max must be a positive integer.");
                }
                else if (String.Equals(arg, "--no-auto-split", StringComparison.OrdinalIgnoreCase))
                    options.AutoSplit = false;
                else
                    throw new Exception("Unknown argument: " + arg);
                index++;
            }

            if (String.IsNullOrEmpty(options.Tag) && String.IsNullOrEmpty(options.TagsFile))
                throw new Exception("Specify --tag or --tags.");

            if (!String.IsNullOrEmpty(lastText))
            {
                if (!String.IsNullOrEmpty(startText) || !String.IsNullOrEmpty(endText))
                    throw new Exception("--last cannot be combined with --start/--end.");
                options.End = DateTime.Now;
                options.Start = options.End.Subtract(ParseDuration(lastText));
            }
            else if (!String.IsNullOrEmpty(startText) || !String.IsNullOrEmpty(endText))
            {
                if (String.IsNullOrEmpty(startText) || String.IsNullOrEmpty(endText))
                    throw new Exception("--start and --end must be used together.");
                options.Start = ParseDateTime(startText);
                options.End = ParseDateTime(endText);
            }
            else
            {
                options.End = DateTime.Now;
                options.Start = options.End.AddHours(-1);
            }

            if (options.Start >= options.End)
                throw new Exception("Start time must be earlier than end time.");
            return options;
        }

        private static int Run(Options options)
        {
            List<string> tagNames = LoadTags(options.Tag, options.TagsFile);
            Console.WriteLine("DeltaV History CLI v" + Version);
            Console.WriteLine("Server      : " + options.Server);
            Console.WriteLine("Tags        : " + tagNames.Count.ToString(CultureInfo.InvariantCulture));
            Console.WriteLine("Start       : " + options.Start.ToString("yyyy-MM-dd HH:mm:ss"));
            Console.WriteLine("End         : " + options.End.ToString("yyyy-MM-dd HH:mm:ss"));
            Console.WriteLine("Output dir  : " + Path.GetFullPath(options.OutputDirectory));
            Console.WriteLine("Max/read    : " + options.MaxSamples.ToString(CultureInfo.InvariantCulture));
            Console.WriteLine("Auto split  : " + options.AutoSplit.ToString());

            HistorianClient client = null;
            try
            {
                client = new HistorianClient(_deltaVRoot,
                    delegate(string message) { Log(message); });
                client.Connect(options.Server);
                Console.WriteLine("Connection  : OK (handle " +
                    client.ConnectionHandle.ToString(CultureInfo.InvariantCulture) + ")");

                List<TagResult> tags = client.ResolveTags(tagNames);
                int valid = 0;
                int invalid = 0;
                int tagIndex;
                Console.WriteLine();
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                {
                    Console.WriteLine("[" + TagStatusText(tags[tagIndex].Status) + "] " +
                        tags[tagIndex].Name);
                    if (tags[tagIndex].Status == 1)
                        valid++;
                    else
                        invalid++;
                }
                Console.WriteLine();
                Console.WriteLine("Resolved    : " + valid.ToString(CultureInfo.InvariantCulture) +
                    " OK, " + invalid.ToString(CultureInfo.InvariantCulture) + " invalid/ambiguous");

                if (options.ValidateOnly)
                    return invalid == 0 ? 0 : 4;
                if (valid == 0)
                    throw new Exception("No valid Historian tags.");
                if (!Directory.Exists(options.OutputDirectory))
                    Directory.CreateDirectory(options.OutputDirectory);

                long totalRows = 0;
                int exportedFiles = 0;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                {
                    TagResult tag = tags[tagIndex];
                    if (tag.Status != 1)
                        continue;
                    Console.WriteLine();
                    Console.WriteLine("Reading     : " + tag.Name);
                    List<HistorySample> coreRows = client.ReadRaw(
                        tag, options.Start, options.End, options.MaxSamples, options.AutoSplit);
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

                    string outputPath = Path.Combine(options.OutputDirectory,
                        BuildOutputFileName(tag.Name, options.Start, options.End));
                    WriteTagCsv(outputPath, tag.Name, options.Server,
                        options.Start, options.End, rows);
                    totalRows += rows.Count;
                    exportedFiles++;
                    Console.WriteLine("Rows        : " + rows.Count.ToString(CultureInfo.InvariantCulture));
                    Console.WriteLine("File        : " + Path.GetFullPath(outputPath));
                }

                WriteMetadata(options, tags, exportedFiles, totalRows);
                Console.WriteLine();
                Console.WriteLine("Files       : " + exportedFiles.ToString(CultureInfo.InvariantCulture));
                Console.WriteLine("Total rows  : " + totalRows.ToString(CultureInfo.InvariantCulture));
                Console.WriteLine("Metadata    : " + Path.GetFullPath(Path.Combine(
                    options.OutputDirectory, "export.meta.txt")));
                Console.WriteLine("Log         : " + Path.GetFullPath("HistoryReader.log"));
                return invalid == 0 ? 0 : 5;
            }
            catch (TargetInvocationException exception)
            {
                Exception inner = exception.InnerException == null
                    ? exception : exception.InnerException;
                Console.WriteLine("ERROR: " + inner.Message);
                LogException("TargetInvocationException", inner);
                return 10;
            }
            catch (Exception exception)
            {
                Console.WriteLine("ERROR: " + exception.Message);
                LogException("Run", exception);
                return 11;
            }
            finally
            {
                if (client != null)
                    client.Dispose();
            }
        }

        private static List<string> LoadTags(string singleTag, string tagsFile)
        {
            List<string> result = new List<string>();
            Dictionary<string, bool> seen = new Dictionary<string, bool>(
                StringComparer.OrdinalIgnoreCase);
            if (!String.IsNullOrEmpty(singleTag))
                AddTag(result, seen, singleTag);
            if (!String.IsNullOrEmpty(tagsFile))
            {
                if (!File.Exists(tagsFile))
                    throw new FileNotFoundException("Tag file not found: " + tagsFile);
                using (StreamReader reader = new StreamReader(tagsFile, Encoding.Default, true))
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        line = line.Trim();
                        if (line.Length > 0 && !line.StartsWith("#"))
                            AddTag(result, seen, line);
                    }
                }
            }
            if (result.Count == 0)
                throw new Exception("No tags were loaded.");
            return result;
        }

        private static void AddTag(List<string> result, Dictionary<string, bool> seen, string tag)
        {
            tag = tag.Trim();
            if (tag.Length == 0 || seen.ContainsKey(tag))
                return;
            seen.Add(tag, true);
            result.Add(tag);
        }

        private static string BuildOutputFileName(string tag, DateTime start, DateTime end)
        {
            return SafeFileName(tag) + "_" + start.ToString("yyyyMMdd_HHmmss") + "_" +
                end.ToString("yyyyMMdd_HHmmss") + ".csv";
        }

        private static void WriteTagCsv(string path, string tag, string server,
            DateTime start, DateTime end, List<SampleRow> rows)
        {
            using (StreamWriter writer = new StreamWriter(path, false, new UTF8Encoding(true)))
            {
                writer.WriteLine("# DeltaV Historian Raw Export");
                writer.WriteLine("# Server=" + server);
                writer.WriteLine("# Tag=" + tag);
                writer.WriteLine("# Start=" + start.ToString("yyyy-MM-dd HH:mm:ss"));
                writer.WriteLine("# End=" + end.ToString("yyyy-MM-dd HH:mm:ss"));
                writer.WriteLine("# Rows=" + rows.Count.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("Timestamp,Value,DataType,Flags");
                int index;
                for (index = 0; index < rows.Count; index++)
                {
                    SampleRow row = rows[index];
                    writer.WriteLine(Csv(row.Timestamp.ToString(
                        "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)) + "," +
                        Csv(row.Value) + "," + Csv(row.DataType) + "," + Csv(row.Flags));
                }
                writer.Flush();
            }
        }

        private static void WriteMetadata(Options options, List<TagResult> tags,
            int exportedFiles, long totalRows)
        {
            string path = Path.Combine(options.OutputDirectory, "export.meta.txt");
            using (StreamWriter writer = new StreamWriter(path, false, Encoding.UTF8))
            {
                writer.WriteLine("DeltaV History CLI v" + Version);
                writer.WriteLine("ExportedAt=" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
                writer.WriteLine("Server=" + options.Server);
                writer.WriteLine("Start=" + options.Start.ToString("yyyy-MM-dd HH:mm:ss"));
                writer.WriteLine("End=" + options.End.ToString("yyyy-MM-dd HH:mm:ss"));
                writer.WriteLine("OutputDirectory=" + Path.GetFullPath(options.OutputDirectory));
                writer.WriteLine("ExportedFiles=" + exportedFiles.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("TotalRows=" + totalRows.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("MaxSamplesPerRead=" + options.MaxSamples.ToString(CultureInfo.InvariantCulture));
                writer.WriteLine("AutoSplit=" + options.AutoSplit.ToString());
                writer.WriteLine();
                writer.WriteLine("[Tags]");
                int index;
                for (index = 0; index < tags.Count; index++)
                    writer.WriteLine(TagStatusText(tags[index].Status) + "\t" + tags[index].Name);
                writer.Flush();
            }
        }

        private static string SafeFileName(string tag)
        {
            string value = tag;
            char[] invalid = Path.GetInvalidFileNameChars();
            int index;
            for (index = 0; index < invalid.Length; index++)
                value = value.Replace(invalid[index], '_');
            value = value.Replace('/', '_').Replace('\\', '_').Replace(':', '_');
            return value.Length > 120 ? value.Substring(0, 120) : value;
        }

        private static DateTime ParseDateTime(string text)
        {
            string[] formats = new string[] {
                "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.fffffff",
                "yyyy-MM-dd HH:mm", "yyyy-MM-ddTHH:mm:ss",
                "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm",
                "yyyy-MM-dd", "yyyy/MM/dd" };
            DateTime value;
            if (DateTime.TryParseExact(text, formats, CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces, out value))
                return value;
            throw new Exception("Invalid date/time: " + text + ". Use yyyy-MM-dd HH:mm:ss");
        }

        private static TimeSpan ParseDuration(string text)
        {
            if (String.IsNullOrEmpty(text))
                throw new Exception("Invalid --last value.");
            text = text.Trim().ToLowerInvariant();
            char unit = text[text.Length - 1];
            string numberText = text;
            if (unit == 'm' || unit == 'h' || unit == 'd')
                numberText = text.Substring(0, text.Length - 1);
            else
                unit = 'm';
            double number;
            if (!Double.TryParse(numberText, NumberStyles.Float,
                CultureInfo.InvariantCulture, out number) || number <= 0)
                throw new Exception("Invalid --last value: " + text);
            if (unit == 'd') return TimeSpan.FromDays(number);
            if (unit == 'h') return TimeSpan.FromHours(number);
            return TimeSpan.FromMinutes(number);
        }

        private static string RequireValue(string[] args, ref int index, string name)
        {
            if (index + 1 >= args.Length)
                throw new Exception(name + " requires a value.");
            index++;
            return args[index];
        }

        private static bool HasArg(string[] args, string value)
        {
            int index;
            for (index = 0; index < args.Length; index++)
                if (String.Equals(args[index], value, StringComparison.OrdinalIgnoreCase))
                    return true;
            return false;
        }

        private static string TagStatusText(int status)
        {
            if (status == 1) return "OK";
            if (status == 2) return "UNKNOWN";
            if (status == 3) return "AMBIGUOUS";
            return "STATUS " + status.ToString(CultureInfo.InvariantCulture);
        }

        private static string Csv(string value)
        {
            if (value == null) value = "";
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        }

        private static void PrintHelp()
        {
            Console.WriteLine("DeltaV History CLI v" + Version);
            Console.WriteLine();
            Console.WriteLine("Read-only DeltaV Continuous Historian RAW exporter.");
            Console.WriteLine("One Historian tag = one CSV file.");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  HistoryReader.exe export --tags tags.txt --last 1h");
            Console.WriteLine("  HistoryReader.exe export --tags tags.txt --last 1d --out-dir D:\\HistoryData");
            Console.WriteLine("  HistoryReader.exe export --tags tags.txt --start \"2026-08-25 08:00:00\" --end \"2026-08-25 14:00:00\"");
            Console.WriteLine("  HistoryReader.exe export --tag \"TI-021007/AI1/PV.CV\" --last 1h");
            Console.WriteLine("  HistoryReader.exe validate --tags tags.txt");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  --server NAME       Historian node, default APP");
            Console.WriteLine("  --tag TAG           Export one tag");
            Console.WriteLine("  --tags FILE         TXT, one tag per line");
            Console.WriteLine("  --start TIME        yyyy-MM-dd HH:mm:ss");
            Console.WriteLine("  --end TIME          yyyy-MM-dd HH:mm:ss");
            Console.WriteLine("  --last N[m|h|d]     e.g. 30m, 6h, 1d");
            Console.WriteLine("  --out-dir DIR       default .\\export");
            Console.WriteLine("  --max N             samples/read, default 10000");
            Console.WriteLine("  --no-auto-split     disable automatic split");
            Console.WriteLine("  --probe             dump DvCH API signatures");
        }

        private static void Log(string text)
        {
            if (_log != null)
                _log.WriteLine(DateTime.Now.ToString("HH:mm:ss.fff") + " " + text);
        }

        private static void LogException(string where, Exception exception)
        {
            Log(where + ": " + exception.GetType().FullName + ": " + exception.Message);
            Log(exception.StackTrace == null ? "" : exception.StackTrace);
            if (exception.InnerException != null)
            {
                Log("Inner: " + exception.InnerException.GetType().FullName + ": " +
                    exception.InnerException.Message);
                Log(exception.InnerException.StackTrace == null
                    ? "" : exception.InnerException.StackTrace);
            }
        }
    }
}
