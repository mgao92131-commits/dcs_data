using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;

namespace DeltaVHistoryCLI
{
    public delegate void HistorianLog(string message);

    public class HistorySample
    {
        public string Tag;
        public DateTime Timestamp;
        public string Value;
        public string DataType;
        public string Flags;
        public string SequenceNo;
        public string ArchiveStatus;
    }

    public class TagResult
    {
        public string Name;
        public int Handle;
        public int Status;
    }

    public static class HistorySampleSet
    {
        public static List<HistorySample> Normalize(List<HistorySample> rows)
        {
            if (rows == null)
                throw new ArgumentNullException("rows");
            rows.Sort(delegate(HistorySample a, HistorySample b)
            {
                return a.Timestamp.CompareTo(b.Timestamp);
            });

            Dictionary<string, bool> seen = new Dictionary<string, bool>(StringComparer.Ordinal);
            List<HistorySample> result = new List<HistorySample>();
            int i;
            for (i = 0; i < rows.Count; i++)
            {
                HistorySample row = rows[i];
                string key = row.Timestamp.Ticks.ToString(CultureInfo.InvariantCulture) + "|" +
                    row.Value + "|" + row.DataType + "|" + row.Flags;
                key += "|" + row.SequenceNo + "|" + row.ArchiveStatus;
                if (!seen.ContainsKey(key))
                {
                    seen.Add(key, true);
                    result.Add(row);
                }
            }
            return result;
        }
    }

    public class HistorianClient : IDisposable
    {
        private readonly string _deltaVRoot;
        private readonly HistorianLog _log;
        private object _readInterface;
        private object _connection;
        private MethodInfo _readRaw;
        private int _connectionHandle = -1;
        private string _assemblyDirectory;
        private bool _resolverAttached;

        public HistorianClient(string deltaVRoot, HistorianLog log)
        {
            _deltaVRoot = String.IsNullOrEmpty(deltaVRoot) ? @"C:\DeltaV" : deltaVRoot;
            _log = log;
        }

        public int ConnectionHandle
        {
            get { return _connectionHandle; }
        }

        public void Connect(string server)
        {
            if (_connection != null)
                throw new InvalidOperationException("HistorianClient is already connected.");
            if (String.IsNullOrEmpty(server))
                throw new ArgumentException("Historian server is required.", "server");

            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomainAssemblyResolve;
            _resolverAttached = true;

            Assembly assembly = LoadDvCHAssembly();
            Type accessType = FindTypeBySimpleName(assembly, "DvCHDataAccess");
            if (accessType == null)
                throw new Exception("Type DvCHDataAccess was not found.");

            MethodInfo initialize = FindMethod(accessType, "Initialize", true, 0);
            if (initialize == null)
                throw new Exception("DvCHDataAccess.Initialize() was not found.");
            initialize.Invoke(null, null);

            _readInterface = GetStaticMember(accessType, "ReadInterface");
            if (_readInterface == null)
                throw new Exception("DvCHDataAccess.ReadInterface returned null.");

            object connectionResult = InvokeNamed(
                _readInterface,
                "createConnection",
                new object[] { server, "DeltaVHistoryCLI", 30 });
            _connectionHandle = Convert.ToInt32(connectionResult, CultureInfo.InvariantCulture);

            try
            {
                _connection = InvokeNamed(
                    _readInterface,
                    "connection",
                    new object[] { _connectionHandle });
            }
            catch
            {
                _connection = InvokeNamed(
                    _readInterface,
                    "getConnection",
                    new object[] { _connectionHandle });
            }
            if (_connection == null)
                throw new Exception("Could not obtain DvCH read connection.");

            _readRaw = FindCompatibleMethod(_connection.GetType(), "readRaw", 6);
            if (_readRaw == null)
                throw new Exception("Six-argument readRaw() overload was not found.");
        }

        public List<TagResult> ResolveTags(List<string> names)
        {
            EnsureConnected();
            if (names == null)
                throw new ArgumentNullException("names");

            ArrayList requested = new ArrayList();
            int i;
            for (i = 0; i < names.Count; i++)
                requested.Add(names[i]);

            object[] args = new object[] { requested, null, null };
            MethodInfo getHandles = FindCompatibleMethod(
                _connection.GetType(),
                "getServerTagHandles",
                3);
            if (getHandles == null)
                throw new Exception("getServerTagHandles() was not found.");
            getHandles.Invoke(_connection, args);

            int[] handles = args[1] as int[];
            int[] status = args[2] as int[];
            if (handles == null || status == null)
                throw new Exception("Historian did not return tag handles/status.");

            List<TagResult> result = new List<TagResult>();
            for (i = 0; i < names.Count; i++)
            {
                TagResult tag = new TagResult();
                tag.Name = names[i];
                tag.Handle = i < handles.Length ? handles[i] : -1;
                tag.Status = i < status.Length ? status[i] : -1;
                result.Add(tag);
            }
            return result;
        }

        public List<HistorySample> ReadRaw(
            TagResult tag,
            DateTime start,
            DateTime end,
            int maxSamples,
            bool autoSplit)
        {
            EnsureConnected();
            if (tag == null)
                throw new ArgumentNullException("tag");
            if (tag.Status != 1)
                throw new ArgumentException("Tag is not valid.", "tag");
            if (start >= end)
                throw new ArgumentException("Start time must be earlier than end time.");
            if (maxSamples <= 0)
                throw new ArgumentOutOfRangeException("maxSamples");

            List<HistorySample> rows = new List<HistorySample>();
            ReadRangeRecursive(tag, start, end, maxSamples, autoSplit, 0, rows);
            return HistorySampleSet.Normalize(rows);
        }

        public void Dispose()
        {
            if (_readInterface != null && _connectionHandle >= 0)
                TryInvoke(_readInterface, "closeConnection", new object[] { _connectionHandle });
            _connectionHandle = -1;
            _connection = null;
            _readRaw = null;
            _readInterface = null;
            if (_resolverAttached)
            {
                AppDomain.CurrentDomain.AssemblyResolve -= CurrentDomainAssemblyResolve;
                _resolverAttached = false;
            }
        }

        private void ReadRangeRecursive(
            TagResult tag,
            DateTime start,
            DateTime end,
            int maxSamples,
            bool autoSplit,
            int depth,
            List<HistorySample> output)
        {
            RawHistorySegment segment = ReadRawSegment(tag, start, end, maxSamples);
            if (segment.Truncated && autoSplit && depth < 20 && end.Subtract(start).TotalSeconds > 2.0)
            {
                DateTime middle = start.AddTicks((end.Ticks - start.Ticks) / 2);
                WriteLog(
                    "Truncated; split depth " + depth.ToString(CultureInfo.InvariantCulture) +
                    ": " + start.ToString("yyyy-MM-dd HH:mm:ss") +
                    " -> " + end.ToString("yyyy-MM-dd HH:mm:ss"));
                Console.WriteLine(
                    "  Truncated  : split " + start.ToString("MM-dd HH:mm:ss") +
                    " .. " + end.ToString("MM-dd HH:mm:ss"));
                ReadRangeRecursive(tag, start, middle, maxSamples, true, depth + 1, output);
                ReadRangeRecursive(tag, middle, end, maxSamples, true, depth + 1, output);
                return;
            }
            if (segment.Truncated)
            {
                throw new Exception(
                    "Historian result is still truncated after automatic splitting. " +
                    "The incomplete segment was rejected: " +
                    start.ToString("yyyy-MM-dd HH:mm:ss.fffffff") + " -> " +
                    end.ToString("yyyy-MM-dd HH:mm:ss.fffffff"));
            }
            output.AddRange(segment.Rows);
        }

        private RawHistorySegment ReadRawSegment(
            TagResult tag,
            DateTime start,
            DateTime end,
            int maxSamples)
        {
            int timeSpanHandle = -1;
            try
            {
                object result = InvokeNamed(_readInterface, "createTimeSpan", new object[0]);
                timeSpanHandle = Convert.ToInt32(result, CultureInfo.InvariantCulture);
                object timeSpan = InvokeNamed(
                    _readInterface,
                    "getTimeSpan",
                    new object[] { timeSpanHandle });
                SetHistorianAbsoluteTime(timeSpan, "setAbsoluteStartTime", start);
                SetHistorianAbsoluteTime(timeSpan, "setAbsoluteEndTime", end);

                ParameterInfo[] parameters = _readRaw.GetParameters();
                object raw = _readRaw.Invoke(
                    _connection,
                    new object[]
                    {
                        timeSpanHandle,
                        tag.Handle,
                        EnumOrNumber(parameters[2].ParameterType, "AllSamples", 3),
                        EnumOrNumber(parameters[3].ParameterType, "None", 0),
                        EnumOrNumber(parameters[4].ParameterType, "None", 0),
                        maxSamples
                    });
                if (raw == null)
                    throw new Exception("readRaw() returned null.");

                RawHistorySegment segment = new RawHistorySegment();
                segment.Truncated = ToBool(GetMemberValue(raw, "dataTruncated"), false);
                IEnumerable samples = GetMemberValue(raw, "dataSamples") as IEnumerable;
                if (samples == null)
                    throw new Exception("RawHistorySamples.dataSamples is not enumerable.");

                foreach (object point in samples)
                {
                    if (point == null)
                        continue;
                    DateTime timestamp;
                    object rawTimestamp = GetMemberValue(point, "timestamp");
                    if (rawTimestamp is DateTime)
                        timestamp = (DateTime)rawTimestamp;
                    else if (!DateTime.TryParse(
                        rawTimestamp == null ? "" : rawTimestamp.ToString(),
                        out timestamp))
                        continue;

                    HistorySample sample = new HistorySample();
                    sample.Tag = tag.Name;
                    sample.Timestamp = timestamp;
                    sample.Value = FormatValue(GetMemberValue(point, "value"));
                    object dataType = GetMemberValue(point, "dataType");
                    sample.DataType = dataType == null ? "" : dataType.ToString();
                    sample.Flags = BuildFlags(point);
                    sample.SequenceNo = FormatValue(GetMemberValue(point, "sequenceNo"));
                    sample.ArchiveStatus = FormatValue(GetMemberValue(point, "archiveStatus"));
                    segment.Rows.Add(sample);
                }
                return segment;
            }
            finally
            {
                if (timeSpanHandle >= 0)
                    TryInvoke(_readInterface, "releaseTimeSpan", new object[] { timeSpanHandle });
            }
        }

        private void EnsureConnected()
        {
            if (_connection == null || _readInterface == null || _readRaw == null)
                throw new InvalidOperationException("HistorianClient is not connected.");
        }

        private Assembly LoadDvCHAssembly()
        {
            const string fileName = "DeltaV.Historian.DvCHDataAccess.dll";
            string assemblyPath = null;
            string local = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName);
            if (File.Exists(local))
                assemblyPath = local;
            if (assemblyPath == null)
            {
                string[] preferred = new string[]
                {
                    Path.Combine(_deltaVRoot, "bin\\" + fileName),
                    Path.Combine(_deltaVRoot, fileName),
                    Path.Combine(_deltaVRoot, "Historian\\" + fileName),
                    Path.Combine(_deltaVRoot, "Excel\\" + fileName)
                };
                int i;
                for (i = 0; i < preferred.Length; i++)
                {
                    if (File.Exists(preferred[i]))
                    {
                        assemblyPath = preferred[i];
                        break;
                    }
                }
            }
            if (assemblyPath == null && Directory.Exists(_deltaVRoot))
                assemblyPath = FindFileSafe(_deltaVRoot, fileName);
            if (assemblyPath == null)
                throw new FileNotFoundException(fileName + " was not found under " + _deltaVRoot + ".");

            _assemblyDirectory = Path.GetDirectoryName(assemblyPath);
            Console.WriteLine("DvCH DLL    : " + assemblyPath);
            WriteLog("Loading DvCH: " + assemblyPath);
            return Assembly.LoadFrom(assemblyPath);
        }

        private Assembly CurrentDomainAssemblyResolve(object sender, ResolveEventArgs args)
        {
            try
            {
                string shortName = new AssemblyName(args.Name).Name + ".dll";
                if (!String.IsNullOrEmpty(_assemblyDirectory))
                {
                    string local = Path.Combine(_assemblyDirectory, shortName);
                    if (File.Exists(local))
                        return Assembly.LoadFrom(local);
                }
                if (Directory.Exists(_deltaVRoot))
                {
                    string found = FindFileSafe(_deltaVRoot, shortName);
                    if (!String.IsNullOrEmpty(found))
                        return Assembly.LoadFrom(found);
                }
            }
            catch (Exception ex)
            {
                WriteLog("AssemblyResolve failed: " + ex.Message);
            }
            return null;
        }

        private static string FindFileSafe(string root, string fileName)
        {
            try
            {
                string[] files = Directory.GetFiles(root, fileName);
                if (files.Length > 0)
                    return files[0];
            }
            catch { }
            string[] directories;
            try { directories = Directory.GetDirectories(root); }
            catch { return null; }
            int i;
            for (i = 0; i < directories.Length; i++)
            {
                string found = FindFileSafe(directories[i], fileName);
                if (!String.IsNullOrEmpty(found))
                    return found;
            }
            return null;
        }

        private static Type FindTypeBySimpleName(Assembly assembly, string name)
        {
            Type[] types = assembly.GetTypes();
            int i;
            for (i = 0; i < types.Length; i++)
                if (types[i].Name == name)
                    return types[i];
            return null;
        }

        private static MethodInfo FindMethod(Type type, string name, bool isStatic, int parameterCount)
        {
            BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic |
                (isStatic ? BindingFlags.Static : BindingFlags.Instance);
            MethodInfo[] methods = type.GetMethods(flags);
            int i;
            for (i = 0; i < methods.Length; i++)
                if (String.Equals(methods[i].Name, name, StringComparison.OrdinalIgnoreCase) &&
                    methods[i].GetParameters().Length == parameterCount)
                    return methods[i];
            return null;
        }

        private static MethodInfo FindCompatibleMethod(Type type, string name, int parameterCount)
        {
            return FindMethod(type, name, false, parameterCount);
        }

        private object InvokeNamed(object target, string methodName, object[] args)
        {
            if (target == null)
                throw new ArgumentNullException("target");
            MethodInfo method = FindCompatibleMethod(target.GetType(), methodName, args.Length);
            if (method == null)
                throw new MissingMethodException(target.GetType().FullName, methodName);
            WriteLog("Invoke: " + method.Name);
            return method.Invoke(target, args);
        }

        private bool TryInvoke(object target, string methodName, object[] args)
        {
            try
            {
                MethodInfo method = FindCompatibleMethod(target.GetType(), methodName, args.Length);
                if (method == null)
                {
                    WriteLog("Cleanup " + methodName + " was not found on " + target.GetType().FullName + ".");
                    return false;
                }
                method.Invoke(target, args);
                return true;
            }
            catch (Exception ex)
            {
                WriteLog("Cleanup " + methodName + " failed: " + ex.Message);
                return false;
            }
        }

        private static object GetStaticMember(Type type, string name)
        {
            BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;
            PropertyInfo property = type.GetProperty(name, flags);
            if (property != null)
                return property.GetValue(null, null);
            MethodInfo getter = type.GetMethod("get_" + name, flags);
            if (getter != null)
                return getter.Invoke(null, null);
            FieldInfo field = type.GetField(name, flags);
            return field == null ? null : field.GetValue(null);
        }

        private static object GetMemberValue(object instance, string name)
        {
            if (instance == null)
                return null;
            BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
            Type type = instance.GetType();
            PropertyInfo[] properties = type.GetProperties(flags);
            int i;
            for (i = 0; i < properties.Length; i++)
            {
                if (String.Equals(properties[i].Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    try { return properties[i].GetValue(instance, null); }
                    catch { }
                }
            }
            FieldInfo[] fields = type.GetFields(flags);
            for (i = 0; i < fields.Length; i++)
            {
                if (String.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    try { return fields[i].GetValue(instance); }
                    catch { }
                }
            }
            MethodInfo getter = type.GetMethod("get_" + name, flags);
            if (getter != null)
            {
                try { return getter.Invoke(instance, null); }
                catch { }
            }
            return null;
        }

        private static void SetHistorianAbsoluteTime(object timeSpan, string methodName, DateTime localTime)
        {
            MethodInfo method = FindCompatibleMethod(timeSpan.GetType(), methodName, 1);
            if (method == null)
                throw new MissingMethodException(timeSpan.GetType().FullName, methodName);
            Type parameterType = method.GetParameters()[0].ParameterType;
            Type effectiveType = parameterType.IsByRef ? parameterType.GetElementType() : parameterType;
            object converted;
            if (effectiveType == typeof(DateTime))
                converted = localTime;
            else if (effectiveType.FullName == "System.Runtime.InteropServices.ComTypes.FILETIME")
            {
                long fileTime = localTime.ToUniversalTime().ToFileTimeUtc();
                object value = Activator.CreateInstance(effectiveType);
                FieldInfo low = effectiveType.GetField("dwLowDateTime");
                FieldInfo high = effectiveType.GetField("dwHighDateTime");
                if (low == null || high == null)
                    throw new Exception("FILETIME fields were not found.");
                low.SetValue(value, unchecked((int)(fileTime & 0xFFFFFFFFL)));
                high.SetValue(value, unchecked((int)(fileTime >> 32)));
                converted = value;
            }
            else
                throw new Exception(methodName + "() expects unsupported time type: " + effectiveType.FullName);
            method.Invoke(timeSpan, new object[] { converted });
        }

        private static object EnumOrNumber(Type type, string preferredName, int fallbackValue)
        {
            Type effectiveType = type.IsByRef ? type.GetElementType() : type;
            if (effectiveType.IsEnum)
            {
                try { return Enum.Parse(effectiveType, preferredName, true); }
                catch { return Enum.ToObject(effectiveType, fallbackValue); }
            }
            return Convert.ChangeType(fallbackValue, effectiveType, CultureInfo.InvariantCulture);
        }

        private static string FormatValue(object value)
        {
            if (value == null)
                return "";
            IFormattable formattable = value as IFormattable;
            return formattable == null
                ? value.ToString()
                : formattable.ToString(null, CultureInfo.InvariantCulture);
        }

        private static string BuildFlags(object point)
        {
            string result = "";
            if (ToBool(GetMemberValue(point, "isHistoryHole"), false)) result += "HistoryHole;";
            if (ToBool(GetMemberValue(point, "isCRHole"), false)) result += "CRHole;";
            if (ToBool(GetMemberValue(point, "isManuallyDeleted"), false)) result += "ManuallyDeleted;";
            if (ToBool(GetMemberValue(point, "isManuallyInserted"), false)) result += "ManuallyInserted;";
            return result.EndsWith(";") ? result.Substring(0, result.Length - 1) : result;
        }

        private static bool ToBool(object value, bool defaultValue)
        {
            if (value == null)
                return defaultValue;
            try { return Convert.ToBoolean(value, CultureInfo.InvariantCulture); }
            catch { return defaultValue; }
        }

        private void WriteLog(string message)
        {
            if (_log != null)
                _log(message);
        }

        private class RawHistorySegment
        {
            public readonly List<HistorySample> Rows = new List<HistorySample>();
            public bool Truncated;
        }
    }
}
