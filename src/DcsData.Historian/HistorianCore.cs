using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Diagnostics;
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

    public class ProcessedHistoryResult
    {
        public string Aggregate;
        public int IntervalSeconds;
        public int ReturnedSlots;
        public int InvalidSlots;
        public readonly List<HistorySample> Samples = new List<HistorySample>();
    }

    public class ProcessedTagResult
    {
        public TagResult Tag;
        public ProcessedHistoryResult Result;
        public Exception Error;
    }

    public class HistorianPerformanceMetrics
    {
        public long RpcMilliseconds;
        public long SampleConvertMilliseconds;
        public long NormalizeMilliseconds;
        public int ReturnedSamples;
        public int InvalidSamples;
        public int NormalizeFastPathTags;
        public int NormalizeFallbackTags;
    }

    public static class HistorySampleSet
    {
        public static List<HistorySample> NormalizeProcessed(
            List<HistorySample> rows,
            out bool fastPath)
        {
            if (rows == null)
                throw new ArgumentNullException("rows");
            if (IsStrictlyOrdered(rows))
            {
                fastPath = true;
                return rows;
            }
            fastPath = false;
            return Normalize(rows);
        }

        public static bool IsStrictlyOrdered(List<HistorySample> rows)
        {
            if (rows == null)
                throw new ArgumentNullException("rows");
            if (rows.Count < 2)
                return true;
            DateTime previous = rows[0].Timestamp;
            int i;
            for (i = 1; i < rows.Count; i++)
            {
                if (rows[i].Timestamp <= previous)
                    return false;
                previous = rows[i].Timestamp;
            }
            return true;
        }

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
        private static readonly object MemberAccessorLock = new object();
        private static readonly Dictionary<string, MemberAccessor> MemberAccessors =
            new Dictionary<string, MemberAccessor>(StringComparer.OrdinalIgnoreCase);
        private readonly string _deltaVRoot;
        private readonly HistorianLog _log;
        private object _readInterface;
        private object _connection;
        private MethodInfo _readProcessed;
        private object _processedSampleType;
        private object _interpolatedAggregate;
        private int _connectionHandle = -1;
        private string _assemblyDirectory;
        private bool _resolverAttached;
        private ProcessedPointAccessorSet _pointAccessors;
        private int _processedPointAccessorBuildCount;
        private HistorianPerformanceMetrics _lastPerformance =
            new HistorianPerformanceMetrics();
        private bool _lastReadHadErrors;

        public HistorianClient(string deltaVRoot, HistorianLog log)
        {
            _deltaVRoot = String.IsNullOrEmpty(deltaVRoot) ? @"C:\DeltaV" : deltaVRoot;
            _log = log;
        }

        public int ConnectionHandle
        {
            get { return _connectionHandle; }
        }

        public HistorianPerformanceMetrics LastPerformance
        {
            get { return _lastPerformance; }
        }

        public int ProcessedPointAccessorBuildCount
        {
            get { return _processedPointAccessorBuildCount; }
        }

        public bool LastReadHadErrors
        {
            get { return _lastReadHadErrors; }
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
            LoadDeltaVAssembly("DeltaV.Historian.Data.dll");
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

            _readProcessed = FindCompatibleMethod(_connection.GetType(), "readProcessed", 4);
            if (_readProcessed == null)
                throw new Exception("Four-argument readProcessed() overload was not found.");
            ParameterInfo[] processedParameters = _readProcessed.GetParameters();
            _processedSampleType = EnumOrNumber(
                processedParameters[2].ParameterType,
                "AllSamples",
                3);
            Type aggregateType = FindLoadedType("DeltaV.Historian.Data.Aggregate");
            if (aggregateType == null || !aggregateType.IsEnum)
                throw new Exception("DeltaV Historian Aggregate enum was not found.");
            _interpolatedAggregate = Enum.Parse(
                aggregateType,
                "InterpolatedValue",
                true);
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

        public ProcessedHistoryResult ReadProcessed(
            TagResult tag,
            DateTime start,
            DateTime end,
            int intervalSeconds)
        {
            if (tag == null)
                throw new ArgumentNullException("tag");

            List<TagResult> tags = new List<TagResult>();
            tags.Add(tag);
            List<ProcessedTagResult> results = ReadProcessedBatch(
                tags,
                start,
                end,
                intervalSeconds);
            if (results.Count != 1)
                throw new Exception("Historian returned an unexpected tag result count.");
            if (results[0].Error != null)
                throw results[0].Error;
            return results[0].Result;
        }

        public List<ProcessedTagResult> ReadProcessedBatch(
            List<TagResult> tags,
            DateTime start,
            DateTime end,
            int intervalSeconds)
        {
            EnsureConnected();
            if (_readProcessed == null)
                throw new MissingMethodException(
                    _connection.GetType().FullName,
                    "readProcessed");
            if (tags == null)
                throw new ArgumentNullException("tags");
            if (start >= end)
                throw new ArgumentException("Start time must be earlier than end time.");
            if (intervalSeconds <= 0)
                throw new ArgumentOutOfRangeException("intervalSeconds");

            _lastPerformance = new HistorianPerformanceMetrics();
            _lastReadHadErrors = false;
            List<ProcessedTagResult> results = new List<ProcessedTagResult>();
            if (tags.Count == 0)
                return results;

            string processedSequence = BuildProcessedSequence(intervalSeconds);

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
                SetHistorianResampleInterval(timeSpan, TimeSpan.FromSeconds(intervalSeconds));

                ArrayList aggregates = new ArrayList();
                aggregates.Add(_interpolatedAggregate);

                int tagIndex;
                for (tagIndex = 0; tagIndex < tags.Count; tagIndex++)
                {
                    TagResult tag = tags[tagIndex];
                    ProcessedTagResult tagResult = new ProcessedTagResult();
                    tagResult.Tag = tag;
                    if (tag == null)
                    {
                        tagResult.Error = new ArgumentNullException("tag");
                    }
                    else if (tag.Status != 1)
                    {
                        tagResult.Error = new ArgumentException("Tag is not valid.", "tag");
                    }
                    else
                    {
                        try
                        {
                            tagResult.Result = ReadProcessedWithTimeSpan(
                                tag,
                                timeSpanHandle,
                                intervalSeconds,
                                aggregates,
                                processedSequence,
                                _lastPerformance);
                        }
                        catch (Exception ex)
                        {
                            tagResult.Error = UnwrapInvocationException(ex);
                            _lastReadHadErrors = true;
                        }
                    }
                    results.Add(tagResult);
                }
                return results;
            }
            catch
            {
                _lastReadHadErrors = true;
                throw;
            }
            finally
            {
                if (timeSpanHandle >= 0)
                    TryInvoke(_readInterface, "releaseTimeSpan", new object[] { timeSpanHandle });
            }
        }

        private ProcessedHistoryResult ReadProcessedWithTimeSpan(
            TagResult tag,
            int timeSpanHandle,
            int intervalSeconds,
            ArrayList aggregates,
            string processedSequence,
            HistorianPerformanceMetrics performance)
        {
            object processed;
            Stopwatch rpcClock = Stopwatch.StartNew();
            try
            {
                processed = _readProcessed.Invoke(
                    _connection,
                    new object[]
                    {
                        timeSpanHandle,
                        tag.Handle,
                        _processedSampleType,
                        aggregates
                    });
            }
            finally
            {
                rpcClock.Stop();
                performance.RpcMilliseconds += rpcClock.ElapsedMilliseconds;
            }
            if (processed == null)
                throw new Exception("readProcessed() returned null.");

            ProcessedHistoryResult readResult = new ProcessedHistoryResult();
            readResult.Aggregate = "InterpolatedValue";
            readResult.IntervalSeconds = intervalSeconds;
            Stopwatch convertClock = Stopwatch.StartNew();
            try
            {
                readResult.ReturnedSlots = ToInt32(
                    GetMemberValue(processed, "nSamples"), 0);

                IEnumerable aggregateResults = GetMemberValue(processed, "dataSamples") as IEnumerable;
                if (aggregateResults == null)
                    throw new Exception("ProcessedHistorySamples.dataSamples is not enumerable.");

                int aggregateCount = 0;
                foreach (object aggregateResult in aggregateResults)
                {
                    aggregateCount++;
                    if (aggregateCount > 1)
                        throw new Exception("readProcessed() returned more than one aggregate result.");
                    IEnumerable samples = aggregateResult as IEnumerable;
                    if (samples == null)
                        throw new Exception("Processed aggregate result is not enumerable.");
                    foreach (object point in samples)
                    {
                        if (point == null)
                            continue;
                        performance.ReturnedSamples++;
                        ProcessedPointAccessorSet accessors = GetProcessedPointAccessors(point.GetType());
                        object archiveStatus = accessors.ArchiveStatus == null
                            ? null
                            : accessors.ArchiveStatus.GetValue(point);
                        if (HasArchiveStatusFlag(archiveStatus, 16))
                        {
                            readResult.InvalidSlots++;
                            performance.InvalidSamples++;
                            continue;
                        }
                        HistorySample sample = BuildHistorySample(
                            tag.Name,
                            point,
                            accessors,
                            archiveStatus,
                            processedSequence);
                        if (sample != null)
                        {
                            sample.Timestamp = ToCollectorLocalTime(sample.Timestamp);
                            readResult.Samples.Add(sample);
                        }
                        else
                        {
                            performance.InvalidSamples++;
                        }
                    }
                }
                if (aggregateCount != 1)
                    throw new Exception("readProcessed() did not return the requested aggregate.");
            }
            finally
            {
                convertClock.Stop();
                performance.SampleConvertMilliseconds += convertClock.ElapsedMilliseconds;
            }

            Stopwatch normalizeClock = Stopwatch.StartNew();
            bool fastPath;
            List<HistorySample> normalized;
            try
            {
                normalized = HistorySampleSet.NormalizeProcessed(
                    readResult.Samples,
                    out fastPath);
            }
            finally
            {
                normalizeClock.Stop();
                performance.NormalizeMilliseconds += normalizeClock.ElapsedMilliseconds;
            }
            if (fastPath)
                performance.NormalizeFastPathTags++;
            else
            {
                performance.NormalizeFallbackTags++;
                WriteLog("WARNING Processed normalization fallback tag=" + tag.Name);
            }
            if (!Object.ReferenceEquals(normalized, readResult.Samples))
            {
                readResult.Samples.Clear();
                readResult.Samples.AddRange(normalized);
            }
            return readResult;
        }

        private static Exception UnwrapInvocationException(Exception exception)
        {
            TargetInvocationException invocation = exception as TargetInvocationException;
            if (invocation != null && invocation.InnerException != null)
                return invocation.InnerException;
            return exception;
        }

        public void Dispose()
        {
            if (_readInterface != null && _connectionHandle >= 0)
                TryInvoke(_readInterface, "closeConnection", new object[] { _connectionHandle });
            _connectionHandle = -1;
            _connection = null;
            _readProcessed = null;
            _processedSampleType = null;
            _interpolatedAggregate = null;
            _pointAccessors = null;
            _readInterface = null;
            if (_resolverAttached)
            {
                AppDomain.CurrentDomain.AssemblyResolve -= CurrentDomainAssemblyResolve;
                _resolverAttached = false;
            }
        }

        private void EnsureConnected()
        {
            if (_connection == null || _readInterface == null || _readProcessed == null)
                throw new InvalidOperationException("HistorianClient is not connected.");
        }

        private Assembly LoadDvCHAssembly()
        {
            return LoadDeltaVAssembly("DeltaV.Historian.DvCHDataAccess.dll");
        }

        private Assembly LoadDeltaVAssembly(string fileName)
        {
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
            Console.WriteLine("DeltaV DLL  : " + assemblyPath);
            WriteLog("Loading DeltaV assembly: " + assemblyPath);
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

        private ProcessedPointAccessorSet GetProcessedPointAccessors(Type pointType)
        {
            if (_pointAccessors == null || _pointAccessors.PointType != pointType)
            {
                _pointAccessors = new ProcessedPointAccessorSet();
                _pointAccessors.PointType = pointType;
                _pointAccessors.Timestamp = FindMemberAccessor(pointType, "timestamp");
                _pointAccessors.Value = FindMemberAccessor(pointType, "value");
                _pointAccessors.DataType = FindMemberAccessor(pointType, "dataType");
                _pointAccessors.SequenceNo = FindMemberAccessor(pointType, "sequenceNo");
                _pointAccessors.ArchiveStatus = FindMemberAccessor(pointType, "archiveStatus");
                _pointAccessors.IsHistoryHole = FindMemberAccessor(pointType, "isHistoryHole");
                _pointAccessors.IsCRHole = FindMemberAccessor(pointType, "isCRHole");
                _pointAccessors.IsManuallyDeleted = FindMemberAccessor(pointType, "isManuallyDeleted");
                _pointAccessors.IsManuallyInserted = FindMemberAccessor(pointType, "isManuallyInserted");
                _processedPointAccessorBuildCount++;
            }
            return _pointAccessors;
        }

        private static object GetMemberValue(object instance, string name)
        {
            if (instance == null)
                return null;
            Type type = instance.GetType();
            string key = type.AssemblyQualifiedName + "|" + name;
            MemberAccessor accessor;
            lock (MemberAccessorLock)
            {
                if (!MemberAccessors.TryGetValue(key, out accessor))
                {
                    accessor = FindMemberAccessor(type, name);
                    MemberAccessors.Add(key, accessor);
                }
            }
            return accessor == null ? null : accessor.GetValue(instance);
        }

        private static MemberAccessor FindMemberAccessor(Type type, string name)
        {
            BindingFlags flags = BindingFlags.Public | BindingFlags.NonPublic |
                BindingFlags.Instance;
            PropertyInfo[] properties = type.GetProperties(flags);
            int i;
            for (i = 0; i < properties.Length; i++)
            {
                if (String.Equals(properties[i].Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    MemberAccessor accessor = new MemberAccessor();
                    accessor.Property = properties[i];
                    return accessor;
                }
            }
            FieldInfo[] fields = type.GetFields(flags);
            for (i = 0; i < fields.Length; i++)
            {
                if (String.Equals(fields[i].Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    MemberAccessor accessor = new MemberAccessor();
                    accessor.Field = fields[i];
                    return accessor;
                }
            }
            MethodInfo getter = type.GetMethod("get_" + name, flags);
            if (getter != null)
            {
                MemberAccessor accessor = new MemberAccessor();
                accessor.Getter = getter;
                return accessor;
            }
            return null;
        }

        private static HistorySample BuildHistorySample(
            string tagName,
            object point,
            ProcessedPointAccessorSet accessors,
            object archiveStatus,
            string processedSequence)
        {
            DateTime timestamp;
            object rawTimestamp = accessors.Timestamp == null
                ? null
                : accessors.Timestamp.GetValue(point);
            if (rawTimestamp is DateTime)
                timestamp = (DateTime)rawTimestamp;
            else if (!DateTime.TryParse(
                rawTimestamp == null ? "" : rawTimestamp.ToString(),
                out timestamp))
                return null;

            HistorySample sample = new HistorySample();
            sample.Tag = tagName;
            sample.Timestamp = timestamp;
            sample.Value = FormatValue(accessors.Value == null ? null : accessors.Value.GetValue(point));
            object dataType = accessors.DataType == null ? null : accessors.DataType.GetValue(point);
            sample.DataType = dataType == null ? "" : dataType.ToString();
            sample.Flags = BuildFlags(point, accessors);
            sample.SequenceNo = processedSequence;
            sample.ArchiveStatus = FormatValue(archiveStatus);
            return sample;
        }

        private static DateTime ToCollectorLocalTime(DateTime timestamp)
        {
            if (timestamp.Kind == DateTimeKind.Utc)
                return timestamp.ToLocalTime();
            return timestamp;
        }

        private static string BuildProcessedSequence(int intervalSeconds)
        {
            return "P:InterpolatedValue:" +
                intervalSeconds.ToString(CultureInfo.InvariantCulture);
        }

        private static Type FindLoadedType(string fullName)
        {
            Type direct = Type.GetType(fullName + ", DeltaV.Historian.Data", false);
            if (direct != null)
                return direct;
            Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();
            int index;
            for (index = 0; index < assemblies.Length; index++)
            {
                Type found = assemblies[index].GetType(fullName, false, true);
                if (found != null)
                    return found;
            }
            return null;
        }

        private static bool HasArchiveStatusFlag(object value, int flag)
        {
            if (value == null)
                return false;
            try
            {
                int numeric = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                return (numeric & flag) != 0;
            }
            catch
            {
                return value.ToString().IndexOf(
                    "AggregateValueInvalid",
                    StringComparison.OrdinalIgnoreCase) >= 0;
            }
        }

        private static int ToInt32(object value, int defaultValue)
        {
            if (value == null)
                return defaultValue;
            try { return Convert.ToInt32(value, CultureInfo.InvariantCulture); }
            catch { return defaultValue; }
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

        private static void SetHistorianResampleInterval(object timeSpan, TimeSpan interval)
        {
            MethodInfo[] methods = timeSpan.GetType().GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            int index;
            for (index = 0; index < methods.Length; index++)
            {
                if (!String.Equals(
                    methods[index].Name,
                    "setResampleInterval",
                    StringComparison.OrdinalIgnoreCase))
                    continue;
                ParameterInfo[] parameters = methods[index].GetParameters();
                if (parameters.Length != 1)
                    continue;
                Type parameterType = parameters[0].ParameterType.IsByRef ?
                    parameters[0].ParameterType.GetElementType() :
                    parameters[0].ParameterType;
                if (parameterType == typeof(TimeSpan))
                {
                    methods[index].Invoke(timeSpan, new object[] { interval });
                    return;
                }
            }
            throw new MissingMethodException(
                timeSpan.GetType().FullName,
                "setResampleInterval(TimeSpan)");
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

        private static string BuildFlags(object point, ProcessedPointAccessorSet accessors)
        {
            string result = "";
            if (ToBool(accessors.IsHistoryHole == null ? null : accessors.IsHistoryHole.GetValue(point), false)) result += "HistoryHole;";
            if (ToBool(accessors.IsCRHole == null ? null : accessors.IsCRHole.GetValue(point), false)) result += "CRHole;";
            if (ToBool(accessors.IsManuallyDeleted == null ? null : accessors.IsManuallyDeleted.GetValue(point), false)) result += "ManuallyDeleted;";
            if (ToBool(accessors.IsManuallyInserted == null ? null : accessors.IsManuallyInserted.GetValue(point), false)) result += "ManuallyInserted;";
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

        private sealed class ProcessedPointAccessorSet
        {
            public Type PointType;
            public MemberAccessor Timestamp;
            public MemberAccessor Value;
            public MemberAccessor DataType;
            public MemberAccessor SequenceNo;
            public MemberAccessor ArchiveStatus;
            public MemberAccessor IsHistoryHole;
            public MemberAccessor IsCRHole;
            public MemberAccessor IsManuallyDeleted;
            public MemberAccessor IsManuallyInserted;
        }

        private class MemberAccessor
        {
            public PropertyInfo Property;
            public FieldInfo Field;
            public MethodInfo Getter;

            public object GetValue(object instance)
            {
                try
                {
                    if (Property != null)
                        return Property.GetValue(instance, null);
                    if (Field != null)
                        return Field.GetValue(instance);
                    if (Getter != null)
                        return Getter.Invoke(instance, null);
                }
                catch
                {
                }
                return null;
            }
        }

    }
}
