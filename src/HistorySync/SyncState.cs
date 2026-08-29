using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace DeltaVHistoryCLI
{
    class SyncState
    {
        public DateTime CheckpointEnd;

        public SyncState Copy()
        {
            SyncState copy = new SyncState();
            copy.CheckpointEnd = CheckpointEnd;
            return copy;
        }
    }

    class SyncStateStore
    {
        private readonly string _path;

        public SyncStateStore(string path)
        {
            _path = Path.GetFullPath(path);
        }

        public SyncState LoadOrCreate(DateTime initialPosition)
        {
            SyncState initial = new SyncState();
            initial.CheckpointEnd = initialPosition;
            return LoadOrCreate(initial);
        }

        public SyncState LoadOrCreate(SyncState initial)
        {
            if (!File.Exists(_path))
            {
                SyncState created = initial.Copy();
                Save(created);
                return created;
            }

            IniConfig config = IniConfig.Load(_path);
            SyncState state = new SyncState();
            state.CheckpointEnd = Parse(
                config.Get("ContinuousSync", "CheckpointEnd", ""),
                "CheckpointEnd");
            Validate(state);
            return state;
        }

        public void Save(SyncState state)
        {
            Validate(state);
            string directory = Path.GetDirectoryName(_path);
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            string temporary = _path + ".tmp." + Guid.NewGuid().ToString("N");
            try
            {
                using (FileStream stream = new FileStream(
                    temporary,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    4096,
                    FileOptions.WriteThrough))
                using (StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(true)))
                {
                    writer.WriteLine("[ContinuousSync]");
                    writer.WriteLine("CheckpointEnd=" + Format(state.CheckpointEnd));
                    writer.Flush();
                    stream.Flush();
                }

                if (File.Exists(_path))
                {
                    try
                    {
                        File.Replace(temporary, _path, null);
                    }
                    catch (UnauthorizedAccessException)
                    {
                        MoveIntoPlace(temporary);
                    }
                    catch (IOException)
                    {
                        MoveIntoPlace(temporary);
                    }
                }
                else
                    File.Move(temporary, _path);
            }
            finally
            {
                if (File.Exists(temporary))
                {
                    try { File.Delete(temporary); }
                    catch { }
                }
            }
        }

        private void MoveIntoPlace(string temporary)
        {
            string oldPath = _path + ".old." + Guid.NewGuid().ToString("N");
            File.Move(_path, oldPath);
            try
            {
                File.Move(temporary, _path);
            }
            catch
            {
                if (!File.Exists(_path) && File.Exists(oldPath))
                    File.Move(oldPath, _path);
                throw;
            }
            if (File.Exists(oldPath))
                File.Delete(oldPath);
        }

        private static void Validate(SyncState state)
        {
            if (state == null)
                throw new ArgumentNullException("state");
            if (state.CheckpointEnd == DateTime.MinValue ||
                state.CheckpointEnd == DateTime.MaxValue)
                throw new InvalidDataException("CheckpointEnd is outside the supported sync range.");
        }

        private static DateTime Parse(string text, string name)
        {
            DateTime value;
            if (!DateTime.TryParseExact(
                text,
                "yyyy-MM-dd HH:mm:ss.fffffff",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out value))
                throw new InvalidDataException("Invalid state value " + name + ": " + text);
            Validate(new SyncState { CheckpointEnd = value });
            return value;
        }

        private static string Format(DateTime value)
        {
            return value.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        }
    }
}
