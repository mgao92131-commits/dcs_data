using System;
using System.Globalization;
using System.IO;

namespace DeltaVHistoryCLI
{
    class SpoolMaintenance
    {
        public static void Run(
            IniConfig config,
            string spoolDirectory,
            string logsDirectory,
            SyncLogger log)
        {
            int archiveDays = config.GetInt("Maintenance", "ArchiveRetentionDays", 7);
            int logDays = config.GetInt("Maintenance", "LogRetentionDays", 30);
            int minimumFreeMB = config.GetInt("Maintenance", "MinFreeSpaceMB", 2048);
            if (archiveDays < 1 || logDays < 1 || minimumFreeMB < 128)
                throw new Exception("Invalid [Maintenance] retention or free-space setting.");

            int archivesDeleted = DeleteOldDirectories(
                Path.Combine(spoolDirectory, "archive"),
                DateTime.Now.AddDays(-archiveDays));
            int logsDeleted = DeleteOldFiles(
                logsDirectory,
                "*.log",
                DateTime.Now.AddDays(-logDays));
            int failedCount = Directory.GetDirectories(
                Path.Combine(spoolDirectory, "failed")).Length;
            int quarantineCount = Directory.GetDirectories(
                Path.Combine(spoolDirectory, "quarantine")).Length;

            if (archivesDeleted > 0 || logsDeleted > 0)
                log.Write("Maintenance deleted archives=" + archivesDeleted.ToString() +
                    " logs=" + logsDeleted.ToString());
            if (failedCount > 0)
                log.Write("WARNING: failed spool batches require attention count=" + failedCount.ToString());
            if (quarantineCount > 0)
                log.Write("WARNING: quarantined spool batches require attention count=" + quarantineCount.ToString());
        }

        public static void EnsureFreeSpace(string path, int minimumFreeMB)
        {
            string root = Path.GetPathRoot(Path.GetFullPath(path));
            DriveInfo drive = new DriveInfo(root);
            long required = (long)minimumFreeMB * 1024L * 1024L;
            if (drive.AvailableFreeSpace < required)
                throw new IOException(
                    "Insufficient disk space on " + root +
                    ". AvailableMB=" +
                    (drive.AvailableFreeSpace / 1024L / 1024L).ToString(CultureInfo.InvariantCulture) +
                    " RequiredMB=" + minimumFreeMB.ToString(CultureInfo.InvariantCulture));
        }

        private static int DeleteOldDirectories(string root, DateTime cutoff)
        {
            if (!Directory.Exists(root))
                return 0;
            int deleted = 0;
            string[] paths = Directory.GetDirectories(root);
            int i;
            for (i = 0; i < paths.Length; i++)
            {
                DirectoryInfo info = new DirectoryInfo(paths[i]);
                if (info.LastWriteTime < cutoff)
                {
                    Directory.Delete(paths[i], true);
                    deleted++;
                }
            }
            return deleted;
        }

        private static int DeleteOldFiles(string root, string pattern, DateTime cutoff)
        {
            if (!Directory.Exists(root))
                return 0;
            int deleted = 0;
            string[] paths = Directory.GetFiles(root, pattern);
            int i;
            for (i = 0; i < paths.Length; i++)
            {
                FileInfo info = new FileInfo(paths[i]);
                if (info.LastWriteTime < cutoff)
                {
                    File.Delete(paths[i]);
                    deleted++;
                }
            }
            return deleted;
        }
    }
}
