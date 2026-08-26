using System;
using System.IO;
using System.ServiceProcess;
using System.Threading;

namespace DeltaVHistoryCLI
{
    class HistorySyncService : ServiceBase
    {
        private Thread _worker;
        private ManualResetEvent _stop;

        public HistorySyncService()
        {
            ServiceName = "DeltaVHistorySync";
            CanStop = true;
            AutoLog = true;
        }

        protected override void OnStart(string[] args)
        {
            _stop = new ManualResetEvent(false);
            _worker = new Thread(new ThreadStart(RunWorker));
            _worker.IsBackground = true;
            _worker.Name = "DeltaV History Sync Worker";
            _worker.Start();
        }

        protected override void OnStop()
        {
            if (_stop != null)
                _stop.Set();
            if (_worker != null && !_worker.Join(30000))
                EventLog.WriteEntry("Sync worker did not stop within 30 seconds.");
        }

        private void RunWorker()
        {
            while (!_stop.WaitOne(0, false))
            {
                try
                {
                    int result = SyncProgram.ExecuteCycle(new string[] { "sync" });
                    if (result != 0 && result != 5 && result != 40)
                        EventLog.WriteEntry("HistorySync returned " + result.ToString());
                }
                catch (Exception ex)
                {
                    EventLog.WriteEntry(ex.ToString());
                }

                int interval = ReadIntervalMilliseconds();
                if (_stop.WaitOne(interval, false))
                    return;
            }
        }

        private static int ReadIntervalMilliseconds()
        {
            try
            {
                string configPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "config.ini");
                IniConfig config = IniConfig.Load(configPath);
                int minutes = config.GetInt("Sync", "IntervalMinutes", 5);
                if (minutes > 0 && minutes <= 1440)
                    return checked(minutes * 60 * 1000);
            }
            catch { }
            return 5 * 60 * 1000;
        }

        public static int RunService()
        {
            ServiceBase.Run(new HistorySyncService());
            return 0;
        }

        public static int RunConsole()
        {
            ManualResetEvent stop = new ManualResetEvent(false);
            ConsoleCancelEventHandler handler = delegate(object sender, ConsoleCancelEventArgs e)
            {
                e.Cancel = true;
                stop.Set();
            };
            Console.CancelKeyPress += handler;
            Console.WriteLine("HistorySync console host started. Press Ctrl+C to stop.");
            try
            {
                while (!stop.WaitOne(0, false))
                {
                    int result = SyncProgram.ExecuteCycle(new string[] { "sync" });
                    Console.WriteLine("Sync exit code: " + result.ToString());
                    if (stop.WaitOne(ReadIntervalMilliseconds(), false))
                        break;
                }
            }
            finally
            {
                Console.CancelKeyPress -= handler;
                stop.Close();
            }
            return 0;
        }
    }
}
