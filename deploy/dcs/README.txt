DeltaV Processed History Collector
==================================

Runtime requirements:

  Windows with .NET Framework 3.5
  DeltaV Historian 10.3 assemblies
  x86 process
  normal user write access to this directory

The collector reads every tag in config\tags.txt with Historian readProcessed,
Aggregate.InterpolatedValue, and the interval configured by
[Sampling] IntervalSeconds (normally 10 seconds).

Setup:

1. Copy config\config.example.ini to config\config.ini.
2. Copy config\tags.example.txt to config\tags.txt.
3. Remove status, alarm, interlock, pulse, and invalid tags from tags.txt.
4. Set Collector Id, Receiver URL, and API key in config.ini.
5. Run scripts\status.cmd, then start-historysync.cmd in the package root for
   hidden background collection. Run stop-historysync.cmd to stop it.

Reliability behavior:

  Pending batches are retried oldest-first for BacklogDrainSeconds. The
  default safety limit is 50 batches or 100 MiB. When the limit is reached,
  state.ini records CollectionPaused=true and the continuous host remains
  alive without reading more Historian data. The paused state retries only the
  pending drain every PendingRetrySeconds. After the Receiver recovers and
  pending drains, collection resumes automatically.

  Each physical window reuses one Historian TimeSpan for serial Processed tag
  reads. Pending data is hashed once and sent from a stream over KeepAlive.

Commands:

  start-historysync.cmd
  stop-historysync.cmd
  scripts\start-historysync.cmd
  scripts\stop-historysync.cmd
  scripts\run.cmd
  scripts\sync.cmd
  scripts\send-pending.cmd
  scripts\status.cmd

`start-historysync.cmd` launches the same continuous host hidden under the
logged-in normal user. `stop-historysync.cmd` sends a named-event stop request;
the current cycle finishes before the process exits. `run.cmd` is the foreground
alternative and stops with Ctrl+C. This package does not install a service,
create a scheduled task, modify the registry, or contain Receiver/PostgreSQL
files. Batch CSV, spool, checkpoint, Receiver, and database protocols remain
unchanged.
