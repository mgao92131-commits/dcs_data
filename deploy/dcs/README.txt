DeltaV Processed History Collector v3.4.1
=========================================

Runtime requirements:

  Windows 7 32-bit
  .NET Framework 3.5
  DeltaV Historian 10.3 assemblies
  x86 process
  normal-user write access to this directory

The collector reads config\\tags.txt with Historian readProcessed,
Aggregate.InterpolatedValue and [Sampling] IntervalSeconds (normally 10).

Setup:

  1. Copy config\\config.example.ini to config\\config.ini.
  2. Copy config\\tags.example.txt to config\\tags.txt.
  3. Remove status, alarm, interlock, pulse, digital-event and invalid tags.
  4. Set Collector Id, Receiver URL and API key.
  5. Run scripts\\status.cmd.
  6. Start start-historysync.cmd or scripts\\start-historysync.cmd.

The DCS has no local batch queue. A single Historian producer may prepare at
most two uncheckpointed in-memory batches, sent by two fixed workers. The
producer waits for a slot before reading the next Historian window. An ACK may
arrive out of order, but CheckpointEnd advances only across the contiguous ACK
prefix.

Transient connection, timeout, HTTP 408/429 and 5xx failures keep the same
batch in its slot and retry every SendRetrySeconds. Authentication, permanent
data errors and invalid ACKs stop the command. The Receiver must return
commit_level=database before CheckpointEnd can advance.

state\\state.ini contains only:

  [ContinuousSync]
  CheckpointEnd=yyyy-MM-dd HH:mm:ss.fffffff

After a restart, the collector reads again from CheckpointEnd minus the
configured overlap. Receiver sample-key idempotency makes the re-read safe.

Commands:

  start-historysync.cmd
  stop-historysync.cmd
  scripts\\start-historysync.cmd
  scripts\\stop-historysync.cmd
  scripts\\run.cmd
  scripts\\sync.cmd
  scripts\\status.cmd

The DCS package does not contain a spool directory or a send command. See
docs\\architecture.md, docs\\deployment.md and docs\\protocol.md in the source
repository for the complete flow and field definitions.
