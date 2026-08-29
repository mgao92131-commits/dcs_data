# DCS processed collector deployment

## Build

The supported build command is:

    scripts\package-dcs.bat

The package at artifacts\dcs_data contains only:

    bin\
    config\
    scripts\
    state\
    logs\

There is no local batch-file directory and no send command.

## Setup

1. Copy the package to a normal-user writable local directory.
2. Copy config\config.example.ini to config\config.ini.
3. Copy config\tags.example.txt to config\tags.txt.
4. Remove status, alarm, interlock, pulse, digital-event and invalid tags.
5. Set the Collector Id, Receiver URL and API key.
6. Run scripts\status.cmd.
7. Start with start-historysync.cmd or scripts\start-historysync.cmd.

run.cmd is the foreground diagnostic host. stop-historysync.cmd sends the
named-event stop request. Stop interrupts the fixed retry wait and exits after
the active operation observes the stop event.

## Configuration

The DCS configuration must define:

    [Historian]
    Server=APP
    ConnectRetries=3
    RetrySeconds=10

    [Sampling]
    IntervalSeconds=10
    MaxFailedTagsPerBatch=5

    [Sync]
    IntervalMinutes=5
    EndDelaySeconds=30
    OverlapSeconds=60
    MaxWindowMinutes=30
    MinWindowSeconds=10

    [Batch]
    TargetRows=25000
    MaxRows=50000
    TargetBytes=10485760
    MaxBytes=20971520

    [Files]
    Tags=..\config\tags.txt
    Logs=..\logs
    State=..\state\state.ini

    [Receiver]
    Enabled=true
    Url=http://192.168.1.10:8080/api/history/batch
    TimeoutSeconds=105
    SendRetrySeconds=30
    AckMode=database
    ApiKey=CHANGE_ME_BEFORE_USE

No legacy queue, pause, drain or asynchronous ACK settings are read.

## Runtime behavior

At first start, if state.ini does not exist, the collector initializes
CheckpointEnd to a 15-minute bootstrap point before the current completed time.
Each subsequent run reads from CheckpointEnd - OverlapSeconds.

For each window the order is fixed:

    Historian read
      -> encode in memory
      -> send
      -> wait for PostgreSQL database ACK
      -> save CheckpointEnd
      -> next window

Transient Receiver failure blocks the active Batch and retries it every
SendRetrySeconds. No later Historian window is read while that retry loop is
active. Permanent data or authentication errors stop the current command and
leave the checkpoint unchanged. After the cause is corrected, rerun the
command; the Receiver sample-key idempotency handles any re-read.

status.cmd reports the configured Historian, Receiver reachability,
CheckpointEnd, sync lag, AckMode=database, and the last logged error.

## 现场验收

1. Start normal synchronization and note CheckpointEnd.
2. Disconnect the Receiver network.
3. Confirm logs show retries of one Batch every 30 seconds.
4. Confirm no new local CSV files are created, the Historian read count does
   not advance to the next window, and CheckpointEnd is unchanged.
5. Restore the network.
6. Confirm the same Batch receives database ACK, checkpoint advances, and the
   collector catches up.
7. Check PostgreSQL for no time gap and no duplicate sample keys.
