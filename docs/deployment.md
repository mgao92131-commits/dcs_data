# Deployment

## Package layout

The DCS package contains only:

    bin\\
    config\\
    scripts\\
    state\\
    logs\\

The v3.4.1 DCS runtime has no local batch queue, spool, pending directory or
`send` command. A prepared batch exists only in memory until the Receiver
returns a PostgreSQL database ACK.

## DCS runtime

The Historian producer is single-threaded. It reads and prepares windows in
order, while two fixed sender workers send at most two uncheckpointed batches.
`WaitForCapacity` runs before the next Historian read, so a full pipeline never
reads a third batch.

Only a contiguous ACK prefix can advance `CheckpointEnd`. If Batch N+1 is
ACKed before Batch N, N+1 remains in memory and no later batch is prepared.
When N is ACKed, the coordinator saves N and then any already ACKed successor
before releasing capacity.

Transient connection, timeout, HTTP 408/429 and HTTP 5xx failures retain the
same batch in its slot and retry it every `SendRetrySeconds`. HTTP 401/403,
other permanent 4xx responses and invalid ACKs stop the command. A stop request
interrupts retry waits; only the contiguous ACK prefix is persisted.

## Configuration

The DCS example uses:

    [Receiver]
    TimeoutSeconds=135
    SendRetrySeconds=30
    AckMode=database

The Receiver example uses `ImportTimeoutSeconds=45` and
`WriteTimeoutSeconds=120`. The timeout budget is therefore:

    ImportTimeoutSeconds < WriteTimeoutSeconds < DCS TimeoutSeconds
    45                    < 120                 < 135

Set the Collector Id, Receiver URL and API key before starting. The only DCS
state file is:

    [ContinuousSync]
    CheckpointEnd=yyyy-MM-dd HH:mm:ss.fffffff

After a restart, the collector reads again from `CheckpointEnd` minus the
configured overlap. Receiver sample-key idempotency makes this re-read safe.

## Field acceptance test

1. Start normal synchronization and record `CheckpointEnd`.
2. Disconnect the Receiver network.
3. Confirm the active batches retry every 30 seconds.
4. Confirm there are no local CSV files, no spool directory, and no third
   Historian batch read after the two pipeline slots are occupied.
5. Confirm `CheckpointEnd` does not advance while the oldest batch is blocked.
6. Restore the network and confirm the oldest batch is ACKed, the contiguous
   checkpoint advances, and the collector catches up automatically.
7. Verify PostgreSQL has no time gap and no duplicate sample keys.
