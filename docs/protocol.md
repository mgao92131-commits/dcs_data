# History batch protocol

## Endpoint

    POST /api/history/batch
    Content-Type: text/csv; charset=utf-8
    Authorization: Bearer <API key>

## Required headers

| Header | Meaning |
| --- | --- |
| X-Collector-Id | Stable DCS collector identity |
| X-Batch-Id | Identity of the in-memory batch |
| X-Batch-Mode | sync, init, or backfill |
| X-Historian-Server | DeltaV Historian node |
| X-Range-Start | Inclusive logical range start |
| X-Range-End | Logical range end |
| X-Row-Count | Number of CSV data rows |
| X-Content-SHA256 | SHA-256 of the exact request body |

## CSV body

The first row is:

    Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus

All fields are quoted. Value remains raw text; numeric values may also be
stored as value_double by PostgreSQL.

## Database ACK

The Receiver returns HTTP 200 only after the PostgreSQL transaction commits:

    {
      "ok": true,
      "committed": true,
      "commit_level": "database",
      "batch_id": "...",
      "sha256": "...",
      "received_rows": 123
    }

DCS validates every field. commit_level=database is the only ACK that can
advance CheckpointEnd.

## Bounded pipeline semantics

The DCS producer may have at most two batches that are not covered by the
durable checkpoint. Historian reads are blocked before a third batch is
prepared. The two batches may be received and processed concurrently by the
Receiver, but CheckpointEnd advances only across a contiguous sequence:

    Checkpoint = N-1
    N   = sending
    N+1 = ACKed out of order
    N+2 = not prepared

An out-of-order ACK is retained in memory. When N is ACKed, the DCS saves N and
then any consecutive ACKed successor, releasing one or more slots only after
the checkpoint writes complete. A transient failure therefore cannot create an
unbounded in-memory backlog.

## Failure handling

| Response/failure | DCS behavior |
| --- | --- |
| HTTP 200 with a valid database ACK | Save the contiguous CheckpointEnd prefix, then release capacity |
| connection/TCP error or timeout | Wait SendRetrySeconds, resend the same Batch |
| HTTP 408/429 or 5xx | Wait SendRetrySeconds, resend the same Batch |
| HTTP 401/403 | Stop with authentication error |
| HTTP 400/409/413 or other permanent 4xx | Stop with permanent batch error |
| malformed ACK or non-database commit level | Stop with permanent protocol error |

The retry request keeps the same BatchId, SHA-256 and body while the process is
alive. If the process restarts before saving the checkpoint, it re-reads the
old range; Receiver/database idempotency makes that safe.
