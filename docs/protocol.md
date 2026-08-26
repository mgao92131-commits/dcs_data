# History batch protocol

## Endpoint

```text
POST /api/history/batch
Content-Type: text/csv; charset=utf-8
Authorization: Bearer <API key>
```

The current Receiver endpoint is:

```text
http://192.168.1.10:8080/api/history/batch
```

Required headers:

| Header | Meaning |
| --- | --- |
| `X-Collector-Id` | Stable DCS collector identity |
| `X-Batch-Id` | Idempotency identity for the batch |
| `X-Batch-Mode` | `sync`, `init`, or `backfill` |
| `X-Historian-Server` | DeltaV Historian node, normally `APP` |
| `X-Range-Start` | Inclusive logical range start |
| `X-Range-End` | Logical range end |
| `X-Row-Count` | Number of CSV data rows |
| `X-Content-SHA256` | SHA-256 of the exact request body |

## CSV body

The first row is:

```text
Tag,Timestamp,Value,DataType,Flags,SequenceNo,ArchiveStatus
```

Values are quoted CSV fields. `Value` remains raw text; numeric values are
also stored as `value_double` when PostgreSQL can parse them.

## ACK

After PostgreSQL transaction commit, the Receiver returns HTTP 200:

```json
{
  "ok": true,
  "committed": true,
  "commit_level": "database",
  "batch_id": "...",
  "sha256": "...",
  "received_rows": 123
}
```

`commit_level=database` is required before the DCS collector advances
`LastCommittedEnd`. An asynchronous/inbox-only Receiver may return
`commit_level=inbox`; that can advance `LastAcceptedEnd` but never proves a
PostgreSQL commit. An ACK without `commit_level` is rejected by the v2 DCS
sender.

## Response handling

| HTTP status | DCS behavior |
| --- | --- |
| `200` | Validate ACK fields, then remove the pending batch |
| `400` | Permanent invalid batch; quarantine/fail and stop the continuous gap |
| `401`, `403` | Authentication failure; pause collection |
| `409` | Batch conflict/permanent ordering failure |
| `413` | Batch too large; fail/quarantine for operator review |
| `5xx`, timeout, connection error | Keep pending and retry later |
