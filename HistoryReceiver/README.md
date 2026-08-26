# HistoryReceiver v2

HistoryReceiver 校验 DCS 批次并在 PostgreSQL 事务提交后返回 ACK。

## 请求处理

```text
HTTP body
 -> Bearer authentication
 -> size / SHA-256 / CSV / row-count validation
 -> PostgreSQL transaction
 -> history_samples UPSERT
 -> imported_batches INSERT
 -> COMMIT
 -> committed=true ACK
```

配置 `SynchronousCommit=true` 时不会先写 inbox 再提前 ACK。数据库不可用或
事务失败返回 HTTP 503，DCS Collector 会把批次保存到 pending outbox。
语义校验失败的批次返回 HTTP 400 并移动到 rejected，不会被无限重试。

同一 BatchId、SHA-256 和行数可以安全重试；数据库中的 `imported_batches`
保证幂等。相同 BatchId 对应不同内容会失败。

## 构建与测试

```bat
go test ./...
go vet ./...
build.bat
```

## 数据库

```bat
psql -d deltav_history -f sql\create_tables.sql
```

`history_samples` 保存 Collector、Tag、时间、文本值、可选数值、数据类型、
Flags、SequenceNo、ArchiveStatus、BatchId 和接收时间。字符串状态值不会再因
`ParseFloat` 失败而丢弃。

样本身份目前使用：

```text
SequenceNo available:
  SHA256(CollectorId + Tag + original Timestamp + SequenceNo)

SequenceNo unavailable (temporary fallback):
  SHA256(CollectorId + Tag + original Timestamp + ValueText)
```

该 fallback 优先避免丢样本，但历史修订值可能并存。上线前必须在目标 DeltaV
版本确认 SequenceNo/ArchiveStatus 的实际属性名与稳定性，再决定最终唯一键。

旧 `history_raw` 保留为 v1 只读历史，不会因身份规则不同而自动复制到新表。
如需迁移，请通过 v2 backfill 重新读取对应历史范围。

## 健康检查

```text
GET http://RECEIVER_IP:8080/healthz
```

响应包括 `database_ok` 和 `inbox_batches`。Receiver archive 默认保留 30 天，
日志默认保留 30 天；rejected 批次只报警，不自动删除。
