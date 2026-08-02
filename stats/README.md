# stats

Stream-processing nodes for RuleGo, built on the [streamsql](https://github.com/rulego/streamsql) engine. Run SQL over real-time IoT/edge telemetry: filter/transform, windowed aggregation, CEP pattern recognition, and stream-table JOIN enrichment.

Docs: https://rulego.cc/en/pages/streamsql-overview/

Two Processor nodes (package `stats/streamsql`):

| Node | Query type | Processing | Output relations |
|------|-----------|------------|------------------|
| `x/streamTransform` | Non-aggregation (filter / transform / JOIN enrichment) | Synchronous (`EmitSync`) | `Success` / `False` / `Failure` |
| `x/streamAggregator` | Aggregation / window / CEP (incl. JOIN + aggregation) | Asynchronous (`Emit`) | `Success` / `Failure` / `stream_event` |

Each node owns its own `streamsql` instance; its internal logging is wired into RuleGo's logger.

## x/streamTransform

Filter, project, compute fields, or enrich each row via stream-table JOIN. Each input row is processed synchronously and routed to `Success` (matched + transformed), `False` (WHERE not matched / `changed_cols` no change — carries the original row), or `Failure` (error). Supports single and array JSON input.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | string | yes | Non-aggregation SQL, e.g. `SELECT temperature, humidity FROM stream WHERE temperature > 20` |
| `tables` | []TableConfig | no | Metadata tables for stream-table JOIN (see below) |

## x/streamAggregator

Windowed / grouped aggregation or CEP pattern recognition (`MATCH_RECOGNIZE`). The original message passes on `Success`; results (an aggregation window firing or a CEP pattern match) are sent on the `stream_event` relation.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sql` | string | yes | Aggregation SQL with GROUP BY / window functions, e.g. `SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('5s')`; or `MATCH_RECOGNIZE` for CEP, e.g. `SELECT * FROM stream MATCH_RECOGNIZE (PATTERN (A{3}) DEFINE A AS temperature > 50)` |
| `tables` | []TableConfig | no | Metadata tables for stream-table JOIN (see below) |

## Metadata tables (stream-table JOIN)

Enrich stream rows with dimension data (device → location/type, etc.) by registering metadata tables and JOINing them in SQL. The JOIN index key is auto-derived from the `ON` clause, so the table `name` must match the JOIN target.

| Field | Description |
|-------|-------------|
| `name` | Table name; **must appear in the SQL's JOIN** (e.g. `JOIN meta m` → `name: "meta"`), otherwise node init fails |
| `source` | `file` / `http` (UI). Backend also supports `inline` (rows embedded in config, never refreshed) |
| `path` | File path (file) or GET URL (http) |
| `format` | `json` / `csv` (default `json`) |
| `refresh` | Reload interval, e.g. `30s`. Empty = default **1h** for file/http; `inline` never refreshes |
| `headers` / `timeout` | HTTP only. `headers` is a JSON object; `timeout` e.g. `10s` (default 10s) |

On reload/refresh failure the previous snapshot is kept (never overwritten with empty data). Refresh runs on a background goroutine that is stopped on node `Destroy`.

### Example — enrich telemetry with device metadata

```json
{
  "id": "enrich",
  "type": "x/streamTransform",
  "name": "device enrichment",
  "configuration": {
    "sql": "SELECT deviceId, m.location, m.type FROM stream s LEFT JOIN meta m ON s.deviceId = m.deviceId WHERE s.temp > 30",
    "tables": [
      {
        "name": "meta",
        "source": "file",
        "path": "/etc/rulego/device_meta.json",
        "format": "json",
        "refresh": "30s"
      }
    ]
  }
}
```

With `device_meta.json` containing `{"deviceId":"d1","location":"plantA","type":"temp"}`, input `{"deviceId":"d1","temp":35}` → `{"deviceId":"d1","location":"plantA","type":"temp"}`.

JOIN also works with aggregation, e.g.:
```sql
SELECT m.location, AVG(temp) AS avg_temp
FROM stream JOIN meta m ON deviceId = m.deviceId
GROUP BY m.location, TumblingWindow('5s')
```

## SQL capabilities

- **Clauses**: `SELECT` (projection / alias / expression), `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `DISTINCT`, `JOIN` (INNER / LEFT, stream-table, composite keys), `CASE WHEN`, `MATCH_RECOGNIZE` (CEP: PATTERN / DEFINE / MEASURES), and `WITH(...)` options (event-time `TIMESTAMP`/`TIMEUNIT`, `ALLOWEDLATENESS`, `IDLETIMEOUT`, `STATETTL`).
- **Windows**: `TumblingWindow`, `SlidingWindow`, `SessionWindow`, `CountingWindow`, `Global Window ... TRIGGER WHEN`.
- **Functions**: aggregations (`sum`/`avg`/`min`/`max`/`count`/`stddev`/`var`/`median`/`percentile`/`collect`/`first_value`/`last_value`/...), window-bound (`window_start`/`window_end`/`row_number`/`lead`/...), and scalars (math, string, conversion, datetime, JSON, hash, array, type-check, conditional). See the streamsql docs for the full list.

## Dependencies

Requires streamsql **>= v1.0.0** (adds the stream-table JOIN API: `RegisterTable`). Earlier versions (e.g. `v0.10.6`) lack it.
