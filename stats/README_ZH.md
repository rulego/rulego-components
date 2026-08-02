# stats

基于 [streamsql](https://github.com/rulego/streamsql) 引擎的 RuleGo 流处理节点。对实时 IoT/边缘遥测数据跑 SQL：过滤/转换、窗口聚合、CEP 模式识别、流-表 JOIN 富化。

文档：https://rulego.cc/pages/streamsql-overview/

两个 Processor 节点（包 `stats/streamsql`）：

| 节点 | 查询类型 | 处理方式 | 输出关系 |
|------|---------|----------|----------|
| `x/streamTransform` | 非聚合（过滤 / 转换 / JOIN 富化） | 同步（`EmitSync`） | `Success` / `False` / `Failure` |
| `x/streamAggregator` | 聚合 / 窗口 / CEP（含 JOIN + 聚合） | 异步（`Emit`） | `Success` / `Failure` / `stream_event` |

每个节点持有独立的 `streamsql` 实例；内部日志已接入 RuleGo 的日志体系。

## x/streamTransform

过滤、投影、字段计算，或对每行做流-表 JOIN 富化。逐条同步处理，命中并转换成功走 `Success`，WHERE 不命中 / `changed_cols` 无变化走 `False`（携带原始行），出错走 `Failure`。支持单条与数组 JSON 输入。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `sql` | string | 是 | 非聚合 SQL，如 `SELECT temperature, humidity FROM stream WHERE temperature > 20` |
| `tables` | []TableConfig | 否 | 流-表 JOIN 的元数据表（见下） |

## x/streamAggregator

窗口 / 分组聚合或 CEP 模式识别（`MATCH_RECOGNIZE`）。原始消息走 `Success`；结果（聚合窗口触发或 CEP 模式命中）走 `stream_event` 关系。

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `sql` | string | 是 | 带 GROUP BY / 窗口函数的聚合 SQL，如 `SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('5s')`；或 CEP 的 `MATCH_RECOGNIZE`，如 `SELECT * FROM stream MATCH_RECOGNIZE (PATTERN (A{3}) DEFINE A AS temperature > 50)` |
| `tables` | []TableConfig | 否 | 流-表 JOIN 的元数据表（见下） |

## 元数据表（流-表 JOIN）

注册元数据表并在 SQL 里 JOIN，用维数据（设备→位置/类型等）富化流数据。JOIN 索引键由 `ON` 子句自动推导，因此表 `name` 必须与 JOIN 目标一致。

| 字段 | 说明 |
|------|------|
| `name` | 表名；**必须出现在 SQL 的 JOIN 里**（如 `JOIN meta m` → `name: "meta"`），否则节点初始化失败 |
| `source` | `file` / `http`（UI）。后端另支持 `inline`（行内嵌配置，不刷新） |
| `path` | 文件路径（file）或 GET URL（http） |
| `format` | `json` / `csv`（默认 `json`） |
| `refresh` | 刷新间隔，如 `30s`。空 = file/http 默认 **1 小时**；`inline` 不刷新 |
| `headers` / `timeout` | 仅 HTTP。`headers` 为 JSON 对象；`timeout` 如 `10s`（默认 10s） |

刷新失败时保留旧快照（绝不以空表覆盖）。刷新在后台 goroutine 运行，节点 `Destroy` 时停止。

### 示例——用设备元数据富化遥测

```json
{
  "id": "enrich",
  "type": "x/streamTransform",
  "name": "设备富化",
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

`device_meta.json` 含 `{"deviceId":"d1","location":"plantA","type":"temp"}` 时，输入 `{"deviceId":"d1","temp":35}` → `{"deviceId":"d1","location":"plantA","type":"temp"}`。

JOIN 也可与聚合同用，如：
```sql
SELECT m.location, AVG(temp) AS avg_temp
FROM stream JOIN meta m ON deviceId = m.deviceId
GROUP BY m.location, TumblingWindow('5s')
```

## SQL 能力

- **子句**：`SELECT`（投影/别名/表达式）、`WHERE`、`GROUP BY`、`HAVING`、`ORDER BY`、`LIMIT`、`DISTINCT`、`JOIN`（INNER / LEFT，流-表，复合键）、`CASE WHEN`、`MATCH_RECOGNIZE`（CEP：PATTERN / DEFINE / MEASURES），以及 `WITH(...)` 选项（事件时间 `TIMESTAMP`/`TIMEUNIT`、`ALLOWEDLATENESS`、`IDLETIMEOUT`、`STATETTL`）。
- **窗口**：`TumblingWindow`、`SlidingWindow`、`SessionWindow`、`CountingWindow`、`Global Window ... TRIGGER WHEN`。
- **函数**：聚合（`sum`/`avg`/`min`/`max`/`count`/`stddev`/`var`/`median`/`percentile`/`collect`/`first_value`/`last_value`/...）、窗口边界（`window_start`/`window_end`/`row_number`/`lead`/...）、标量（数学/字符串/转换/日期时间/JSON/哈希/数组/类型判断/条件）。完整列表见 streamsql 文档。

## 依赖

需要 streamsql **>= v1.0.0**（新增流-表 JOIN API：`RegisterTable`）。更早版本（如 `v0.10.6`）没有。
