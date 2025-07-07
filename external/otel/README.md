# OpenTelemetry Metrics Component

This component allows you to send metrics to OpenTelemetry Collector using OTLP protocol.

## Configuration

```json
{
  "id": "otel1",
  "type": "x/otel",
  "configuration": {
    "server": "localhost:4318",
    "protocol": "HTTP",
    "metricsExpr": "${msg.metrics}",
    "metrics": [
      {
        "metricName": "http_requests_total",
        "description": "Total HTTP requests",
        "unit": "1",
        "opType": "COUNTER",
        "value": "${msg.value}",
        "labels": "${msg.labels}"
      }
    ]
  }
}
```

## Configuration Parameters

- **server**: OTLP endpoint address (e.g., `localhost:4318` for HTTP, `localhost:4317` for gRPC)
- **protocol**: Transport protocol, supports `HTTP` and `GRPC` (default: `HTTP`)
- **metricsExpr**: Expression to extract metrics from message payload (supports single metric object or array)
- **metrics**: Static metric configurations that will be created during initialization

## Metric Configuration

Each metric supports the following fields:

- **metricName**: Name of the metric
- **description**: Description of the metric
- **unit**: Unit of measurement (e.g., "1" for counters, "s" for seconds, "B" for bytes)
- **opType**: Operation type - `COUNTER`, `GAUGE`, or `HISTOGRAM`
- **value**: Metric value (supports expressions like `${msg.value}`)
- **labels**: Metric labels as JSON object (supports expressions like `${msg.labels}`)

## Supported Metric Types

### Counter
Monotonically increasing values (e.g., request count, error count)

```json
{
  "metricName": "requests_total",
  "description": "Total number of requests",
  "unit": "1",
  "opType": "COUNTER",
  "value": "1",
  "labels": "{\"method\":\"GET\",\"status\":\"200\"}"
}
```

### Gauge
Values that can go up and down (e.g., memory usage, temperature)

```json
{
  "metricName": "memory_usage_bytes",
  "description": "Current memory usage",
  "unit": "B",
  "opType": "GAUGE",
  "value": "${msg.memoryUsage}",
  "labels": "{\"instance\":\"server1\"}"
}
```

### Histogram
Distribution of values (e.g., request duration, response size)

```json
{
  "metricName": "request_duration_seconds",
  "description": "Request duration in seconds",
  "unit": "s",
  "opType": "HISTOGRAM",
  "value": "${msg.duration}",
  "labels": "{\"endpoint\":\"${msg.endpoint}\"}"
}
```

## Usage Examples

### Static Metrics
Define metrics in the configuration that will be created during initialization:

```json
{
  "server": "localhost:4318",
  "protocol": "HTTP",
  "metrics": [
    {
      "metricName": "api_requests_total",
      "description": "Total API requests",
      "unit": "1",
      "opType": "COUNTER",
      "value": "${msg.count}",
      "labels": "${msg.labels}"
    }
  ]
}
```

Message payload:
```json
{
  "count": 1,
  "labels": {
    "method": "POST",
    "endpoint": "/api/users",
    "status": "201"
  }
}
```

### Dynamic Metrics
Extract metric configurations from message payload:

```json
{
  "server": "localhost:4318",
  "protocol": "HTTP",
  "metricsExpr": "${msg.metrics}"
}
```

Message payload with single metric:
```json
{
  "metrics": {
    "metricName": "custom_counter",
    "description": "Custom counter metric",
    "unit": "1",
    "opType": "COUNTER",
    "value": 5.0,
    "labels": {
      "source": "application",
      "environment": "production"
    }
  }
}
```

Message payload with multiple metrics:
```json
{
  "metrics": [
    {
      "metricName": "batch_processed",
      "description": "Number of items processed in batch",
      "unit": "1",
      "opType": "COUNTER",
      "value": 100.0,
      "labels": {
        "batch_id": "batch_001"
      }
    },
    {
      "metricName": "processing_time",
      "description": "Time taken to process batch",
      "unit": "s",
      "opType": "HISTOGRAM",
      "value": 2.5,
      "labels": {
        "batch_id": "batch_001"
      }
    }
  ]
}
```

### Mixed Configuration
You can combine static metrics with dynamic metrics:

```json
{
  "server": "localhost:4318",
  "protocol": "HTTP",
  "metricsExpr": "${msg.dynamicMetrics}",
  "metrics": [
    {
      "metricName": "static_counter",
      "description": "Static counter metric",
      "unit": "1",
      "opType": "COUNTER",
      "value": "1",
      "labels": "{\"type\":\"static\"}"
    }
  ]
}
```

## Environment Variables

The component respects the following environment variables for testing:

- `SKIP_OTEL_TESTS`: Set to "true" to skip OTel tests
- `OTEL_EXPORTER_OTLP_ENDPOINT`: HTTP endpoint for OTLP exporter
- `OTEL_EXPORTER_OTLP_GRPC_ENDPOINT`: gRPC endpoint for OTLP exporter

## Testing

Run the tests with:

```bash
go test -v ./external/otel/
```

The tests include:
- Basic node initialization
- Counter, Gauge, and Histogram metrics
- Dynamic metrics from message payload
- Both HTTP and gRPC protocols
- Configuration validation
- Error handling