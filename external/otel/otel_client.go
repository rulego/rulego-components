package otel

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rulego/rulego/utils/str"

	"github.com/rulego/rulego/utils/json"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	COUNTER   = "COUNTER"
	GAUGE     = "GAUGE"
	HISTOGRAM = "HISTOGRAM"
)

// Configuration example
//
//	{
//		"id": "otel1",
//		"type": "x/otelMetrics",
//		"configuration": {
//		  "endpoint": "localhost:4317",
//		  "metricsExpr": "${msg.metrics}", // Supports single metric objects or arrays of metric objects
//		  "metrics": [{ // List of metrics supported by configuration methods
//		    "metricName": "http_requests",
//		    "description": "HTTP requests made",
//		    "unit": "1",
//		    "opType": "COUNTER",
//		    "value": "${msg.value}", // Supports expressions
//		    "labels": "${msg.labels}" // Supports expressions
//		  }]
//		}
//	  }
//
// Register the node
func init() {
	_ = rulego.Registry.Register(&OtelNode{})
}

// MetricConfig Configuration of individual metrics
type MetricConfig struct {
	MetricName  string `json:"metricName" label:"Metric Name" desc:"Metric name" required:"true"`
	Description string `json:"description" label:"Description" desc:"Metric description"`
	Unit        string `json:"unit" label:"Unit" desc:"Metric unit, e.g. ms, count"`
	OpType      string `json:"opType" label:"Op Type" desc:"Metric operation: counter(increment), gauge(set), histogram(record)" required:"true"`
	Value       string `json:"value" label:"Value" desc:"Metric value expression, supports ${metadata.key} and ${msg.key} substitution"`
	Labels      string `json:"labels" label:"Labels" desc:"Metric labels in JSON format, e.g. {\"host\":\"server1\"}"`
}

// MetricValue is a single metric configuration, with Value and Labels values already obtained
type MetricValue struct {
	// Indicator name
	MetricName string `json:"metricName"`
	// Indicator description
	Description string `json:"description"`
	// Unit is the unit used to describe the value of an indicator. For example, for counters (COUNTER), the unit might be "1" (representing count);
	// For time measurements, units may be "s" (seconds), "ms" (milliseconds), "us" (microseconds), or "ns" (nanoseconds);
	// For data measurements, the unit may be "B" (bytes);
	// For proportional measurements, the unit might be "%" (percentage).
	// OpenTelemetry uses a SI (International System of Units) based unit system, but also supports some common non-SI units.
	// For example, for HTTP request counts, the unit can be "1"; For HTTP response time, the unit can be "s".
	Unit string `json:"unit"`
	// Operation types: COUNTER, GAUGE, HISTOGRAM
	OpType string `json:"opType"`
	// Indicator values
	Value float64 `json:"value"`
	// Tags
	Labels map[string]string `json:"labels"`
}

func (m MetricValue) GetConfig() MetricConfig {
	return MetricConfig{
		MetricName:  m.MetricName,
		Description: m.Description,
		Unit:        m.Unit,
		OpType:      m.OpType,
		Value:       "",
		Labels:      "",
	}
}

// OtelNodeConfiguration node configuration
type OtelNodeConfiguration struct {
	Server     string         `json:"server" label:"Server" desc:"OTel Collector address, format: host:port" required:"true" ref:"primary"`
	Protocol   string         `json:"protocol" label:"Protocol" desc:"Export protocol: grpc, http, default grpc"`
	MetricExpr string         `json:"metricExpr" label:"Metric Expression" desc:"JSON expression to extract metrics from message"`
	Metrics    []MetricConfig `json:"metrics" label:"Metrics" desc:"Custom metric configuration list"`
}

// Metric indicator example
type Metric struct {
	Config         MetricConfig
	ValueTemplate  el.Template
	LabelsTemplate el.Template
	Counter        metric.Float64Counter
	Gauge          metric.Float64UpDownCounter
	Histogram      metric.Float64Histogram
}

// OtelNode OpenTelemetry client component for recording various metrics
// Run to obtain metric data through message loads and send it to backend systems via OTLP protocol, such as Prometheus, Datadog, InfluxDB, etc
type OtelNode struct {
	base.SharedNode[*Client]
	// Node configuration
	Config OtelNodeConfiguration
	// Indicator cache, where key is the indicator name
	metricsCache map[string]*Metric
	// Indicator cache locks
	metricsCacheMu sync.RWMutex
	// Indicator expression
	metricsExpr el.Template
}

// Type returns the component type
func (x *OtelNode) Type() string {
	return "x/otel"
}

func (x *OtelNode) New() types.Node {
	return &OtelNode{
		Config: OtelNodeConfiguration{
			Server:     "localhost:4318",
			Protocol:   "HTTP",
			MetricExpr: "${msg.metrics}",
		},
		metricsCache: make(map[string]*Metric),
	}
}

// Init initializes the component
func (x *OtelNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if x.Config.Server == "" {
		return errors.New("server is required")
	}
	// Compile the indicator expression
	if x.Config.MetricExpr != "" {
		if metricsExpr, err := el.NewTemplate(strings.TrimSpace(x.Config.MetricExpr)); err != nil {
			return err
		} else {
			x.metricsExpr = metricsExpr
		}
	}
	x.Config.Protocol = strings.TrimSpace(strings.ToUpper(x.Config.Protocol))
	if x.Config.Protocol == "" {
		x.Config.Protocol = "HTTP"
	}
	// Initialize the shared MeterProvider
	err = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*Client, error) {
		return x.initMeterProvider()
	}, func(client *Client) error {
		// Cleanup callback function
		return client.MeterProvider.Shutdown(context.Background())
	})

	// Precompiled configuration of indicator templates
	for _, metricConfig := range x.Config.Metrics {
		metricConfig.MetricName = strings.TrimSpace(metricConfig.MetricName)
		if metricConfig.MetricName == "" {
			continue
		}
		metricConfig.Unit = strings.TrimSpace(metricConfig.Unit)
		metricConfig.OpType = strings.TrimSpace(metricConfig.OpType)
		metricConfig.Value = strings.TrimSpace(metricConfig.Value)
		metricConfig.Labels = strings.TrimSpace(metricConfig.Labels)

		var valueTemplate, labelsTemplate el.Template
		if metricConfig.Value != "" {
			valueTemplate, err = el.NewTemplate(metricConfig.Value)
			if err != nil {
				return err
			}
		}
		if metricConfig.Labels != "" {
			labelsTemplate, err = el.NewTemplate(metricConfig.Labels)
			if err != nil {
				return err
			}
		}
		m, err := x.getOrCreateMetric(metricConfig)
		if err != nil {
			return err
		}
		m.ValueTemplate = valueTemplate
		m.LabelsTemplate = labelsTemplate
		x.metricsCache[metricConfig.MetricName] = m
	}

	return nil
}

type Client struct {
	MeterProvider *sdkmetric.MeterProvider
	Meter         metric.Meter
}

// buildEndpointURL
// For the HTTP protocol, WithEndpoint only requires the host: port format
// For the gRPC protocol, WithEndpoint requires host: port format
func (x *OtelNode) buildEndpointURL() string {
	server := strings.TrimSpace(x.Config.Server)

	// If the server is empty, use the default value
	if server == "" {
		server = "localhost:4318"
	}

	// Remove protocol prefixes (if present)
	if strings.HasPrefix(server, "http://") {
		server = strings.TrimPrefix(server, "http://")
	} else if strings.HasPrefix(server, "https://") {
		server = strings.TrimPrefix(server, "https://")
	}

	// Remove the path section (if present)
	if idx := strings.Index(server, "/"); idx != -1 {
		server = server[:idx]
	}

	// Return to a pure host: port format
	return server
}

// initMeterProvider initializes MeterProvider
func (x *OtelNode) initMeterProvider() (*Client, error) {
	var exporter sdkmetric.Exporter
	var err error

	// Build a complete endpoint URL
	endpoint := x.buildEndpointURL()

	// Create an OTLP exporter
	if strings.ToUpper(x.Config.Protocol) == "GRPC" {
		// Create an OTLP gRPC exporter
		exporter, err = otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(endpoint),
			otlpmetricgrpc.WithInsecure(),
		)
	} else {
		// Create an OTLP HTTP exporter
		exporter, err = otlpmetrichttp.New(
			context.Background(),
			otlpmetrichttp.WithEndpoint(endpoint),
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	// Create a MeterProvider
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(10*time.Second),
			),
		),
	)
	client := &Client{
		MeterProvider: mp,
		Meter:         mp.Meter(x.Type()),
	}

	return client, nil
}

// OnMsg processes a message
func (x *OtelNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	var metrics []MetricValue
	// Manage the allocation indicators
	for _, metricConfig := range x.Config.Metrics {
		m, err := x.getOrCreateMetric(metricConfig)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// Execute the value expression
		value, err := x.getValue(m.Config, m.ValueTemplate, evn)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// Execute the tag expression
		attrs, err := x.getLabels(m.Config, m.LabelsTemplate, evn)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		metrics = append(metrics, MetricValue{
			MetricName:  metricConfig.MetricName,
			Description: metricConfig.Description,
			Unit:        metricConfig.Unit,
			OpType:      metricConfig.OpType,
			Value:       value,
			Labels:      attrs,
		})
	}

	// Handling dynamic indicators
	if x.metricsExpr != nil {
		var out interface{}
		out, err := x.metricsExpr.Execute(evn)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		switch v := out.(type) {
		case map[string]interface{}:
			var metricValue MetricValue
			if err := maps.Map2Struct(v, &metricValue); err != nil {
				ctx.TellFailure(msg, err)
				return
			}
			metrics = append(metrics, metricValue)
		case []interface{}:
			for _, item := range v {
				if m, ok := item.(map[string]interface{}); ok {
					var metricValues MetricValue
					if err := maps.Map2Struct(m, &metricValues); err != nil {
						ctx.TellFailure(msg, err)
						return
					}
					metrics = append(metrics, metricValues)
				}
			}
		default:
		}

	}
	// Address each indicator
	for _, cfg := range metrics {
		if cfg.MetricName == "" {
			continue
		}
		// Obtain or create metrics
		m, err := x.getOrCreateMetric(cfg.GetConfig())
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		value := cfg.Value
		labels := cfg.Labels

		// Tag conversion
		var attrs []attribute.KeyValue
		for k, v := range labels {
			attrs = append(attrs, attribute.String(k, v))
		}

		// Record the indicators
		switch cfg.OpType {
		case COUNTER:
			m.Counter.Add(ctx.GetContext(), value, metric.WithAttributes(attrs...))
		case GAUGE:
			m.Gauge.Add(ctx.GetContext(), value, metric.WithAttributes(attrs...))
		case HISTOGRAM:
			m.Histogram.Record(ctx.GetContext(), value, metric.WithAttributes(attrs...))
		}
	}
	ctx.TellSuccess(msg)
}

func (x *OtelNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *OtelNode) Desc() string {
	return "OpenTelemetry client for sending metrics/traces/logs via OTLP. Supports grpc and http protocols. Routes to Success/Failure"
}

// getOrCreateMetric to get or create metrics
func (x *OtelNode) getOrCreateMetric(cfg MetricConfig) (*Metric, error) {
	// First, try to read the lock to obtain it
	x.metricsCacheMu.RLock()
	if m, ok := x.metricsCache[cfg.MetricName]; ok {
		x.metricsCacheMu.RUnlock()
		return m, nil
	}
	x.metricsCacheMu.RUnlock()

	// Create a write lock
	x.metricsCacheMu.Lock()
	defer x.metricsCacheMu.Unlock()

	// Double-check to avoid duplicate creations
	if m, ok := x.metricsCache[cfg.MetricName]; ok {
		return m, nil
	}
	var err error
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return nil, err
	}
	if client.Meter == nil {
		return nil, errors.New("meter is nil")
	}
	m := &Metric{
		Config: cfg,
	}

	// Create metrics
	switch cfg.OpType {
	case COUNTER:
		m.Counter, err = client.Meter.Float64Counter(
			cfg.MetricName,
			metric.WithDescription(cfg.Description),
			metric.WithUnit(cfg.Unit),
		)
	case GAUGE:
		m.Gauge, err = client.Meter.Float64UpDownCounter(
			cfg.MetricName,
			metric.WithDescription(cfg.Description),
			metric.WithUnit(cfg.Unit),
		)
	case HISTOGRAM:
		m.Histogram, err = client.Meter.Float64Histogram(
			cfg.MetricName,
			metric.WithDescription(cfg.Description),
			metric.WithUnit(cfg.Unit),
		)
	default:
		return nil, fmt.Errorf("unsupported operation type: %s", cfg.OpType)
	}

	if err != nil {
		return nil, err
	}

	x.metricsCache[cfg.MetricName] = m
	return m, nil
}

// getValue to get the metric value
func (x *OtelNode) getValue(config MetricConfig, valueTemplate el.Template, evn map[string]interface{}) (float64, error) {
	if valueTemplate == nil {
		if config.Value != "" {
			//String replacement is a floating-point number
			if value, err := strconv.ParseFloat(config.Value, 64); err != nil {
				return 0, err
			} else {
				return value, nil
			}
		} else {
			return 0, nil
		}
	} else if out, err := valueTemplate.Execute(evn); err != nil {
		return 0, err
	} else {
		switch v := out.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(config.Value, 64)
		default:
			return 0, fmt.Errorf("invalid value type: %T", out)
		}
	}
}

// getLabels to get labels
func (x *OtelNode) getLabels(config MetricConfig, labelsTemplate el.Template, evn map[string]interface{}) (map[string]string, error) {
	if labelsTemplate == nil {
		if config.Labels != "" {
			var labels map[string]string
			if err := json.Unmarshal([]byte(config.Labels), &labels); err == nil {
				return labels, nil
			}
		}
		return nil, nil
	} else if out, err := labelsTemplate.Execute(evn); err != nil {
		return nil, err
	} else {
		return str.ToStringMapString(out), nil
	}
}
