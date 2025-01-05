package otel

import (
	"context"
	"errors"
	"fmt"
	"github.com/rulego/rulego/utils/json"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"strings"
	"sync"
	"time"

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

// 配置示例
//
//	{
//		"id": "otel1",
//		"type": "x/otelMetrics",
//		"configuration": {
//		  "endpoint": "localhost:4317",
//		  "metricsExpr": "${msg.metrics}" // 支持单个指标对象或指标对象数组
//		}
//	  }
//
// 注册节点
func init() {
	_ = rulego.Registry.Register(&OtelNode{})
}

// MetricConfig 单个指标配置
type MetricConfig struct {
	// 指标名称
	MetricName string
	// 指标描述
	Description string
	// Unit 指标的单位，用于描述指标值的度量单位。例如，对于计数器（COUNTER），单位可能是 "1"（表示计数）；
	// 对于时间度量，单位可能是 "s"（秒）、"ms"（毫秒）、"us"（微秒）或 "ns"（纳秒）；
	// 对于数据量度量，单位可能是 "B"（字节）；
	// 对于比例度量，单位可能是 "%"（百分比）。
	// OpenTelemetry 使用基于 SI（国际单位制）的单位系统，但也支持一些常见的非 SI 单位。
	// 例如，对于 HTTP 请求的计数，单位可以是 "1"；对于 HTTP 响应时间，单位可以是 "s"。
	Unit string
	// 操作类型：COUNTER, GAUGE, HISTOGRAM
	OpType string
	// 指标值
	Value float64
	// 标签
	Labels map[string]string
}

// OtelNodeConfiguration 节点配置
type OtelNodeConfiguration struct {
	// OTLP后端系统地址，例如: localhost:4318
	Server string
	// 传输协议，支持 grpc 和 http，默认http
	Protocol string
	// 指标取值表达式，支持单个指标对象或指标对象数组
	// 通过expr表达式从消息负荷中获取指标配置
	// 指标配置示例：
	// [
	//   {
	//     "metricName": "http_requests",
	//     "description": "HTTP requests made",
	//     "unit": "1",
	//     "opType": "COUNTER",
	//     "value": 10,
	//     "labels": {
	//       "method": "GET",
	//       "path": "/api/v1/data"
	//     }
	//   }
	//]
	Expr string
}

// Metric 指标实例
type Metric struct {
	Config    MetricConfig
	Counter   metric.Float64Counter
	Gauge     metric.Float64UpDownCounter
	Histogram metric.Float64Histogram
}

// OtelNode OpenTelemetry客户端组件，用于记录各种指标
// 运行通过消息负荷获取指标数据，通过OTLP协议发送到后端系统，如：Prometheus、Datadog、InfluxDB等
type OtelNode struct {
	base.SharedNode[*Client]
	// 节点配置
	Config OtelNodeConfiguration
	// 指标缓存,key为指标名称
	metricsCache map[string]*Metric
	// 指标缓存锁
	metricsCacheMu sync.RWMutex
	// 指标表达式
	metricsExpr el.Template
	// OTEL客户端
	client *Client
}

// Type 返回组件类型
func (x *OtelNode) Type() string {
	return "x/otel"
}

func (x *OtelNode) New() types.Node {
	return &OtelNode{
		Config: OtelNodeConfiguration{
			Server:   "localhost:4318",
			Protocol: "HTTP",
			Expr:     "${msg.metrics}",
		},
		metricsCache: make(map[string]*Metric),
	}
}

// Init 初始化组件
func (x *OtelNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if x.Config.Server == "" {
		return errors.New("server is required")
	}
	// 编译指标表达式
	if x.Config.Expr != "" {
		if metricsExpr, err := el.NewTemplate(strings.TrimSpace(x.Config.Expr)); err != nil {
			return err
		} else {
			x.metricsExpr = metricsExpr
		}
	}
	x.Config.Protocol = strings.TrimSpace(strings.ToUpper(x.Config.Protocol))
	if x.Config.Protocol == "" {
		x.Config.Protocol = "HTTP"
	}
	// 初始化共享MeterProvider
	err = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*Client, error) {
		return x.initMeterProvider()
	})

	return nil
}

type Client struct {
	MeterProvider *sdkmetric.MeterProvider
	Meter         metric.Meter
}

// initMeterProvider 初始化MeterProvider
func (x *OtelNode) initMeterProvider() (*Client, error) {
	if x.client != nil && x.client.Meter != nil {
		return x.client, nil
	}
	x.Locker.Lock()
	defer x.Locker.Unlock()
	if x.client != nil && x.client.Meter != nil {
		return x.client, nil
	}
	var exporter sdkmetric.Exporter
	var err error

	// 创建OTLP导出器
	if strings.ToUpper(x.Config.Protocol) == "GRPC" {
		// 创建OTLP gRPC导出器
		exporter, err = otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(x.Config.Server),
			otlpmetricgrpc.WithInsecure(),
		)
	} else {
		// 创建OTLP HTTP导出器
		exporter, err = otlpmetrichttp.New(
			context.Background(),
			otlpmetrichttp.WithEndpoint(x.Config.Server),
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %v", err)
	}

	// 创建MeterProvider
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(10*time.Second),
			),
		),
	)
	x.client = &Client{
		MeterProvider: mp,
		Meter:         mp.Meter(x.Type()),
	}

	return x.client, nil
}

// OnMsg 处理消息
func (x *OtelNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var out interface{}
	var err error

	if x.metricsExpr != nil {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		// 获取指标配置
		out, err = x.metricsExpr.Execute(evn)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
	} else {
		if err = json.Unmarshal([]byte(msg.Data), &out); err == nil {
			ctx.TellFailure(msg, err)
		}
	}
	var metrics []MetricConfig
	switch v := out.(type) {
	case MetricConfig:
		metrics = []MetricConfig{v}
	case []MetricConfig:
		metrics = v
	case map[string]interface{}:
		var metric MetricConfig
		if err := maps.Map2Struct(v, &metric); err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		metrics = []MetricConfig{metric}
	case []interface{}:
		metrics = make([]MetricConfig, len(v))
		for i, item := range v {
			if m, ok := item.(map[string]interface{}); ok {
				if err := maps.Map2Struct(m, &metrics[i]); err != nil {
					ctx.TellFailure(msg, err)
					return
				}
			}
		}
	default:
		ctx.TellFailure(msg, fmt.Errorf("invalid metrics type: %T", out))
		return
	}

	// 处理每个指标
	for _, cfg := range metrics {
		// 获取或创建指标
		m, err := x.getOrCreateMetric(cfg)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// 转换标签
		var attrs []attribute.KeyValue
		for k, v := range cfg.Labels {
			attrs = append(attrs, attribute.String(k, v))
		}

		// 记录指标
		switch cfg.OpType {
		case COUNTER:
			m.Counter.Add(ctx.GetContext(), cfg.Value, metric.WithAttributes(attrs...))
		case GAUGE:
			m.Gauge.Add(ctx.GetContext(), cfg.Value, metric.WithAttributes(attrs...))
		case HISTOGRAM:
			m.Histogram.Record(ctx.GetContext(), cfg.Value, metric.WithAttributes(attrs...))
		}
	}

	ctx.TellSuccess(msg)
}

func (x *OtelNode) Destroy() {
	if x.client != nil && x.client.MeterProvider != nil {
		_ = x.client.MeterProvider.Shutdown(context.Background())
		x.client = nil
	}
}

// getOrCreateMetric 获取或创建指标
func (x *OtelNode) getOrCreateMetric(cfg MetricConfig) (*Metric, error) {
	// 先尝试读锁获取
	x.metricsCacheMu.RLock()
	if m, ok := x.metricsCache[cfg.MetricName]; ok {
		x.metricsCacheMu.RUnlock()
		return m, nil
	}
	x.metricsCacheMu.RUnlock()

	// 获取写锁创建
	x.metricsCacheMu.Lock()
	defer x.metricsCacheMu.Unlock()

	// 双重检查,避免重复创建
	if m, ok := x.metricsCache[cfg.MetricName]; ok {
		return m, nil
	}
	var err error
	client, err := x.SharedNode.Get()
	if err != nil {
		return nil, err
	}
	if client.Meter == nil {
		return nil, errors.New("meter is nil")
	}
	m := &Metric{Config: cfg}

	// 创建指标
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
