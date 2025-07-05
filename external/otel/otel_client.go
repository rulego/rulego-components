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

// 配置示例
//
//	{
//		"id": "otel1",
//		"type": "x/otelMetrics",
//		"configuration": {
//		  "endpoint": "localhost:4317",
//		  "metricsExpr": "${msg.metrics}", // 支持单个指标对象或指标对象数组
//		  "metrics": [{ // 支持配置方式的指标列表
//		    "metricName": "http_requests",
//		    "description": "HTTP requests made",
//		    "unit": "1",
//		    "opType": "COUNTER",
//		    "value": "${msg.value}", // 支持表达式
//		    "labels": "${msg.labels}" // 支持表达式
//		  }]
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
	MetricName string `json:"metricName"`
	// 指标描述
	Description string `json:"description"`
	// Unit 指标的单位，用于描述指标值的度量单位。例如，对于计数器（COUNTER），单位可能是 "1"（表示计数）；
	// 对于时间度量，单位可能是 "s"（秒）、"ms"（毫秒）、"us"（微秒）或 "ns"（纳秒）；
	// 对于数据量度量，单位可能是 "B"（字节）；
	// 对于比例度量，单位可能是 "%"（百分比）。
	// OpenTelemetry 使用基于 SI（国际单位制）的单位系统，但也支持一些常见的非 SI 单位。
	// 例如，对于 HTTP 请求的计数，单位可以是 "1"；对于 HTTP 响应时间，单位可以是 "s"。
	Unit string `json:"unit"`
	// 操作类型：COUNTER, GAUGE, HISTOGRAM
	OpType string `json:"opType"`
	// 指标值，支持动态取值，如：${msg.value} 格式为：float64
	Value string `json:"value"`
	// 标签，支持动态取值，如：${msg.labels} 格式为：{"key1":"value1","key2":"value2"}
	Labels string `json:"labels"`
}

// MetricValue 单个指标配置，已经获取 Value和 Labels 值
type MetricValue struct {
	// 指标名称
	MetricName string `json:"metricName"`
	// 指标描述
	Description string `json:"description"`
	// Unit 指标的单位，用于描述指标值的度量单位。例如，对于计数器（COUNTER），单位可能是 "1"（表示计数）；
	// 对于时间度量，单位可能是 "s"（秒）、"ms"（毫秒）、"us"（微秒）或 "ns"（纳秒）；
	// 对于数据量度量，单位可能是 "B"（字节）；
	// 对于比例度量，单位可能是 "%"（百分比）。
	// OpenTelemetry 使用基于 SI（国际单位制）的单位系统，但也支持一些常见的非 SI 单位。
	// 例如，对于 HTTP 请求的计数，单位可以是 "1"；对于 HTTP 响应时间，单位可以是 "s"。
	Unit string `json:"unit"`
	// 操作类型：COUNTER, GAUGE, HISTOGRAM
	OpType string `json:"opType"`
	// 指标值
	Value float64 `json:"value"`
	// 标签
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

// OtelNodeConfiguration 节点配置
type OtelNodeConfiguration struct {
	// OTLP后端系统地址，例如: localhost:4318
	Server string
	// 传输协议，支持 grpc 和 http，默认http
	Protocol string
	// 动态指标配置和取值表达式，支持单个指标对象或指标对象数组
	// 如果指标器不存在则，动态创建
	// MetricExpr 和 Metrics 允许同时存在，合并后发送
	// 通过expr表达式从消息负荷中获取指标配置
	// 指标配置示例：
	// {
	//  "metrics":[
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
	//}
	// 则可以通过 ${msg.metrics} 方式获取指标配置
	MetricExpr string
	// 指标配置列表，会在初始化创建指标器
	// MetricExpr 和 Metrics 允许同时存在，合并后发送
	Metrics []MetricConfig
}

// Metric 指标实例
type Metric struct {
	Config         MetricConfig
	ValueTemplate  el.Template
	LabelsTemplate el.Template
	Counter        metric.Float64Counter
	Gauge          metric.Float64UpDownCounter
	Histogram      metric.Float64Histogram
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
}

// Type 返回组件类型
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
	// 初始化共享MeterProvider
	err = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*Client, error) {
		return x.initMeterProvider()
	}, func(client *Client) error {
		// 清理回调函数
		return client.MeterProvider.Shutdown(context.Background())
	})

	// 预编译配置的指标模板
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

// initMeterProvider 初始化MeterProvider
func (x *OtelNode) initMeterProvider() (*Client, error) {
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
	client := &Client{
		MeterProvider: mp,
		Meter:         mp.Meter(x.Type()),
	}

	return client, nil
}

// OnMsg 处理消息
func (x *OtelNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	var metrics []MetricValue
	// 处理配置的指标
	for _, metricConfig := range x.Config.Metrics {
		m, err := x.getOrCreateMetric(metricConfig)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// 执行值表达式
		value, err := x.getValue(m.Config, m.ValueTemplate, evn)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		// 执行标签表达式
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

	// 处理动态指标
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
	// 处理每个指标
	for _, cfg := range metrics {
		if cfg.MetricName == "" {
			continue
		}
		// 获取或创建指标
		m, err := x.getOrCreateMetric(cfg.GetConfig())
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		value := cfg.Value
		labels := cfg.Labels

		// 转换标签
		var attrs []attribute.KeyValue
		for k, v := range labels {
			attrs = append(attrs, attribute.String(k, v))
		}

		// 记录指标
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

// getValue 获取指标值
func (x *OtelNode) getValue(config MetricConfig, valueTemplate el.Template, evn map[string]interface{}) (float64, error) {
	if valueTemplate == nil {
		if config.Value != "" {
			//字符串换换为浮点数
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

// getLabels 获取标签
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
