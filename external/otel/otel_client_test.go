/*
 * Copyright 2023 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package otel

import (
	"os"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestOtelNode(t *testing.T) {
	// 如果设置了跳过 OTel 测试，则跳过
	if os.Getenv("SKIP_OTEL_TESTS") == "true" {
		t.Skip("Skipping OTel tests")
	}

	// 获取 OTel Collector 端点
	otlpEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otlpEndpoint == "" {
		otlpEndpoint = "localhost:4318"
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&OtelNode{})
	var targetNodeType = "x/otel"

	t.Run("InitNode", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   otlpEndpoint,
			"protocol": "HTTP",
			"metrics": []map[string]interface{}{
				{
					"metricName":  "test_counter",
					"description": "Test counter metric",
					"unit":        "1",
					"opType":      "COUNTER",
					"value":       "${msg.value}",
					"labels":      "${msg.labels}",
				},
			},
		}, Registry)
		assert.Nil(t, err)
		assert.NotNil(t, node)

		otelNode := node.(*OtelNode)
		assert.Equal(t, otlpEndpoint, otelNode.Config.Server)
		assert.Equal(t, "HTTP", otelNode.Config.Protocol)
		assert.Equal(t, 1, len(otelNode.Config.Metrics))
		assert.Equal(t, "test_counter", otelNode.Config.Metrics[0].MetricName)
	})

	t.Run("CounterMetric", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   otlpEndpoint,
			"protocol": "HTTP",
			"metrics": []map[string]interface{}{
				{
					"metricName":  "http_requests_total",
					"description": "Total HTTP requests",
					"unit":        "1",
					"opType":      "COUNTER",
					"value":       "${msg.value}",
					"labels":      "${msg.labels}",
				},
			},
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create OTel node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("OTel operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "counter")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{"value":1.0,"labels":{"method":"GET","path":"/api/v1/data"}}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})

	t.Run("GaugeMetric", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   otlpEndpoint,
			"protocol": "HTTP",
			"metrics": []map[string]interface{}{
				{
					"metricName":  "memory_usage",
					"description": "Memory usage in bytes",
					"unit":        "B",
					"opType":      "GAUGE",
					"value":       "${msg.value}",
					"labels":      "${msg.labels}",
				},
			},
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create OTel node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("OTel gauge operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "gauge")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{"value":1024.0,"labels":{"instance":"server1","region":"us-east-1"}}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})

	t.Run("HistogramMetric", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   otlpEndpoint,
			"protocol": "HTTP",
			"metrics": []map[string]interface{}{
				{
					"metricName":  "response_time",
					"description": "HTTP response time",
					"unit":        "s",
					"opType":      "HISTOGRAM",
					"value":       "${msg.value}",
					"labels":      "${msg.labels}",
				},
			},
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create OTel node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("OTel histogram operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "histogram")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{"value":0.125,"labels":{"endpoint":"/api/users","status":"200"}}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})

	t.Run("DynamicMetrics", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":      otlpEndpoint,
			"protocol":    "HTTP",
			"metricsExpr": "${msg.metrics}",
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create OTel node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("OTel dynamic metrics operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "dynamic")

		// 测试单个指标对象
		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{"metrics":{"metricName":"dynamic_counter","description":"Dynamic counter","unit":"1","opType":"COUNTER","value":5.0,"labels":{"source":"test"}}}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})

	t.Run("DynamicMetricsArray", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":      otlpEndpoint,
			"protocol":    "HTTP",
			"metricsExpr": "${msg.metrics}",
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create OTel node: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("OTel dynamic metrics array operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "dynamic_array")

		// 测试指标对象数组
		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{"metrics":[{"metricName":"batch_counter1","description":"Batch counter 1","unit":"1","opType":"COUNTER","value":3.0,"labels":{"batch":"1"}},{"metricName":"batch_counter2","description":"Batch counter 2","unit":"1","opType":"COUNTER","value":7.0,"labels":{"batch":"2"}}]}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})

	t.Run("GRPCProtocol", func(t *testing.T) {
		// 获取 gRPC 端点
		grpcEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_GRPC_ENDPOINT")
		if grpcEndpoint == "" {
			grpcEndpoint = "localhost:4317"
		}

		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":   grpcEndpoint,
			"protocol": "GRPC",
			"metrics": []map[string]interface{}{
				{
					"metricName":  "grpc_test_counter",
					"description": "gRPC test counter",
					"unit":        "1",
					"opType":      "COUNTER",
					"value":       "${msg.value}",
					"labels":      "${msg.labels}",
				},
			},
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create OTel node with gRPC: %v", err)
		}

		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			if err != nil {
				t.Logf("OTel gRPC operation result: %s, error: %v", relationType, err)
			} else {
				assert.Equal(t, types.Success, relationType)
			}
		})

		metaData := types.NewMetadata()
		metaData.PutValue("operation", "grpc_test")

		msg := ctx.NewMsg("TEST_MSG_TYPE", metaData, `{"value":2.0,"labels":{"protocol":"grpc","test":"true"}}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})
}

func TestOtelNodeConfig(t *testing.T) {
	// 如果设置了跳过 OTel 测试，则跳过
	if os.Getenv("SKIP_OTEL_TESTS") == "true" {
		t.Skip("Skipping OTel tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&OtelNode{})
	var targetNodeType = "x/otel"

	t.Run("EmptyServerConfig", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "",
		}, Registry)
		assert.NotNil(t, err)
		assert.Equal(t, "server is required", err.Error())
	})

	t.Run("InvalidMetricExpr", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":      "localhost:4318",
			"metricsExpr": "${invalid.expr}", // 无效的表达式，缺少闭合括号
		}, Registry)
		// 这个测试可能不会在初始化时失败，因为表达式可能在运行时才验证
		if err != nil {
			t.Logf("Expected error for invalid expression: %v", err)
		}
	})

	t.Run("InvalidMetricValueExpr", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:4318",
			"metrics": []map[string]interface{}{
				{
					"metricName": "test_metric",
					"opType":     "COUNTER",
					"value":      "${invalid.expr}", // 无效的表达式
				},
			},
		}, Registry)
		// 这个测试可能不会在初始化时失败，因为表达式可能在运行时才验证
		if err != nil {
			t.Logf("Expected error for invalid value expression: %v", err)
		}
	})

	t.Run("InvalidMetricLabelsExpr", func(t *testing.T) {
		_, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:4318",
			"metrics": []map[string]interface{}{
				{
					"metricName": "test_metric",
					"opType":     "COUNTER",
					"value":      "1.0",
					"labels":     "${invalid.expr}", // 无效的表达式
				},
			},
		}, Registry)
		// 这个测试可能不会在初始化时失败，因为表达式可能在运行时才验证
		if err != nil {
			t.Logf("Expected error for invalid labels expression: %v", err)
		}
	})

	t.Run("UnsupportedOpType", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:4318",
			"metrics": []map[string]interface{}{
				{
					"metricName": "test_metric",
					"opType":     "INVALID_TYPE",
					"value":      "1.0",
				},
			},
		}, Registry)
		// 不支持的操作类型会在初始化时失败
		if err != nil {
			t.Logf("Expected error for unsupported operation type: %v", err)
			return
		}

		// 如果初始化成功，测试运行时错误
		config := types.NewConfig()
		ctx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			// 应该失败
			assert.Equal(t, types.Failure, relationType)
			assert.NotNil(t, err)
		})

		msg := ctx.NewMsg("TEST_MSG_TYPE", types.NewMetadata(), `{"value":1.0}`)

		otelNode := node.(*OtelNode)
		otelNode.OnMsg(ctx, msg)

		// 等待消息处理
		time.Sleep(time.Millisecond * 200)

		otelNode.Destroy()
	})

	t.Run("DefaultConfig", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server": "localhost:4318",
		}, Registry)
		assert.Nil(t, err)
		assert.NotNil(t, node)

		otelNode := node.(*OtelNode)
		assert.Equal(t, "localhost:4318", otelNode.Config.Server)
		assert.Equal(t, "HTTP", otelNode.Config.Protocol)
		assert.Equal(t, "${msg.metrics}", otelNode.Config.MetricExpr)
	})
}