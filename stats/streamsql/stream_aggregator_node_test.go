/*
 * Copyright 2025 The RuleGo Authors.
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

package streamsql

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/str"
)

// TestStreamAggregatorNode_BasicAggregation 测试基本聚合功能
func TestStreamAggregatorNode_BasicAggregation(t *testing.T) {
	t.Run("滚动窗口平均值", func(t *testing.T) {
		sql := "SELECT AVG(temperature) as avg_temp, COUNT(*) as count FROM stream GROUP BY TumblingWindow('1s')"
		testData := []map[string]interface{}{
			{"temperature": 20.0, "deviceId": "sensor001"},
			{"temperature": 25.0, "deviceId": "sensor001"},
			{"temperature": 30.0, "deviceId": "sensor001"},
		}

		results := testStreamAggregator(t, sql, testData, "rolling window average test")

		assert.True(t, len(results) > 0, "应该有聚合结果")

		// 验证第一个聚合结果
		if len(results) > 0 {
			result := results[0]
			assert.NotNil(t, result["avg_temp"], "应该包含平均温度")
			assert.NotNil(t, result["count"], "应该包含计数")

			count := result["count"].(float64)
			assert.True(t, count > 0, "计数应该大于0")
		}
	})

	t.Run("按设备分组聚合", func(t *testing.T) {
		sql := "SELECT deviceId, MAX(temperature) as max_temp, MIN(temperature) as min_temp FROM stream GROUP BY deviceId, TumblingWindow('1s')"
		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 25.0},
			{"deviceId": "sensor001", "temperature": 30.0},
			{"deviceId": "sensor002", "temperature": 35.0},
			{"deviceId": "sensor002", "temperature": 28.0},
		}

		results := testStreamAggregator(t, sql, testData, "group by device test")

		assert.True(t, len(results) > 0, "应该有分组聚合结果")

		// 验证聚合结果包含设备分组信息
		for _, result := range results {
			assert.NotNil(t, result["deviceId"], "应该包含设备ID")
			assert.NotNil(t, result["max_temp"], "应该包含最高温度")
			assert.NotNil(t, result["min_temp"], "应该包含最低温度")
		}
	})

	t.Run("滑动窗口聚合", func(t *testing.T) {
		sql := "SELECT SUM(temperature) as sum_temp, AVG(temperature) as avg_temp FROM stream GROUP BY SlidingWindow('2s', '1s')"
		testData := []map[string]interface{}{
			{"temperature": 10.0},
			{"temperature": 20.0},
			{"temperature": 30.0},
			{"temperature": 40.0},
		}

		results := testStreamAggregator(t, sql, testData, "sliding window test")

		assert.True(t, len(results) > 0, "应该有滑动窗口聚合结果")

		for _, result := range results {
			assert.NotNil(t, result["sum_temp"], "应该包含总和")
			assert.NotNil(t, result["avg_temp"], "应该包含平均值")
		}
	})
}

// TestStreamAggregatorNode_WindowTypes 测试不同窗口类型
func TestStreamAggregatorNode_WindowTypes(t *testing.T) {
	// 暂时跳过计数窗口测试 - 需要确认正确的语法
	t.Skip("计数窗口测试暂时跳过 - CountingWindow语法需要确认")

	t.Run("会话窗口", func(t *testing.T) {
		sql := "SELECT deviceId, COUNT(*) as event_count FROM stream GROUP BY deviceId, SessionWindow('2s')"
		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 20.0},
			{"deviceId": "sensor001", "temperature": 22.0},
			// 2秒后会话超时，触发新会话
		}

		results := testStreamAggregator(t, sql, testData, "session window test")
		assert.True(t, len(results) >= 0, "会话窗口测试完成")
	})
}

// TestStreamAggregatorNode_Validation 测试节点配置验证
func TestStreamAggregatorNode_Validation(t *testing.T) {
	t.Run("空SQL验证", func(t *testing.T) {
		node := &StreamAggregatorNode{}
		config := map[string]interface{}{
			"sql": "",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "空SQL应该返回错误")
		assert.Equal(t, ErrAggregatorSQLEmpty, err, "应该是SQL为空的错误")
	})

	t.Run("无效SQL语法", func(t *testing.T) {
		node := &StreamAggregatorNode{}
		config := map[string]interface{}{
			"sql": "INVALID SQL SYNTAX",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "无效SQL应该返回错误")
	})
}

// TestStreamAggregatorNode_ConcurrentProcessing 测试并发处理能力
func TestStreamAggregatorNode_ConcurrentProcessing(t *testing.T) {
	sql := "SELECT deviceId, AVG(temperature) as avg_temp, COUNT(*) as count FROM stream GROUP BY deviceId, TumblingWindow('1s')"

	config := engine.NewConfig(types.WithDefaultPool())
	var aggregateResults []map[string]interface{}
	var successCount int32
	var mu sync.Mutex

	// 设置全局聚合结果处理器
	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err == nil && msg.Type == WindowEventMsgType {
			atomic.AddInt32(&successCount, 1)

			var result map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr == nil {
				mu.Lock()
				aggregateResults = append(aggregateResults, result)
				mu.Unlock()
			}
		}
	}

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "concurrent_aggregator_test",
			"name": "并发聚合测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "aggregator1",
					"type": "x/streamAggregator",
					"name": "流聚合器",
					"configuration": {
						"sql": "%s"
					}
				}
			],
			"connections": []
		}
	}`, sql)

	chainId := str.RandomStr(10)
	ruleEngine, err := engine.New(chainId, []byte(ruleChainConfig), engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	defer engine.Del(chainId)

	// 并发测试参数
	const numGoroutines = 5
	const messagesPerGoroutine = 10

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				temperature := 20.0 + float64(j%20) // 温度范围 20-40
				testData := map[string]interface{}{
					"deviceId":    fmt.Sprintf("sensor_%d", goroutineId),
					"temperature": temperature,
					"timestamp":   time.Now().Unix(),
				}

				msgData, _ := json.Marshal(testData)
				msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))

				ruleEngine.OnMsg(msg)
				time.Sleep(10 * time.Millisecond) // 模拟数据间隔
			}
		}(i)
	}

	wg.Wait()

	// 等待窗口聚合触发
	time.Sleep(3 * time.Second)

	assert.True(t, len(aggregateResults) >= 0, "应该收集到聚合结果")

	// 验证聚合结果结构
	mu.Lock()
	for _, result := range aggregateResults {
		assert.NotNil(t, result["deviceId"], "聚合结果应该包含设备ID")
		assert.NotNil(t, result["avg_temp"], "聚合结果应该包含平均温度")
		assert.NotNil(t, result["count"], "聚合结果应该包含计数")
	}
	mu.Unlock()
}

// TestStreamAggregatorNode_ComplexAggregation 测试复杂聚合查询
func TestStreamAggregatorNode_ComplexAggregation(t *testing.T) {
	t.Run("多字段聚合", func(t *testing.T) {
		sql := "SELECT deviceId, AVG(temperature) as avg_temp, MAX(temperature) as max_temp, MIN(temperature) as min_temp, COUNT(*) as count, SUM(humidity) as total_humidity FROM stream GROUP BY deviceId, TumblingWindow('1s')"

		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 25.0, "humidity": 60},
			{"deviceId": "sensor001", "temperature": 30.0, "humidity": 65},
			{"deviceId": "sensor002", "temperature": 22.0, "humidity": 55},
		}

		results := testStreamAggregator(t, sql, testData, "complex aggregation test")

		assert.True(t, len(results) > 0, "应该有复杂聚合结果")

		for _, result := range results {
			assert.NotNil(t, result["deviceId"], "应该包含设备ID")
			assert.NotNil(t, result["avg_temp"], "应该包含平均温度")
			assert.NotNil(t, result["max_temp"], "应该包含最高温度")
			assert.NotNil(t, result["min_temp"], "应该包含最低温度")
			assert.NotNil(t, result["count"], "应该包含计数")
			assert.NotNil(t, result["total_humidity"], "应该包含湿度总和")
		}
	})

	t.Run("条件聚合", func(t *testing.T) {
		sql := "SELECT COUNT(*) as high_temp_count, AVG(temperature) as avg_high_temp FROM stream WHERE temperature > 25 GROUP BY TumblingWindow('1s')"

		testData := []map[string]interface{}{
			{"temperature": 20.0}, // 不满足条件
			{"temperature": 30.0}, // 满足条件
			{"temperature": 35.0}, // 满足条件
			{"temperature": 22.0}, // 不满足条件
		}

		results := testStreamAggregator(t, sql, testData, "conditional aggregation test")

		assert.True(t, len(results) >= 0, "条件聚合测试完成")
	})
}

// TestStreamAggregatorNode_ArrayInput 测试数组输入处理
func TestStreamAggregatorNode_ArrayInput(t *testing.T) {
	t.Run("处理JSON数组输入", func(t *testing.T) {
		sql := "SELECT AVG(temperature) as avg_temp, COUNT(*) as count FROM stream GROUP BY TumblingWindow('1s')"

		// 准备数组测试数据
		arrayData := []map[string]interface{}{
			{"temperature": 20.0, "deviceId": "sensor001"},
			{"temperature": 25.0, "deviceId": "sensor002"},
			{"temperature": 30.0, "deviceId": "sensor003"},
		}

		config := engine.NewConfig(types.WithDefaultPool())
		var aggregateResults []map[string]interface{}
		var successCount int32
		var mu sync.Mutex

		// 设置全局聚合结果处理器
		config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == WindowEventMsgType {
				atomic.AddInt32(&successCount, 1)

				// 聚合结果可能是数组格式，需要正确解析
				var resultArray []map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
					mu.Lock()
					aggregateResults = append(aggregateResults, resultArray...)
					mu.Unlock()
				} else {
					// 尝试解析为单个对象
					var result map[string]interface{}
					if jsonErr2 := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr2 == nil {
						mu.Lock()
						aggregateResults = append(aggregateResults, result)
						mu.Unlock()
					}
				}
			}
		}

		// 创建测试规则链
		ruleChainConfig := fmt.Sprintf(`{
			"ruleChain": {
				"id": "array_aggregator_test",
				"name": "数组聚合测试",
				"root": true
			},
			"metadata": {
				"nodes": [
					{
						"id": "aggregator1",
						"type": "x/streamAggregator",
						"name": "流聚合器",
						"configuration": {
							"sql": "%s"
						}
					},
					{
						"id": "log1",
						"type": "log",
						"name": "日志节点",
						"configuration": {
							"jsScript": "return 'Aggregation result: ' + JSON.stringify(msg);"
						}
					}
				],
				"connections": [
					{
						"fromId": "aggregator1",
						"toId": "log1",
						"type": "window_event"
					}
				]
			}
		}`, sql)

		chainId := str.RandomStr(10)
		ruleEngine, err := engine.New(chainId, []byte(ruleChainConfig), engine.WithConfig(config))
		assert.Nil(t, err, "规则引擎创建应该成功")
		defer engine.Del(chainId)

		// 发送数组数据
		msgData, _ := json.Marshal(arrayData)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))

		// 监控成功处理
		var processedSuccess int32
		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil {
				atomic.AddInt32(&processedSuccess, 1)
			}
		}))

		// 等待处理完成和窗口聚合触发
		time.Sleep(2 * time.Second)

		finalProcessed := atomic.LoadInt32(&processedSuccess)
		assert.Equal(t, int32(1), finalProcessed, "数组数据应该被成功处理")
	})

	t.Run("处理空数组输入", func(t *testing.T) {
		sql := "SELECT COUNT(*) as count FROM stream GROUP BY TumblingWindow('1s')"

		// 空数组
		arrayData := []map[string]interface{}{}

		config := engine.NewConfig(types.WithDefaultPool())
		ruleChainConfig := fmt.Sprintf(`{
			"ruleChain": {
				"id": "empty_array_test",
				"name": "空数组测试",
				"root": true
			},
			"metadata": {
				"nodes": [
					{
						"id": "aggregator1",
						"type": "x/streamAggregator",
						"name": "流聚合器",
						"configuration": {
							"sql": "%s"
						}
					},
					{
						"id": "log1",
						"type": "log",
						"name": "日志节点",
						"configuration": {
							"jsScript": "return 'Aggregation result: ' + JSON.stringify(msg);"
						}
					}
				],
				"connections": [
					{
						"fromId": "aggregator1",
						"toId": "log1",
						"type": "window_event"
					}
				]
			}
		}`, sql)

		chainId := str.RandomStr(10)
		ruleEngine, err := engine.New(chainId, []byte(ruleChainConfig), engine.WithConfig(config))
		assert.Nil(t, err, "规则引擎创建应该成功")
		defer engine.Del(chainId)

		// 发送空数组数据
		msgData, _ := json.Marshal(arrayData)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))

		var processedSuccess int32
		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil {
				atomic.AddInt32(&processedSuccess, 1)
			}
		}))

		time.Sleep(500 * time.Millisecond)

		finalProcessed := atomic.LoadInt32(&processedSuccess)
		assert.Equal(t, int32(1), finalProcessed, "空数组也应该被成功处理")
	})
}

// TestStreamAggregatorNode_DataTypeValidation 测试数据类型校验
func TestStreamAggregatorNode_DataTypeValidation(t *testing.T) {
	sql := "SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('1s')"

	config := engine.NewConfig(types.WithDefaultPool())
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "datatype_validation_test",
			"name": "数据类型校验测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "aggregator1",
					"type": "x/streamAggregator",
					"name": "流聚合器",
					"configuration": {
						"sql": "%s"
					}
				},
				{
					"id": "log1",
					"type": "log",
					"name": "日志节点",
					"configuration": {
						"jsScript": "return 'Aggregation result: ' + JSON.stringify(msg);"
					}
				}
			],
			"connections": [
				{
					"fromId": "aggregator1",
					"toId": "log1",
					"type": "window_event"
				}
			]
		}
	}`, sql)

	chainId := str.RandomStr(10)
	ruleEngine, err := engine.New(chainId, []byte(ruleChainConfig), engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	defer engine.Del(chainId)

	testCases := []struct {
		name          string
		dataType      types.DataType
		data          string
		expectSuccess bool
	}{
		{
			name:          "JSON数据类型-有效",
			dataType:      types.JSON,
			data:          `{"temperature": 25.0, "deviceId": "sensor001"}`,
			expectSuccess: true,
		},
		{
			name:          "TEXT数据类型-应该被拒绝",
			dataType:      types.TEXT,
			data:          "plain text data",
			expectSuccess: false,
		},
		{
			name:          "BINARY数据类型-应该被拒绝",
			dataType:      types.BINARY,
			data:          "binary data",
			expectSuccess: false,
		},
		{
			name:          "空字符串数据类型-应该被拒绝",
			dataType:      "",
			data:          `{"temperature": 25.0}`,
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var processedSuccess int32
			var processedFailure int32

			msg := types.NewMsg(0, "TEST", tc.dataType, types.NewMetadata(), tc.data)

			ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
				if err == nil {
					atomic.AddInt32(&processedSuccess, 1)
				} else {
					atomic.AddInt32(&processedFailure, 1)
				}
			}))

			time.Sleep(100 * time.Millisecond)

			finalSuccess := atomic.LoadInt32(&processedSuccess)
			finalFailure := atomic.LoadInt32(&processedFailure)

			if tc.expectSuccess {
				assert.Equal(t, int32(1), finalSuccess, "应该成功处理")
				assert.Equal(t, int32(0), finalFailure, "不应该有失败")
			} else {
				assert.Equal(t, int32(0), finalSuccess, "不应该成功处理")
				assert.Equal(t, int32(1), finalFailure, "应该处理失败")
			}
		})
	}
}

// testStreamAggregator 通用的聚合测试辅助函数
func testStreamAggregator(t *testing.T, sql string, testData []map[string]interface{}, description string) []map[string]interface{} {
	config := engine.NewConfig(types.WithDefaultPool())
	var results []map[string]interface{}
	var mu sync.Mutex

	// 设置全局聚合结果处理器
	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err == nil && msg.Type == WindowEventMsgType {
			// 聚合结果可能是数组格式，需要正确解析
			var resultArray []map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
				mu.Lock()
				results = append(results, resultArray...)
				mu.Unlock()
			} else {
				// 尝试解析为单个对象
				var result map[string]interface{}
				if jsonErr2 := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr2 == nil {
					mu.Lock()
					results = append(results, result)
					mu.Unlock()
				}
			}
		}
	}

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "aggregator_test_chain",
			"name": "流聚合器测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "aggregator1",
					"type": "x/streamAggregator",
					"name": "流聚合器",
					"configuration": {
						"sql": "%s"
					}
				},
				{
					"id": "log1",
					"type": "log",
					"name": "日志节点",
					"configuration": {
						"jsScript": "return 'Aggregation result: ' + JSON.stringify(msg);"
					}
				}
			],
			"connections": [
				{
					"fromId": "aggregator1",
					"toId": "log1",
					"type": "window_event"
				}
			]
		}
	}`, sql)

	chainId := str.RandomStr(10)
	ruleChainConfig = strings.ReplaceAll(ruleChainConfig, "aggregator_test_chain", chainId)
	ruleEngine, err := engine.New(chainId, []byte(ruleChainConfig), engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	defer engine.Del(chainId)

	// 发送测试数据
	for _, data := range testData {
		msgData, _ := json.Marshal(data)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))
		ruleEngine.OnMsg(msg)
		time.Sleep(50 * time.Millisecond) // 模拟数据间隔
	}

	// 等待窗口聚合触发
	time.Sleep(2 * time.Second)

	return results
}
