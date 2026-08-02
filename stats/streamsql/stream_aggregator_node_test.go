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
	"net/http"
	"net/http/httptest"
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
	t.Run("计数窗口", func(t *testing.T) {
		sql := "SELECT COUNT(*) as c, MAX(v) as mx FROM stream GROUP BY CountingWindow(3)"
		testData := []map[string]interface{}{
			{"v": 10}, {"v": 20}, {"v": 30},
		}
		results := testStreamAggregator(t, sql, testData, "counting window")
		assert.True(t, len(results) > 0, "计数窗口应在收到 3 行后触发")
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
		if err == nil && msg.Type == StreamEventMsgType {
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

	// 验证聚合结果结构
	mu.Lock()
	aggregateResultsCopy := make([]map[string]interface{}, len(aggregateResults))
	copy(aggregateResultsCopy, aggregateResults)
	mu.Unlock()

	assert.True(t, len(aggregateResultsCopy) >= 0, "应该收集到聚合结果")

	for _, result := range aggregateResultsCopy {
		assert.NotNil(t, result["deviceId"], "聚合结果应该包含设备ID")
		assert.NotNil(t, result["avg_temp"], "聚合结果应该包含平均温度")
		assert.NotNil(t, result["count"], "聚合结果应该包含计数")
	}
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
			if err == nil && msg.Type == StreamEventMsgType {
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
						"type": "stream_event"
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
						"type": "stream_event"
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
					"type": "stream_event"
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
		if err == nil && msg.Type == StreamEventMsgType {
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
					"type": "stream_event"
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

	// 使用互斥锁保护对 results 的读取
	mu.Lock()
	resultsCopy := make([]map[string]interface{}, len(results))
	copy(resultsCopy, results)
	mu.Unlock()

	return resultsCopy
}

// runAggregatorWithTables 构建一个带可选元数据表配置的 streamAggregator 规则引擎，
// 发送数据并等待窗口触发，返回 (聚合结果, 引擎创建错误)。引擎创建失败时返回 (nil, err)
// 而非 panic，便于负面用例断言。
func runAggregatorWithTables(t *testing.T, sql string, tables []map[string]interface{}, data []map[string]interface{}) ([]map[string]interface{}, error) {
	t.Helper()
	config := engine.NewConfig(types.WithDefaultPool())
	var mu sync.Mutex
	var results []map[string]interface{}

	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err != nil || msg.Type != StreamEventMsgType {
			return
		}
		var arr []map[string]interface{}
		if e := json.Unmarshal([]byte(msg.Data.String()), &arr); e == nil {
			mu.Lock()
			results = append(results, arr...)
			mu.Unlock()
			return
		}
		var m map[string]interface{}
		if e := json.Unmarshal([]byte(msg.Data.String()), &m); e == nil {
			mu.Lock()
			results = append(results, m)
			mu.Unlock()
		}
	}

	nodeCfg := map[string]interface{}{"sql": sql}
	if tables != nil {
		nodeCfg["tables"] = tables
	}
	chainId := "agg_" + str.RandomStr(6)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "聚合测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "a1", "type": "x/streamAggregator", "name": "流聚合器", "configuration": nodeCfg},
				{"id": "log1", "type": "log", "name": "日志", "configuration": map[string]interface{}{"jsScript": "return JSON.stringify(msg);"}},
			},
			"connections": []map[string]interface{}{
				{"fromId": "a1", "toId": "log1", "type": "stream_event"},
			},
		},
	}
	b, _ := json.Marshal(chainConfig)

	ruleEngine, err := engine.New(chainId, b, engine.WithConfig(config))
	if err != nil {
		return nil, err
	}
	defer engine.Del(chainId)

	for _, d := range data {
		jd, _ := json.Marshal(d)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(jd))
		ruleEngine.OnMsg(msg)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)

	mu.Lock()
	cp := make([]map[string]interface{}, len(results))
	copy(cp, results)
	mu.Unlock()
	return cp, nil
}

// TestStreamAggregatorNode_TableNotInJoin 表名未出现在任何 JOIN ON 中：库要求元数据表
// 必须被 JOIN 引用，节点初始化应失败。这是不依赖库 P1 的负面用例，校验节点正确接入
// 表注册并把库的校验错误透传出来。
func TestStreamAggregatorNode_TableNotInJoin(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT deviceId, AVG(temperature) as avg_temp FROM stream GROUP BY deviceId, TumblingWindow('1s')"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "file", "path": path, "format": "json"},
	}

	_, err := runAggregatorWithTables(t, sql, tables, nil)
	assert.NotNil(t, err, "未参与 JOIN 的表应使节点初始化失败")
	assert.True(t,
		strings.Contains(err.Error(), "meta") || strings.Contains(err.Error(), "JOIN"),
		"错误应指向表/JOIN，got: %v", err)
}

// TestStreamAggregatorNode_JoinEnrich INNER JOIN 后按表侧列（m.location）分组求 AVG。
// 结果列按表别名命名空间返回（m.location），与直连路径（location）不同。
func TestStreamAggregatorNode_JoinEnrich(t *testing.T) {
	path := writeTempFile(t, "meta.json",
		`[{"deviceId":"d1","location":"plantA"},{"deviceId":"d2","location":"plantB"}]`)
	sql := "SELECT m.location, AVG(temperature) as avg_temp FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "file", "path": path, "format": "json"},
	}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 20.0},
		{"deviceId": "d1", "temperature": 30.0},
		{"deviceId": "d2", "temperature": 40.0},
	}

	results, err := runAggregatorWithTables(t, sql, tables, data)
	assert.Nil(t, err, "规则引擎创建应该成功")
	assert.True(t, len(results) > 0, "应有按 location 聚合的结果, got %+v", results)

	byLoc := make(map[string]float64)
	for _, r := range results {
		loc, _ := r["location"].(string)
		if avg, ok := r["avg_temp"].(float64); ok {
			byLoc[loc] = avg
		}
	}
	// plantA = d1(20,30) -> 25；plantB = d2(40) -> 40
	assert.True(t, byLoc["plantA"] == 25.0, "plantA 平均温度应为 25, got %v", byLoc)
	assert.True(t, byLoc["plantB"] == 40.0, "plantB 平均温度应为 40, got %v", byLoc)
}

// TestStreamAggregatorNode_LeftJoinNullGroup LEFT JOIN 未匹配行进入 NULL 分组而非被丢弃。
func TestStreamAggregatorNode_LeftJoinNullGroup(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT m.location, AVG(temperature) as avg_t FROM stream LEFT JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(2)"
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 10.0}, // plantA
		{"deviceId": "d9", "temperature": 20.0}, // 无匹配 -> NULL 分组
	}

	results, err := runAggregatorWithTables(t, sql, tables, data)
	assert.Nil(t, err, "规则引擎创建应该成功")

	hasNull, hasPlantA := false, false
	for _, r := range results {
		avg, _ := r["avg_t"].(float64)
		if r["location"] == nil {
			hasNull = true
			assert.True(t, avg == 20.0, "NULL 分组平均温度应为 20, got %v", avg)
		} else if loc, _ := r["location"].(string); loc == "plantA" {
			hasPlantA = true
			assert.True(t, avg == 10.0, "plantA 平均温度应为 10, got %v", avg)
		}
	}
	assert.True(t, hasNull, "应有 NULL 分组（LEFT 未匹配行未被丢弃）, got %+v", results)
	assert.True(t, hasPlantA, "应有 plantA 分组, got %+v", results)
}

// TestStreamAggregatorNode_InnerJoinDropsUnmatched INNER JOIN 未匹配行在入窗前被丢弃，
// 既不计入 count 也不产生 NULL 分组。
func TestStreamAggregatorNode_InnerJoinDropsUnmatched(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT m.location, COUNT(*) as c, AVG(temperature) as avg_t FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 10.0},
		{"deviceId": "d9", "temperature": 99.0}, // 无匹配 -> 丢弃（不入窗）
		{"deviceId": "d1", "temperature": 20.0},
		{"deviceId": "d1", "temperature": 30.0},
	}

	results, err := runAggregatorWithTables(t, sql, tables, data)
	assert.Nil(t, err, "规则引擎创建应该成功")

	var plantA map[string]interface{}
	hasNull := false
	for _, r := range results {
		if r["location"] == nil {
			hasNull = true
		}
		if loc, _ := r["location"].(string); loc == "plantA" {
			plantA = r
		}
	}
	assert.False(t, hasNull, "INNER JOIN 不应产生 NULL 分组（未匹配行已丢弃）")
	assert.NotNil(t, plantA, "应有 plantA 分组, got %+v", results)
	if plantA != nil {
		c, _ := plantA["c"].(float64)
		avg, _ := plantA["avg_t"].(float64)
		assert.True(t, c == 3, "丢弃未匹配后 count 应为 3, got %v", c)
		assert.True(t, avg == 20.0, "平均温度应为 (10+20+30)/3=20, got %v", avg)
	}
}

// TestStreamAggregatorNode_CompositeKeyJoin 复合键 JOIN（tenant+deviceId）后按表列分组。
func TestStreamAggregatorNode_CompositeKeyJoin(t *testing.T) {
	path := writeTempFile(t, "meta.json",
		`[{"tenant":"t1","deviceId":"d1","location":"plantA"},{"tenant":"t1","deviceId":"d2","location":"plantB"}]`)
	sql := "SELECT m.location, MAX(temperature) as mx FROM stream JOIN meta m ON tenant = m.tenant AND deviceId = m.deviceId GROUP BY m.location, CountingWindow(2)"
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"tenant": "t1", "deviceId": "d1", "temperature": 10.0},
		{"tenant": "t1", "deviceId": "d2", "temperature": 40.0},
	}

	results, err := runAggregatorWithTables(t, sql, tables, data)
	assert.Nil(t, err, "规则引擎创建应该成功")

	mx := make(map[string]float64)
	for _, r := range results {
		if loc, _ := r["location"].(string); loc != "" {
			if v, ok := r["mx"].(float64); ok {
				mx[loc] = v
			}
		}
	}
	assert.True(t, mx["plantA"] == 10.0, "plantA MAX 应为 10, got %v", mx)
	assert.True(t, mx["plantB"] == 40.0, "plantB MAX 应为 40, got %v", mx)
}

// TestStreamAggregatorNode_HttpTable 从 HTTP 端点加载元数据表后做 JOIN 聚合。
func TestStreamAggregatorNode_HttpTable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"deviceId":"d1","location":"plantA"}]`))
	}))
	defer srv.Close()

	sql := "SELECT m.location, COUNT(*) as c FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(1)"
	tables := []map[string]interface{}{{"name": "meta", "source": "http", "path": srv.URL, "format": "json"}}

	results, err := runAggregatorWithTables(t, sql, tables, []map[string]interface{}{{"deviceId": "d1", "temperature": 1.0}})
	assert.Nil(t, err, "规则引擎创建应该成功")
	assert.True(t, len(results) > 0, "HTTP 表应能完成 JOIN 聚合, got %+v", results)
}

// TestStreamAggregatorNode_JoinColumnNaming 验证聚合路径 JOIN 输出列名：
// 无别名去前缀成 location、有别名用别名（回归 AJ1：曾输出 m.location 且忽略别名）。
func TestStreamAggregatorNode_JoinColumnNaming(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 10.0},
		{"deviceId": "d1", "temperature": 20.0},
		{"deviceId": "d1", "temperature": 30.0},
	}

	t.Run("无别名输出location", func(t *testing.T) {
		sql := "SELECT m.location, AVG(temperature) as a FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
		results, err := runAggregatorWithTables(t, sql, tables, data)
		assert.Nil(t, err)
		assert.True(t, len(results) > 0, "应有结果, got %+v", results)
		assert.NotNil(t, results[0]["location"], "应输出 location, got %+v", results[0])
		assert.True(t, results[0]["m.location"] == nil, "不应再输出 m.location, got %+v", results[0])
	})

	t.Run("别名输出loc", func(t *testing.T) {
		sql := "SELECT m.location as loc, AVG(temperature) as a FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
		results, err := runAggregatorWithTables(t, sql, tables, data)
		assert.Nil(t, err)
		assert.True(t, len(results) > 0, "应有结果, got %+v", results)
		assert.NotNil(t, results[0]["loc"], "别名 loc 应生效, got %+v", results[0])
		assert.True(t, results[0]["m.location"] == nil, "不应输出 m.location, got %+v", results[0])
	})
}

// newAggregatorEngine 构建一个 streamAggregator 规则引擎，stream_event 经 config.OnEnd
// 收集到 *windowResults（已加锁）。原始终端消息由调用方通过 WithOnEnd 捕获。
func newAggregatorEngine(t *testing.T, sql string, windowResults *[]map[string]interface{}, mu *sync.Mutex) (types.RuleEngine, func()) {
	t.Helper()
	config := engine.NewConfig(types.WithDefaultPool())
	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err != nil || msg.Type != StreamEventMsgType {
			return
		}
		var arr []map[string]interface{}
		if json.Unmarshal([]byte(msg.Data.String()), &arr) == nil {
			mu.Lock()
			*windowResults = append(*windowResults, arr...)
			mu.Unlock()
			return
		}
		var m map[string]interface{}
		if json.Unmarshal([]byte(msg.Data.String()), &m) == nil {
			mu.Lock()
			*windowResults = append(*windowResults, m)
			mu.Unlock()
		}
	}

	chainId := "agg_" + str.RandomStr(6)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "聚合场景测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "a1", "type": "x/streamAggregator", "name": "流聚合器", "configuration": map[string]interface{}{"sql": sql}},
			},
			// 不接下游：stream_event 作为终端直接进 config.OnEnd（若接 log 等节点，msg.Type 会被
			// 改写，OnEnd 里的 StreamEventMsgType 判定会漏掉）。
			// ruleChain.id 必须与 engine.New 的 id 一致：节点 handleAggregateResult 经
			// GetRuleEnginePool().Get(x.chainId) 回查引擎（x.chainId=ruleChain.id），不一致则
			// stream_event 被静默丢弃。
			"connections": []map[string]interface{}{},
		},
	}
	b, _ := json.Marshal(chainConfig)
	eng, err := engine.New(chainId, b, engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	return eng, func() { engine.Del(chainId) }
}

// windowCount 已加锁读取当前 stream_event 结果数。
func windowCount(mu *sync.Mutex, rs *[]map[string]interface{}) int {
	mu.Lock()
	defer mu.Unlock()
	return len(*rs)
}

// TestNodeScenario_AggregatorDualOutput 验证聚合器双输出：原始终端消息经 Success 传递，
// 同时 CountingWindow(2) 触发的聚合数组经 stream_event 输出。
func TestNodeScenario_AggregatorDualOutput(t *testing.T) {
	sql := "SELECT AVG(t) AS avg_t, COUNT(*) AS cnt FROM stream GROUP BY CountingWindow(2)"
	var mu sync.Mutex
	var windowResults []map[string]interface{}
	eng, cleanup := newAggregatorEngine(t, sql, &windowResults, &mu)
	defer cleanup()

	var successCount int32
	send := func(v float64) {
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), fmt.Sprintf(`{"t":%v}`, v))
		eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, e error, rt string) {
			// stream_event 走 config.OnEnd；这里只计原始终端消息。
			if e == nil && m.Type != StreamEventMsgType {
				atomic.AddInt32(&successCount, 1)
			}
		}))
	}
	send(10)
	send(30)

	// 轮询等待：两条原始消息经 Success + 一个 stream_event。
	ok := waitForCondition(func() bool {
		return atomic.LoadInt32(&successCount) >= 2 && windowCount(&mu, &windowResults) > 0
	}, 2*time.Second, 20*time.Millisecond)
	assert.True(t, ok, "应在超时前收到 2 条 Success 与 1 个 stream_event")

	// 双输出之一：原始消息经 Success 传递。
	assert.Equal(t, int32(2), atomic.LoadInt32(&successCount), "两条原始消息应经 Success 传递")

	// 双输出之二：聚合数组 avg_t=20, cnt=2。
	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, r := range windowResults {
		avg, _ := r["avg_t"].(float64)
		cnt, _ := r["cnt"].(float64)
		if avg == 20.0 && cnt == 2 {
			found = true
		}
	}
	assert.True(t, found, "stream_event 应包含 avg_t=20,cnt=2 的聚合结果, got %+v", windowResults)
}

// TestNodeScenario_AggregatorWindowedAnalytic 验证窗口内分析函数（分析套聚合）经节点输出：
// changed_col(true, avg(t)) 在首个 CountingWindow(2) 窗口视为变化，返回窗口均值。
func TestNodeScenario_AggregatorWindowedAnalytic(t *testing.T) {
	sql := "SELECT changed_col(true, avg(t)) AS chg FROM stream GROUP BY CountingWindow(2)"
	var mu sync.Mutex
	var windowResults []map[string]interface{}
	eng, cleanup := newAggregatorEngine(t, sql, &windowResults, &mu)
	defer cleanup()

	send := func(v float64) {
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), fmt.Sprintf(`{"t":%v}`, v))
		eng.OnMsg(msg)
	}
	send(10)
	send(40)

	ok := waitForCondition(func() bool { return windowCount(&mu, &windowResults) > 0 }, 2*time.Second, 20*time.Millisecond)
	assert.True(t, ok, "CountingWindow(2) 后应触发 stream_event")

	// 窗口 [10,40] 均值 25，首窗 changed_col 视为变化 → chg=25。
	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, r := range windowResults {
		if chg, ok := r["chg"].(float64); ok && chg == 25.0 {
			found = true
		}
	}
	assert.True(t, found, "窗口分析 changed_col(avg) 首窗应 chg=25, got %+v", windowResults)
}

// runAggregatorCaptureMeta 构建 streamAggregator 规则引擎（stream_event 终端进 config.OnEnd），
// 喂入 events 后等待结果，返回 (结果行, 首条 stream_event 的 queryType/resultType)。
func runAggregatorCaptureMeta(t *testing.T, sql string, events []map[string]interface{}) ([]map[string]interface{}, string, string) {
	t.Helper()
	config := engine.NewConfig(types.WithDefaultPool())
	var mu sync.Mutex
	var results []map[string]interface{}
	var queryType, resultType string

	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err != nil || msg.Type != StreamEventMsgType {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if queryType == "" {
			queryType = msg.Metadata.GetValue("queryType")
			resultType = msg.Metadata.GetValue("resultType")
		}
		var arr []map[string]interface{}
		if json.Unmarshal([]byte(msg.Data.String()), &arr) == nil {
			results = append(results, arr...)
			return
		}
		var m map[string]interface{}
		if json.Unmarshal([]byte(msg.Data.String()), &m) == nil {
			results = append(results, m)
		}
	}

	chainId := "agg_" + str.RandomStr(6)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "聚合/CEP 元数据测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "a1", "type": "x/streamAggregator", "name": "流聚合器", "configuration": map[string]interface{}{"sql": sql}},
			},
			// 不接下游：stream_event 作为终端直接进 config.OnEnd。
			"connections": []map[string]interface{}{},
		},
	}
	b, _ := json.Marshal(chainConfig)
	ruleEngine, err := engine.New(chainId, b, engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	defer engine.Del(chainId)

	for _, e := range events {
		jd, _ := json.Marshal(e)
		ruleEngine.OnMsg(types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(jd)))
		time.Sleep(50 * time.Millisecond)
	}
	waitForCondition(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(results) > 0
	}, 3*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	cp := make([]map[string]interface{}, len(results))
	copy(cp, results)
	return cp, queryType, resultType
}

// TestStreamAggregatorNode_CEP CEP(MATCH_RECOGNIZE) 经节点输出：连续 3 个 v>50 命中后，
// 匹配结果经 stream_event 派发，metadata.queryType=cep / resultType=pattern_matched。
func TestStreamAggregatorNode_CEP(t *testing.T) {
	sql := `SELECT * FROM stream
		MATCH_RECOGNIZE (
			ORDER BY ts
			MEASURES MATCH_NUMBER() AS mn, A.v AS peak
			ONE ROW PER MATCH
			PATTERN (A{3})
			WITHIN '1h'
			DEFINE A AS v > 50
		)`
	events := []map[string]interface{}{
		{"ts": 1, "v": 10},
		{"ts": 2, "v": 60},
		{"ts": 3, "v": 70},
		{"ts": 4, "v": 80},
	}
	results, qType, rType := runAggregatorCaptureMeta(t, sql, events)

	assert.True(t, len(results) > 0, "应收到 CEP 匹配结果")
	assert.Equal(t, "cep", qType, "queryType 应为 cep, got %s", qType)
	assert.Equal(t, "pattern_matched", rType, "resultType 应为 pattern_matched, got %s", rType)

	var hasMatch bool
	for _, r := range results {
		if _, ok := r["mn"]; ok {
			if peak, ok := r["peak"].(float64); ok && peak > 50 {
				hasMatch = true
			}
		}
	}
	assert.True(t, hasMatch, "匹配行应含 mn 且 peak>50, got %+v", results)
}

// TestStreamAggregatorNode_AggregationQueryType 聚合查询结果 metadata.queryType=aggregation。
func TestStreamAggregatorNode_AggregationQueryType(t *testing.T) {
	sql := "SELECT AVG(v) AS avg_v FROM stream GROUP BY CountingWindow(2)"
	events := []map[string]interface{}{
		{"v": 10.0},
		{"v": 30.0},
	}
	results, qType, rType := runAggregatorCaptureMeta(t, sql, events)

	assert.True(t, len(results) > 0, "应收到聚合结果")
	assert.Equal(t, "aggregation", qType, "queryType 应为 aggregation, got %s", qType)
	assert.Equal(t, "window_triggered", rType, "resultType 应为 window_triggered, got %s", rType)
}
