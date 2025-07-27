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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/str"
)

// TestStreamTransformNode_BasicTransform 测试基本数据转换功能
func TestStreamTransformNode_BasicTransform(t *testing.T) {
	t.Run("温度单位转换", func(t *testing.T) {
		sql := "SELECT deviceId, temperature * 1.8 + 32 as temp_fahrenheit, humidity FROM stream"
		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 25.0, "humidity": 60},
			{"deviceId": "sensor002", "temperature": 30.0, "humidity": 70},
		}

		results := testStreamTransform(t, sql, testData, "temperature conversion test")

		assert.Equal(t, 2, len(results), "应该有2个转换结果")

		// 验证第一个结果
		firstResult := results[0]
		assert.Equal(t, "sensor001", firstResult["deviceId"], "设备ID应该正确")
		assert.Equal(t, float64(77), firstResult["temp_fahrenheit"].(float64), "华氏温度应该正确")
		assert.Equal(t, float64(60), firstResult["humidity"].(float64), "湿度应该正确")
	})

	t.Run("字段过滤和选择", func(t *testing.T) {
		sql := "SELECT temperature, humidity FROM stream WHERE temperature > 20"
		testData := []map[string]interface{}{
			{"temperature": 25.0, "humidity": 60, "other": "ignore"},
			{"temperature": 15.0, "humidity": 70, "other": "ignore"}, // 会被过滤
			{"temperature": 30.0, "humidity": 80, "other": "ignore"},
		}

		results := testStreamTransform(t, sql, testData, "field filtering test")

		assert.Equal(t, 2, len(results), "应该有2个过滤结果")

		// 验证结果不包含其他字段
		for _, result := range results {
			_, hasOther := result["other"]
			assert.False(t, hasOther, "结果不应包含未选择的字段")
			assert.NotNil(t, result["temperature"], "应该包含温度字段")
			assert.NotNil(t, result["humidity"], "应该包含湿度字段")
		}
	})

	t.Run("字段别名和计算", func(t *testing.T) {
		sql := "SELECT deviceId as id, temperature as temp, temperature + humidity as comfort_index FROM stream"
		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 25.0, "humidity": 60},
		}

		results := testStreamTransform(t, sql, testData, "field alias test")

		assert.Equal(t, 1, len(results), "应该有1个转换结果")

		result := results[0]
		assert.Equal(t, "sensor001", result["id"], "别名字段应该正确")
		assert.Equal(t, float64(25), result["temp"].(float64), "温度别名应该正确")
		assert.Equal(t, float64(85), result["comfort_index"].(float64), "计算字段应该正确")
	})
}

// TestStreamTransformNode_Validation 测试节点配置验证
func TestStreamTransformNode_Validation(t *testing.T) {
	t.Run("空SQL验证", func(t *testing.T) {
		node := &StreamTransformNode{}
		config := map[string]interface{}{
			"sql": "",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "空SQL应该返回错误")
		assert.Equal(t, ErrTransformSQLEmpty, err, "应该是SQL为空的错误")
	})

	t.Run("拒绝聚合查询", func(t *testing.T) {
		node := &StreamTransformNode{}
		config := map[string]interface{}{
			"sql": "SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "聚合查询应该被拒绝")
		assert.True(t, err.Error() != "", "错误信息不应为空")
	})

	t.Run("接受转换查询", func(t *testing.T) {
		node := &StreamTransformNode{}
		config := map[string]interface{}{
			"sql": "SELECT temperature * 1.8 + 32 as temp_fahrenheit FROM stream",
		}

		err := node.Init(types.NewConfig(), config)
		assert.Nil(t, err, "转换查询应该被接受")
	})

	t.Run("无效SQL语法", func(t *testing.T) {
		node := &StreamTransformNode{}
		config := map[string]interface{}{
			"sql": "INVALID SQL SYNTAX",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "无效SQL应该返回错误")
	})
}

// TestStreamTransformNode_ConcurrentProcessing 测试并发处理
func TestStreamTransformNode_ConcurrentProcessing(t *testing.T) {
	sql := "SELECT deviceId, temperature, humidity FROM stream WHERE temperature > 20"

	config := engine.NewConfig(types.WithDefaultPool())
	var successCount int32
	var failureCount int32
	var mu sync.Mutex
	var results []map[string]interface{}

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "concurrent_transform_test",
			"name": "并发转换测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "transform1",
					"type": "x/streamTransform",
					"name": "流转换器",
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
	const numGoroutines = 10
	const messagesPerGoroutine = 20

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				temperature := 15.0 + float64(j%30) // 温度范围 15-45
				testData := map[string]interface{}{
					"deviceId":    fmt.Sprintf("sensor_%d", goroutineId),
					"temperature": temperature,
					"humidity":    60.0,
				}

				msgData, _ := json.Marshal(testData)
				msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(msgData))

				ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
					if err != nil {
						atomic.AddInt32(&failureCount, 1)
					} else {
						atomic.AddInt32(&successCount, 1)

						// 收集结果
						var result map[string]interface{}
						if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr == nil {
							mu.Lock()
							results = append(results, result)
							mu.Unlock()
						}
					}
				}))

				time.Sleep(time.Millisecond) // 模拟真实间隔
			}
		}(i)
	}

	wg.Wait()

	// 等待所有消息处理完成
	time.Sleep(500 * time.Millisecond)

	finalSuccess := atomic.LoadInt32(&successCount)

	assert.True(t, finalSuccess > 0, "应该有成功处理的消息")
	
	// 验证所有结果都有正确的字段
	mu.Lock()
	resultsCount := len(results)
	resultsCopy := make([]map[string]interface{}, len(results))
	copy(resultsCopy, results)
	mu.Unlock()
	
	assert.True(t, resultsCount > 0, "应该收集到转换结果")
	
	for _, result := range resultsCopy {
		assert.NotNil(t, result["deviceId"], "结果应该包含设备ID")
		assert.NotNil(t, result["temperature"], "结果应该包含温度")
		assert.NotNil(t, result["humidity"], "结果应该包含湿度")
	}
}

// TestStreamTransformNode_EdgeCases 测试边界情况
func TestStreamTransformNode_EdgeCases(t *testing.T) {
	t.Run("空数据处理", func(t *testing.T) {
		sql := "SELECT * FROM stream"
		testData := []map[string]interface{}{
			{}, // 空对象
		}

		results := testStreamTransform(t, sql, testData, "empty data test")
		assert.Equal(t, 1, len(results), "空数据也应该被处理")
	})

	t.Run("特殊字符处理", func(t *testing.T) {
		sql := "SELECT deviceId, message FROM stream"
		testData := []map[string]interface{}{
			{"deviceId": "sensor-001", "message": "hello, world!"},
			{"deviceId": "sensor_002", "message": "测试中文字符"},
		}

		results := testStreamTransform(t, sql, testData, "special characters test")
		assert.Equal(t, 2, len(results), "特殊字符应该被正确处理")

		assert.Equal(t, "sensor-001", results[0]["deviceId"], "特殊字符设备ID应该正确")
		assert.Equal(t, "测试中文字符", results[1]["message"], "中文字符应该被正确处理")
	})

	t.Run("数值类型处理", func(t *testing.T) {
		sql := "SELECT intVal, floatVal, boolVal FROM stream"
		testData := []map[string]interface{}{
			{"intVal": 42, "floatVal": 3.14, "boolVal": true},
			{"intVal": 0, "floatVal": -1.5, "boolVal": false},
		}

		results := testStreamTransform(t, sql, testData, "data types test")
		assert.Equal(t, 2, len(results), "不同数据类型应该被正确处理")

		// 验证数据类型保持
		assert.Equal(t, float64(42), results[0]["intVal"].(float64), "整数应该被正确处理")
		assert.Equal(t, 3.14, results[0]["floatVal"].(float64), "浮点数应该被正确处理")
		assert.Equal(t, true, results[0]["boolVal"].(bool), "布尔值应该被正确处理")
	})
}

// TestStreamTransformNode_ArrayInput 测试数组输入处理
func TestStreamTransformNode_ArrayInput(t *testing.T) {
	t.Run("处理JSON数组输入-全部成功", func(t *testing.T) {
		sql := "SELECT temperature * 1.8 + 32 as temp_fahrenheit, deviceId FROM stream"

		// 准备数组测试数据
		arrayData := []map[string]interface{}{
			{"temperature": 0.0, "deviceId": "sensor001"},   // 32°F
			{"temperature": 100.0, "deviceId": "sensor002"}, // 212°F
			{"temperature": 25.0, "deviceId": "sensor003"},  // 77°F
		}

		results := testStreamTransformArray(t, sql, arrayData, "array input - all success")

		assert.Equal(t, 3, len(results), "应该有3个转换结果")

		// 验证转换结果
		expectedTemps := []float64{32.0, 212.0, 77.0}
		for i, result := range results {
			assert.Equal(t, expectedTemps[i], result["temp_fahrenheit"].(float64),
				"第%d个结果的华氏温度应该正确", i+1)
			assert.Equal(t, fmt.Sprintf("sensor%03d", i+1), result["deviceId"],
				"第%d个结果的设备ID应该正确", i+1)
		}
	})

	t.Run("处理JSON数组输入-部分过滤", func(t *testing.T) {
		sql := "SELECT temperature, deviceId FROM stream WHERE temperature > 20"

		// 准备包含过滤条件的数组测试数据
		arrayData := []map[string]interface{}{
			{"temperature": 15.0, "deviceId": "sensor001"}, // 被过滤
			{"temperature": 25.0, "deviceId": "sensor002"}, // 通过
			{"temperature": 10.0, "deviceId": "sensor003"}, // 被过滤
			{"temperature": 30.0, "deviceId": "sensor004"}, // 通过
		}

		results := testStreamTransformArray(t, sql, arrayData, "array input - partial filtering")

		assert.Equal(t, 2, len(results), "应该有2个过滤后的结果")

		// 验证过滤结果
		for _, result := range results {
			temp := result["temperature"].(float64)
			assert.True(t, temp > 20, "过滤后的温度应该大于20")
		}
	})

	t.Run("处理JSON数组输入-全部过滤", func(t *testing.T) {
		sql := "SELECT temperature, deviceId FROM stream WHERE temperature > 100"

		// 准备全部被过滤的数组测试数据
		arrayData := []map[string]interface{}{
			{"temperature": 15.0, "deviceId": "sensor001"},
			{"temperature": 25.0, "deviceId": "sensor002"},
			{"temperature": 10.0, "deviceId": "sensor003"},
		}

		testStreamTransformArrayFailure(t, sql, arrayData, "array input - all filtered")
	})

	t.Run("处理空数组输入", func(t *testing.T) {
		sql := "SELECT temperature, deviceId FROM stream"

		// 空数组
		arrayData := []map[string]interface{}{}

		testStreamTransformArrayFailure(t, sql, arrayData, "empty array input")
	})

	t.Run("处理复杂数组转换", func(t *testing.T) {
		sql := "SELECT deviceId, temperature, humidity, temperature * 1.8 + 32 as temp_fahrenheit FROM stream"

		arrayData := []map[string]interface{}{
			{"temperature": 20.0, "humidity": 60.0, "deviceId": "sensor001"},
			{"temperature": 30.0, "humidity": 70.0, "deviceId": "sensor002"},
			{"temperature": 15.0, "humidity": 50.0, "deviceId": "sensor003"},
		}

		results := testStreamTransformArray(t, sql, arrayData, "complex array transformation")

		assert.Equal(t, 3, len(results), "应该有3个复杂转换结果")

		// 验证复杂转换结果
		for i, result := range results {
			assert.NotNil(t, result["deviceId"], "应该包含设备ID")
			assert.NotNil(t, result["temperature"], "应该包含温度")
			assert.NotNil(t, result["humidity"], "应该包含湿度")
			assert.NotNil(t, result["temp_fahrenheit"], "应该包含华氏温度")

			// 验证华氏温度计算正确性
			temp := result["temperature"].(float64)
			expectedFahrenheit := temp*1.8 + 32
			actualFahrenheit := result["temp_fahrenheit"].(float64)
			assert.Equal(t, expectedFahrenheit, actualFahrenheit,
				"第%d个结果的华氏温度应该正确", i+1)
		}
	})
}

// TestStreamTransformNode_DataTypeValidation 测试数据类型校验
func TestStreamTransformNode_DataTypeValidation(t *testing.T) {
	sql := "SELECT temperature, deviceId FROM stream WHERE temperature > 20"

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
					"id": "transform1",
					"type": "x/streamTransform",
					"name": "流转换器",
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
			name:          "JSON数组数据类型-有效",
			dataType:      types.JSON,
			data:          `[{"temperature": 25.0, "deviceId": "sensor001"}, {"temperature": 30.0, "deviceId": "sensor002"}]`,
			expectSuccess: true,
		},
		{
			name:          "TEXT数据类型-应该被拒绝",
			dataType:      types.TEXT,
			data:          "temperature=25.0,deviceId=sensor001",
			expectSuccess: false,
		},
		{
			name:          "BINARY数据类型-应该被拒绝",
			dataType:      types.BINARY,
			data:          "binary sensor data",
			expectSuccess: false,
		},
		{
			name:          "空字符串数据类型-应该被拒绝",
			dataType:      "",
			data:          `{"temperature": 25.0, "deviceId": "sensor001"}`,
			expectSuccess: false,
		},
		{
			name:          "XML数据类型-应该被拒绝",
			dataType:      "XML",
			data:          `<sensor><temperature>25.0</temperature><deviceId>sensor001</deviceId></sensor>`,
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var processedSuccess int32
			var processedFailure int32

			msg := types.NewMsg(0, "TEST", tc.dataType, types.NewMetadata(), tc.data)

			ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
				if err == nil && msg.Metadata.GetValue(Match) == MatchTrue {
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

// testStreamTransform 通用的转换测试辅助函数
func testStreamTransform(t *testing.T, sql string, testData []map[string]interface{}, description string) []map[string]interface{} {
	config := engine.NewConfig(types.WithDefaultPool())
	var results []map[string]interface{}
	var successCount int32
	var mu sync.Mutex

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "transform_test_chain",
			"name": "流转换器测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "transform1",
					"type": "x/streamTransform",
					"name": "流转换器",
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

	// 发送测试数据
	for _, data := range testData {
		msgData, _ := json.Marshal(data)
		msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(msgData))

		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Metadata.GetValue(Match) == MatchTrue {
				atomic.AddInt32(&successCount, 1)

				// 解析转换结果
				var result map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr == nil {
					mu.Lock()
					results = append(results, result)
					mu.Unlock()
				}
			}
		}))

		time.Sleep(10 * time.Millisecond) // 给处理时间
	}

	// 等待处理完成
	time.Sleep(100 * time.Millisecond)

	// 使用互斥锁保护对 results 的读取
	mu.Lock()
	resultsCopy := make([]map[string]interface{}, len(results))
	copy(resultsCopy, results)
	mu.Unlock()

	return resultsCopy
}

// testStreamTransformArray 数组转换测试辅助函数（成功情况）
func testStreamTransformArray(t *testing.T, sql string, testData []map[string]interface{}, description string) []map[string]interface{} {
	config := engine.NewConfig(types.WithDefaultPool())
	var results []map[string]interface{}
	var successCount int32
	var mu sync.Mutex

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "array_transform_test_chain",
			"name": "数组转换器测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "transform1",
					"type": "x/streamTransform",
					"name": "流转换器",
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

	// 发送数组测试数据
	msgData, _ := json.Marshal(testData)
	msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(msgData))

	ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err == nil && msg.Metadata.GetValue(Match) == MatchTrue {
			atomic.AddInt32(&successCount, 1)

			// 解析转换结果数组
			var resultArray []map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
				mu.Lock()
				results = resultArray
				mu.Unlock()
			}
		}
	}))

	time.Sleep(100 * time.Millisecond)

	finalSuccess := atomic.LoadInt32(&successCount)

	assert.Equal(t, int32(1), finalSuccess, "数组应该成功转换")
	
	// 使用互斥锁保护对 results 的读取
	mu.Lock()
	resultsCopy := make([]map[string]interface{}, len(results))
	copy(resultsCopy, results)
	mu.Unlock()
	
	return resultsCopy
}

// testStreamTransformArrayFailure 数组转换测试辅助函数（失败情况）
func testStreamTransformArrayFailure(t *testing.T, sql string, testData []map[string]interface{}, description string) {
	config := engine.NewConfig(types.WithDefaultPool())
	var failureCount int32

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "array_transform_failure_test",
			"name": "数组转换失败测试",
			"root": true
		},
		"metadata": {
			"nodes": [
				{
					"id": "transform1",
					"type": "x/streamTransform",
					"name": "流转换器",
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

	// 发送数组测试数据
	msgData, _ := json.Marshal(testData)
	msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(msgData))

	ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err != nil || msg.Metadata.GetValue(Match) == MatchFalse {
			atomic.AddInt32(&failureCount, 1)
		}
	}))

	time.Sleep(100 * time.Millisecond)

	finalFailure := atomic.LoadInt32(&failureCount)

	assert.Equal(t, int32(1), finalFailure, "数组应该处理失败")
}
