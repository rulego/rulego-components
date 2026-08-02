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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/str"
)

// TestStreamTransformNode_JoinEnrich verifies the node loads metadata tables
// from configuration and enriches stream rows via stream-table JOIN.
func TestStreamTransformNode_JoinEnrich(t *testing.T) {
	sql := "SELECT deviceId, m.location, m.type FROM stream JOIN meta m ON deviceId = m.deviceId"
	tables := []map[string]interface{}{
		{
			"name": "meta",
			"rows": []map[string]interface{}{
				{"deviceId": "d1", "location": "plantA", "type": "temp"},
				{"deviceId": "d2", "location": "plantB", "type": "humid"},
			},
		},
	}

	// Build the rule chain config with both sql and tables, marshalling to keep
	// the nested rows JSON valid (no manual escaping).
	nodeCfg := map[string]interface{}{"sql": sql, "tables": tables}

	config := engine.NewConfig(types.WithDefaultPool())
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": "join_test_chain", "name": "JOIN测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "transform1", "type": "x/streamTransform", "name": "流转换器", "configuration": nodeCfg},
			},
			"connections": []interface{}{},
		},
	}
	chainConfigBytes, _ := json.Marshal(chainConfig)

	chainId := str.RandomStr(10)
	ruleEngine, err := engine.New(chainId, chainConfigBytes, engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	defer engine.Del(chainId)

	var mu sync.Mutex
	var results []map[string]interface{}
	var successCount int32

	send := func(data map[string]interface{}) {
		msgData, _ := json.Marshal(data)
		msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(msgData))
		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Metadata.GetValue(Match) == MatchTrue {
				atomic.AddInt32(&successCount, 1)
				var result map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr == nil {
					mu.Lock()
					results = append(results, result)
					mu.Unlock()
				}
			}
		}))
		time.Sleep(10 * time.Millisecond)
	}

	send(map[string]interface{}{"deviceId": "d1", "temperature": 35})
	// d3 has no metadata -> INNER JOIN drops it (Filtered path, not counted).
	send(map[string]interface{}{"deviceId": "d3"})
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, int32(1), atomic.LoadInt32(&successCount), "只有 d1 命中元数据")
	if len(results) != 1 {
		t.Fatalf("results len=%d, want 1", len(results))
	}
	assert.Equal(t, "d1", results[0]["deviceId"], "deviceId 正确")
	assert.Equal(t, "plantA", results[0]["location"], "location 富化正确")
	assert.Equal(t, "temp", results[0]["type"], "type 富化正确")
}

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

		testStreamTransformArrayFiltered(t, sql, arrayData, "array input - all filtered")
	})

	t.Run("处理空数组输入", func(t *testing.T) {
		sql := "SELECT temperature, deviceId FROM stream"

		// 空数组
		arrayData := []map[string]interface{}{}

		testStreamTransformArrayFiltered(t, sql, arrayData, "empty array input")
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

// TestStreamTransformNode_FilteredRelation 验证被过滤的行（WHERE 不满足 / changed_cols 无变化）走 Filtered 链而非 Failure。
func TestStreamTransformNode_FilteredRelation(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())

	newEngine := func(t *testing.T, sql string) (types.RuleEngine, func()) {
		chainConfig := fmt.Sprintf(`{
			"ruleChain": {"id": "filtered_%s", "name": "过滤关系测试", "root": true},
			"metadata": {
				"nodes": [{"id": "t1", "type": "x/streamTransform", "name": "流转换器", "configuration": {"sql": %q}}],
				"connections": []
			}
		}`, str.RandomStr(6), sql)
		id := str.RandomStr(10)
		eng, err := engine.New(id, []byte(chainConfig), engine.WithConfig(config))
		assert.Nil(t, err, "规则引擎创建应该成功")
		return eng, func() { engine.Del(id) }
	}

	// send 发送一条消息并返回最终的 relationType（带超时，避免链路无回调时挂死）。
	send := func(eng types.RuleEngine, data map[string]interface{}) string {
		b, _ := json.Marshal(data)
		msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(b))
		out := make(chan string, 1)
		eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, e error, rt string) {
			select {
			case out <- rt:
			default:
			}
		}))
		select {
		case rt := <-out:
			return rt
		case <-time.After(2 * time.Second):
			t.Fatalf("消息处理超时")
			return ""
		}
	}

	t.Run("WHERE不满足走Filtered", func(t *testing.T) {
		eng, cleanup := newEngine(t, "SELECT temperature FROM stream WHERE temperature > 100")
		defer cleanup()
		rt := send(eng, map[string]interface{}{"temperature": 25.0})
		assert.Equal(t, types.False, rt, "WHERE 不满足应走 Filtered，而非 Failure")
	})

	t.Run("changed_cols无变化走Filtered", func(t *testing.T) {
		eng, cleanup := newEngine(t, `SELECT changed_cols("c_", true, temperature) FROM stream`)
		defer cleanup()
		// 首次：视为变化 → Success
		assert.Equal(t, types.Success, send(eng, map[string]interface{}{"temperature": 23.0}), "首次应走 Success")
		// 再次同值：无变化 → Filtered
		assert.Equal(t, types.False, send(eng, map[string]interface{}{"temperature": 23.0}), "无变化应走 Filtered")
	})
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

// testStreamTransformArrayFiltered 数组转换测试辅助函数：全部元素被过滤（无出错）→ 走 Filtered 链。
func testStreamTransformArrayFiltered(t *testing.T, sql string, testData []map[string]interface{}, description string) {
	config := engine.NewConfig(types.WithDefaultPool())
	var filteredCount int32

	// 创建测试规则链
	ruleChainConfig := fmt.Sprintf(`{
		"ruleChain": {
			"id": "array_transform_filtered_test",
			"name": "数组过滤测试",
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
		if relationType == types.False {
			atomic.AddInt32(&filteredCount, 1)
		}
	}))

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&filteredCount), "数组全部被过滤应走 Filtered: %s", description)
}

// newJoinEngine 构建一个带元数据表配置的 streamTransform 规则引擎，返回引擎与清理函数。
func newJoinEngine(t *testing.T, sql string, tables []map[string]interface{}) (types.RuleEngine, func()) {
	t.Helper()
	config := engine.NewConfig(types.WithDefaultPool())
	nodeCfg := map[string]interface{}{"sql": sql}
	if tables != nil {
		nodeCfg["tables"] = tables
	}
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": "join_" + str.RandomStr(6), "name": "JOIN测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "t1", "type": "x/streamTransform", "name": "流转换器", "configuration": nodeCfg},
			},
			"connections": []interface{}{},
		},
	}
	b, _ := json.Marshal(chainConfig)
	chainId := str.RandomStr(10)
	eng, err := engine.New(chainId, b, engine.WithConfig(config))
	assert.Nil(t, err, "规则引擎创建应该成功")
	return eng, func() { engine.Del(chainId) }
}

// sendJoinMsg 发送一条消息并同步等待结果（命中 WHERE/JOIN 返回结果 map，否则 nil）。
func sendJoinMsg(t *testing.T, eng types.RuleEngine, data map[string]interface{}) map[string]interface{} {
	t.Helper()
	out := make(chan map[string]interface{}, 1)
	b, _ := json.Marshal(data)
	msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(b))
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err == nil && msg.Metadata.GetValue(Match) == MatchTrue {
			var r map[string]interface{}
			if e := json.Unmarshal([]byte(msg.Data.String()), &r); e == nil {
				select {
				case out <- r:
				default:
				}
				return
			}
		}
		select {
		case out <- nil:
		default:
		}
	}))
	select {
	case r := <-out:
		return r
	case <-time.After(2 * time.Second):
		t.Fatalf("消息处理超时")
		return nil
	}
}

// TestStreamTransformNode_FileTableEnrich 验证从文件加载元数据表并完成流-表 JOIN 富化。
func TestStreamTransformNode_FileTableEnrich(t *testing.T) {
	path := writeTempFile(t, "meta.json",
		`[{"deviceId":"d1","location":"plantA","type":"temp"},{"deviceId":"d2","location":"plantB","type":"humid"}]`)
	sql := "SELECT deviceId, m.location, m.type FROM stream JOIN meta m ON deviceId = m.deviceId"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "file", "path": path, "format": "json"},
	}
	eng, cleanup := newJoinEngine(t, sql, tables)
	defer cleanup()

	r := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d1", "temperature": 35})
	assert.NotNil(t, r, "d1 命中元数据应返回结果")
	assert.Equal(t, "plantA", r["location"], "location 富化正确")
	assert.Equal(t, "temp", r["type"], "type 富化正确")

	// d3 无元数据，INNER JOIN 丢弃 -> 返回 nil（走 Filtered）
	r2 := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d3"})
	assert.Nil(t, r2, "INNER JOIN 无匹配应被丢弃")
}

// TestStreamTransformNode_LeftJoinNoMatch 验证 LEFT JOIN 无匹配时保留流行，表侧列为空。
func TestStreamTransformNode_LeftJoinNoMatch(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT deviceId, m.location FROM stream LEFT JOIN meta m ON deviceId = m.deviceId"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "file", "path": path, "format": "json"},
	}
	eng, cleanup := newJoinEngine(t, sql, tables)
	defer cleanup()

	r := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d1"})
	assert.NotNil(t, r, "d1 应命中")
	assert.Equal(t, "plantA", r["location"], "d1 富化正确")

	// d9 无匹配，LEFT JOIN 保留流行
	r2 := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d9"})
	assert.NotNil(t, r2, "LEFT JOIN 无匹配也应保留流行")
	assert.Equal(t, "d9", r2["deviceId"], "deviceId 保留")
	assert.True(t, r2["location"] == nil, "无匹配时表侧列应为空")
}

// TestStreamTransformNode_HTTPTableEnrich 验证从 HTTP 端点加载元数据表并富化。
func TestStreamTransformNode_HTTPTableEnrich(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"deviceId":"d1","location":"plantA"}]`))
	}))
	defer srv.Close()

	sql := "SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "http", "path": srv.URL, "format": "json"},
	}
	eng, cleanup := newJoinEngine(t, sql, tables)
	defer cleanup()

	r := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d1"})
	assert.NotNil(t, r, "http 表应能富化")
	assert.Equal(t, "plantA", r["location"], "location 正确")
}

// TestStreamTransformNode_TableRefresh 验证后台刷新 goroutine 重新加载文件后，后续流数据看到新值。
func TestStreamTransformNode_TableRefresh(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	assert.Nil(t, os.WriteFile(path, []byte(`[{"deviceId":"d1","location":"plantA"}]`), 0644), "写初始文件")

	sql := "SELECT deviceId, m.location FROM stream JOIN meta m ON deviceId = m.deviceId"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "file", "path": path, "format": "json", "refresh": "100ms"},
	}
	eng, cleanup := newJoinEngine(t, sql, tables)
	defer cleanup()

	r := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d1"})
	assert.NotNil(t, r, "首次应返回")
	assert.Equal(t, "plantA", r["location"], "初始 location=plantA")

	// 改写文件，轮询直到刷新生效（周期 100ms，给 3s 余量）。
	assert.Nil(t, os.WriteFile(path, []byte(`[{"deviceId":"d1","location":"plantB"}]`), 0644), "改写文件")
	got := "plantA"
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r2 := sendJoinMsg(t, eng, map[string]interface{}{"deviceId": "d1"}); r2 != nil {
			got, _ = r2["location"].(string)
			if got == "plantB" {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.Equal(t, "plantB", got, "刷新后应看到 plantB")
}

// sendNodeMsg 发送一条 JSON map 消息并同步等待回调，返回 (结果 map, relationType, err)。
// 结果 map 仅在 Success 且 MatchTrue 时填充。
func sendNodeMsg(t *testing.T, eng types.RuleEngine, data map[string]interface{}) (map[string]interface{}, string, error) {
	t.Helper()
	b, _ := json.Marshal(data)
	msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(b))
	return captureNodeEnd(t, eng, msg)
}

// sendNodeRaw 发送指定 DataType 的原始字符串消息，返回 (relationType, err)。
// 用于非 JSON 数据类型或数组原始 JSON 的边界/负面用例。
func sendNodeRaw(t *testing.T, eng types.RuleEngine, dt types.DataType, raw string) (string, error) {
	t.Helper()
	msg := types.NewMsg(0, "TEST", dt, types.NewMetadata(), raw)
	_, rt, err := captureNodeEnd(t, eng, msg)
	return rt, err
}

// captureNodeEnd 驱动一条消息并捕获其 OnEnd（带超时，避免无回调时挂死）。
func captureNodeEnd(t *testing.T, eng types.RuleEngine, msg types.RuleMsg) (map[string]interface{}, string, error) {
	t.Helper()
	type end struct {
		m   map[string]interface{}
		rt  string
		err error
	}
	c := make(chan end, 1)
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, e error, rt string) {
		r := end{rt: rt, err: e}
		if e == nil && m.Metadata.GetValue(Match) == MatchTrue {
			var parsed map[string]interface{}
			if json.Unmarshal([]byte(m.Data.String()), &parsed) == nil {
				r.m = parsed
			}
		}
		select {
		case c <- r:
		default:
		}
	}))
	select {
	case r := <-c:
		return r.m, r.rt, r.err
	case <-time.After(2 * time.Second):
		t.Fatalf("消息处理超时")
		return nil, "", nil
	}
}

// TestNodeScenario_AnalyticState 验证分析函数在节点实例内跨消息保留状态（streamTransform 路径）。
// 通过同一引擎顺序发送多条消息，断言状态随事件演进。
func TestNodeScenario_AnalyticState(t *testing.T) {
	t.Run("lag跨消息保留前值", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT lag(temperature) AS prev_temp FROM stream", nil)
		defer cleanup()

		r1, rt1, err1 := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 23.0})
		assert.Nil(t, err1, "首条不应出错")
		assert.Equal(t, types.Success, rt1, "首条应走 Success")
		assert.True(t, r1["prev_temp"] == nil, "首条 lag 无前值应为 nil, got %v", r1["prev_temp"])

		r2, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 25.0})
		assert.Equal(t, 23.0, r2["prev_temp"], "第二条 lag 应为上一条 23")

		r3, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 27.0})
		assert.Equal(t, 25.0, r3["prev_temp"], "第三条 lag 应为上一条 25")
	})

	t.Run("acc_sum规则生命周期累积", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT acc_sum(value) AS total FROM stream", nil)
		defer cleanup()

		r1, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"value": 10.0})
		assert.Equal(t, 10.0, r1["total"], "首次累积 = 10")

		r2, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"value": 20.0})
		assert.Equal(t, 30.0, r2["total"], "累积 = 10+20=30")

		r3, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"value": 30.0})
		assert.Equal(t, 60.0, r3["total"], "累积 = 10+20+30=60")
	})

	t.Run("changed_col唯一输出未变化抑制", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT changed_col(true, temperature) AS chg FROM stream", nil)
		defer cleanup()

		// 首次视为变化，返回新值。
		r1, rt1, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 23.0})
		assert.Equal(t, types.Success, rt1, "首次变化应走 Success")
		assert.Equal(t, 23.0, r1["chg"], "首次变化返回新值 23")

		// 未变化：changed_col 为唯一输出时整行抑制（omitEmpty）→ 节点 Filtered，非 Success。
		r2, rt2, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 23.0})
		assert.Nil(t, r2, "未变化整行抑制，EmitSync 返回 nil")
		assert.Equal(t, types.False, rt2, "changed_col 唯一输出未变化应抑制为 Filtered")

		r3, rt3, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 25.0})
		assert.Equal(t, types.Success, rt3, "再次变化走 Success")
		assert.Equal(t, 25.0, r3["chg"], "再次变化返回 25")
	})

	t.Run("changed_col配普通字段不抑制", func(t *testing.T) {
		// 配普通字段时结果行永不为空 → 不抑制；chg 仅在变化时出现。
		eng, cleanup := newJoinEngine(t, "SELECT ts, changed_col(true, temperature) AS chg FROM stream", nil)
		defer cleanup()

		r1, rt1, _ := sendNodeMsg(t, eng, map[string]interface{}{"ts": 1.0, "temperature": 23.0})
		assert.Equal(t, types.Success, rt1, "首次变化走 Success")
		assert.Equal(t, 23.0, r1["chg"], "首次 chg=23")
		assert.Equal(t, 1.0, r1["ts"], "普通字段 ts 总输出")

		// 未变化：行不抑制（ts 存在），chg 列不出现。
		r2, rt2, _ := sendNodeMsg(t, eng, map[string]interface{}{"ts": 2.0, "temperature": 23.0})
		assert.Equal(t, types.Success, rt2, "配普通字段未变化仍输出行，不走 Filtered")
		assert.True(t, r2["chg"] == nil, "未变化 chg 不出现, got %v", r2["chg"])
		assert.Equal(t, 2.0, r2["ts"], "ts 仍输出")
	})

	t.Run("had_changed布尔变化检测", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT had_changed(true, temperature) AS changed FROM stream", nil)
		defer cleanup()

		r1, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 23.0})
		assert.Equal(t, true, r1["changed"], "首次视为变化 = true")

		r2, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 23.0})
		assert.Equal(t, false, r2["changed"], "同值未变化 = false")

		r3, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temperature": 25.0})
		assert.Equal(t, true, r3["changed"], "变化 = true")
	})
}

// TestNodeScenario_FilteredVsFailure 区分 Filtered（无错误的过滤）与 Failure（真错误）两条关系链。
func TestNodeScenario_FilteredVsFailure(t *testing.T) {
	t.Run("数组全部被过滤走Filtered", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT temperature FROM stream WHERE temperature > 100", nil)
		defer cleanup()
		// 元素均为合法 map，但 WHERE 全不满足 → 无错误 → Filtered。
		rt, err := sendNodeRaw(t, eng, types.JSON, `[{"temperature":15},{"temperature":25}]`)
		assert.Nil(t, err, "全部过滤不应带错误")
		assert.Equal(t, types.False, rt, "全部过滤应走 Filtered 而非 Failure")
	})

	t.Run("数组含坏元素（其余被过滤）走Failure", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT temperature FROM stream WHERE temperature > 100", nil)
		defer cleanup()
		// 第一个元素合法但被 WHERE 过滤，第二个为非 map（JSON number）转换出错；
		// 无成功项且有出错 → Failure，证明 Failure 专用于错误而非过滤。
		rt, err := sendNodeRaw(t, eng, types.JSON, `[{"temperature":15}, 123]`)
		assert.NotNil(t, err, "坏元素应带错误")
		assert.Equal(t, types.Failure, rt, "含出错元素应走 Failure，而非 Filtered")
	})

	t.Run("数组全为非map坏元素走Failure", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT temperature FROM stream", nil)
		defer cleanup()
		rt, err := sendNodeRaw(t, eng, types.JSON, `[123, 456]`)
		assert.NotNil(t, err, "非 map 元素应带错误")
		assert.Equal(t, types.Failure, rt, "全坏元素应走 Failure")
	})

	t.Run("非JSON数据类型走Failure", func(t *testing.T) {
		eng, cleanup := newJoinEngine(t, "SELECT temperature FROM stream", nil)
		defer cleanup()
		rt, err := sendNodeRaw(t, eng, types.TEXT, `temperature=25`)
		assert.NotNil(t, err, "非 JSON 应带错误")
		assert.Equal(t, types.Failure, rt, "非 JSON 数据类型应走 Failure")
	})
}

// TestNodeScenario_JoinCompositeKey 复合键 INNER JOIN：命中富化、未命中走 Filtered（非 Failure）。
func TestNodeScenario_JoinCompositeKey(t *testing.T) {
	sql := "SELECT deviceId, m.location FROM stream JOIN meta m ON tenant = m.tenant AND deviceId = m.deviceId"
	tables := []map[string]interface{}{
		{
			"name": "meta",
			"rows": []map[string]interface{}{
				{"tenant": "t1", "deviceId": "d1", "location": "plantA"},
				{"tenant": "t1", "deviceId": "d2", "location": "plantB"},
			},
		},
	}
	eng, cleanup := newJoinEngine(t, sql, tables)
	defer cleanup()

	// 复合键完全命中。
	r, rt, err := sendNodeMsg(t, eng, map[string]interface{}{"tenant": "t1", "deviceId": "d1"})
	assert.Nil(t, err, "命中不应出错")
	assert.Equal(t, types.Success, rt, "命中应走 Success")
	assert.Equal(t, "plantA", r["location"], "复合键富化 location=plantA")

	// 复合键部分匹配（tenant 同、deviceId 不同）→ INNER JOIN 丢弃 → Filtered。
	r2, rt2, err2 := sendNodeMsg(t, eng, map[string]interface{}{"tenant": "t1", "deviceId": "d9"})
	assert.Nil(t, err2, "INNER JOIN 无匹配不是错误")
	assert.Nil(t, r2, "INNER JOIN 无匹配不应返回结果")
	assert.Equal(t, types.False, rt2, "INNER JOIN 无匹配应走 Filtered 而非 Failure")
}

// TestStreamTransformNode_RejectCEP CEP(MATCH_RECOGNIZE) SQL 应在 Init 期被拒绝（fail-fast），
// 指向 x/streamAggregator，而不是每条消息在 EmitSync 才报错。
func TestStreamTransformNode_RejectCEP(t *testing.T) {
	sql := `SELECT * FROM stream MATCH_RECOGNIZE (ORDER BY ts MEASURES MATCH_NUMBER() AS mn ONE ROW PER MATCH PATTERN (A{2}) WITHIN '1h' DEFINE A AS v > 0)`
	node := &StreamTransformNode{}
	err := node.Init(types.NewConfig(), map[string]interface{}{"sql": sql})
	assert.NotNil(t, err, "CEP SQL 应被 transform 节点拒绝")
	assert.True(t, errors.Is(err, ErrTransformNotSupportCEP), "应是 CEP 专有拒绝错误, got %v", err)
}
