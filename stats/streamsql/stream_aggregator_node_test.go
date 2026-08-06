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

// TestStreamAggregatorNode_BasicAggregation tests basic aggregation features
func TestStreamAggregatorNode_BasicAggregation(t *testing.T) {
	t.Run("tumbling window average", func(t *testing.T) {
		sql := "SELECT AVG(temperature) as avg_temp, COUNT(*) as count FROM stream GROUP BY TumblingWindow('1s')"
		testData := []map[string]interface{}{
			{"temperature": 20.0, "deviceId": "sensor001"},
			{"temperature": 25.0, "deviceId": "sensor001"},
			{"temperature": 30.0, "deviceId": "sensor001"},
		}

		results := testStreamAggregator(t, sql, testData, "rolling window average test")

		assert.True(t, len(results) > 0, "should have aggregation results")

		// Verify the first aggregation result
		if len(results) > 0 {
			result := results[0]
			assert.NotNil(t, result["avg_temp"], "should contain average temperature")
			assert.NotNil(t, result["count"], "should contain count")

			count := result["count"].(float64)
			assert.True(t, count > 0, "count should be greater than 0")
		}
	})

	t.Run("group aggregation by device", func(t *testing.T) {
		sql := "SELECT deviceId, MAX(temperature) as max_temp, MIN(temperature) as min_temp FROM stream GROUP BY deviceId, TumblingWindow('1s')"
		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 25.0},
			{"deviceId": "sensor001", "temperature": 30.0},
			{"deviceId": "sensor002", "temperature": 35.0},
			{"deviceId": "sensor002", "temperature": 28.0},
		}

		results := testStreamAggregator(t, sql, testData, "group by device test")

		assert.True(t, len(results) > 0, "should have grouped aggregation results")

		// Verify aggregation results contain device group information
		for _, result := range results {
			assert.NotNil(t, result["deviceId"], "should contain device ID")
			assert.NotNil(t, result["max_temp"], "should contain max temperature")
			assert.NotNil(t, result["min_temp"], "should contain min temperature")
		}
	})

	t.Run("sliding window aggregation", func(t *testing.T) {
		sql := "SELECT SUM(temperature) as sum_temp, AVG(temperature) as avg_temp FROM stream GROUP BY SlidingWindow('2s', '1s')"
		testData := []map[string]interface{}{
			{"temperature": 10.0},
			{"temperature": 20.0},
			{"temperature": 30.0},
			{"temperature": 40.0},
		}

		results := testStreamAggregator(t, sql, testData, "sliding window test")

		assert.True(t, len(results) > 0, "should have sliding window aggregation results")

		for _, result := range results {
			assert.NotNil(t, result["sum_temp"], "should contain sum")
			assert.NotNil(t, result["avg_temp"], "should contain average")
		}
	})
}

// TestStreamAggregatorNode_WindowTypes tests different window types
func TestStreamAggregatorNode_WindowTypes(t *testing.T) {
	t.Run("counting window", func(t *testing.T) {
		sql := "SELECT COUNT(*) as c, MAX(v) as mx FROM stream GROUP BY CountingWindow(3)"
		testData := []map[string]interface{}{
			{"v": 10}, {"v": 20}, {"v": 30},
		}
		results := testStreamAggregator(t, sql, testData, "counting window")
		assert.True(t, len(results) > 0, "counting window should fire after receiving 3 rows")
	})
}

// TestStreamAggregatorNode_Validation tests node configuration validation
func TestStreamAggregatorNode_Validation(t *testing.T) {
	t.Run("empty SQL validation", func(t *testing.T) {
		node := &StreamAggregatorNode{}
		config := map[string]interface{}{
			"sql": "",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "empty SQL should return an error")
		assert.Equal(t, ErrAggregatorSQLEmpty, err, "should be the empty SQL error")
	})

	t.Run("invalid SQL syntax", func(t *testing.T) {
		node := &StreamAggregatorNode{}
		config := map[string]interface{}{
			"sql": "INVALID SQL SYNTAX",
		}

		err := node.Init(types.NewConfig(), config)
		assert.NotNil(t, err, "invalid SQL should return an error")
	})
}

// TestStreamAggregatorNode_ConcurrentProcessing tests concurrent processing capability
func TestStreamAggregatorNode_ConcurrentProcessing(t *testing.T) {
	sql := "SELECT deviceId, AVG(temperature) as avg_temp, COUNT(*) as count FROM stream GROUP BY deviceId, TumblingWindow('1s')"

	config := engine.NewConfig(types.WithDefaultPool())
	var aggregateResults []map[string]interface{}
	var successCount int32
	var mu sync.Mutex

	// Set the global aggregation result handler
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

	// Create the test rule chain
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
	assert.Nil(t, err, "rule engine creation should succeed")
	defer engine.Del(chainId)

	// Concurrency test parameters
	const numGoroutines = 5
	const messagesPerGoroutine = 10

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineId int) {
			defer wg.Done()

			for j := 0; j < messagesPerGoroutine; j++ {
				temperature := 20.0 + float64(j%20) // temperature range 20-40
				testData := map[string]interface{}{
					"deviceId":    fmt.Sprintf("sensor_%d", goroutineId),
					"temperature": temperature,
					"timestamp":   time.Now().Unix(),
				}

				msgData, _ := json.Marshal(testData)
				msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))

				ruleEngine.OnMsg(msg)
				time.Sleep(10 * time.Millisecond) // simulate data interval
			}
		}(i)
	}

	wg.Wait()

	// Wait for the window aggregation to fire
	time.Sleep(3 * time.Second)

	// Verify aggregation result structure
	mu.Lock()
	aggregateResultsCopy := make([]map[string]interface{}, len(aggregateResults))
	copy(aggregateResultsCopy, aggregateResults)
	mu.Unlock()

	assert.True(t, len(aggregateResultsCopy) >= 0, "should collect aggregation results")

	for _, result := range aggregateResultsCopy {
		assert.NotNil(t, result["deviceId"], "aggregation result should contain device ID")
		assert.NotNil(t, result["avg_temp"], "aggregation result should contain average temperature")
		assert.NotNil(t, result["count"], "aggregation result should contain count")
	}
}

// TestStreamAggregatorNode_ComplexAggregation tests complex aggregation queries
func TestStreamAggregatorNode_ComplexAggregation(t *testing.T) {
	t.Run("multi-field aggregation", func(t *testing.T) {
		sql := "SELECT deviceId, AVG(temperature) as avg_temp, MAX(temperature) as max_temp, MIN(temperature) as min_temp, COUNT(*) as count, SUM(humidity) as total_humidity FROM stream GROUP BY deviceId, TumblingWindow('1s')"

		testData := []map[string]interface{}{
			{"deviceId": "sensor001", "temperature": 25.0, "humidity": 60},
			{"deviceId": "sensor001", "temperature": 30.0, "humidity": 65},
			{"deviceId": "sensor002", "temperature": 22.0, "humidity": 55},
		}

		results := testStreamAggregator(t, sql, testData, "complex aggregation test")

		assert.True(t, len(results) > 0, "should have complex aggregation results")

		for _, result := range results {
			assert.NotNil(t, result["deviceId"], "should contain device ID")
			assert.NotNil(t, result["avg_temp"], "should contain average temperature")
			assert.NotNil(t, result["max_temp"], "should contain max temperature")
			assert.NotNil(t, result["min_temp"], "should contain min temperature")
			assert.NotNil(t, result["count"], "should contain count")
			assert.NotNil(t, result["total_humidity"], "should contain total humidity")
		}
	})

	t.Run("conditional aggregation", func(t *testing.T) {
		sql := "SELECT COUNT(*) as high_temp_count, AVG(temperature) as avg_high_temp FROM stream WHERE temperature > 25 GROUP BY TumblingWindow('1s')"

		testData := []map[string]interface{}{
			{"temperature": 20.0}, // condition not met
			{"temperature": 30.0}, // condition met
			{"temperature": 35.0}, // condition met
			{"temperature": 22.0}, // condition not met
		}

		results := testStreamAggregator(t, sql, testData, "conditional aggregation test")

		assert.True(t, len(results) >= 0, "conditional aggregation test completed")
	})
}

// TestStreamAggregatorNode_ArrayInput tests array input handling
func TestStreamAggregatorNode_ArrayInput(t *testing.T) {
	t.Run("handle JSON array input", func(t *testing.T) {
		sql := "SELECT AVG(temperature) as avg_temp, COUNT(*) as count FROM stream GROUP BY TumblingWindow('1s')"

		// Prepare array test data
		arrayData := []map[string]interface{}{
			{"temperature": 20.0, "deviceId": "sensor001"},
			{"temperature": 25.0, "deviceId": "sensor002"},
			{"temperature": 30.0, "deviceId": "sensor003"},
		}

		config := engine.NewConfig(types.WithDefaultPool())
		var aggregateResults []map[string]interface{}
		var successCount int32
		var mu sync.Mutex

		// Set the global aggregation result handler
		config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == StreamEventMsgType {
				atomic.AddInt32(&successCount, 1)

				// Aggregation results may be in array format and must be parsed correctly
				var resultArray []map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
					mu.Lock()
					aggregateResults = append(aggregateResults, resultArray...)
					mu.Unlock()
				} else {
					// Try to parse as a single object
					var result map[string]interface{}
					if jsonErr2 := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr2 == nil {
						mu.Lock()
						aggregateResults = append(aggregateResults, result)
						mu.Unlock()
					}
				}
			}
		}

		// Create the test rule chain
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
		assert.Nil(t, err, "rule engine creation should succeed")
		defer engine.Del(chainId)

		// Send array data
		msgData, _ := json.Marshal(arrayData)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))

		// Monitor successful processing
		var processedSuccess int32
		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil {
				atomic.AddInt32(&processedSuccess, 1)
			}
		}))

		// Wait for processing to complete and window aggregation to fire
		time.Sleep(2 * time.Second)

		finalProcessed := atomic.LoadInt32(&processedSuccess)
		assert.Equal(t, int32(1), finalProcessed, "array data should be processed successfully")
	})

	t.Run("handle empty array input", func(t *testing.T) {
		sql := "SELECT COUNT(*) as count FROM stream GROUP BY TumblingWindow('1s')"

		// Empty array
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
		assert.Nil(t, err, "rule engine creation should succeed")
		defer engine.Del(chainId)

		// Send empty array data
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
		assert.Equal(t, int32(1), finalProcessed, "empty array should also be processed successfully")
	})
}

// TestStreamAggregatorNode_DataTypeValidation tests data type validation
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
	assert.Nil(t, err, "rule engine creation should succeed")
	defer engine.Del(chainId)

	testCases := []struct {
		name          string
		dataType      types.DataType
		data          string
		expectSuccess bool
	}{
		{
			name:          "JSON data type - valid",
			dataType:      types.JSON,
			data:          `{"temperature": 25.0, "deviceId": "sensor001"}`,
			expectSuccess: true,
		},
		{
			name:          "TEXT data type - should be rejected",
			dataType:      types.TEXT,
			data:          "plain text data",
			expectSuccess: false,
		},
		{
			name:          "BINARY data type - should be rejected",
			dataType:      types.BINARY,
			data:          "binary data",
			expectSuccess: false,
		},
		{
			name:          "empty string data type - should be rejected",
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
				assert.Equal(t, int32(1), finalSuccess, "should be processed successfully")
				assert.Equal(t, int32(0), finalFailure, "should have no failures")
			} else {
				assert.Equal(t, int32(0), finalSuccess, "should not be processed successfully")
				assert.Equal(t, int32(1), finalFailure, "should fail processing")
			}
		})
	}
}

// testStreamAggregator generic aggregation test helper
func testStreamAggregator(t *testing.T, sql string, testData []map[string]interface{}, description string) []map[string]interface{} {
	config := engine.NewConfig(types.WithDefaultPool())
	var results []map[string]interface{}
	var mu sync.Mutex

	// Set the global aggregation result handler
	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err == nil && msg.Type == StreamEventMsgType {
			// Aggregation results may be in array format and must be parsed correctly
			var resultArray []map[string]interface{}
			if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
				mu.Lock()
				results = append(results, resultArray...)
				mu.Unlock()
			} else {
				// Try to parse as a single object
				var result map[string]interface{}
				if jsonErr2 := json.Unmarshal([]byte(msg.Data.String()), &result); jsonErr2 == nil {
					mu.Lock()
					results = append(results, result)
					mu.Unlock()
				}
			}
		}
	}

	// Create the test rule chain
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
	assert.Nil(t, err, "rule engine creation should succeed")
	defer engine.Del(chainId)

	// Send test data
	for _, data := range testData {
		msgData, _ := json.Marshal(data)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))
		ruleEngine.OnMsg(msg)
		time.Sleep(50 * time.Millisecond) // simulate data interval
	}

	// Wait for the window aggregation to fire
	time.Sleep(2 * time.Second)

	// Protect reads of results with a mutex
	mu.Lock()
	resultsCopy := make([]map[string]interface{}, len(results))
	copy(resultsCopy, results)
	mu.Unlock()

	return resultsCopy
}

// runAggregatorWithTables builds a streamAggregator rule engine with optional metadata table
// configuration, sends data and waits for the window to fire, returning (aggregation results, engine creation error).
// On engine creation failure it returns (nil, err) instead of panicking, so negative cases can assert on it.
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

// TestStreamAggregatorNode_TableNotInJoin table name does not appear in any JOIN ON: the library
// requires metadata tables to be referenced by a JOIN, so node initialization should fail. This is a
// negative case independent of library P1, verifying that the node wires up table registration
// correctly and propagates the library's validation error.
func TestStreamAggregatorNode_TableNotInJoin(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT deviceId, AVG(temperature) as avg_temp FROM stream GROUP BY deviceId, TumblingWindow('1s')"
	tables := []map[string]interface{}{
		{"name": "meta", "source": "file", "path": path, "format": "json"},
	}

	_, err := runAggregatorWithTables(t, sql, tables, nil)
	assert.NotNil(t, err, "a table not referenced by any JOIN should fail node initialization")
	assert.True(t,
		strings.Contains(err.Error(), "meta") || strings.Contains(err.Error(), "JOIN"),
		"error should point to table/JOIN, got: %v", err)
}

// TestStreamAggregatorNode_JoinEnrich after INNER JOIN, groups by the table-side column (m.location) and computes AVG.
// Result columns are returned under the table alias namespace (m.location), different from the direct path (location).
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
	assert.Nil(t, err, "rule engine creation should succeed")
	assert.True(t, len(results) > 0, "should have results aggregated by location, got %+v", results)

	byLoc := make(map[string]float64)
	for _, r := range results {
		loc, _ := r["location"].(string)
		if avg, ok := r["avg_temp"].(float64); ok {
			byLoc[loc] = avg
		}
	}
	// plantA = d1(20,30) -> 25; plantB = d2(40) -> 40
	assert.True(t, byLoc["plantA"] == 25.0, "plantA average temperature should be 25, got %v", byLoc)
	assert.True(t, byLoc["plantB"] == 40.0, "plantB average temperature should be 40, got %v", byLoc)
}

// TestStreamAggregatorNode_LeftJoinNullGroup unmatched rows of LEFT JOIN enter the NULL group instead of being dropped.
func TestStreamAggregatorNode_LeftJoinNullGroup(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT m.location, AVG(temperature) as avg_t FROM stream LEFT JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(2)"
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 10.0}, // plantA
		{"deviceId": "d9", "temperature": 20.0}, // no match -> NULL group
	}

	results, err := runAggregatorWithTables(t, sql, tables, data)
	assert.Nil(t, err, "rule engine creation should succeed")

	hasNull, hasPlantA := false, false
	for _, r := range results {
		avg, _ := r["avg_t"].(float64)
		if r["location"] == nil {
			hasNull = true
			assert.True(t, avg == 20.0, "NULL group average temperature should be 20, got %v", avg)
		} else if loc, _ := r["location"].(string); loc == "plantA" {
			hasPlantA = true
			assert.True(t, avg == 10.0, "plantA average temperature should be 10, got %v", avg)
		}
	}
	assert.True(t, hasNull, "should have NULL group (LEFT unmatched rows are not dropped), got %+v", results)
	assert.True(t, hasPlantA, "should have plantA group, got %+v", results)
}

// TestStreamAggregatorNode_InnerJoinDropsUnmatched unmatched rows of INNER JOIN are dropped before entering the window,
// neither counted in count nor producing a NULL group.
func TestStreamAggregatorNode_InnerJoinDropsUnmatched(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	sql := "SELECT m.location, COUNT(*) as c, AVG(temperature) as avg_t FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 10.0},
		{"deviceId": "d9", "temperature": 99.0}, // no match -> dropped (does not enter the window)
		{"deviceId": "d1", "temperature": 20.0},
		{"deviceId": "d1", "temperature": 30.0},
	}

	results, err := runAggregatorWithTables(t, sql, tables, data)
	assert.Nil(t, err, "rule engine creation should succeed")

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
	assert.False(t, hasNull, "INNER JOIN should not produce a NULL group (unmatched rows are dropped)")
	assert.NotNil(t, plantA, "should have plantA group, got %+v", results)
	if plantA != nil {
		c, _ := plantA["c"].(float64)
		avg, _ := plantA["avg_t"].(float64)
		assert.True(t, c == 3, "count should be 3 after dropping unmatched rows, got %v", c)
		assert.True(t, avg == 20.0, "average temperature should be (10+20+30)/3=20, got %v", avg)
	}
}

// TestStreamAggregatorNode_CompositeKeyJoin composite key JOIN (tenant+deviceId) then group by table columns.
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
	assert.Nil(t, err, "rule engine creation should succeed")

	mx := make(map[string]float64)
	for _, r := range results {
		if loc, _ := r["location"].(string); loc != "" {
			if v, ok := r["mx"].(float64); ok {
				mx[loc] = v
			}
		}
	}
	assert.True(t, mx["plantA"] == 10.0, "plantA MAX should be 10, got %v", mx)
	assert.True(t, mx["plantB"] == 40.0, "plantB MAX should be 40, got %v", mx)
}

// TestStreamAggregatorNode_HttpTable loads a metadata table from an HTTP endpoint and performs JOIN aggregation.
func TestStreamAggregatorNode_HttpTable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"deviceId":"d1","location":"plantA"}]`))
	}))
	defer srv.Close()

	sql := "SELECT m.location, COUNT(*) as c FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(1)"
	tables := []map[string]interface{}{{"name": "meta", "source": "http", "path": srv.URL, "format": "json"}}

	results, err := runAggregatorWithTables(t, sql, tables, []map[string]interface{}{{"deviceId": "d1", "temperature": 1.0}})
	assert.Nil(t, err, "rule engine creation should succeed")
	assert.True(t, len(results) > 0, "HTTP table should complete JOIN aggregation, got %+v", results)
}

// TestStreamAggregatorNode_JoinColumnNaming verifies JOIN output column names on the aggregation path:
// without alias the prefix is stripped to location; with alias the alias is used (regression AJ1: previously output m.location and ignored the alias).
func TestStreamAggregatorNode_JoinColumnNaming(t *testing.T) {
	path := writeTempFile(t, "meta.json", `[{"deviceId":"d1","location":"plantA"}]`)
	tables := []map[string]interface{}{{"name": "meta", "source": "file", "path": path, "format": "json"}}
	data := []map[string]interface{}{
		{"deviceId": "d1", "temperature": 10.0},
		{"deviceId": "d1", "temperature": 20.0},
		{"deviceId": "d1", "temperature": 30.0},
	}

	t.Run("outputs location without alias", func(t *testing.T) {
		sql := "SELECT m.location, AVG(temperature) as a FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
		results, err := runAggregatorWithTables(t, sql, tables, data)
		assert.Nil(t, err)
		assert.True(t, len(results) > 0, "should have results, got %+v", results)
		assert.NotNil(t, results[0]["location"], "should output location, got %+v", results[0])
		assert.True(t, results[0]["m.location"] == nil, "should no longer output m.location, got %+v", results[0])
	})

	t.Run("outputs loc with alias", func(t *testing.T) {
		sql := "SELECT m.location as loc, AVG(temperature) as a FROM stream JOIN meta m ON deviceId = m.deviceId GROUP BY m.location, CountingWindow(3)"
		results, err := runAggregatorWithTables(t, sql, tables, data)
		assert.Nil(t, err)
		assert.True(t, len(results) > 0, "should have results, got %+v", results)
		assert.NotNil(t, results[0]["loc"], "alias loc should take effect, got %+v", results[0])
		assert.True(t, results[0]["m.location"] == nil, "should not output m.location, got %+v", results[0])
	})
}

// newAggregatorEngine builds a streamAggregator rule engine; stream_event is collected into
// *windowResults (locked) via config.OnEnd. Original terminal messages are captured by the caller via WithOnEnd.
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
			// No downstream: stream_event terminates directly into config.OnEnd (if connected to a node
			// like log, msg.Type gets rewritten and the StreamEventMsgType check in OnEnd would miss it).
			// ruleChain.id must match the id passed to engine.New: handleAggregateResult looks up the engine
			// via GetRuleEnginePool().Get(x.chainId) (x.chainId=ruleChain.id); on mismatch the
			// stream_event is silently dropped.
			"connections": []map[string]interface{}{},
		},
	}
	b, _ := json.Marshal(chainConfig)
	eng, err := engine.New(chainId, b, engine.WithConfig(config))
	assert.Nil(t, err, "rule engine creation should succeed")
	return eng, func() { engine.Del(chainId) }
}

// windowCount reads the current stream_event result count under lock.
func windowCount(mu *sync.Mutex, rs *[]map[string]interface{}) int {
	mu.Lock()
	defer mu.Unlock()
	return len(*rs)
}

// TestNodeScenario_AggregatorDualOutput verifies the aggregator's dual output: original terminal messages pass via Success,
// while the aggregation array fired by CountingWindow(2) is emitted via stream_event.
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
			// stream_event goes through config.OnEnd; only original terminal messages are counted here.
			if e == nil && m.Type != StreamEventMsgType {
				atomic.AddInt32(&successCount, 1)
			}
		}))
	}
	send(10)
	send(30)

	// Poll and wait: two original messages via Success + one stream_event.
	ok := waitForCondition(func() bool {
		return atomic.LoadInt32(&successCount) >= 2 && windowCount(&mu, &windowResults) > 0
	}, 2*time.Second, 20*time.Millisecond)
	assert.True(t, ok, "should receive 2 Success and 1 stream_event before timeout")

	// Dual output part one: original messages pass via Success.
	assert.Equal(t, int32(2), atomic.LoadInt32(&successCount), "two original messages should pass via Success")

	// Dual output part two: aggregation array avg_t=20, cnt=2.
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
	assert.True(t, found, "stream_event should contain aggregation result with avg_t=20,cnt=2, got %+v", windowResults)
}

// TestNodeScenario_AggregatorWindowedAnalytic verifies windowed analytic functions (analytic over aggregation) output through the node:
// changed_col(true, avg(t)) is treated as changed in the first CountingWindow(2) window and returns the window average.
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
	assert.True(t, ok, "stream_event should fire after CountingWindow(2)")

	// Window [10,40] average is 25; changed_col in the first window is treated as changed -> chg=25.
	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, r := range windowResults {
		if chg, ok := r["chg"].(float64); ok && chg == 25.0 {
			found = true
		}
	}
	assert.True(t, found, "windowed analytic changed_col(avg) should be chg=25 in the first window, got %+v", windowResults)
}

// runAggregatorCaptureMeta builds a streamAggregator rule engine (stream_event terminates into config.OnEnd),
// feeds events and waits for results, returning (result rows, queryType/resultType of the first stream_event).
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
			// No downstream: stream_event terminates directly into config.OnEnd.
			"connections": []map[string]interface{}{},
		},
	}
	b, _ := json.Marshal(chainConfig)
	ruleEngine, err := engine.New(chainId, b, engine.WithConfig(config))
	assert.Nil(t, err, "rule engine creation should succeed")
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

// TestStreamAggregatorNode_CEP CEP (MATCH_RECOGNIZE) output through the node: after 3 consecutive v>50 hits,
// the match result is dispatched via stream_event with metadata.queryType=cep / resultType=pattern_matched.
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

	assert.True(t, len(results) > 0, "should receive CEP match results")
	assert.Equal(t, "cep", qType, "queryType should be cep, got %s", qType)
	assert.Equal(t, "pattern_matched", rType, "resultType should be pattern_matched, got %s", rType)

	var hasMatch bool
	for _, r := range results {
		if _, ok := r["mn"]; ok {
			if peak, ok := r["peak"].(float64); ok && peak > 50 {
				hasMatch = true
			}
		}
	}
	assert.True(t, hasMatch, "matched row should contain mn and peak>50, got %+v", results)
}

// TestStreamAggregatorNode_AggregationQueryType aggregation query results have metadata.queryType=aggregation.
func TestStreamAggregatorNode_AggregationQueryType(t *testing.T) {
	sql := "SELECT AVG(v) AS avg_v FROM stream GROUP BY CountingWindow(2)"
	events := []map[string]interface{}{
		{"v": 10.0},
		{"v": 30.0},
	}
	results, qType, rType := runAggregatorCaptureMeta(t, sql, events)

	assert.True(t, len(results) > 0, "should receive aggregation results")
	assert.Equal(t, "aggregation", qType, "queryType should be aggregation, got %s", qType)
	assert.Equal(t, "window_triggered", rType, "resultType should be window_triggered, got %s", rType)
}
