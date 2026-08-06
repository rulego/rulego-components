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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/str"
)

// TestPivotPointArray pure-function cases for point array pivoting
func TestPivotPointArray(t *testing.T) {
	t.Run("pivot a standard point array into a wide row with timestamp", func(t *testing.T) {
		data := []interface{}{
			map[string]interface{}{"name": "temp", "value": 25.3, "timestamp": float64(1721900000000000000)},
			map[string]interface{}{"name": "hum", "value": float64(60), "timestamp": float64(1721900000000000000)},
		}
		row, ok := pivotPointArray(data)
		assert.True(t, ok, "point array should be recognized")
		assert.Equal(t, 25.3, row["temp"], "temp value should be pivoted")
		assert.Equal(t, float64(60), row["hum"], "hum value should be pivoted")
		ts, hasTs := row["timestamp"].(int64)
		assert.True(t, hasTs, "pivoted row should contain int64 timestamp")
		assert.True(t, ts > 0, "timestamp should be positive ns")
	})

	t.Run("timestamp takes the max of valid points", func(t *testing.T) {
		data := []interface{}{
			map[string]interface{}{"name": "temp", "value": 25.3, "timestamp": float64(1000)},
			map[string]interface{}{"name": "hum", "value": float64(60), "timestamp": float64(3000)},
		}
		row, _ := pivotPointArray(data)
		assert.Equal(t, int64(3000), row["timestamp"], "timestamp should be the max of both points")
	})

	t.Run("points without timestamp do not attach the key", func(t *testing.T) {
		data := []interface{}{
			map[string]interface{}{"name": "temp", "value": 25.3},
			map[string]interface{}{"name": "hum", "value": float64(60)},
		}
		row, _ := pivotPointArray(data)
		_, hasTs := row["timestamp"]
		assert.False(t, hasTs, "points without timestamp should not attach the key")
	})

	t.Run("skip bad points", func(t *testing.T) {
		data := []interface{}{
			map[string]interface{}{"name": "temp", "value": 25.3},
			map[string]interface{}{"name": "bad", "value": nil, "error": "timeout"},
		}
		row, ok := pivotPointArray(data)
		assert.True(t, ok, "point array containing bad points is still recognized as a point array")
		assert.Equal(t, 1, len(row), "only normal points are kept")
	})

	t.Run("all bad points return an empty row", func(t *testing.T) {
		data := []interface{}{
			map[string]interface{}{"name": "bad1", "value": nil, "error": "timeout"},
			map[string]interface{}{"name": "bad2", "value": nil, "error": "offline"},
		}
		row, ok := pivotPointArray(data)
		assert.True(t, ok, "all bad points is still recognized as a point array")
		assert.Equal(t, 0, len(row), "pivot result of all bad points is an empty row")
	})

	t.Run("non-point array falls back", func(t *testing.T) {
		data := []interface{}{
			map[string]interface{}{"temperature": 20.0},
			map[string]interface{}{"temperature": 25.0},
		}
		_, ok := pivotPointArray(data)
		assert.False(t, ok, "array without the name/value contract should fall back")

		data2 := []interface{}{
			map[string]interface{}{"name": "", "value": 1},
		}
		_, ok = pivotPointArray(data2)
		assert.False(t, ok, "array with empty name does not satisfy the point contract and should fall back")
	})

	t.Run("empty array and non-array fall back", func(t *testing.T) {
		_, ok := pivotPointArray([]interface{}{})
		assert.False(t, ok, "empty array should fall back")
		_, ok = pivotPointArray(map[string]interface{}{"name": "temp"})
		assert.False(t, ok, "non-array should fall back")
	})
}

// TestStreamNodes_New_DefaultInputFormat New() defaults must align with the frontend form
func TestStreamNodes_New_DefaultInputFormat(t *testing.T) {
	agg := (&StreamAggregatorNode{}).New().(*StreamAggregatorNode)
	assert.Equal(t, InputFormatAuto, agg.Config.InputFormat, "aggregator default inputFormat should be auto")
	trans := (&StreamTransformNode{}).New().(*StreamTransformNode)
	assert.Equal(t, InputFormatAuto, trans.Config.InputFormat, "transform default inputFormat should be auto")
}

// newInputFormatAggregatorEngine builds an aggregator rule chain with inputFormat configuration
func newInputFormatAggregatorEngine(t *testing.T, sql, inputFormat string, onEnd func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string)) (types.RuleEngine, error) {
	t.Helper()
	config := engine.NewConfig(types.WithDefaultPool())
	if onEnd != nil {
		config.OnEnd = onEnd
	}
	nodeCfg := map[string]interface{}{"sql": sql}
	if inputFormat != "" {
		nodeCfg["inputFormat"] = inputFormat
	}
	// Keep the DSL ruleChain.id consistent with engine.New's chainId (following existing test helper convention)
	chainId := str.RandomStr(10)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "inputFormat测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "agg1", "type": "x/streamAggregator", "name": "流聚合器", "configuration": nodeCfg},
				{"id": "log1", "type": "log", "name": "日志节点", "configuration": map[string]interface{}{"jsScript": "return 'Aggregation result: ' + JSON.stringify(msg);"}},
			},
			"connections": []map[string]interface{}{
				{"fromId": "agg1", "toId": "log1", "type": RelationTypeStreamEvent},
			},
		},
	}
	chainJSON, err := json.Marshal(chainConfig)
	assert.Nil(t, err, "rule chain serialization should succeed")
	ruleEngine, err := engine.New(chainId, chainJSON, engine.WithConfig(config))
	if err == nil {
		t.Cleanup(func() { engine.Del(chainId) })
	}
	return ruleEngine, err
}

// TestStreamAggregatorNode_InputFormatColumns columns mode of the aggregator
func TestStreamAggregatorNode_InputFormatColumns(t *testing.T) {
	t.Run("pivot point array to wide row then window aggregation", func(t *testing.T) {
		sql := "SELECT AVG(temp) AS avg_temp, AVG(hum) AS avg_hum FROM stream GROUP BY TumblingWindow('1s')"
		points := []map[string]interface{}{
			{"name": "temp", "value": 20.0, "timestamp": 1721900000000000000},
			{"name": "hum", "value": 60.0, "timestamp": 1721900000000000000},
		}

		var aggregateResults []map[string]interface{}
		var mu sync.Mutex
		var eventCount int32
		onEnd := func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == StreamEventMsgType {
				atomic.AddInt32(&eventCount, 1)
				var resultArray []map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
					mu.Lock()
					aggregateResults = append(aggregateResults, resultArray...)
					mu.Unlock()
				}
			}
		}

		ruleEngine, err := newInputFormatAggregatorEngine(t, sql, InputFormatColumns, onEnd)
		assert.Nil(t, err, "rule engine creation should succeed")

		// Send the same point array twice; the window average is still 20/60
		for i := 0; i < 2; i++ {
			msgData, _ := json.Marshal(points)
			msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))
			ruleEngine.OnMsg(msg)
		}

		time.Sleep(2 * time.Second)

		assert.True(t, atomic.LoadInt32(&eventCount) >= 1, "window should fire and produce aggregation results")
		mu.Lock()
		defer mu.Unlock()
		if len(aggregateResults) == 0 {
			t.Fatal("should produce aggregation rows")
		}
		row := aggregateResults[len(aggregateResults)-1]
		assert.Equal(t, float64(20), row["avg_temp"], "temp window average should be 20")
		assert.Equal(t, float64(60), row["avg_hum"], "hum window average should be 60")
		ts, hasTs := row["timestamp"]
		assert.True(t, hasTs, "window end timestamp should be auto-injected")
		assert.True(t, toInt64Ns(ts) > 0, "timestamp should be positive ns")
	})

	t.Run("multi-cycle collection of real data auto-attaches window timestamp", func(t *testing.T) {
		sql := "SELECT AVG(voltage) AS avg_voltage, AVG(current) AS avg_current FROM stream GROUP BY TumblingWindow('1s')"
		// Three collection cycles, one group of points per cycle (simulating x/iotRead periodic output)
		cycles := [][]map[string]interface{}{
			{{"name": "voltage", "value": 220.0}, {"name": "current", "value": 5.0}},
			{{"name": "voltage", "value": 230.0}, {"name": "current", "value": 6.0}},
			{{"name": "voltage", "value": 225.0}, {"name": "current", "value": 5.5}},
		}

		var aggregateResults []map[string]interface{}
		var mu sync.Mutex
		var eventCount int32
		onEnd := func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == StreamEventMsgType {
				atomic.AddInt32(&eventCount, 1)
				var resultArray []map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
					mu.Lock()
					aggregateResults = append(aggregateResults, resultArray...)
					mu.Unlock()
				}
			}
		}

		ruleEngine, err := newInputFormatAggregatorEngine(t, sql, InputFormatColumns, onEnd)
		assert.Nil(t, err, "rule engine creation should succeed")

		for _, cycle := range cycles {
			msgData, _ := json.Marshal(cycle)
			ruleEngine.OnMsg(types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData)))
		}

		time.Sleep(2 * time.Second)

		assert.True(t, atomic.LoadInt32(&eventCount) >= 1, "window should fire")
		mu.Lock()
		defer mu.Unlock()
		if len(aggregateResults) == 0 {
			t.Fatal("should produce aggregation rows")
		}
		row := aggregateResults[len(aggregateResults)-1]
		assert.Equal(t, float64(225), row["avg_voltage"], "three-cycle voltage average should be 225")
		assert.Equal(t, float64(5.5), row["avg_current"], "three-cycle current average should be 5.5")
		ts, hasTs := row["timestamp"]
		assert.True(t, hasTs, "window end timestamp should be auto-injected")
		assert.True(t, toInt64Ns(ts) > 0, "timestamp should be positive ns")
		_, hasRawVoltage := row["voltage"]
		assert.False(t, hasRawVoltage, "aggregation result should not retain raw point fields")
	})

	t.Run("all bad points produce no aggregation result", func(t *testing.T) {
		sql := "SELECT AVG(temp) AS avg_temp FROM stream GROUP BY TumblingWindow('1s')"
		points := []map[string]interface{}{
			{"name": "temp", "value": nil, "error": "timeout"},
		}

		var eventCount int32
		onEnd := func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == StreamEventMsgType {
				atomic.AddInt32(&eventCount, 1)
			}
		}

		ruleEngine, err := newInputFormatAggregatorEngine(t, sql, InputFormatColumns, onEnd)
		assert.Nil(t, err, "rule engine creation should succeed")

		msgData, _ := json.Marshal(points)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))

		var successCount int32
		ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}))

		time.Sleep(2 * time.Second)

		assert.Equal(t, int32(1), atomic.LoadInt32(&successCount), "original message should go through Success as usual")
		assert.Equal(t, int32(0), atomic.LoadInt32(&eventCount), "all bad points should produce no aggregation result")
	})

	t.Run("non-point array falls back to row-by-row processing", func(t *testing.T) {
		sql := "SELECT AVG(temperature) AS avg_temp FROM stream GROUP BY TumblingWindow('1s')"
		plain := []map[string]interface{}{
			{"temperature": 20.0},
			{"temperature": 30.0},
		}

		var eventCount int32
		var lastRow map[string]interface{}
		var mu sync.Mutex
		onEnd := func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == StreamEventMsgType {
				atomic.AddInt32(&eventCount, 1)
				var resultArray []map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil && len(resultArray) > 0 {
					mu.Lock()
					lastRow = resultArray[len(resultArray)-1]
					mu.Unlock()
				}
			}
		}

		ruleEngine, err := newInputFormatAggregatorEngine(t, sql, InputFormatColumns, onEnd)
		assert.Nil(t, err, "rule engine creation should succeed")

		msgData, _ := json.Marshal(plain)
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData))
		ruleEngine.OnMsg(msg)

		time.Sleep(2 * time.Second)

		assert.True(t, atomic.LoadInt32(&eventCount) >= 1, "window should fire after falling back to row-by-row processing")
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(t, float64(25), lastRow["avg_temp"], "row-by-row stream average should be 25")
	})

	t.Run("window_end projects window time", func(t *testing.T) {
		sql := "SELECT window_end() AS ts, AVG(temp) AS avg_temp FROM stream GROUP BY TumblingWindow('1s')"
		points := []map[string]interface{}{
			{"name": "temp", "value": 20.0, "timestamp": 1721900000000000000},
		}

		var aggregateResults []map[string]interface{}
		var mu sync.Mutex
		var eventCount int32
		onEnd := func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
			if err == nil && msg.Type == StreamEventMsgType {
				atomic.AddInt32(&eventCount, 1)
				var resultArray []map[string]interface{}
				if jsonErr := json.Unmarshal([]byte(msg.Data.String()), &resultArray); jsonErr == nil {
					mu.Lock()
					aggregateResults = append(aggregateResults, resultArray...)
					mu.Unlock()
				}
			}
		}

		ruleEngine, err := newInputFormatAggregatorEngine(t, sql, InputFormatColumns, onEnd)
		assert.Nil(t, err, "rule engine creation should succeed")

		msgData, _ := json.Marshal(points)
		ruleEngine.OnMsg(types.NewMsg(0, "TELEMETRY", types.JSON, types.NewMetadata(), string(msgData)))

		time.Sleep(2 * time.Second)

		assert.True(t, atomic.LoadInt32(&eventCount) >= 1, "window should fire")
		mu.Lock()
		defer mu.Unlock()
		if len(aggregateResults) == 0 {
			t.Fatal("should produce aggregation rows")
		}
		row := aggregateResults[len(aggregateResults)-1]
		ts, ok := row["ts"]
		assert.True(t, ok, "result should contain the ts column projected by window_end")
		assert.True(t, toInt64Ns(ts) > 0, "ts should be a positive ns timestamp")
		assert.Equal(t, float64(20), row["avg_temp"], "avg_temp should be 20")
	})
}

// TestStreamAggregatorNode_InputFormatInvalid invalid inputFormat should fail at Init
func TestStreamAggregatorNode_InputFormatInvalid(t *testing.T) {
	_, err := newInputFormatAggregatorEngine(t, "SELECT AVG(temp) AS avg_temp FROM stream GROUP BY TumblingWindow('1s')", "wide", nil)
	assert.NotNil(t, err, "invalid inputFormat should fail rule chain creation")
}

// newInputFormatTransformEngine builds a transform rule chain with inputFormat configuration
func newInputFormatTransformEngine(t *testing.T, sql, inputFormat string) (types.RuleEngine, error) {
	t.Helper()
	config := engine.NewConfig(types.WithDefaultPool())
	nodeCfg := map[string]interface{}{"sql": sql}
	if inputFormat != "" {
		nodeCfg["inputFormat"] = inputFormat
	}
	// Keep the DSL ruleChain.id consistent with engine.New's chainId
	chainId := str.RandomStr(10)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "inputFormat测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "t1", "type": "x/streamTransform", "name": "流转换器", "configuration": nodeCfg},
			},
			"connections": []interface{}{},
		},
	}
	chainJSON, err := json.Marshal(chainConfig)
	assert.Nil(t, err, "rule chain serialization should succeed")
	ruleEngine, err := engine.New(chainId, chainJSON, engine.WithConfig(config))
	if err == nil {
		t.Cleanup(func() { engine.Del(chainId) })
	}
	return ruleEngine, err
}

// runTransformColumns sends a point array and captures the single-record result outlet
func runTransformColumns(t *testing.T, ruleEngine types.RuleEngine, points []map[string]interface{}) (string, string, error) {
	t.Helper()
	msgData, _ := json.Marshal(points)
	msg := types.NewMsg(0, "TEST", types.JSON, types.NewMetadata(), string(msgData))

	var relation, data string
	var retErr error
	var done int32
	ruleEngine.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		relation = relationType
		data = msg.Data.String()
		retErr = err
		atomic.StoreInt32(&done, 1)
	}))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&done), "message should complete processing")
	return relation, data, retErr
}

// TestStreamTransformNode_InputFormatColumns columns mode of transform
func TestStreamTransformNode_InputFormatColumns(t *testing.T) {
	t.Run("pivot point array then output a single flat map", func(t *testing.T) {
		ruleEngine, err := newInputFormatTransformEngine(t, "SELECT temp, hum, temp + hum AS total FROM stream WHERE temp > 0", InputFormatColumns)
		assert.Nil(t, err, "rule engine creation should succeed")

		points := []map[string]interface{}{
			{"name": "temp", "value": 25.0, "timestamp": 1721900000000000000},
			{"name": "hum", "value": 60.0, "timestamp": 1721900000000000000},
		}
		relation, data, retErr := runTransformColumns(t, ruleEngine, points)
		assert.Nil(t, retErr, "columns transform should not error")
		assert.Equal(t, types.Success, relation, "should go through the Success chain")

		var row map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(data), &row), "output should be a single flat map")
		assert.Equal(t, float64(25), row["temp"], "temp column should be kept")
		assert.Equal(t, float64(60), row["hum"], "hum column should be kept")
		assert.Equal(t, float64(85), row["total"], "cross-point computed column should take effect")
	})

	t.Run("bad points are skipped", func(t *testing.T) {
		ruleEngine, err := newInputFormatTransformEngine(t, "SELECT temp FROM stream WHERE temp > 0", InputFormatColumns)
		assert.Nil(t, err, "rule engine creation should succeed")

		points := []map[string]interface{}{
			{"name": "temp", "value": 25.0},
			{"name": "bad", "value": nil, "error": "timeout"},
		}
		relation, data, retErr := runTransformColumns(t, ruleEngine, points)
		assert.Nil(t, retErr, "columns transform should not error")
		assert.Equal(t, types.Success, relation, "should go through the Success chain")

		var row map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(data), &row), "output should be a single flat map")
		_, hasBad := row["bad"]
		assert.False(t, hasBad, "bad points should not enter the wide row")
	})

	t.Run("all bad points go to the False chain", func(t *testing.T) {
		ruleEngine, err := newInputFormatTransformEngine(t, "SELECT temp FROM stream WHERE temp > 0", InputFormatColumns)
		assert.Nil(t, err, "rule engine creation should succeed")

		points := []map[string]interface{}{
			{"name": "bad", "value": nil, "error": "timeout"},
		}
		relation, _, retErr := runTransformColumns(t, ruleEngine, points)
		assert.Nil(t, retErr, "all bad points is filter semantics, not an error")
		assert.Equal(t, types.False, relation, "all bad points should go to the False chain")
	})

	t.Run("WHERE filter goes to the False chain", func(t *testing.T) {
		ruleEngine, err := newInputFormatTransformEngine(t, "SELECT temp FROM stream WHERE temp > 100", InputFormatColumns)
		assert.Nil(t, err, "rule engine creation should succeed")

		points := []map[string]interface{}{
			{"name": "temp", "value": 25.0},
		}
		relation, _, retErr := runTransformColumns(t, ruleEngine, points)
		assert.Nil(t, retErr, "WHERE filtering is not an error")
		assert.Equal(t, types.False, relation, "unsatisfied WHERE should go to the False chain")
	})

	t.Run("non-point array falls back to row-by-row processing", func(t *testing.T) {
		ruleEngine, err := newInputFormatTransformEngine(t, "SELECT temperature FROM stream WHERE temperature > 0", InputFormatColumns)
		assert.Nil(t, err, "rule engine creation should succeed")

		plain := []map[string]interface{}{
			{"temperature": 20.0},
			{"temperature": 25.0},
		}
		relation, data, retErr := runTransformColumns(t, ruleEngine, plain)
		assert.Nil(t, retErr, "row-by-row fallback should not error")
		assert.Equal(t, types.Success, relation, "should go through the Success chain")

		var rows []map[string]interface{}
		assert.Nil(t, json.Unmarshal([]byte(data), &rows), "fallback mode output should be an array")
		assert.Equal(t, 2, len(rows), "row-by-row results should be merged into an array")
	})
}

// TestStreamTransformNode_InputFormatInvalid invalid inputFormat should fail at Init
func TestStreamTransformNode_InputFormatInvalid(t *testing.T) {
	_, err := newInputFormatTransformEngine(t, "SELECT temperature FROM stream WHERE temperature > 0", "wide")
	assert.NotNil(t, err, "invalid inputFormat should fail rule chain creation")
}
