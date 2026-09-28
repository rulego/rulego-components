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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/utils/json"
	"github.com/rulego/rulego/utils/str"
)

// joinFeed is one input message for a stream-stream JOIN test: by default the
// stream name goes into the message metadata under the node's streamKey value;
// metadata overrides the message metadata entirely (for ${} expression cases).
type joinFeed struct {
	streamName string
	data       map[string]interface{}
	// asArray feeds data wrapped in a one-element JSON array (array input path)
	asArray bool
	// omitStreamKey drops the stream-name metadata key (missing-key negative case)
	omitStreamKey bool
	// metadata replaces the default stream-name metadata entirely
	metadata map[string]string
}

// joinTestResult collects what the rule chain observed.
type joinTestResult struct {
	mu       sync.Mutex
	results  []map[string]interface{} // stream_event payloads
	joinMeta []string                 // queryType metadata of stream_event messages
	failure  int32                    // messages routed to Failure
	success  int32                    // messages routed to Success (original data passthrough)
}

// runStreamJoin builds a rule chain with one x/streamAggregator node running a
// WITHIN JOIN SQL, feeds the inputs, and waits for the engine to settle.
func runStreamJoin(t *testing.T, sql, streamKey string, feeds []joinFeed, wait time.Duration) *joinTestResult {
	t.Helper()

	config := engine.NewConfig(types.WithDefaultPool())
	out := joinTestResult{}

	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		switch relationType {
		case types.Failure:
			atomic.AddInt32(&out.failure, 1)
		case types.Success:
			atomic.AddInt32(&out.success, 1)
		}
		if err != nil || msg.Type != StreamEventMsgType {
			return
		}
		out.mu.Lock()
		out.joinMeta = append(out.joinMeta, msg.Metadata.GetValue("queryType"))
		out.mu.Unlock()
		var arr []map[string]interface{}
		if e := json.Unmarshal([]byte(msg.Data.String()), &arr); e == nil {
			out.mu.Lock()
			out.results = append(out.results, arr...)
			out.mu.Unlock()
			return
		}
		var m map[string]interface{}
		if e := json.Unmarshal([]byte(msg.Data.String()), &m); e == nil {
			out.mu.Lock()
			out.results = append(out.results, m)
			out.mu.Unlock()
		}
	}

	nodeCfg := map[string]interface{}{"sql": sql}
	if streamKey != "" {
		nodeCfg["streamKey"] = streamKey
	}
	chainId := "join_" + str.RandomStr(6)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "流流JOIN测试", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "j1", "type": "x/streamAggregator", "name": "流流JOIN", "configuration": nodeCfg},
			},
			"connections": []map[string]interface{}{},
		},
	}
	b, _ := json.Marshal(chainConfig)

	ruleEngine, err := engine.New(chainId, b, engine.WithConfig(config))
	if err != nil {
		t.Fatalf("rule engine creation failed: %v", err)
	}
	defer engine.Del(chainId)

	for _, f := range feeds {
		jd, _ := json.Marshal(f.data)
		body := string(jd)
		if f.asArray {
			body = "[" + body + "]"
		}
		md := types.NewMetadata()
		if f.metadata != nil {
			for k, v := range f.metadata {
				md.PutValue(k, v)
			}
		} else if !f.omitStreamKey {
			md.PutValue(streamKey, f.streamName)
		}
		msg := types.NewMsg(0, "TELEMETRY", types.JSON, md, body)
		ruleEngine.OnMsg(msg)
		time.Sleep(80 * time.Millisecond)
	}
	time.Sleep(wait)

	out.mu.Lock()
	defer out.mu.Unlock()
	return &out
}

// TestStreamAggregatorNode_StreamJoinInner two streams correlated through the
// component: rows carry the stream name in metadata, matched rows flow out via
// stream_event with queryType=stream_join, originals via Success.
func TestStreamAggregatorNode_StreamJoinInner(t *testing.T) {
	sql := "SELECT s.deviceId, s.temperature AS temp, v.vibration AS vib " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId " +
		"WHERE s.temperature > 75 AND v.vibration > 30"

	out := runStreamJoin(t, sql, "streamName", []joinFeed{
		{streamName: "tempStream", data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
		{streamName: "tempStream", data: map[string]interface{}{"deviceId": "d2", "temperature": 76.0}},
		{streamName: "tempStream", data: map[string]interface{}{"deviceId": "d3", "temperature": 24.0}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d1", "vibration": 35.2}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d2", "vibration": 41.0}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d9", "vibration": 33.0}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d1", "vibration": 28.0}},
	}, 1500*time.Millisecond)

	if len(out.results) != 2 {
		t.Fatalf("expected 2 matched rows (d1,d2), got %d: %+v", len(out.results), out.results)
	}
	byDevice := make(map[string]map[string]interface{})
	for _, r := range out.results {
		dev, _ := r["deviceId"].(string)
		byDevice[dev] = r
	}
	d1 := byDevice["d1"]
	if d1 == nil {
		t.Fatalf("missing d1 result: %+v", out.results)
	}
	if temp, _ := d1["temp"].(float64); temp != 80.5 {
		t.Fatalf("d1 temp should be 80.5, got %v", d1["temp"])
	}
	if vib, _ := d1["vib"].(float64); vib != 35.2 {
		t.Fatalf("d1 vib should be 35.2, got %v", d1["vib"])
	}
	d2 := byDevice["d2"]
	if d2 == nil {
		t.Fatalf("missing d2 result: %+v", out.results)
	}
	if vib, _ := d2["vib"].(float64); vib != 41.0 {
		t.Fatalf("d2 vib should be 41, got %v", d2["vib"])
	}
	// d3 temp=24 is filtered by WHERE; d9 has no temp row; d1's second vibration 28 is filtered
	if _, ok := byDevice["d3"]; ok {
		t.Fatalf("d3 should be filtered by WHERE: %+v", out.results)
	}
	if _, ok := byDevice["d9"]; ok {
		t.Fatalf("d9 has no tempStream row and must not match: %+v", out.results)
	}
	for _, q := range out.joinMeta {
		if q != "stream_join" {
			t.Fatalf("stream_event queryType should be stream_join, got %q (all: %v)", q, out.joinMeta)
		}
	}
	if out.failure != 0 {
		t.Fatalf("no message should fail, got %d failures", out.failure)
	}
}

// TestStreamAggregatorNode_StreamJoinLeftAbsence LEFT JOIN absence detection:
// the command without an ACK is NULL-complemented at window close and flows out
// via stream_event; the command ACKed on time produces nothing.
func TestStreamAggregatorNode_StreamJoinLeftAbsence(t *testing.T) {
	sql := "SELECT c.cmdId, c.deviceId, r.ackCode " +
		"FROM cmdStream AS c LEFT JOIN ackStream AS r WITHIN 1 SECONDS " +
		"ON c.cmdId = r.cmdId " +
		"WHERE r.ackCode IS NULL"

	out := runStreamJoin(t, sql, "streamName", []joinFeed{
		{streamName: "cmdStream", data: map[string]interface{}{"cmdId": "c1", "deviceId": "d1"}},
		{streamName: "ackStream", data: map[string]interface{}{"cmdId": "c1", "ackCode": 0}},
		{streamName: "cmdStream", data: map[string]interface{}{"cmdId": "c2", "deviceId": "d2"}},
	}, 3*time.Second)

	if len(out.results) != 1 {
		t.Fatalf("expected exactly 1 absence alert (c2), got %d: %+v", len(out.results), out.results)
	}
	r := out.results[0]
	if r["cmdId"] != "c2" {
		t.Fatalf("alert should be for c2, got %+v", r)
	}
	if v, ok := r["ackCode"]; ok && v != nil {
		t.Fatalf("ackCode should be nil for the unmatched command, got %v", v)
	}
}

// TestStreamAggregatorNode_StreamJoinUnknownStream an input routed to a stream
// name that is not part of the SQL must fail the message (the library error
// lists the known stream names), not be silently dropped.
func TestStreamAggregatorNode_StreamJoinUnknownStream(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "streamName", []joinFeed{
		{streamName: "nosuchStream", data: map[string]interface{}{"deviceId": "d1"}},
		{streamName: "tempStream", data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d1", "vibration": 35.2}},
	}, 800*time.Millisecond)

	if out.failure == 0 {
		t.Fatalf("unknown stream name must route to Failure")
	}
	if len(out.results) != 1 {
		t.Fatalf("valid rows should still match despite the failed one, got %+v", out.results)
	}
	if vib, _ := out.results[0]["vibration"].(float64); vib != 35.2 {
		t.Fatalf("matched row vibration should be 35.2, got %+v", out.results[0])
	}
}

// TestStreamAggregatorNode_StreamJoinMissingStreamKey a JOIN query configured
// with streamKey but an input message missing that metadata key must fail.
func TestStreamAggregatorNode_StreamJoinMissingStreamKey(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "streamName", []joinFeed{
		{streamName: "tempStream", omitStreamKey: true, data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
	}, 500*time.Millisecond)

	if out.failure == 0 {
		t.Fatalf("missing stream-name metadata must route to Failure")
	}
	if len(out.results) != 0 {
		t.Fatalf("no rows should match when the left row never arrived, got %+v", out.results)
	}
}

// TestStreamAggregatorNode_StreamJoinArrayInput array input (inputFormat=auto)
// in JOIN mode: each element is routed by the message metadata stream name.
func TestStreamAggregatorNode_StreamJoinArrayInput(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "streamName", []joinFeed{
		{streamName: "tempStream", asArray: true, data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
		{streamName: "tempStream", asArray: true, data: map[string]interface{}{"deviceId": "d2", "temperature": 76.0}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d1", "vibration": 35.2}},
		{streamName: "vibrationStream", data: map[string]interface{}{"deviceId": "d2", "vibration": 41.0}},
	}, 1500*time.Millisecond)

	if len(out.results) != 2 {
		t.Fatalf("expected 2 matched rows from array-fed temperatures, got %d: %+v", len(out.results), out.results)
	}
	devices := make(map[string]bool)
	for _, r := range out.results {
		dev, _ := r["deviceId"].(string)
		devices[dev] = true
	}
	if !devices["d1"] || !devices["d2"] {
		t.Fatalf("both d1 and d2 should match, got %+v", out.results)
	}
}

// TestStreamAggregatorNode_StreamJoinSingleStreamFallback a JOIN SQL with an
// empty streamKey keeps single-stream feeding (Emit == EmitTo(FROM stream)):
// rows land on the FROM stream only; the right side simply never matches here.
func TestStreamAggregatorNode_StreamJoinSingleStreamFallback(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 1 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "", []joinFeed{
		{streamName: "tempStream", data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
	}, 1500*time.Millisecond)

	if out.failure != 0 {
		t.Fatalf("single-stream fallback must not fail messages, got %d failures", out.failure)
	}
	if len(out.results) != 0 {
		t.Fatalf("right side was never fed, no rows can match: %+v", out.results)
	}
}

// TestStreamAggregatorNode_StreamJoinExprFromMetadata streamKey as a ${}
// expression: the resolution result IS the stream name. Here the name comes
// from message metadata via ${metadata.streamName}.
func TestStreamAggregatorNode_StreamJoinExprFromMetadata(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "${metadata.streamName}", []joinFeed{
		{streamName: "tempStream", metadata: map[string]string{"streamName": "tempStream"}, data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
		{streamName: "vibrationStream", metadata: map[string]string{"streamName": "vibrationStream"}, data: map[string]interface{}{"deviceId": "d1", "vibration": 35.2}},
	}, 1500*time.Millisecond)

	if out.failure != 0 {
		t.Fatalf("no message should fail, got %d failures", out.failure)
	}
	if len(out.results) != 1 {
		t.Fatalf("expected 1 matched row via ${metadata.streamName}, got %d: %+v", len(out.results), out.results)
	}
	if vib, _ := out.results[0]["vibration"].(float64); vib != 35.2 {
		t.Fatalf("matched row vibration should be 35.2, got %+v", out.results[0])
	}
}

// TestStreamAggregatorNode_StreamJoinExprFromBody streamKey=${msg.stream}:
// the stream name is taken from the message body itself.
func TestStreamAggregatorNode_StreamJoinExprFromBody(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "${msg.stream}", []joinFeed{
		{streamName: "tempStream", data: map[string]interface{}{"stream": "tempStream", "deviceId": "d1", "temperature": 80.5}},
		{streamName: "vibrationStream", data: map[string]interface{}{"stream": "vibrationStream", "deviceId": "d1", "vibration": 35.2}},
	}, 1500*time.Millisecond)

	if out.failure != 0 {
		t.Fatalf("no message should fail, got %d failures", out.failure)
	}
	if len(out.results) != 1 {
		t.Fatalf("expected 1 matched row via ${msg.stream}, got %d: %+v", len(out.results), out.results)
	}
	if vib, _ := out.results[0]["vibration"].(float64); vib != 35.2 {
		t.Fatalf("matched row vibration should be 35.2, got %+v", out.results[0])
	}
}

// TestStreamAggregatorNode_StreamJoinExprResolvesEmpty a ${} expression that
// resolves to empty (metadata key absent) must fail the message.
func TestStreamAggregatorNode_StreamJoinExprResolvesEmpty(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	out := runStreamJoin(t, sql, "${metadata.nosuch}", []joinFeed{
		{streamName: "tempStream", metadata: map[string]string{"other": "x"}, data: map[string]interface{}{"deviceId": "d1", "temperature": 80.5}},
	}, 500*time.Millisecond)

	if out.failure == 0 {
		t.Fatalf("an expression resolving to an empty stream name must route to Failure")
	}
	if len(out.results) != 0 {
		t.Fatalf("no rows should match when the left row never arrived, got %+v", out.results)
	}
}

// TestStreamAggregatorNode_StreamJoinChainTwoStage 两段式组合："JOIN 节点(A) →
// stream_event → 聚合节点(B)"。验证三点：
//  1. A 的 stream_event 负载（JSON 数组）被 B 逐元素喂入自己的流；
//  2. A 的 SQL 投影 s.ts 后，匹配行携带时间戳（B 事件时间窗口的时间来源约定）；
//  3. 正确连线（只连 stream_event）下原始遥测不会污染 B 的窗口（cnt 精确等于匹配数）。
func TestStreamAggregatorNode_StreamJoinChainTwoStage(t *testing.T) {
	joinSQL := "SELECT s.deviceId, s.temperature AS temp, v.vibration AS vib, s.ts " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId " +
		"WHERE s.temperature > 75 AND v.vibration > 30"
	aggSQL := "SELECT deviceId, COUNT(*) AS cnt FROM joinOut GROUP BY deviceId, CountingWindow(2)"

	config := engine.NewConfig(types.WithDefaultPool())
	var mu sync.Mutex
	var aggResults []map[string]interface{}
	var joinPayloads []string

	config.OnEnd = func(ctx types.RuleContext, msg types.RuleMsg, err error, relationType string) {
		if err != nil || msg.Type != StreamEventMsgType {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch msg.Metadata.GetValue("queryType") {
		case "stream_join":
			joinPayloads = append(joinPayloads, msg.Data.String())
		case "aggregation":
			var arr []map[string]interface{}
			if e := json.Unmarshal([]byte(msg.Data.String()), &arr); e == nil {
				aggResults = append(aggResults, arr...)
			}
		}
	}

	chainId := "chain_" + str.RandomStr(6)
	chainConfig := map[string]interface{}{
		"ruleChain": map[string]interface{}{"id": chainId, "name": "两段式", "root": true},
		"metadata": map[string]interface{}{
			"nodes": []map[string]interface{}{
				{"id": "joinA", "type": "x/streamAggregator", "name": "双流关联",
					"configuration": map[string]interface{}{"sql": joinSQL, "streamKey": "streamName"}},
				{"id": "aggB", "type": "x/streamAggregator", "name": "窗口聚合",
					"configuration": map[string]interface{}{"sql": aggSQL}},
			},
			"connections": []map[string]interface{}{
				{"fromId": "joinA", "toId": "aggB", "type": "stream_event"},
			},
		},
	}
	b, _ := json.Marshal(chainConfig)
	ruleEngine, err := engine.New(chainId, b, engine.WithConfig(config))
	if err != nil {
		t.Fatalf("rule engine creation failed: %v", err)
	}
	defer engine.Del(chainId)

	type feed struct {
		streamName string
		body       string
	}
	feeds := []feed{
		{"tempStream", `{"deviceId":"d1","temperature":80.5,"ts":1698700000000}`},
		{"tempStream", `{"deviceId":"d2","temperature":76.0,"ts":1698700000200}`},
		{"vibrationStream", `{"deviceId":"d1","vibration":35.2}`},
		{"vibrationStream", `{"deviceId":"d2","vibration":41.0}`},
		{"vibrationStream", `{"deviceId":"d1","vibration":36.0}`},
	}
	for _, f := range feeds {
		md := types.NewMetadata()
		md.PutValue("streamName", f.streamName)
		ruleEngine.OnMsg(types.NewMsg(0, "TELEMETRY", types.JSON, md, f.body))
		time.Sleep(120 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// A 产出 3 条匹配行：d1×2（一对多）、d2×1；每条负载是单元素数组且带投影出的 ts。
	if len(joinPayloads) != 3 {
		t.Fatalf("A should emit 3 matched rows, got %d: %v", len(joinPayloads), joinPayloads)
	}
	for _, p := range joinPayloads {
		if !strings.Contains(p, `"ts"`) {
			t.Fatalf("matched row should carry the projected ts field (B's event-time basis): %s", p)
		}
	}
	// B 的窗口：d1 攒满 2 行触发 {d1, cnt=2}；d2 仅 1 行挂起。
	// 若原始遥测误入 B（Success 误连），cnt 会被原始行撑爆——cnt==2 即证明无污染。
	if len(aggResults) != 1 {
		t.Fatalf("B should emit exactly one window result, got %d: %v", len(aggResults), aggResults)
	}
	if dev, _ := aggResults[0]["deviceId"].(string); dev != "d1" {
		t.Fatalf("window result should be for d1, got %v", aggResults[0])
	}
	if cnt, _ := aggResults[0]["cnt"].(float64); cnt != 2 {
		t.Fatalf("cnt should be exactly 2 (matched rows only, no raw telemetry pollution), got %v", aggResults[0])
	}
}

// TestStreamAggregatorNode_StreamJoinLeftHit the hit path of LEFT JOIN WITHIN: the right side
// arrives within the window, the matched row is emitted via stream_event carrying the right-side column.
func TestStreamAggregatorNode_StreamJoinLeftHit(t *testing.T) {
	sql := "SELECT c.cmdId, c.deviceId, r.ackCode " +
		"FROM cmdStream AS c LEFT JOIN ackStream AS r WITHIN 1 SECONDS " +
		"ON c.cmdId = r.cmdId"

	out := runStreamJoin(t, sql, "streamName", []joinFeed{
		{streamName: "cmdStream", data: map[string]interface{}{"cmdId": "c1", "deviceId": "d1"}},
		{streamName: "ackStream", data: map[string]interface{}{"cmdId": "c1", "ackCode": 0}},
	}, 1500*time.Millisecond)

	if len(out.results) != 1 {
		t.Fatalf("LEFT JOIN hit should emit exactly one matched row, got %d: %+v", len(out.results), out.results)
	}
	r := out.results[0]
	if r["cmdId"] != "c1" {
		t.Fatalf("matched row should be c1, got %+v", r)
	}
	v, ok := r["ackCode"].(float64)
	if !ok || v != 0 {
		t.Fatalf("matched row should carry right-side column ackCode=0, got %+v", r)
	}
}

// TestStreamAggregatorNode_StreamJoinUnsupportedTypes RIGHT/FULL JOIN is rejected at load time.
func TestStreamAggregatorNode_StreamJoinUnsupportedTypes(t *testing.T) {
	for _, joinType := range []string{"RIGHT", "FULL"} {
		sql := "SELECT s.deviceId FROM tempStream AS s " + joinType + " JOIN vibrationStream AS v WITHIN 2 SECONDS ON s.deviceId = v.deviceId"
		config := engine.NewConfig(types.WithDefaultPool())
		chainId := "joinrej_" + str.RandomStr(6)
		chainConfig := map[string]interface{}{
			"ruleChain": map[string]interface{}{"id": chainId, "name": "JOIN拒绝测试", "root": true},
			"metadata": map[string]interface{}{
				"nodes": []map[string]interface{}{
					{"id": "j1", "type": "x/streamAggregator", "name": "JOIN", "configuration": map[string]interface{}{"sql": sql, "streamKey": "streamName"}},
				},
				"connections": []map[string]interface{}{},
			},
		}
		b, _ := json.Marshal(chainConfig)
		_, err := engine.New(chainId, b, engine.WithConfig(config))
		if err == nil {
			engine.Del(chainId)
			t.Fatalf("%s JOIN must be rejected at load time", joinType)
		}
	}
}

// TestStreamTransformNode_RejectStreamJoin the EmitSync-based transform node
// cannot consume stream-stream JOIN queries; Init must reject them up front.
func TestStreamTransformNode_RejectStreamJoin(t *testing.T) {
	sql := "SELECT s.deviceId, v.vibration " +
		"FROM tempStream AS s JOIN vibrationStream AS v WITHIN 2 SECONDS " +
		"ON s.deviceId = v.deviceId"

	transform := &StreamTransformNode{}
	err := transform.Init(types.Config{}, types.Configuration{"sql": sql})
	if err != nil {
		transform.Destroy()
		if !strings.Contains(err.Error(), "x/streamAggregator") {
			t.Fatalf("error should point to x/streamAggregator, got %v", err)
		}
		return
	}
	transform.Destroy()
	t.Fatalf("transform node must reject stream-stream JOIN SQL")
}
