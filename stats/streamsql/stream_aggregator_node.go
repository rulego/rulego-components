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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"github.com/rulego/streamsql"
)

// RelationTypeStreamEvent is the stream event relation type: the unified result
// outlet for aggregation window triggers and CEP pattern matches.
const RelationTypeStreamEvent = "stream_event"

// StreamEventMsgType is the stream event message type, used to identify aggregation/CEP result messages.
const StreamEventMsgType = "stream_event"

func init() {
	_ = rulego.Registry.Register(&StreamAggregatorNode{})
}

// StreamAggregatorNodeConfiguration is the stream aggregator node configuration
type StreamAggregatorNodeConfiguration struct {
	// SQL is the aggregation query statement (must contain GROUP BY, aggregation or window functions).
	// Example: SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('5s')
	SQL string `json:"sql" label:"SQL" desc:"Aggregation SQL query. Must contain GROUP BY/window functions. Example: SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')" required:"true"`

	// InputFormat controls how JSON array input enters the stream.
	// auto (default): each array element becomes one stream row (long format).
	// columns: a [{name,value,...}] array is pivoted into one flat row {name:value}
	// (wide format); elements with a non-empty error are skipped.
	InputFormat string `json:"inputFormat" label:"Input Format" desc:"Array input mode. auto (default): each element enters the stream as one row (long format); columns: pivot a [{name,value}] array into one flat row {name:value} (wide format, for cross-field SQL)"`

	// Tables is the optional list of metadata tables for stream-table JOIN.
	// Each table is loaded at Init (inline/file/http), registered for JOIN, and
	// optionally refreshed. JOIN works with both transform and aggregation/window
	// queries. See TableConfig.
	Tables []TableConfig `json:"tables"`
}

// StreamAggregatorNode stream aggregator node
//
// Features:
// - Processes aggregation queries (window aggregation, grouped aggregation, statistics) or CEP (MATCH_RECOGNIZE) pattern recognition
// - Supports single record and array input; array elements are added to the stream one by one
// - Results (aggregation window trigger / CEP pattern match) are routed through the `stream_event` relation instead of the regular Success chain
// - The original input data (single or array) continues through the `Success` chain, keeping the data flow continuous
//
// Data flow:
// - Input data -> added to stream -> original data passes through the Success chain
// - Aggregation/CEP trigger -> results pass through the stream_event chain
//
// Notes:
// - Aggregation results are returned via the global `Config.OnEnd` callback, not via the message processing context's OnEnd callback
// - Aggregation runs asynchronously and does not block the flow of original data
// - Window trigger timing is decided automatically by the StreamSQL engine based on time windows or data volume
type StreamAggregatorNode struct {
	// Node configuration
	Config StreamAggregatorNodeConfiguration
	// StreamSQL instance used to execute SQL aggregation queries
	streamsql *streamsql.Streamsql
	// tables manages the metadata tables for stream-table JOIN (load/register/refresh), closed on Destroy
	tables *tableManager
	// Rule chain ID, used for callback handling of aggregation results
	chainId string
	// Own node ID, used to specify the delivery path of aggregation results
	selfNodeId string
	// Chain context, used to obtain the rule engine instance
	chainCtx types.ChainCtx
	// isCEP marks whether the current query is a MATCH_RECOGNIZE (CEP) query, determining the queryType of result messages
	isCEP bool
}

// Type returns the component type identifier
func (x *StreamAggregatorNode) Type() string {
	return "x/streamAggregator"
}

// New creates a stream aggregator node instance
func (x *StreamAggregatorNode) New() types.Node {
	return &StreamAggregatorNode{
		Config: StreamAggregatorNodeConfiguration{
			InputFormat: InputFormatAuto,
		},
	}
}

// Error definitions
var (
	ErrAggregatorSQLEmpty     = errors.New("aggregator SQL query is required")
	ErrNotAggregatorQuery     = errors.New("SQL does not contain aggregation functions, use x/streamTransform instead")
	ErrAggregatorSQLExecution = errors.New("failed to execute aggregator SQL")
	ErrAggregatorChainCtxNil  = errors.New("chain context is nil")
	ErrAggregatorNodeIdEmpty  = errors.New("self node id is empty")
	ErrAggregatorChainIdEmpty = errors.New("chain id is empty")
)

// Init initializes the node
// Called when the node is loaded, to validate the configuration and initialize the StreamSQL instance
func (x *StreamAggregatorNode) Init(ruleConfig types.Config, configuration types.Configuration) (err error) {
	err = maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// Validate SQL configuration
	if x.Config.SQL == "" {
		return ErrAggregatorSQLEmpty
	}

	// Validate input format configuration
	if err = validateInputFormat(x.Config.InputFormat); err != nil {
		return err
	}

	// Get the chain context
	x.chainCtx = base.NodeUtils.GetChainCtx(configuration)
	if x.chainCtx == nil {
		return ErrAggregatorChainCtxNil
	}

	// Get the self node ID
	selfDef := base.NodeUtils.GetSelfDefinition(configuration)
	if selfDef.Id == "" {
		return ErrAggregatorNodeIdEmpty
	}
	x.selfNodeId = selfDef.Id

	// Get the rule chain ID
	if x.chainCtx.GetNodeId().Id == "" {
		return ErrAggregatorChainIdEmpty
	}
	x.chainId = x.chainCtx.GetNodeId().Id

	// Create the StreamSQL instance, wiring logs into the rulego logging system
	x.streamsql = streamsql.New(streamsql.WithLogger(newRulegoLogger(ruleConfig.Logger)))

	// Execute SQL initialization
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAggregatorSQLExecution, err)
	}
	// Stop the started streamsql instance if Init fails
	defer func() {
		if err != nil {
			x.streamsql.Stop()
		}
	}()

	// Validate that it is an aggregation or CEP (MATCH_RECOGNIZE) query; both use the async Emit+sink pipeline
	x.isCEP = x.streamsql.IsCEPQuery()
	if !x.streamsql.IsAggregationQuery() && !x.isCEP {
		return fmt.Errorf("%w: SQL='%s'", ErrNotAggregatorQuery, x.Config.SQL)
	}

	// Set the aggregation result handling callback
	x.streamsql.AddSink(func(results []map[string]interface{}) {
		x.handleAggregateResult(results)
	})

	// Load metadata tables (stream-table JOIN, must follow Execute). If any table
	// fails, close the already-started refresh goroutines to avoid leaks.
	x.tables = newTableManager(x.streamsql)
	for _, tbl := range x.Config.Tables {
		if err := x.tables.register(tbl); err != nil {
			x.tables.Close()
			return err
		}
	}

	return nil
}

// OnMsg handles a message
// Supports single records and arrays:
// - Single record: added directly to the aggregation stream
// - Array (inputFormat=auto, default): each element is added to the aggregation stream one by one
// - Array (inputFormat=columns): an IoT point array is pivoted into a single wide row before
//   entering the aggregation stream; non-point arrays fall back to row-by-row processing
// In all cases, the original message continues through the Success chain
func (x *StreamAggregatorNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if x.streamsql == nil {
		ctx.TellFailure(msg, ErrStreamsqlInstanceNil)
		return
	}

	// Validate the data type; only JSON is supported
	if msg.DataType != types.JSON {
		ctx.TellFailure(msg, fmt.Errorf("%w: current type is %s", ErrUnsupportedDataType, msg.DataType))
		return
	}

	// Get the JSON data; streamsql handles type conversion internally
	data, err := msg.GetJsonData()
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("%w: %v", ErrDataProcessingFailed, err))
		return
	}

	// Check whether the data is an array
	if x.isArrayData(data) {
		// columns mode: pivot an IoT point array into a single wide row before entering the stream
		if x.Config.InputFormat == InputFormatColumns {
			if row, ok := pivotPointArray(data); ok {
				if len(row) > 0 {
					x.streamsql.Emit(row)
				}
				// Do not Emit when all points are bad; the original message flows on as usual
				ctx.TellSuccess(msg)
				return
			}
			// Not a point array; fall back to row-by-row processing
		}
		// Process array data
		err := x.processArrayData(data)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("%w: %v", ErrArrayProcessingFailed, err))
			return
		}
	} else {
		// Process single data: convert to map[string]interface{} and add to the StreamSQL stream
		mapData, err := x.convertToMapStringInterface(data)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("data type conversion failed: %w", err))
			return
		}
		x.streamsql.Emit(mapData)
	}

	// Data successfully added to the aggregation stream; the original message flows on
	ctx.TellSuccess(msg)
}

// isArrayData checks whether the data is an array
func (x *StreamAggregatorNode) isArrayData(data interface{}) bool {
	// Check directly whether the data type is a slice
	switch data.(type) {
	case []interface{}:
		return true
	case []map[string]interface{}:
		return true
	default:
		return false
	}
}

// processArrayData processes array data, adding each element to the aggregation stream
func (x *StreamAggregatorNode) processArrayData(data interface{}) error {
	// Try to convert to []interface{}
	var arr []interface{}

	switch v := data.(type) {
	case []interface{}:
		arr = v
	case []map[string]interface{}:
		// Convert []map[string]interface{} to []interface{}
		arr = make([]interface{}, len(v))
		for i, item := range v {
			arr[i] = item
		}
	default:
		return fmt.Errorf("unsupported array data type: %T", data)
	}

	// Iterate over the array, adding each element to the aggregation stream.
	// Each element must be converted to map[string]interface{}
	for _, item := range arr {
		mapItem, err := x.convertToMapStringInterface(item)
		if err != nil {
			return fmt.Errorf("array element type conversion failed: %w", err)
		}
		x.streamsql.Emit(mapItem)
	}

	return nil
}

// handleAggregateResult handles aggregation/CEP results
// Called back by the StreamSQL engine when a window fires, an aggregation condition is met, or a CEP pattern matches.
// Results are wrapped as stream_event messages and passed to the next node through the stream_event relation chain
func (x *StreamAggregatorNode) handleAggregateResult(results []map[string]interface{}) {
	// Create metadata for the result message
	metadata := types.NewMetadata()
	if x.isCEP {
		metadata.PutValue("queryType", "cep")
		metadata.PutValue("resultType", "pattern_matched")
	} else {
		metadata.PutValue("queryType", "aggregation")
		metadata.PutValue("resultType", "window_triggered")
	}

	// Inject timestamp (window end time, ns) into each row: parsed from the window_id ("startns_endns")
	// auto-stamped by streamsql, so downstream (e.g. x/tsdbWrite) can persist data/window time instead of
	// write time. Existing timestamps on rows are not overwritten.
	// Note: only time window (tumbling/sliding/session/counting) results carry window_id; global window
	// and CEP (MATCH_RECOGNIZE) results are not stamped (streamsql's processCEP/TypeGlobal skip stampWindowID),
	// so they have no timestamp and downstream persistence falls back to write time. ts<=0 is treated as invalid and not injected.
	for _, r := range results {
		if _, has := r[pointTimestampKey]; has {
			continue
		}
		if id, ok := r[windowIDKey].(string); ok {
			if ts, ok := windowIDEndNs(id); ok && ts > 0 {
				r[pointTimestampKey] = ts
			}
		}
	}

	// Send results through the rule engine
	if e, ok := x.chainCtx.GetRuleEnginePool().Get(x.chainId); ok {
		msg := types.NewMsg(0, StreamEventMsgType, types.JSON, metadata, str.ToString(results))
		// Results pass through the stream_event relation chain (aggregation window trigger or CEP pattern match)
		e.OnMsg(msg, types.WithTellNext(x.selfNodeId, RelationTypeStreamEvent))
	}
}

// windowIDEndNs parses the window end time (ns) from streamsql's window_id ("startns_endns").
func windowIDEndNs(id string) (int64, bool) {
	idx := strings.IndexByte(id, '_')
	if idx < 0 || idx >= len(id)-1 {
		return 0, false
	}
	n, err := strconv.ParseInt(id[idx+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// convertToMapStringInterface converts different map types to map[string]interface{}
// Supported types: map[string]interface{}, map[string]string
func (x *StreamAggregatorNode) convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Already the target type; return directly
		return v, nil
	case map[string]string:
		// Convert map[string]string to map[string]interface{}
		result := make(map[string]interface{}, len(v))
		for key, value := range v {
			result[key] = value
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported data type: %T, expected map type", data)
	}
}

// Destroy destroys the node and releases resources
func (x *StreamAggregatorNode) Destroy() {
	if x.tables != nil {
		x.tables.Close()
	}
	if x.streamsql != nil {
		x.streamsql.Stop()
		x.streamsql = nil
	}
}

// Def returns the component form definition
func (x *StreamAggregatorNode) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc:          "Stream aggregation & CEP node. Runs aggregation (GROUP BY/window) or MATCH_RECOGNIZE. Original data passes via Success, results via stream_event",
		RelationTypes: &[]string{types.Success, types.Failure, RelationTypeStreamEvent},
	}
}
