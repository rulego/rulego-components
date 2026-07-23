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

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"github.com/rulego/streamsql"
)

// RelationTypeWindowEvent represents the window event relationship type, used for the link delivery of aggregated results
const RelationTypeWindowEvent = "window_event"

// WindowEventMsgType represents the type of window event message used to identify the aggregated result message
const WindowEventMsgType = "window_event"

func init() {
	_ = rulego.Registry.Register(&StreamAggregatorNode{})
}

// StreamAggregatorNodeConfiguration: Stream aggregator node configuration
type StreamAggregatorNodeConfiguration struct {
	// SQL is the aggregation query statement (must contain GROUP BY, aggregation or window functions).
	// Example: SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('5s')
	SQL string `json:"sql" label:"SQL" desc:"Aggregation SQL query. Must contain GROUP BY/window functions. Example: SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')" required:"true"`
}

// StreamAggregatorNode is a stream aggregator node
//
// Function Description:
// - Specialized in handling aggregated queries, such as window aggregation, packet aggregation, statistical calculations, etc
// - Supports both single and array data inputs, with array data added to the aggregate stream one by one
// - The aggregated results are passed to the next node via the `window_event` chain of relationships, rather than through the regular Success chain
// - Raw input data (whether single or array) continues to be passed through the `Success` chain, maintaining the continuity of the data flow
//
// Data Flow:
// - Input data -> Add to the aggregate stream -> Deliver the raw data via the Success chain
// - Aggregation Trigger -> Aggregate results are passed through window_event chain
//
// Notes:
// - Aggregated results are returned via the global `Config.OnEnd` callback, rather than via the OnEnd callback of the message processing context
// - Aggregation computation is performed asynchronously and does not block the flow of raw data
// - The window trigger timing is automatically determined by the StreamSQL engine based on the time window or data volume
type StreamAggregatorNode struct {
	// Node configuration
	Config StreamAggregatorNodeConfiguration
	// StreamSQL instance, used to execute SQL aggregate queries
	streamsql *streamsql.Streamsql
	// Rule chain ID, used for callback processing of aggregated results
	chainId string
	// The node ID specifies the transmission path for aggregation results
	selfNodeId string
	// Chain context, used to obtain instances of the rule engine
	chainCtx types.ChainCtx
}

// Type returns the component type identifier
func (x *StreamAggregatorNode) Type() string {
	return "x/streamAggregator"
}

// New Create a node instance of the stream aggregator
func (x *StreamAggregatorNode) New() types.Node {
	return &StreamAggregatorNode{
		Config: StreamAggregatorNodeConfiguration{},
	}
}

// Misdefinition
var (
	ErrAggregatorSQLEmpty     = errors.New("aggregator SQL query is required")
	ErrNotAggregatorQuery     = errors.New("SQL does not contain aggregation functions, use x/streamTransform instead")
	ErrAggregatorSQLExecution = errors.New("failed to execute aggregator SQL")
	ErrAggregatorChainCtxNil  = errors.New("chain context is nil")
	ErrAggregatorNodeIdEmpty  = errors.New("self node id is empty")
	ErrAggregatorChainIdEmpty = errors.New("chain id is empty")
)

// Init initializes the node
// This method is called when a node is loaded to verify configuration and initialize the StreamSQL instance
func (x *StreamAggregatorNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// Verify SQL configuration
	if x.Config.SQL == "" {
		return ErrAggregatorSQLEmpty
	}

	// Obtain the chain context
	x.chainCtx = base.NodeUtils.GetChainCtx(configuration)
	if x.chainCtx == nil {
		return ErrAggregatorChainCtxNil
	}

	// Obtain its own node ID
	selfDef := base.NodeUtils.GetSelfDefinition(configuration)
	if selfDef.Id == "" {
		return ErrAggregatorNodeIdEmpty
	}
	x.selfNodeId = selfDef.Id

	// Obtain the rule chain ID
	if x.chainCtx.GetNodeId().Id == "" {
		return ErrAggregatorChainIdEmpty
	}
	x.chainId = x.chainCtx.GetNodeId().Id

	// Create a StreamSQL instance
	x.streamsql = streamsql.New()

	// Execute SQL initialization
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAggregatorSQLExecution, err)
	}

	// Verify whether it is an aggregate query
	if !x.streamsql.IsAggregationQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrNotAggregatorQuery, x.Config.SQL)
	}

	// Set the aggregation result to handle callbacks
	x.streamsql.AddSink(func(results []map[string]interface{}) {
		x.handleAggregateResult(results)
	})

	return nil
}

// OnMsg processes a message
// Supports single data entries and array data:
// - Single data entry: Directly added to the aggregate stream
// - Array data: traverse each element and add each element to the aggregate stream one by one
// In either case, the original message will continue to be transmitted through the Success chain
func (x *StreamAggregatorNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if x.streamsql == nil {
		ctx.TellFailure(msg, ErrStreamsqlInstanceNil)
		return
	}

	// Verify data types, only support JSON types
	if msg.DataType != types.JSON {
		ctx.TellFailure(msg, fmt.Errorf("%w: current type is %s", ErrUnsupportedDataType, msg.DataType))
		return
	}

	// Obtain JSON data, and streamsql internally handles type conversions
	data, err := msg.GetJsonData()
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("%w: %v", ErrDataProcessingFailed, err))
		return
	}

	// Check if the data is an array
	if x.isArrayData(data) {
		// Processing array data
		err := x.processArrayData(data)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("%w: %v", ErrArrayProcessingFailed, err))
			return
		}
	} else {
		// Processing single data entry: convert it to map[string]interface{} type and add it to the StreamSQL stream
		mapData, err := x.convertToMapStringInterface(data)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("data type conversion failed: %w", err))
			return
		}
		x.streamsql.Emit(mapData)
	}

	// Data successfully joined the aggregate flow, and the original message continued to circulate
	ctx.TellSuccess(msg)
}

// isArrayData checks whether the data is an array
func (x *StreamAggregatorNode) isArrayData(data interface{}) bool {
	// Directly check whether the data type is slice
	switch data.(type) {
	case []interface{}:
		return true
	case []map[string]interface{}:
		return true
	default:
		return false
	}
}

// processArrayData processes array data and adds each element to the aggregate stream
func (x *StreamAggregatorNode) processArrayData(data interface{}) error {
	// Try converting to []interface{}
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

	// Traverse the array and add each element to the aggregate stream
	// Each element needs to be converted to the map[string]interface{} type
	for _, item := range arr {
		mapItem, err := x.convertToMapStringInterface(item)
		if err != nil {
			return fmt.Errorf("array element type conversion failed: %w", err)
		}
		x.streamsql.Emit(mapItem)
	}

	return nil
}

// handleAggregateResult handles the aggregate result
// When the window is triggered or aggregation conditions are met, this method is called back by the StreamSQL engine
// The aggregated results are packaged into special window_event messages and passed to the next node through window_event chain of relationships
func (x *StreamAggregatorNode) handleAggregateResult(results []map[string]interface{}) {
	// Create metadata for the aggregated result message
	metadata := types.NewMetadata()
	metadata.PutValue("queryType", "aggregation")
	metadata.PutValue("resultType", "window_triggered")

	// Aggregate results are sent through the rules engine
	if e, ok := x.chainCtx.GetRuleEnginePool().Get(x.chainId); ok {
		msg := types.NewMsg(0, WindowEventMsgType, types.JSON, metadata, str.ToString(results))
		// Send the aggregated results to the next node using window_event chain of relationships
		e.OnMsg(msg, types.WithTellNext(x.selfNodeId, RelationTypeWindowEvent))
	}
}

// convertToMapStringInterface Converts different types of maps to map[string]interface{}
// Supported types include: map[string]interface{}, map[string]string
func (x *StreamAggregatorNode) convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// Already a target type, go straight back
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

// Destroy nodes to release resources
func (x *StreamAggregatorNode) Destroy() {
	if x.streamsql != nil {
		x.streamsql.Stop()
		x.streamsql = nil
	}
}

// Def returns the component form definition
func (x *StreamAggregatorNode) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc:          "Stream aggregation node. Processes aggregation SQL with window functions. Original data passes via Success, aggregation results via window_event",
		RelationTypes: &[]string{types.Success, types.Failure, RelationTypeWindowEvent},
	}
}
