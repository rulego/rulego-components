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

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"github.com/rulego/streamsql"
)

func init() {
	_ = rulego.Registry.Register(&StreamTransformNode{})
}

// StreamTransformNodeConfiguration: Stream Converter node configuration
type StreamTransformNodeConfiguration struct {
	// SQL is the non-aggregation query statement (filter, transform, field selection).
	// Example: SELECT temperature, humidity FROM stream WHERE temperature > 20
	SQL string `json:"sql" label:"SQL" desc:"Non-aggregation SQL for filtering/transforming. Example: SELECT temperature FROM stream WHERE temperature > 20" required:"true"`
}

// StreamTransformNode Stream Transformer node
//
// Function Description:
// - Specialized in handling non-aggregated queries, such as data filtering, field conversion, format transformation, etc
// - Supports single data entry and array data input:
//   - Single data entry: Directly converts and processes
//   - Array data: Traverse and transform each element, merging the successful results into an array output
//
// - If the data meets the WHERE conditions and the conversion is `Success`ful, output via the 'Success' chain; otherwise, output via the `Failure` chain
// - For array inputs, as long as any element successfully converts, the merging result will be output via the Success chain
//
// Data Flow:
// - Single data: input -> SQL conversion -> Success/Failure output
// - Array data: input array -> Convert one by one -> merge successful result -> Success output (if there are successful entries) / Failure output (all failures)
//
// Notes:
// - Conversion processing is synchronous and blocks the current message processing
// - Data that does not match WHERE conditions will be filtered out and not included in the output results
// - For array input, failure in some element conversion does not affect the overall result, only the number of elements in the final array
type StreamTransformNode struct {
	// Node configuration
	Config StreamTransformNodeConfiguration
	// StreamSQL instance used to execute SQL conversion queries
	streamsql *streamsql.Streamsql
}

// Type returns the component type identifier
func (x *StreamTransformNode) Type() string {
	return "x/streamTransform"
}

// New: Create a Stream Converter node instance
func (x *StreamTransformNode) New() types.Node {
	return &StreamTransformNode{
		Config: StreamTransformNodeConfiguration{},
	}
}

// Misdefinitions and constants
var (
	ErrTransformSQLEmpty      = errors.New("transform SQL query is required")
	ErrNotTransformQuery      = errors.New("SQL contains aggregation functions, use x/streamAggregator instead")
	ErrTransformSQLExecution  = errors.New("failed to execute transform SQL")
	ErrNotMatchWhereCondition = errors.New("not match WHERE condition")
	ErrStreamsqlInstanceNil   = errors.New("streamsql instance is nil")
	ErrArrayProcessingFailed  = errors.New("failed to process array data")
	ErrUnsupportedDataType    = errors.New("only JSON data type is supported")
	ErrDataProcessingFailed   = errors.New("failed to process message data")

	// Metadata keys, used to indicate whether the data matches conversion conditions
	Match      = "match"
	MatchTrue  = "true"
	MatchFalse = "false"
)

// Init initializes the node
// This method is called when a node is loaded to verify configuration and initialize the StreamSQL instance
func (x *StreamTransformNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// Verify SQL configuration
	if x.Config.SQL == "" {
		return ErrTransformSQLEmpty
	}

	// Create a StreamSQL instance
	x.streamsql = streamsql.New()

	// Execute SQL initialization
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransformSQLExecution, err)
	}

	// Verify whether the query is non-aggregated
	if x.streamsql.IsAggregationQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrNotTransformQuery, x.Config.SQL)
	}

	return nil
}

// OnMsg processes a message
// Supports single data entries and array data:
//   - Single data entry: Directly performs SQL transformation; if successful, output via the Success chain; if failed, output via the Failure chain
//   - Array data: Traverse each element for transformation, merging all successful results into an array output
//     If at least one element is successfully converted, output via the Success chain; If all fail, output is done via the Failure chain
func (x *StreamTransformNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
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
		x.processArrayData(ctx, msg, data)
	} else {
		// Processing individual data entries
		x.processSingleData(ctx, msg, data)
	}
}

// isArrayData checks whether the data is an array
func (x *StreamTransformNode) isArrayData(data interface{}) bool {
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

// processSingleData handles the conversion of individual data entries
func (x *StreamTransformNode) processSingleData(ctx types.RuleContext, msg types.RuleMsg, data interface{}) {
	// Convert to map[string]interface{} type, supporting multiple map types
	mapData, err := x.convertToMapStringInterface(data)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("data type conversion failed: %w", err))
		return
	}

	// Data conversion is processed simultaneously
	result, err := x.streamsql.EmitSync(mapData)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("transform processing failed: %w", err))
		return
	}

	if result != nil {
		// After successful conversion, update the message data and send it to the next node
		msg.SetData(str.ToString(result))
		msg.SetDataType(types.JSON)
		msg.Metadata.PutValue(Match, MatchTrue)
		ctx.TellSuccess(msg)
	} else {
		// The data is filtered and does not meet the WHERE condition, and output via Failure
		msg.Metadata.PutValue(Match, MatchFalse)
		ctx.TellFailure(msg, ErrNotMatchWhereCondition)
	}
}

// processArrayData handles array data transformation
// Traverse each element in the array, perform the transformation, and merge the successful conversion results into a new array
func (x *StreamTransformNode) processArrayData(ctx types.RuleContext, msg types.RuleMsg, data interface{}) {
	// Try converting to []interface{}
	var inputArray []interface{}

	switch v := data.(type) {
	case []interface{}:
		inputArray = v
	case []map[string]interface{}:
		// Convert []map[string]interface{} to []interface{}
		inputArray = make([]interface{}, len(v))
		for i, item := range v {
			inputArray[i] = item
		}
	default:
		ctx.TellFailure(msg, fmt.Errorf("%w: unsupported array data type: %T", ErrArrayProcessingFailed, data))
		return
	}

	var transformedResults []interface{}
	var failedCount int

	// Traverse the array elements and convert them one by one
	for _, item := range inputArray {
		// Convert to map[string]interface{} type, supporting multiple map types
		mapItem, err := x.convertToMapStringInterface(item)
		if err != nil {
			// If type conversion fails, record the number of failures and continue processing the next element
			failedCount++
			continue
		}

		result, err := x.streamsql.EmitSync(mapItem)
		if err != nil {
			// If a conversion error occurs, record the number of failures and continue processing the next element
			failedCount++
			continue
		}

		if result != nil {
			// If the conversion is successful and meets the WHERE criteria, add it to the result array
			transformedResults = append(transformedResults, result)
		} else {
			// If the WHERE condition is not met, it will be filtered out and the number of failures will be recorded
			failedCount++
		}
	}

	// Assess the handling results
	if len(transformedResults) > 0 {
		// At least one element was successfully converted
		resultJson, err := json.Marshal(transformedResults)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("%w: failed to marshal results: %v", ErrArrayProcessingFailed, err))
			return
		}

		// Update message data
		msg.SetData(string(resultJson))
		msg.SetDataType(types.JSON)
		msg.Metadata.PutValue(Match, MatchTrue)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", str.ToString(len(transformedResults)))
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellSuccess(msg)
	} else {
		// All elements fail to convert or are filtered
		msg.Metadata.PutValue(Match, MatchFalse)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", "0")
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellFailure(msg, fmt.Errorf("all array elements failed transformation or were filtered out"))
	}
}

// convertToMapStringInterface Converts different types of maps to map[string]interface{}
// Supported types include: map[string]interface{}, map[string]string
func (x *StreamTransformNode) convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
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
func (x *StreamTransformNode) Destroy() {
	if x.streamsql != nil {
		x.streamsql.Stop()
		x.streamsql = nil
	}
}

// Desc returns the component description
func (x *StreamTransformNode) Desc() string {
	return "Stream transform node. Processes non-aggregation SQL for filtering and field transformation. Supports single and array JSON input. Routes to Success/Failure"
}
