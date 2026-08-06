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

// StreamTransformNodeConfiguration is the stream transform node configuration
type StreamTransformNodeConfiguration struct {
	// SQL is the non-aggregation query statement (filter, transform, field selection).
	// Example: SELECT temperature, humidity FROM stream WHERE temperature > 20
	SQL string `json:"sql" label:"SQL" desc:"Non-aggregation SQL for filtering/transforming. Example: SELECT temperature FROM stream WHERE temperature > 20" required:"true"`

	// InputFormat controls how JSON array input enters the stream.
	// auto (default): each array element is transformed row by row and results merge into an array.
	// columns: a [{name,value,...}] array is pivoted into one flat row {name:value}
	// (wide format) and transformed as a single record; elements with a non-empty error are skipped.
	InputFormat string `json:"inputFormat" label:"Input Format" desc:"Array input mode. auto (default): each element is transformed as one row and results merge into an array; columns: pivot a [{name,value}] array into one flat row and output a single flat map (for cross-field SQL)"`

	// Tables is the optional list of metadata tables for stream-table JOIN.
	// Each table is loaded at Init from inline rows / a file / an HTTP endpoint;
	// the index key is auto-derived from the JOIN ON clause. Tables may declare
	// a Refresh interval to reload periodically. See TableConfig.
	Tables []TableConfig `json:"tables"`
}

// StreamTransformNode stream transform node
//
// Features:
// - Dedicated to non-aggregation queries, such as data filtering, field transformation, format conversion, etc.
// - Supports single record and array input:
//   - Single record: transformed directly
//   - Array: each element is transformed and successfully transformed results are merged into an array output
//
// - Data that matches the WHERE condition and is transformed successfully is routed to the `Success` chain, otherwise to the `Failure` chain
// - For array input, as long as any element is transformed successfully, the merged result is routed to the Success chain
//
// Data flow:
// - Single record: input -> SQL transform -> Success/Failure output
// - Array: input array -> transform one by one -> merge successful results -> Success output (if any succeeded) / Failure output (all failed)
//
// Notes:
// - Transformation is synchronous and blocks processing of the current message
// - Data that does not match the WHERE condition is filtered out and not included in the output
// - For array input, transformation failure of some elements does not affect the overall result, only the element count of the final array
type StreamTransformNode struct {
	// Node configuration
	Config StreamTransformNodeConfiguration
	// StreamSQL instance used to execute SQL transform queries
	streamsql *streamsql.Streamsql
	// tables manages the metadata tables for stream-table JOIN (load/register/refresh), closed on Destroy
	tables *tableManager
}

// Type returns the component type identifier
func (x *StreamTransformNode) Type() string {
	return "x/streamTransform"
}

// New creates a stream transform node instance
func (x *StreamTransformNode) New() types.Node {
	return &StreamTransformNode{
		Config: StreamTransformNodeConfiguration{
			InputFormat: InputFormatAuto,
		},
	}
}

// Error definitions and constants
var (
	ErrTransformSQLEmpty      = errors.New("transform SQL query is required")
	ErrNotTransformQuery      = errors.New("SQL contains aggregation functions, use x/streamAggregator instead")
	ErrTransformNotSupportCEP = errors.New("SQL contains MATCH_RECOGNIZE (CEP), use x/streamAggregator instead")
	ErrTransformSQLExecution  = errors.New("failed to execute transform SQL")
	ErrStreamsqlInstanceNil   = errors.New("streamsql instance is nil")
	ErrArrayProcessingFailed  = errors.New("failed to process array data")
	ErrUnsupportedDataType    = errors.New("only JSON data type is supported")
	ErrDataProcessingFailed   = errors.New("failed to process message data")

	// Metadata key name identifying whether the data matched the transform condition
	Match      = "match"
	MatchTrue  = "true"
	MatchFalse = "false"
)

// Init initializes the node
// Called when the node is loaded, to validate the configuration and initialize the StreamSQL instance
func (x *StreamTransformNode) Init(ruleConfig types.Config, configuration types.Configuration) (err error) {
	err = maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// Validate SQL configuration
	if x.Config.SQL == "" {
		return ErrTransformSQLEmpty
	}

	// Validate input format configuration
	if err = validateInputFormat(x.Config.InputFormat); err != nil {
		return err
	}

	// Create the StreamSQL instance, wiring logs into the rulego logging system
	x.streamsql = streamsql.New(streamsql.WithLogger(newRulegoLogger(ruleConfig.Logger)))

	// Execute SQL initialization
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransformSQLExecution, err)
	}
	// Stop the started streamsql instance if Init fails
	defer func() {
		if err != nil {
			x.streamsql.Stop()
		}
	}()

	// Validate that it is a non-aggregation, non-CEP query (aggregation/CEP use the async pipeline of x/streamAggregator)
	if x.streamsql.IsAggregationQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrNotTransformQuery, x.Config.SQL)
	}
	if x.streamsql.IsCEPQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrTransformNotSupportCEP, x.Config.SQL)
	}

	// Load metadata tables for stream-table JOIN (must follow Execute). Each table
	// is loaded from inline rows / file / http, registered (key auto-derived from
	// the JOIN ON clause), and optionally refreshed on a background goroutine.
	// If any table fails, close the already-started refresh goroutines to avoid leaks.
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
//   - Single record: transformed directly by SQL; success goes to Success; filtered out (WHERE not satisfied or changed_cols unchanged) goes to False; error goes to Failure
//   - Array (inputFormat=auto, default): each element is transformed and all successful results are merged into an array output.
//     At least one success goes to Success; all failed (no success) with error elements goes to Failure; all filtered (no errors) goes to False
//   - Array (inputFormat=columns): an IoT point array is pivoted into a single wide row and processed as a single record,
//     outputting a single flat map; all bad points go to False; non-point arrays fall back to row-by-row processing
func (x *StreamTransformNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
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
		// columns mode: pivot an IoT point array into a single wide row and process as a single record
		if x.Config.InputFormat == InputFormatColumns {
			if row, ok := pivotPointArray(data); ok {
				if len(row) == 0 {
					// All bad points: equivalent to everything being filtered
					msg.Metadata.PutValue(Match, MatchFalse)
					ctx.TellNext(msg, types.False)
					return
				}
				x.processSingleData(ctx, msg, row)
				return
			}
			// Not a point array; fall back to row-by-row processing
		}
		// Process array data
		x.processArrayData(ctx, msg, data)
	} else {
		// Process single data
		x.processSingleData(ctx, msg, data)
	}
}

// isArrayData checks whether the data is an array
func (x *StreamTransformNode) isArrayData(data interface{}) bool {
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

// processSingleData handles single record transformation
func (x *StreamTransformNode) processSingleData(ctx types.RuleContext, msg types.RuleMsg, data interface{}) {
	// Convert to map[string]interface{}; supports multiple map types
	mapData, err := x.convertToMapStringInterface(data)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("data type conversion failed: %w", err))
		return
	}

	// Process the data transformation synchronously
	result, err := x.streamsql.EmitSync(mapData)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("transform processing failed: %w", err))
		return
	}

	if result != nil {
		// Transformation succeeded; update the message data and send to the next node
		msg.SetData(str.ToString(result))
		msg.SetDataType(types.JSON)
		msg.Metadata.PutValue(Match, MatchTrue)
		ctx.TellSuccess(msg)
	} else {
		// Data filtered out (WHERE not satisfied or changed_cols unchanged); not an error, route to the False chain
		msg.Metadata.PutValue(Match, MatchFalse)
		ctx.TellNext(msg, types.False)
	}
}

// processArrayData handles array data transformation
// Transforms each element in the array and merges the successfully transformed results into a new array
func (x *StreamTransformNode) processArrayData(ctx types.RuleContext, msg types.RuleMsg, data interface{}) {
	// Try to convert to []interface{}
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
	var errorCount int    // number of transform/type errors
	var filteredCount int // number filtered out (WHERE not satisfied or changed_cols unchanged)

	// Iterate over array elements, transforming them one by one
	for _, item := range inputArray {
		// Convert to map[string]interface{}; supports multiple map types
		mapItem, err := x.convertToMapStringInterface(item)
		if err != nil {
			// Type conversion failed; count the error and continue with the next element
			errorCount++
			continue
		}

		result, err := x.streamsql.EmitSync(mapItem)
		if err != nil {
			// Transformation error; count the error and continue with the next element
			errorCount++
			continue
		}

		if result != nil {
			// Transformation succeeded and matched the WHERE condition; add to the result array
			transformedResults = append(transformedResults, result)
		} else {
			// Did not match the WHERE condition or unchanged; filtered out
			filteredCount++
		}
	}

	failedCount := errorCount + filteredCount

	// Determine the processing result
	if len(transformedResults) > 0 {
		// At least one element was transformed successfully
		resultJson, err := json.Marshal(transformedResults)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("%w: failed to marshal results: %v", ErrArrayProcessingFailed, err))
			return
		}

		// Update the message data
		msg.SetData(string(resultJson))
		msg.SetDataType(types.JSON)
		msg.Metadata.PutValue(Match, MatchTrue)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", str.ToString(len(transformedResults)))
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellSuccess(msg)
	} else if errorCount > 0 {
		// No successful results and some elements errored; route to Failure (real error)
		msg.Metadata.PutValue(Match, MatchFalse)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", "0")
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellFailure(msg, fmt.Errorf("%w: %d failed, %d filtered", ErrArrayProcessingFailed, errorCount, filteredCount))
	} else {
		// All filtered out with no errors; route to False (not an error)
		msg.Metadata.PutValue(Match, MatchFalse)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", "0")
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellNext(msg, types.False)
	}
}

// convertToMapStringInterface converts different map types to map[string]interface{}
// Supported types: map[string]interface{}, map[string]string
func (x *StreamTransformNode) convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
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
func (x *StreamTransformNode) Destroy() {
	// Stop table refresh goroutines before stopping StreamSQL, so refreshes do not write tables to a stopped instance.
	if x.tables != nil {
		x.tables.Close()
	}
	if x.streamsql != nil {
		x.streamsql.Stop()
		x.streamsql = nil
	}
}

// Desc returns the component description
func (x *StreamTransformNode) Desc() string {
	return "Stream transform node. Processes non-aggregation SQL for filtering and field transformation. Supports single and array JSON input. Routes to Success/False/Failure"
}

// Def returns the component form definition
func (x *StreamTransformNode) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc:          "Stream transform node. Processes non-aggregation SQL for filtering and transformation. Success: transformed; False: WHERE not matched or changed_cols no-change; Failure: error",
		RelationTypes: &[]string{types.Success, types.False, types.Failure},
	}
}
