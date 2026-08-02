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

// StreamTransformNodeConfiguration 流转换器节点配置
type StreamTransformNodeConfiguration struct {
	// SQL is the non-aggregation query statement (filter, transform, field selection).
	// Example: SELECT temperature, humidity FROM stream WHERE temperature > 20
	SQL string `json:"sql" label:"SQL" desc:"Non-aggregation SQL for filtering/transforming. Example: SELECT temperature FROM stream WHERE temperature > 20" required:"true"`

	// Tables is the optional list of metadata tables for stream-table JOIN.
	// Each table is loaded at Init from inline rows / a file / an HTTP endpoint;
	// the index key is auto-derived from the JOIN ON clause. Tables may declare
	// a Refresh interval to reload periodically. See TableConfig.
	Tables []TableConfig `json:"tables"`
}

// StreamTransformNode 流转换器节点
//
// 功能说明：
// - 专门处理非聚合查询，如数据过滤、字段转换、格式变换等
// - 支持单条数据和数组数据输入：
//   - 单条数据：直接进行转换处理
//   - 数组数据：遍历转换每个元素，将成功转换的结果合并成数组输出
//
// - 数据符合WHERE条件并转换成功，则通过`Success`链输出，否则通过`Failure`链输出
// - 对于数组输入，只要有任何元素转换成功，就会通过Success链输出合并结果
//
// 数据流向：
// - 单条数据：输入 -> SQL转换 -> Success/Failure输出
// - 数组数据：输入数组 -> 逐个转换 -> 合并成功结果 -> Success输出（如有成功项）/ Failure输出（全部失败）
//
// 注意事项：
// - 转换处理是同步的，会阻塞当前消息的处理
// - WHERE条件不匹配的数据会被过滤掉，不包含在输出结果中
// - 对于数组输入，部分元素转换失败不会影响整体结果，只影响最终数组的元素数量
type StreamTransformNode struct {
	// 节点配置
	Config StreamTransformNodeConfiguration
	// StreamSQL实例，用于执行SQL转换查询
	streamsql *streamsql.Streamsql
	// tables 管理流-表 JOIN 的元数据表（加载/注册/刷新），Destroy 时关闭
	tables *tableManager
}

// Type 返回组件类型标识
func (x *StreamTransformNode) Type() string {
	return "x/streamTransform"
}

// New 创建流转换器节点实例
func (x *StreamTransformNode) New() types.Node {
	return &StreamTransformNode{
		Config: StreamTransformNodeConfiguration{},
	}
}

// 错误定义和常量
var (
	ErrTransformSQLEmpty      = errors.New("transform SQL query is required")
	ErrNotTransformQuery      = errors.New("SQL contains aggregation functions, use x/streamAggregator instead")
	ErrTransformNotSupportCEP = errors.New("SQL contains MATCH_RECOGNIZE (CEP), use x/streamAggregator instead")
	ErrTransformSQLExecution  = errors.New("failed to execute transform SQL")
	ErrStreamsqlInstanceNil   = errors.New("streamsql instance is nil")
	ErrArrayProcessingFailed  = errors.New("failed to process array data")
	ErrUnsupportedDataType    = errors.New("only JSON data type is supported")
	ErrDataProcessingFailed   = errors.New("failed to process message data")

	// 元数据键名，用于标识数据是否匹配转换条件
	Match      = "match"
	MatchTrue  = "true"
	MatchFalse = "false"
)

// Init 初始化节点
// 该方法在节点被加载时调用，用于验证配置和初始化StreamSQL实例
func (x *StreamTransformNode) Init(ruleConfig types.Config, configuration types.Configuration) (err error) {
	err = maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// 验证SQL配置
	if x.Config.SQL == "" {
		return ErrTransformSQLEmpty
	}

	// 创建StreamSQL实例，日志接入 rulego 日志体系
	x.streamsql = streamsql.New(streamsql.WithLogger(newRulegoLogger(ruleConfig.Logger)))

	// 执行SQL初始化
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransformSQLExecution, err)
	}
	// Init 失败时停止已启动的 streamsql 实例
	defer func() {
		if err != nil {
			x.streamsql.Stop()
		}
	}()

	// 验证是否为非聚合、非 CEP 查询（聚合/CEP 走 x/streamAggregator 的异步管道）
	if x.streamsql.IsAggregationQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrNotTransformQuery, x.Config.SQL)
	}
	if x.streamsql.IsCEPQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrTransformNotSupportCEP, x.Config.SQL)
	}

	// Load metadata tables for stream-table JOIN (must follow Execute). Each table
	// is loaded from inline rows / file / http, registered (key auto-derived from
	// the JOIN ON clause), and optionally refreshed on a background goroutine.
	// 任一表失败则关闭已启动的刷新 goroutine，避免泄漏。
	x.tables = newTableManager(x.streamsql)
	for _, tbl := range x.Config.Tables {
		if err := x.tables.register(tbl); err != nil {
			x.tables.Close()
			return err
		}
	}

	return nil
}

// OnMsg 处理消息
// 支持单条数据和数组数据：
//   - 单条数据：直接进行SQL转换，成功走 Success；被过滤（WHERE 不满足或 changed_cols 无变化）走 False；出错走 Failure
//   - 数组数据：遍历每个元素进行转换，将所有成功的结果合并成数组输出
//     至少一个成功走 Success；全部失败（无成功）且有出错元素走 Failure；全部被过滤（无出错）走 False
func (x *StreamTransformNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	if x.streamsql == nil {
		ctx.TellFailure(msg, ErrStreamsqlInstanceNil)
		return
	}

	// 验证数据类型，只支持JSON类型
	if msg.DataType != types.JSON {
		ctx.TellFailure(msg, fmt.Errorf("%w: current type is %s", ErrUnsupportedDataType, msg.DataType))
		return
	}

	// 获取JSON数据，streamsql内部会处理类型转换
	data, err := msg.GetJsonData()
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("%w: %v", ErrDataProcessingFailed, err))
		return
	}

	// 检查数据是否为数组
	if x.isArrayData(data) {
		// 处理数组数据
		x.processArrayData(ctx, msg, data)
	} else {
		// 处理单条数据
		x.processSingleData(ctx, msg, data)
	}
}

// isArrayData 检查数据是否为数组
func (x *StreamTransformNode) isArrayData(data interface{}) bool {
	// 直接检查数据类型是否为slice
	switch data.(type) {
	case []interface{}:
		return true
	case []map[string]interface{}:
		return true
	default:
		return false
	}
}

// processSingleData 处理单条数据转换
func (x *StreamTransformNode) processSingleData(ctx types.RuleContext, msg types.RuleMsg, data interface{}) {
	// 转换为map[string]interface{}类型，支持多种map类型
	mapData, err := x.convertToMapStringInterface(data)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("data type conversion failed: %w", err))
		return
	}

	// 同步处理数据转换
	result, err := x.streamsql.EmitSync(mapData)
	if err != nil {
		ctx.TellFailure(msg, fmt.Errorf("transform processing failed: %w", err))
		return
	}

	if result != nil {
		// 转换成功，更新消息数据并发送到下一个节点
		msg.SetData(str.ToString(result))
		msg.SetDataType(types.JSON)
		msg.Metadata.PutValue(Match, MatchTrue)
		ctx.TellSuccess(msg)
	} else {
		// 数据被过滤（WHERE 不满足或 changed_cols 无变化），非错误，走 False 链
		msg.Metadata.PutValue(Match, MatchFalse)
		ctx.TellNext(msg, types.False)
	}
}

// processArrayData 处理数组数据转换
// 遍历数组中的每个元素，进行转换处理，将成功转换的结果合并成新数组
func (x *StreamTransformNode) processArrayData(ctx types.RuleContext, msg types.RuleMsg, data interface{}) {
	// 尝试转换为 []interface{}
	var inputArray []interface{}

	switch v := data.(type) {
	case []interface{}:
		inputArray = v
	case []map[string]interface{}:
		// 将 []map[string]interface{} 转换为 []interface{}
		inputArray = make([]interface{}, len(v))
		for i, item := range v {
			inputArray[i] = item
		}
	default:
		ctx.TellFailure(msg, fmt.Errorf("%w: unsupported array data type: %T", ErrArrayProcessingFailed, data))
		return
	}

	var transformedResults []interface{}
	var errorCount int    // 转换/类型出错数
	var filteredCount int // 被过滤数（WHERE 不满足或 changed_cols 无变化）

	// 遍历数组元素，逐个进行转换
	for _, item := range inputArray {
		// 转换为map[string]interface{}类型，支持多种map类型
		mapItem, err := x.convertToMapStringInterface(item)
		if err != nil {
			// 类型转换失败，记录出错数，继续处理下一个元素
			errorCount++
			continue
		}

		result, err := x.streamsql.EmitSync(mapItem)
		if err != nil {
			// 转换出错，记录出错数，继续处理下一个元素
			errorCount++
			continue
		}

		if result != nil {
			// 转换成功且符合WHERE条件，添加到结果数组
			transformedResults = append(transformedResults, result)
		} else {
			// 不符合WHERE条件或无变化，被过滤掉
			filteredCount++
		}
	}

	failedCount := errorCount + filteredCount

	// 判断处理结果
	if len(transformedResults) > 0 {
		// 至少有一个元素转换成功
		resultJson, err := json.Marshal(transformedResults)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("%w: failed to marshal results: %v", ErrArrayProcessingFailed, err))
			return
		}

		// 更新消息数据
		msg.SetData(string(resultJson))
		msg.SetDataType(types.JSON)
		msg.Metadata.PutValue(Match, MatchTrue)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", str.ToString(len(transformedResults)))
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellSuccess(msg)
	} else if errorCount > 0 {
		// 无成功结果且有出错元素，走 Failure（真错误）
		msg.Metadata.PutValue(Match, MatchFalse)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", "0")
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellFailure(msg, fmt.Errorf("%w: %d failed, %d filtered", ErrArrayProcessingFailed, errorCount, filteredCount))
	} else {
		// 全部被过滤、无错误，走 False（非错误）
		msg.Metadata.PutValue(Match, MatchFalse)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", "0")
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellNext(msg, types.False)
	}
}

// convertToMapStringInterface 将不同类型的map转换为map[string]interface{}
// 支持的类型包括：map[string]interface{}, map[string]string
func (x *StreamTransformNode) convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		// 已经是目标类型，直接返回
		return v, nil
	case map[string]string:
		// 转换 map[string]string 为 map[string]interface{}
		result := make(map[string]interface{}, len(v))
		for key, value := range v {
			result[key] = value
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported data type: %T, expected map type", data)
	}
}

// Destroy 销毁节点，释放资源
func (x *StreamTransformNode) Destroy() {
	// 先停止表刷新 goroutine，再停 StreamSQL，避免刷新向已停止实例写表。
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
