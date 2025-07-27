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
	// SQL查询语句，仅支持非聚合查询（过滤、转换、字段选择等）
	// 转换查询示例：
	//   SELECT temperature, humidity FROM stream WHERE temperature > 20
	//   SELECT deviceId, temperature * 1.8 + 32 as temp_fahrenheit FROM stream
	//   SELECT *, CASE WHEN status = 'active' THEN 1 ELSE 0 END as status_code FROM stream
	//   SELECT deviceId, UPPER(deviceName) as name, temperature FROM stream WHERE deviceId LIKE 'sensor_%'
	// 支持的操作：
	// - 字段选择：SELECT field1, field2 FROM stream
	// - 字段重命名：SELECT field1 as new_name FROM stream
	// - 数学运算：SELECT field1 * 2 + 10 as calculated FROM stream
	// - 条件过滤：WHERE 子句进行数据过滤
	// - 字符串函数：UPPER, LOWER, SUBSTR, CONCAT等
	// - 数学函数：ABS, ROUND, CEIL, FLOOR等
	// - 条件表达式：CASE WHEN ... THEN ... ELSE ... END
	SQL string `json:"sql"`
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
	ErrTransformSQLExecution  = errors.New("failed to execute transform SQL")
	ErrNotMatchWhereCondition = errors.New("not match WHERE condition")
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
func (x *StreamTransformNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// 验证SQL配置
	if x.Config.SQL == "" {
		return ErrTransformSQLEmpty
	}

	// 创建StreamSQL实例
	x.streamsql = streamsql.New()

	// 执行SQL初始化
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransformSQLExecution, err)
	}

	// 验证是否为非聚合查询
	if x.streamsql.IsAggregationQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrNotTransformQuery, x.Config.SQL)
	}

	return nil
}

// OnMsg 处理消息
// 支持单条数据和数组数据：
//   - 单条数据：直接进行SQL转换，成功则通过Success链输出，失败则通过Failure链输出
//   - 数组数据：遍历每个元素进行转换，将所有成功的结果合并成数组输出
//     如果至少有一个元素转换成功，则通过Success链输出；如果全部失败，则通过Failure链输出
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
	// 同步处理数据转换，streamsql内部会自动处理类型转换
	result, err := x.streamsql.EmitSync(data)
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
		// 数据被过滤，不符合WHERE条件，通过Failure输出
		msg.Metadata.PutValue(Match, MatchFalse)
		ctx.TellFailure(msg, ErrNotMatchWhereCondition)
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
	var failedCount int

	// 遍历数组元素，逐个进行转换
	for _, item := range inputArray {
		// streamsql内部会自动处理类型转换
		result, err := x.streamsql.EmitSync(item)
		if err != nil {
			// 转换出错，记录失败次数，继续处理下一个元素
			failedCount++
			continue
		}

		if result != nil {
			// 转换成功且符合WHERE条件，添加到结果数组
			transformedResults = append(transformedResults, result)
		} else {
			// 不符合WHERE条件，被过滤掉，记录失败次数
			failedCount++
		}
	}

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
	} else {
		// 所有元素都转换失败或被过滤
		msg.Metadata.PutValue(Match, MatchFalse)
		msg.Metadata.PutValue("originalCount", str.ToString(len(inputArray)))
		msg.Metadata.PutValue("transformedCount", "0")
		msg.Metadata.PutValue("failedCount", str.ToString(failedCount))

		ctx.TellFailure(msg, fmt.Errorf("all array elements failed transformation or were filtered out"))
	}
}

// Destroy 销毁节点，释放资源
// 该方法在节点被卸载时调用，用于清理StreamSQL实例和相关资源
func (x *StreamTransformNode) Destroy() {
	if x.streamsql != nil {
		x.streamsql.Stop()
		x.streamsql = nil
	}
}
