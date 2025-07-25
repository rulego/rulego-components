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

// RelationTypeWindowEvent 表示窗口事件关系类型，用于聚合结果的链路传递
const RelationTypeWindowEvent = "window_event"

// WindowEventMsgType 表示窗口事件消息类型，用于标识聚合结果消息
const WindowEventMsgType = "window_event"

func init() {
	_ = rulego.Registry.Register(&StreamAggregatorNode{})
}

// StreamAggregatorNodeConfiguration 流聚合器节点配置
type StreamAggregatorNodeConfiguration struct {
	// SQL查询语句，仅支持聚合查询（包含GROUP BY、聚合函数、窗口函数等）
	// 聚合查询示例：
	//   SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('5s')
	//   SELECT deviceId, MAX(temperature) as max_temp FROM stream GROUP BY deviceId, SlidingWindow('1m', '30s')
	//   SELECT COUNT(*) as count, SUM(value) as total FROM stream GROUP BY TumblingWindow('10s')
	// 支持的聚合函数：COUNT, SUM, AVG, MAX, MIN, FIRST, LAST等
	// 支持的窗口函数：TumblingWindow, SlidingWindow, SessionWindow等
	SQL string `json:"sql"`
}

// StreamAggregatorNode 流聚合器节点
//
// 功能说明：
// - 专门处理聚合查询，如窗口聚合、分组聚合、统计计算等
// - 支持单条数据和数组数据输入，数组数据会被逐条添加到聚合流中
// - 聚合结果通过 `window_event` 关系链传递到下一个节点，而不是通过普通的Success链
// - 原始输入数据（无论单条还是数组）都会通过 `Success` 链继续传递，保持数据流的连续性
//
// 数据流向：
// - 输入数据 -> 添加到聚合流 -> 原始数据通过Success链传递
// - 聚合触发 -> 聚合结果通过window_event链传递
//
// 注意事项：
// - 聚合结果通过全局`Config.OnEnd`回调返回，而不是通过消息处理上下文的OnEnd回调返回
// - 聚合计算是异步进行的，不会阻塞原始数据的流转
// - 窗口触发时机由StreamSQL引擎根据时间窗口或数据量自动决定
type StreamAggregatorNode struct {
	// 节点配置
	Config StreamAggregatorNodeConfiguration
	// StreamSQL实例，用于执行SQL聚合查询
	streamsql *streamsql.Streamsql
	// 规则链ID，用于聚合结果的回调处理
	chainId string
	// 自身节点ID，用于指定聚合结果的传递路径
	selfNodeId string
	// 链上下文，用于获取规则引擎实例
	chainCtx types.ChainCtx
}

// Type 返回组件类型标识
func (x *StreamAggregatorNode) Type() string {
	return "x/streamAggregator"
}

// New 创建流聚合器节点实例
func (x *StreamAggregatorNode) New() types.Node {
	return &StreamAggregatorNode{
		Config: StreamAggregatorNodeConfiguration{},
	}
}

// 错误定义
var (
	ErrAggregatorSQLEmpty     = errors.New("aggregator SQL query is required")
	ErrNotAggregatorQuery     = errors.New("SQL does not contain aggregation functions, use x/streamTransform instead")
	ErrAggregatorSQLExecution = errors.New("failed to execute aggregator SQL")
	ErrAggregatorChainCtxNil  = errors.New("chain context is nil")
	ErrAggregatorNodeIdEmpty  = errors.New("self node id is empty")
	ErrAggregatorChainIdEmpty = errors.New("chain id is empty")
)

// Init 初始化节点
// 该方法在节点被加载时调用，用于验证配置和初始化StreamSQL实例
func (x *StreamAggregatorNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// 验证SQL配置
	if x.Config.SQL == "" {
		return ErrAggregatorSQLEmpty
	}

	// 获取链上下文
	x.chainCtx = base.NodeUtils.GetChainCtx(configuration)
	if x.chainCtx == nil {
		return ErrAggregatorChainCtxNil
	}

	// 获取自身节点ID
	selfDef := base.NodeUtils.GetSelfDefinition(configuration)
	if selfDef.Id == "" {
		return ErrAggregatorNodeIdEmpty
	}
	x.selfNodeId = selfDef.Id

	// 获取规则链ID
	if x.chainCtx.GetNodeId().Id == "" {
		return ErrAggregatorChainIdEmpty
	}
	x.chainId = x.chainCtx.GetNodeId().Id

	// 创建StreamSQL实例
	x.streamsql = streamsql.New()

	// 执行SQL初始化
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAggregatorSQLExecution, err)
	}

	// 验证是否为聚合查询
	if !x.streamsql.IsAggregationQuery() {
		return fmt.Errorf("%w: SQL='%s'", ErrNotAggregatorQuery, x.Config.SQL)
	}

	// 设置聚合结果处理回调
	x.streamsql.AddSink(func(result interface{}) {
		x.handleAggregateResult(result)
	})

	return nil
}

// OnMsg 处理消息
// 支持单条数据和数组数据：
// - 单条数据：直接添加到聚合流中
// - 数组数据：遍历每个元素并逐条添加到聚合流中
// 无论哪种情况，原始消息都会通过Success链继续传递
func (x *StreamAggregatorNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
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
		err := x.processArrayData(data)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("%w: %v", ErrArrayProcessingFailed, err))
			return
		}
	} else {
		// 处理单条数据：直接添加到StreamSQL流中，streamsql会自动处理类型
		x.streamsql.Emit(data)
	}

	// 数据成功加入聚合流，原始消息继续流转
	ctx.TellSuccess(msg)
}

// isArrayData 检查数据是否为数组
func (x *StreamAggregatorNode) isArrayData(data interface{}) bool {
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

// processArrayData 处理数组数据，将每个元素添加到聚合流中
func (x *StreamAggregatorNode) processArrayData(data interface{}) error {
	// 尝试转换为 []interface{}
	var arr []interface{}

	switch v := data.(type) {
	case []interface{}:
		arr = v
	case []map[string]interface{}:
		// 将 []map[string]interface{} 转换为 []interface{}
		arr = make([]interface{}, len(v))
		for i, item := range v {
			arr[i] = item
		}
	default:
		return fmt.Errorf("unsupported array data type: %T", data)
	}

	// 遍历数组，将每个元素添加到聚合流中
	// streamsql内部会自动处理每个元素的类型转换
	for _, item := range arr {
		x.streamsql.Emit(item)
	}

	return nil
}

// handleAggregateResult 处理聚合结果
// 当窗口触发或聚合条件满足时，该方法会被StreamSQL引擎回调
// 聚合结果会被包装成特殊的window_event消息，通过window_event关系链传递到下一个节点
func (x *StreamAggregatorNode) handleAggregateResult(result interface{}) {
	// 创建聚合结果消息的元数据
	metadata := types.NewMetadata()
	metadata.PutValue("queryType", "aggregation")
	metadata.PutValue("resultType", "window_triggered")

	// 通过规则引擎发送聚合结果
	if e, ok := x.chainCtx.GetRuleEnginePool().Get(x.chainId); ok {
		msg := types.NewMsg(0, WindowEventMsgType, types.JSON, metadata, str.ToString(result))
		// 发送聚合结果到下一个节点，使用window_event关系链
		e.OnMsg(msg, types.WithTellNext(x.selfNodeId, RelationTypeWindowEvent))
	}
}

// Destroy 销毁节点，释放资源
// 该方法在节点被卸载时调用，用于清理StreamSQL实例和相关资源
func (x *StreamAggregatorNode) Destroy() {
	if x.streamsql != nil {
		x.streamsql.Stop()
		x.streamsql = nil
	}
}
