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

// RelationTypeStreamEvent 流事件关系类型：聚合窗口触发或 CEP 模式命中的统一结果出口。
const RelationTypeStreamEvent = "stream_event"

// StreamEventMsgType 流事件消息类型，用于标识聚合/CEP 结果消息。
const StreamEventMsgType = "stream_event"

func init() {
	_ = rulego.Registry.Register(&StreamAggregatorNode{})
}

// StreamAggregatorNodeConfiguration 流聚合器节点配置
type StreamAggregatorNodeConfiguration struct {
	// SQL is the aggregation query statement (must contain GROUP BY, aggregation or window functions).
	// Example: SELECT AVG(temperature) as avg_temp FROM stream GROUP BY TumblingWindow('5s')
	SQL string `json:"sql" label:"SQL" desc:"Aggregation SQL query. Must contain GROUP BY/window functions. Example: SELECT AVG(temperature) FROM stream GROUP BY TumblingWindow('5s')" required:"true"`

	// Tables is the optional list of metadata tables for stream-table JOIN.
	// Each table is loaded at Init (inline/file/http), registered for JOIN, and
	// optionally refreshed. JOIN works with both transform and aggregation/window
	// queries. See TableConfig.
	Tables []TableConfig `json:"tables"`
}

// StreamAggregatorNode 流聚合器节点
//
// 功能说明：
// - 处理聚合查询（窗口聚合、分组聚合、统计计算）或 CEP(MATCH_RECOGNIZE) 模式识别
// - 支持单条数据和数组数据输入，数组数据会被逐条添加到流中
// - 结果（聚合窗口触发 / CEP 模式命中）通过 `stream_event` 关系链传递，而不是普通的 Success 链
// - 原始输入数据（无论单条还是数组）都会通过 `Success` 链继续传递，保持数据流的连续性
//
// 数据流向：
// - 输入数据 -> 添加到流 -> 原始数据通过 Success 链传递
// - 聚合/CEP 触发 -> 结果通过 stream_event 链传递
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
	// tables 管理流-表 JOIN 的元数据表（加载/注册/刷新），Destroy 时关闭
	tables *tableManager
	// 规则链ID，用于聚合结果的回调处理
	chainId string
	// 自身节点ID，用于指定聚合结果的传递路径
	selfNodeId string
	// 链上下文，用于获取规则引擎实例
	chainCtx types.ChainCtx
	// isCEP 标记当前是否为 MATCH_RECOGNIZE(CEP) 查询，决定结果消息的 queryType
	isCEP bool
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
func (x *StreamAggregatorNode) Init(ruleConfig types.Config, configuration types.Configuration) (err error) {
	err = maps.Map2Struct(configuration, &x.Config)
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

	// 创建StreamSQL实例，日志接入 rulego 日志体系
	x.streamsql = streamsql.New(streamsql.WithLogger(newRulegoLogger(ruleConfig.Logger)))

	// 执行SQL初始化
	err = x.streamsql.Execute(x.Config.SQL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAggregatorSQLExecution, err)
	}
	// Init 失败时停止已启动的 streamsql 实例
	defer func() {
		if err != nil {
			x.streamsql.Stop()
		}
	}()

	// 验证是否为聚合或 CEP(MATCH_RECOGNIZE) 查询；两者都走异步 Emit+sink 管道
	x.isCEP = x.streamsql.IsCEPQuery()
	if !x.streamsql.IsAggregationQuery() && !x.isCEP {
		return fmt.Errorf("%w: SQL='%s'", ErrNotAggregatorQuery, x.Config.SQL)
	}

	// 设置聚合结果处理回调
	x.streamsql.AddSink(func(results []map[string]interface{}) {
		x.handleAggregateResult(results)
	})

	// 加载元数据表（流-表 JOIN，须在 Execute 之后）。任一表失败则关闭已启动的
	// 刷新 goroutine，避免泄漏。
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
		// 处理单条数据：转换为map[string]interface{}类型后添加到StreamSQL流中
		mapData, err := x.convertToMapStringInterface(data)
		if err != nil {
			ctx.TellFailure(msg, fmt.Errorf("data type conversion failed: %w", err))
			return
		}
		x.streamsql.Emit(mapData)
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
	// 需要将每个元素转换为map[string]interface{}类型
	for _, item := range arr {
		mapItem, err := x.convertToMapStringInterface(item)
		if err != nil {
			return fmt.Errorf("array element type conversion failed: %w", err)
		}
		x.streamsql.Emit(mapItem)
	}

	return nil
}

// handleAggregateResult 处理聚合/CEP 结果
// 当窗口触发、聚合条件满足或 CEP 模式命中时，该方法会被 StreamSQL 引擎回调
// 结果会被包装成 stream_event 消息，通过 stream_event 关系链传递到下一个节点
func (x *StreamAggregatorNode) handleAggregateResult(results []map[string]interface{}) {
	// 创建结果消息的元数据
	metadata := types.NewMetadata()
	if x.isCEP {
		metadata.PutValue("queryType", "cep")
		metadata.PutValue("resultType", "pattern_matched")
	} else {
		metadata.PutValue("queryType", "aggregation")
		metadata.PutValue("resultType", "window_triggered")
	}

	// 通过规则引擎发送结果
	if e, ok := x.chainCtx.GetRuleEnginePool().Get(x.chainId); ok {
		msg := types.NewMsg(0, StreamEventMsgType, types.JSON, metadata, str.ToString(results))
		// 结果经 stream_event 关系链传递（聚合窗口触发或 CEP 模式命中）
		e.OnMsg(msg, types.WithTellNext(x.selfNodeId, RelationTypeStreamEvent))
	}
}

// convertToMapStringInterface 将不同类型的map转换为map[string]interface{}
// 支持的类型包括：map[string]interface{}, map[string]string
func (x *StreamAggregatorNode) convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
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
