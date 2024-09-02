/*
 * Copyright 2024 The RuleGo Authors.
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

package opengemini

import (
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// QueryConfig 定义 OpenGemini 客户端配置
type QueryConfig struct {
	WriteConfig
	Command string
}

// QueryNode opengemini 查询节点
type QueryNode struct {
	*WriteNode
	Config QueryConfig
}

// New 实现 Node 接口，创建新实例
func (x *QueryNode) New() types.Node {
	return &QueryNode{}
}

// Type 实现 Node 接口，返回组件类型
func (x *QueryNode) Type() string {
	return "opengeminiQuery"
}

// Init 初始化 OpenGemini 客户端
func (x *QueryNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	x.WriteNode = &WriteNode{}
	if err = x.WriteNode.Init(ruleConfig, configuration); err != nil {
		return err
	}
	return nil
}

// OnMsg 实现 Node 接口，处理消息
func (x *QueryNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	q := opengemini.Query{
		Database: x.Config.Database,
		Command:  x.Config.Command,
	}
	if client, err := x.SharedNode.GetInstance(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if res, err := client.Query(q); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.DataType = types.JSON
			msg.Data = str.ToString(res)
			ctx.TellSuccess(msg)
		}
	}

}
