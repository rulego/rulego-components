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
	"errors"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

func init() {
	_ = rulego.Registry.Register(&QueryNode{})
}

// QueryConfig 定义 OpenGemini 客户端配置
type QueryConfig struct {
	// Server 服务器地址，格式：host:port，多个地址用逗号分隔
	Server string
	// Database 数据库，允许使用 ${} 占位符变量
	Database string
	// 查询语句，允许使用 ${} 占位符变量
	Command string
	// Username 用户名
	Username string
	// Password 密码
	Password string
	// Token 如果token 不为空，则使用token认证，否则使用用户名密码认证
	Token string
}

// QueryNode opengemini 查询节点
type QueryNode struct {
	*WriteNode
	Config          QueryConfig
	commandTemplate str.Template
}

// New 实现 Node 接口，创建新实例
func (x *QueryNode) New() types.Node {
	return &QueryNode{
		Config: QueryConfig{
			Server:   "127.0.0.1:8086",
			Database: "db0",
			Command:  "select * from cpu_load",
		},
	}
}

// Type 实现 Node 接口，返回组件类型
func (x *QueryNode) Type() string {
	return "x/opengeminiQuery"
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
	x.commandTemplate = str.NewTemplate(x.Config.Command)
	return nil
}

// OnMsg 实现 Node 接口，处理消息
func (x *QueryNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	database := x.Config.Database
	command := x.Config.Command
	if !x.databaseTemplate.IsNotVar() || !x.commandTemplate.IsNotVar() {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		database = str.ExecuteTemplate(database, evn)
		command = str.ExecuteTemplate(command, evn)
	}

	q := opengemini.Query{
		Database: database,
		Command:  command,
	}
	if client, err := x.SharedNode.Get(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if res, err := client.Query(q); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.DataType = types.JSON
			msg.Data = str.ToString(res)
			if err := hasError(res); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				ctx.TellSuccess(msg)
			}
		}
	}

}
func hasError(result *opengemini.QueryResult) error {
	if len(result.Error) > 0 {
		return errors.New(result.Error)
	}
	for _, res := range result.Results {
		if len(res.Error) > 0 {
			return errors.New(res.Error)
		}
	}
	return nil
}
