/*
 * Copyright 2023 The RuleGo Authors.
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

package redis

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration 节点配置
type ClientNodeConfiguration struct {
	// Server redis服务器地址
	Server string
	// PoolSize 连接池大小
	PoolSize int
	// Cmd 执行命令，例如SET/GET/DEL
	// 可以使用${}占位符读取metadata元数据
	// 支持${msg.data}获取消息负荷，${msg.type}获取消息类型
	Cmd string
	// Params 执行命令参数
	// 可以使用${}占位符读取metadata元数据
	// 支持${msg.data}获取消息负荷，${msg.type}获取消息类型
	Params []interface{}
}

// ClientNode redis客户端节点，
// 成功：转向Success链，redis执行结果存放在msg.Data
// 失败：转向Failure链
type ClientNode struct {
	//节点配置
	Config      ClientNodeConfiguration
	redisClient *redis.Client
}

// Type 返回组件类型
func (x *ClientNode) Type() string {
	return "x/redisClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server: "127.0.0.1:6379",
		Cmd:    "GET",
		Params: []interface{}{"${key}"},
	}}
}

// Init 初始化组件
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		x.redisClient = redis.NewClient(&redis.Options{
			Addr:     x.Config.Server,
			PoolSize: x.Config.PoolSize,
		})
		err = x.redisClient.Ping(context.Background()).Err()
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var data interface{}
	var err error
	metadataCopy := msg.Metadata.Copy()
	metadataCopy.PutValue("msg.data", msg.Data)
	metadataCopy.PutValue("msg.type", msg.Type)

	var args []interface{}
	cmd := str.SprintfDict(x.Config.Cmd, metadataCopy.Values())
	args = append(args, cmd)
	for _, item := range x.Config.Params {
		if itemStr, ok := item.(string); ok {
			args = append(args, str.SprintfDict(itemStr, metadataCopy.Values()))
		} else {
			args = append(args, item)
		}
	}

	//请求redis服务器，并得到返回结果
	data, err = x.redisClient.Do(ctx.GetContext(), args...).Result()

	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Data = str.ToString(data)
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁组件
func (x *ClientNode) Destroy() {
	if x.redisClient != nil {
		_ = x.redisClient.Close()
	}
}
