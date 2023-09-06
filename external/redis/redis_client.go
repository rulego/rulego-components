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
	"github.com/go-redis/redis"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	DataKey = "$data"
	TypeKey = "$type"
)

// ClientNodeConfiguration 节点配置
type ClientNodeConfiguration struct {
	// Cmd 执行命令，例如SET/GET/DEL
	//可以使用${}占位符读取metadata元数据，其中$data和$type是预留字段，分别代表msg payload和msg type
	Cmd string
	// Params 执行命令参数
	//可以使用${}占位符读取metadata元数据，其中$data和$type是预留字段，分别代表msg payload和msg type
	Params []interface{}
	// PoolSize 连接池大小
	PoolSize int
	// Server redis服务器地址
	Server string
}

//ClientNode redis客户端节点，
//成功：转向Success链，redis执行结果存放在msg.Data
//失败：转向Failure链
type ClientNode struct {
	config      ClientNodeConfiguration
	redisClient *redis.Client
}

// Type 返回组件类型
func (x *ClientNode) Type() string {
	return "x/redisClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{}
}

// Init 初始化组件
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.config)
	if err == nil {
		x.redisClient = redis.NewClient(&redis.Options{
			Addr:     x.config.Server,
			PoolSize: x.config.PoolSize,
		})
		err = x.redisClient.Ping().Err()
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) error {
	var data interface{}
	var err error

	var args []interface{}
	cmd := x.SprintfDict(x.config.Cmd, msg)
	args = append(args, cmd)
	for _, item := range x.config.Params {
		if itemStr, ok := item.(string); ok {
			args = append(args, x.SprintfDict(itemStr, msg))
		} else {
			args = append(args, item)
		}
	}

	//请求redis服务器，并得到返回结果
	data, err = x.redisClient.Do(args...).Result()

	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Data = str.ToString(data)
		ctx.TellSuccess(msg)
	}
	return err
}

// Destroy 销毁组件
func (x *ClientNode) Destroy() {
	if x.redisClient != nil {
		_ = x.redisClient.Close()
	}
}

//SprintfDict 执行占位符变量替换
//${}占位符读取metadata元数据，其中$data和$type是预留字段，分别代表msg payload和msg type
func (x *ClientNode) SprintfDict(value string, msg types.RuleMsg) string {
	if value == DataKey {
		return msg.Data
	} else if value == TypeKey {
		return msg.Type
	} else {
		return str.SprintfDict(value, msg.Metadata.Values())
	}
}
