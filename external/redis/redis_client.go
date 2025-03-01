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
	"fmt"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"strings"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration 节点配置
type ClientNodeConfiguration struct {
	// Server redis服务器地址
	Server string
	// Password 密码
	Password string
	// PoolSize 连接池大小
	PoolSize int
	// Db 数据库index
	Db int
	// Cmd 执行命令，例如SET/GET/DEL
	// 支持${metadata.key}占位符读取metadata元数据
	// 支持${msg.key}占位符读取消息负荷指定key数据
	// 支持${data}获取消息原始负荷
	Cmd string
	// ParamsExpr 动态参数表达式。ParamsExpr和Params同时存在则优先使用ParamsExpr
	ParamsExpr string
	// Params 执行命令参数
	// 支持${metadata.key}占位符读取metadata元数据
	// 支持${msg.key}占位符读取消息负荷指定key数据
	// 支持${data}获取消息原始负荷
	Params []interface{}
}

// ClientNode redis客户端节点，
// 成功：转向Success链，redis执行结果存放在msg.Data
// 失败：转向Failure链
type ClientNode struct {
	base.SharedNode[*redis.Client]
	//节点配置
	Config ClientNodeConfiguration
	client *redis.Client
	//cmd是否有变量
	cmdHasVar bool
	//参数是否有变量
	paramsHasVar      bool
	paramsExprProgram *vm.Program
}

// Type 返回组件类型
func (x *ClientNode) Type() string {
	return "x/redisClient"
}

func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server: "127.0.0.1:6379",
		Cmd:    "GET",
		Params: []interface{}{"${metadata.key}"},
		Db:     0,
	}}
}

// Init 初始化组件
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//初始化客户端
		_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*redis.Client, error) {
			return x.initClient()
		})

		if str.CheckHasVar(x.Config.Cmd) {
			x.cmdHasVar = true
		}
		//检查是参数否有变量
		for _, item := range x.Config.Params {
			if v, ok := item.(string); ok && str.CheckHasVar(v) {
				x.paramsHasVar = true
				break
			}
		}

		if exprV := strings.TrimSpace(x.Config.ParamsExpr); exprV != "" {
			if program, err := expr.Compile(exprV, expr.AllowUndefinedVariables()); err != nil {
				return err
			} else {
				x.paramsExprProgram = program
			}
		}
	}
	return err
}

// OnMsg 处理消息
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var data interface{}
	var err error
	var evn map[string]interface{}
	if x.cmdHasVar || x.paramsHasVar || x.paramsExprProgram != nil {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	var cmd = x.Config.Cmd
	if x.cmdHasVar {
		cmd = str.ExecuteTemplate(x.Config.Cmd, evn)
	}
	cmd = strings.ToLower(strings.TrimSpace(cmd))

	var args []interface{}
	args = append(args, cmd)
	if x.paramsExprProgram != nil {
		var exprVm = vm.VM{}
		if out, err := exprVm.Run(x.paramsExprProgram, evn); err != nil {
			ctx.TellFailure(msg, err)
			return
		} else {
			if v, ok := out.([]interface{}); ok {
				args = append(args, v...)
			} else {
				args = append(args, out)
			}
		}
	} else if x.paramsHasVar {
		for _, item := range x.Config.Params {
			if itemStr, ok := item.(string); ok {
				args = append(args, str.ExecuteTemplate(itemStr, evn))
			} else {
				args = append(args, item)
			}
		}
	} else {
		args = append(args, x.Config.Params...)
	}

	client, err := x.SharedNode.Get()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if cmd == "hgetall" {
		if len(args) < 2 {
			ctx.TellFailure(msg, fmt.Errorf("hgetall need one param"))
			return
		}
		//hgetall特殊处理强制，返回值转换成确定的map[string][string]类型
		data, err = client.HGetAll(ctx.GetContext(), str.ToString(args[1])).Result()
	} else {
		//请求redis服务器，并得到返回结果
		data, err = client.Do(ctx.GetContext(), args...).Result()
	}

	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Data = str.ToString(data)
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁组件
func (x *ClientNode) Destroy() {
	if x.client != nil {
		_ = x.client.Close()
	}
}

func (x *ClientNode) initClient() (*redis.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		x.client = redis.NewClient(&redis.Options{
			Addr:     x.Config.Server,
			PoolSize: x.Config.PoolSize,
			DB:       x.Config.Db,
			Password: x.Config.Password,
		})
		return x.client, x.client.Ping(context.Background()).Err()
	}
}
