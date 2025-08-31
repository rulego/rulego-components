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
	"strings"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/redis/go-redis/v9"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration Redis客户端节点配置
type ClientNodeConfiguration struct {
	// Server Redis服务器地址，格式：host:port
	Server string `json:"server"`
	// Password Redis密码
	Password string `json:"password"`
	// DB Redis数据库索引
	Db int `json:"db"`
	// PoolSize 连接池大小
	PoolSize int `json:"poolSize"`
	// Cmd Redis命令，支持${metadata.key}和${data}变量替换，也支持命令和参数一起提供（如"SET key value"）
	Cmd string `json:"cmd"`
	// Params 命令参数，支持${metadata.key}和${data}变量替换
	Params []interface{} `json:"params"`
}

// ClientNode redis客户端节点，
// 成功：转向Success链，redis执行结果存放在msg.Data
// 失败：转向Failure链
type ClientNode struct {
	base.SharedNode[*redis.Client]
	//节点配置
	Config ClientNodeConfiguration
	//是否有变量需要替换
	hasVar            bool
	paramsExprProgram *vm.Program
	// cmdTemplate 命令模板，用于解析动态命令
	cmdTemplate el.Template
	// paramsTemplates 参数模板列表，用于解析动态参数
	paramsTemplates []el.Template
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
		// 验证cmd字段不能为空
		if strings.TrimSpace(x.Config.Cmd) == "" {
			return fmt.Errorf("cmd field cannot be empty")
		}

		//初始化客户端
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*redis.Client, error) {
			return x.initClient()
		}, func(client *redis.Client) error {
			// 清理回调函数
			return client.Close()
		})

		// 构建命令模板
		if cmdTemplate, err := el.NewTemplate(x.Config.Cmd); err != nil {
			return fmt.Errorf("failed to create cmd template: %w", err)
		} else {
			x.cmdTemplate = cmdTemplate
			if cmdTemplate.HasVar() {
				x.hasVar = true
			}
		}

		// 构建参数模板
		if len(x.Config.Params) > 0 {
			x.paramsTemplates = make([]el.Template, len(x.Config.Params))
			for i, param := range x.Config.Params {
				if param == nil {
					return fmt.Errorf("param at index %d is nil", i)
				}
				if paramTemplate, err := el.NewTemplate(param); err != nil {
					return fmt.Errorf("failed to create param template at index %d: %w", i, err)
				} else {
					x.paramsTemplates[i] = paramTemplate
					if paramTemplate.HasVar() {
						x.hasVar = true
					}
				}
			}
		}

		// 检测旧版本ParamsExpr配置
		if paramsExprValue, exists := configuration["paramsExpr"]; exists {
			if exprV := strings.TrimSpace(fmt.Sprintf("%v", paramsExprValue)); exprV != "" {
				if program, err := expr.Compile(exprV, expr.AllowUndefinedVariables()); err != nil {
					return fmt.Errorf("failed to compile paramsExpr: %w", err)
				} else {
					x.paramsExprProgram = program
					x.hasVar = true
				}
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

	// 检查是否需要构建环境变量
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}

	var cmd string
	var args []interface{}

	// 使用新的模板系统处理命令
	if x.cmdTemplate != nil {
		if cmdResult, err := x.cmdTemplate.Execute(evn); err != nil {
			ctx.TellFailure(msg, fmt.Errorf("failed to execute cmd template: %w", err))
			return
		} else {
			cmdStr := str.ToString(cmdResult)
			// 解析命令字符串，支持"SET key value"格式
			cmdParts := strings.Fields(strings.TrimSpace(cmdStr))
			if len(cmdParts) == 0 {
				ctx.TellFailure(msg, fmt.Errorf("empty command"))
				return
			}
			cmd = strings.ToLower(cmdParts[0])
			// 如果命令包含参数，将其添加到args中
			for _, part := range cmdParts[1:] {
				args = append(args, part)
			}
		}
	} else {
		ctx.TellFailure(msg, fmt.Errorf("cmd template is not initialized"))
		return
	}

	// 构建完整的Redis命令参数
	var redisArgs []interface{}
	redisArgs = append(redisArgs, cmd)
	redisArgs = append(redisArgs, args...)

	// 使用新的模板系统处理参数
	if len(x.paramsTemplates) > 0 {
		for _, paramTemplate := range x.paramsTemplates {
			if paramResult, err := paramTemplate.Execute(evn); err != nil {
				ctx.TellFailure(msg, fmt.Errorf("failed to execute param template: %w", err))
				return
			} else {
				redisArgs = append(redisArgs, paramResult)
			}
		}
	} else if x.paramsExprProgram != nil {
		// 兼容旧的ParamsExpr
		var exprVm = vm.VM{}
		if out, err := exprVm.Run(x.paramsExprProgram, evn); err != nil {
			ctx.TellFailure(msg, err)
			return
		} else {
			if v, ok := out.([]interface{}); ok {
				redisArgs = append(redisArgs, v...)
			} else {
				redisArgs = append(redisArgs, out)
			}
		}
	} else {
		// 静态参数
		redisArgs = append(redisArgs, x.Config.Params...)
	}

	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	if cmd == "hgetall" {
		if len(redisArgs) < 2 {
			ctx.TellFailure(msg, fmt.Errorf("hgetall need one param"))
			return
		}
		//hgetall特殊处理强制，返回值转换成确定的map[string][string]类型
		data, err = client.HGetAll(ctx.GetContext(), str.ToString(redisArgs[1])).Result()
	} else {
		//请求redis服务器，并得到返回结果
		data, err = client.Do(ctx.GetContext(), redisArgs...).Result()
	}

	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.SetData(str.ToString(data))
		ctx.TellSuccess(msg)
	}
}

func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

func (x *ClientNode) initClient() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     x.Config.Server,
		PoolSize: x.Config.PoolSize,
		DB:       x.Config.Db,
		Password: x.Config.Password,
	})
	err := client.Ping(context.Background()).Err()
	return client, err
}
