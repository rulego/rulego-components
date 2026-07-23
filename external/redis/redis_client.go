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

// Register the node
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration Redis client node configuration
type ClientNodeConfiguration struct {
	// Server Redis server address, format: host:port
	Server string `json:"server" label:"Server" desc:"Redis server address, e.g. 127.0.0.1:6379" required:"true" ref:"primary"`
	// Password: Redis password
	Password string `json:"password" label:"Password" desc:"Redis password" ref:"shared"`
	// DB Redis database index
	Db int `json:"db" label:"DB" desc:"Redis database index"`
	// PoolSize: Connect the pool size
	PoolSize int `json:"poolSize" label:"Pool Size" desc:"Connection pool size"`
	// cmd Redis command, supports replacing ${metadata.key} and ${data} variables
	Cmd string `json:"cmd" label:"Command" desc:"Redis command, e.g. GET, SET. Supports ${metadata.key} substitution" required:"true"`
	// Params command parameters, supporting ${metadata.key} and ${data} variable replacement
	Params []interface{} `json:"params" label:"Params" desc:"Command parameters. Supports ${metadata.key} substitution"`
}

// ClientNode redis client node,
// Success: Switch to the Success chain, and the Redis execution result is stored in msg.Data
// Failure: Switch to the Failure chain
type ClientNode struct {
	base.SharedNode[*redis.Client]
	//Node configuration
	Config ClientNodeConfiguration
	//Are there variables that need to be replaced?
	hasVar            bool
	paramsExprProgram *vm.Program
	// cmdTemplate command template, used to parse dynamic commands
	cmdTemplate el.Template
	// paramsTemplates: parameter template list, used to parse dynamic parameters
	paramsTemplates []el.Template
}

// Type returns the component type
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

// Init initializes the component
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		// Verify that the cmd field cannot be empty
		if strings.TrimSpace(x.Config.Cmd) == "" {
			return fmt.Errorf("cmd field cannot be empty")
		}

		//Initialize the client
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*redis.Client, error) {
			return x.initClient()
		}, func(client *redis.Client) error {
			// Cleanup callback function
			return client.Close()
		})

		// Build command templates
		if cmdTemplate, err := el.NewTemplate(x.Config.Cmd); err != nil {
			return fmt.Errorf("failed to create cmd template: %w", err)
		} else {
			x.cmdTemplate = cmdTemplate
			if cmdTemplate.HasVar() {
				x.hasVar = true
			}
		}

		// Build parameter templates
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

		// Detect the older version of ParamsExpr configuration
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

// OnMsg processes a message
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var data interface{}
	var err error
	var evn map[string]interface{}

	// Check if you need to build environment variables
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}

	var cmd string
	var args []interface{}

	// Handle commands using the new template system
	if x.cmdTemplate != nil {
		if cmdResult, err := x.cmdTemplate.Execute(evn); err != nil {
			ctx.TellFailure(msg, fmt.Errorf("failed to execute cmd template: %w", err))
			return
		} else {
			cmdStr := str.ToString(cmdResult)
			// Parses command strings, supports the "SET key value" format
			cmdParts := strings.Fields(strings.TrimSpace(cmdStr))
			if len(cmdParts) == 0 {
				ctx.TellFailure(msg, fmt.Errorf("empty command"))
				return
			}
			cmd = strings.ToLower(cmdParts[0])
			// If the command contains parameters, add them to the args
			for _, part := range cmdParts[1:] {
				args = append(args, part)
			}
		}
	} else {
		ctx.TellFailure(msg, fmt.Errorf("cmd template is not initialized"))
		return
	}

	// Build complete Redis command parameters
	var redisArgs []interface{}
	redisArgs = append(redisArgs, cmd)
	redisArgs = append(redisArgs, args...)

	// Parameters are handled using a new template system
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
		// Compatible with the old ParamsExpr
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
		// Static parameters
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
		//hgetall special handles forced, converting the return value to a definite map[string][string] type
		data, err = client.HGetAll(ctx.GetContext(), str.ToString(redisArgs[1])).Result()
	} else {
		//Request the Redis server and receive the return result
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

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "Redis client for executing commands (GET, SET, etc.). cmd and params support ${metadata.key} substitution. Routes to Success/Failure"
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
