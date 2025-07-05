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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/json"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

func init() {
	_ = rulego.Registry.Register(&WriteNode{})
}

// WriteConfig 定义 OpenGemini 客户端配置
type WriteConfig struct {
	// Server 服务器地址，格式：host:port，多个地址用逗号分隔
	Server string
	// Database 数据库，允许使用 ${} 占位符变量
	Database string
	// Username 用户名
	Username string
	// Password 密码
	Password string
	// Token 如果 Token 不为空，使用 opengemini.AuthTypeToken 认证
	Token string
}

// WriteNode opengemini 写节点
type WriteNode struct {
	base.SharedNode[opengemini.Client]
	Config           WriteConfig
	opengeminiConfig *opengemini.Config
	databaseTemplate str.Template
}

// New 实现 Node 接口，创建新实例
func (x *WriteNode) New() types.Node {
	return &WriteNode{
		Config: WriteConfig{
			Server:   "127.0.0.1:8086",
			Database: "db0",
		},
	}
}

// Type 实现 Node 接口，返回组件类型
func (x *WriteNode) Type() string {
	return "x/opengeminiWrite"
}

// Init 初始化 OpenGemini 客户端
func (x *WriteNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if opengeminiConfig, err := x.createOpengeminiConfig(); err != nil {
		return err
	} else {
		x.opengeminiConfig = opengeminiConfig
	}
	x.databaseTemplate = str.NewTemplate(x.Config.Database)
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (opengemini.Client, error) {
		return x.initClient()
	}, func(client opengemini.Client) error {
		return client.Close()
	})
	return nil
}

// OnMsg 实现 Node 接口，处理消息
func (x *WriteNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {

	if client, err := x.SharedNode.GetSafely(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		database := x.databaseTemplate.ExecuteFn(func() map[string]any {
			return base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		})
		var points []*opengemini.Point
		if msg.DataType == types.JSON {
			var point opengemini.Point
			//首先解析是否是多条
			if err := json.Unmarshal([]byte(msg.GetData()), &points); err != nil {
				//如果不是数组，则解析为单条
				if err := json.Unmarshal([]byte(msg.GetData()), &point); err != nil {
					ctx.TellFailure(msg, err)
					return
				} else {
					points = append(points, &point)
				}
			}
		} else {
			//解析 Line Protocol
			if points, err = parseMultiLineProtocol(msg.GetData()); err != nil {
				ctx.TellFailure(msg, err)
				return
			}
		}
		for _, point := range points {
			if point.Time.IsZero() {
				point.Time = time.Now()
			}
		}
		if err = client.WriteBatchPoints(context.Background(), database, points); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			ctx.TellSuccess(msg)
		}
	}
}

func (x *WriteNode) GetInstance() (interface{}, error) {
	return x.SharedNode.GetInstance()
}

func (x *WriteNode) Destroy() {
	_ = x.SharedNode.Close()
}

// initClient 初始化客户端
func (x *WriteNode) initClient() (opengemini.Client, error) {
	// 创建 OpenGemini 客户端
	return opengemini.NewClient(x.opengeminiConfig)
}

func (x *WriteNode) createOpengeminiConfig() (*opengemini.Config, error) {
	var addresses []opengemini.Address
	servers := strings.Split(x.Config.Server, ",")
	for _, server := range servers {
		addr := strings.Split(server, ":")
		if len(addr) < 2 {
			return nil, fmt.Errorf("must host:port format")
		}
		host := addr[0]
		if port, err := strconv.ParseInt(addr[1], 10, 64); err != nil {
			return nil, err
		} else {
			addresses = append(addresses, opengemini.Address{
				Host: host,
				Port: int(port),
			})
		}
	}
	config := opengemini.Config{
		Addresses: addresses,
	}
	var authConfig opengemini.AuthConfig
	if x.Config.Token != "" {
		authConfig.AuthType = opengemini.AuthTypeToken
		authConfig.Token = x.Config.Token
		config.AuthConfig = &authConfig
	} else if x.Config.Username != "" {
		authConfig.AuthType = opengemini.AuthTypePassword
		authConfig.Username = x.Config.Username
		authConfig.Password = x.Config.Password
		config.AuthConfig = &authConfig
	}

	return &config, nil
}
