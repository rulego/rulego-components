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
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/json"
	"github.com/rulego/rulego/utils/maps"
	"strconv"
	"strings"
	"time"
)

// WriteConfig 定义 OpenGemini 客户端配置
type WriteConfig struct {
	// Server provided server address host:port.
	Server string
	// Database provided database name.
	Database string
	// Username provided username when used opengemini.AuthTypePassword.
	Username string
	// Password provided password when used opengemini.AuthTypePassword.
	Password string
	// Token provided token when used opengemini.AuthTypeToken.
	Token string
}

// WriteNode opengemini 写节点
type WriteNode struct {
	base.SharedNode[opengemini.Client]
	client opengemini.Client
	Config WriteConfig
}

// New 实现 Node 接口，创建新实例
func (x *WriteNode) New() types.Node {
	return &WriteNode{}
}

// Type 实现 Node 接口，返回组件类型
func (x *WriteNode) Type() string {
	return "opengeminiWrite"
}

// Init 初始化 OpenGemini 客户端
func (x *WriteNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	_ = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (opengemini.Client, error) {
		return x.initClient()
	})

	return nil
}

// OnMsg 实现 Node 接口，处理消息
func (x *WriteNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {

	if client, err := x.SharedNode.GetInstance(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if msg.DataType == types.JSON {
			var point opengemini.Point
			if err := json.Unmarshal([]byte(msg.Data), &point); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				if point.Time.UnixMilli() == 0 {
					point.Time = time.Now()
				}
				err = client.WritePoint(x.Config.Database, &point, func(err error) {
					if err != nil {
						ctx.TellFailure(msg, err)
					} else {
						ctx.TellSuccess(msg)
					}
				})
			}
		} else {
			//解析 Line Protocol
			if points, err := parseMultiLineProtocol(msg.Data); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				if err = client.WriteBatchPoints(context.Background(), x.Config.Database, points); err != nil {
					ctx.TellFailure(msg, err)
				} else {
					ctx.TellSuccess(msg)
				}
			}
		}
	}
}

// Destroy 清理资源
func (x *WriteNode) Destroy() {
	if x.client != nil {
		_ = x.client.Close()
	}
}

func (x *WriteNode) GetInstance() (interface{}, error) {
	return x.SharedNode.GetInstance()
}

// initClient 初始化客户端
func (x *WriteNode) initClient() (opengemini.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else if x.client == nil && x.TryConnect() {
		var err error
		servers := strings.Split(x.Config.Server, ",")
		var addresses []*opengemini.Address
		for _, server := range servers {
			addr := strings.Split(server, ":")
			if len(addr) < 2 {
				return nil, fmt.Errorf("must host:port format")
			}
			host := addr[0]
			if port, err := strconv.ParseInt(addr[1], 10, 64); err != nil {
				return nil, err
			} else {
				addresses = append(addresses, &opengemini.Address{
					Host: host,
					Port: int(port),
				})
			}
		}
		var authConfig opengemini.AuthConfig
		if x.Config.Token != "" {
			authConfig.AuthType = opengemini.AuthTypeToken
		} else {
			authConfig.AuthType = opengemini.AuthTypePassword
			authConfig.Username = x.Config.Username
			authConfig.Password = x.Config.Password
		}
		config := opengemini.Config{
			Addresses:  addresses,
			AuthConfig: &authConfig,
		}
		// 创建 OpenGemini 客户端
		x.client, err = opengemini.NewClient(&config)
		return x.client, err
	} else {
		return nil, base.ErrClientNotInit
	}
}
