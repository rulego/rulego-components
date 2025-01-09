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
package wukongim

import (
	"time"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/WuKongIM/WuKongIMGoSDK/pkg/wksdk"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&WukongimSender{})
}

// ClientNodeConfiguration 节点配置
type WukongimSenderConfiguration struct {
	// 服务器地址
	Server string
	// 用户UID
	UID string
	// 登录密码
	Token string
	// 连接超时
	ConnectTimeout int64
	// Proto版本
	ProtoVersion int
	// 心跳间隔
	PingInterval int64
	// 是否自动重连
	Reconnect bool
	// 频道ID
	ChannelID string `json:"channel_id"`
	// 频道类型
	ChannelType uint8 `json:"channel_type"`
	// 是否持久化，默认 false
	NoPersist bool
	// 是否同步一次(写模式)，默认 false
	SyncOnce bool
	// 是否显示红点，默认true
	RedDot bool
	// 是否需要加密，默认false
	NoEncrypt bool
}

// WukongimSender wksdk.Client客户端节点，
// 成功：转向Success链，发送消息执行结果存放在msg.Data
// 失败：转向Failure链
type WukongimSender struct {
	base.SharedNode[*wksdk.Client]
	//节点配置
	Config WukongimSenderConfiguration
	client *wksdk.Client
}

// Type 返回组件类型
func (x *WukongimSender) Type() string {
	return "x/wukongimSender"
}

func (x *WukongimSender) New() types.Node {
	return &WukongimSender{Config: WukongimSenderConfiguration{
		Server:         "tcp://127.0.0.1:5100",
		UID:            "test1",
		Token:          "test1",
		ConnectTimeout: 5,
		ProtoVersion:   wkproto.LatestVersion,
		PingInterval:   30,
		Reconnect:      true,
		ChannelID:      "test2",
		ChannelType:    wkproto.ChannelTypePerson,
		NoPersist:      false,
		SyncOnce:       false,
		RedDot:         true,
		NoEncrypt:      false,
	}}
}

// Init 初始化组件
func (x *WukongimSender) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//初始化客户端
		err = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*wksdk.Client, error) {
			return x.initClient()
		})
	}
	return err
}

// OnMsg 处理消息
func (x *WukongimSender) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	client, err := x.SharedNode.Get()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	packet, err := client.SendMessage([]byte(msg.Data),
		wkproto.Channel{
			ChannelType: x.Config.ChannelType,
			ChannelID:   x.Config.ChannelID,
		},
		wksdk.SendOptionWithNoPersist(x.Config.NoPersist),
		wksdk.SendOptionWithSyncOnce(x.Config.SyncOnce),
		wksdk.SendOptionWithRedDot(x.Config.RedDot),
		wksdk.SendOptionWithNoEncrypt(x.Config.NoEncrypt))
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.Data = str.ToString(packet)
		ctx.TellSuccess(msg)
	}
}

// Destroy 销毁组件
func (x *WukongimSender) Destroy() {
	if x.client != nil {
		_ = x.client.Disconnect()
	}
}

func (x *WukongimSender) initClient() (*wksdk.Client, error) {
	if x.client != nil {
		return x.client, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.client != nil {
			return x.client, nil
		}
		x.client = wksdk.NewClient(x.Config.Server,
			wksdk.WithConnectTimeout(time.Duration(x.Config.ConnectTimeout)*time.Second),
			wksdk.WithProtoVersion(x.Config.ProtoVersion),
			wksdk.WithUID(x.Config.UID),
			wksdk.WithToken(x.Config.Token),
			wksdk.WithPingInterval(time.Duration(x.Config.PingInterval)*time.Second),
			wksdk.WithReconnect(x.Config.Reconnect),
		)
		err := x.client.Connect()
		return x.client, err
	}
}
