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
	"strconv"
	"time"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/WuKongIM/WuKongIMGoSDK/pkg/wksdk"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&WukongimSender{})
}

// WukongimSenderConfiguration 节点配置
type WukongimSenderConfiguration struct {
	// 服务器地址
	Server string
	// 用户UID
	UID string
	// 登录密码
	Token string
	// 频道类型 允许使用 ${} 占位符变量
	//	ChannelTypePerson uint8 = 1 // 个人频道
	//	ChannelTypeGroup           uint8 = 2 // 群组频道
	//	ChannelTypeCustomerService uint8 = 3 // 客服频道
	//	ChannelTypeCommunity       uint8 = 4 // 社区频道
	//	ChannelTypeCommunityTopic  uint8 = 5 // 社区话题频道
	//	ChannelTypeInfo            uint8 = 6 // 资讯频道（有临时订阅者的概念，查看资讯的时候加入临时订阅，退出资讯的时候退出临时订阅）
	//	ChannelTypeData            uint8 = 7 // 数据频道
	ChannelType string
	// 频道ID 允许使用 ${} 占位符变量
	// 如果 ChannelType=1 则填写用户：UID
	ChannelID string
	// 连接超时，单位秒
	ConnectTimeout int64
	// Proto版本
	ProtoVersion int
	// 心跳间隔，单位秒
	PingInterval int64
	// 是否自动重连
	Reconnect bool
	// 是否自动确认消息
	AutoAck bool
	// 是否不存储，默认 false
	NoPersist bool
	// 是否同步一次(写模式)，默认 false
	SyncOnce bool
	// 是否显示红点，默认true
	RedDot bool
	// 是否不需要加密，默认false
	NoEncrypt bool
}

// WukongimSender wksdk.Client客户端节点，
// 成功：转向Success链，发送消息执行结果存放在msg.Data
// 失败：转向Failure链
type WukongimSender struct {
	base.SharedNode[*wksdk.Client]
	//节点配置
	Config              WukongimSenderConfiguration
	channelIdTemplate   el.Template
	channelTypeTemplate el.Template
	hasVar              bool
}

// Type 返回组件类型
func (x *WukongimSender) Type() string {
	return "x/wukongimSender"
}

func (x *WukongimSender) New() types.Node {
	return &WukongimSender{Config: WukongimSenderConfiguration{
		Server:         "tcp://175.27.245.108:15100",
		UID:            "test1",
		Token:          "test1",
		ConnectTimeout: 5,
		ProtoVersion:   wkproto.LatestVersion,
		PingInterval:   30,
		Reconnect:      true,
		AutoAck:        true,
		ChannelType:    "1",
		ChannelID:      "test2",
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
		err = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*wksdk.Client, error) {
			return x.initClient()
		}, func(client *wksdk.Client) error {
			// 清理回调函数
			return client.Disconnect()
		})
	}
	// 初始化频道ID模板
	channelIdTemplate, err := el.NewTemplate(x.Config.ChannelID)
	if err != nil {
		return err
	}
	x.channelIdTemplate = channelIdTemplate

	// 初始化频道类型模板
	channelTypeTemplate, err := el.NewTemplate(x.Config.ChannelType)
	if err != nil {
		return err
	}
	x.channelTypeTemplate = channelTypeTemplate

	// 设置统一的 hasVar 变量
	x.hasVar = channelIdTemplate.HasVar() || channelTypeTemplate.HasVar()
	return nil
}

// OnMsg 处理消息
func (x *WukongimSender) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	ctype := x.Config.ChannelType
	cid := x.Config.ChannelID
	var utype uint64 = 1
	var err error
	if x.hasVar {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		ctype = x.channelTypeTemplate.ExecuteAsString(evn)
		cid = x.channelIdTemplate.ExecuteAsString(evn)
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	utype, err = strconv.ParseUint(ctype, 10, 8)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	packet, err := client.SendMessage([]byte(msg.GetData()),
		wkproto.Channel{
			ChannelType: uint8(utype),
			ChannelID:   cid,
		},
		wksdk.SendOptionWithNoPersist(x.Config.NoPersist),
		wksdk.SendOptionWithSyncOnce(x.Config.SyncOnce),
		wksdk.SendOptionWithRedDot(x.Config.RedDot),
		wksdk.SendOptionWithNoEncrypt(x.Config.NoEncrypt))
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		msg.SetData(str.ToString(packet))
		ctx.TellSuccess(msg)
	}
}

func (x *WukongimSender) Destroy() {
	_ = x.SharedNode.Close()
}

func (x *WukongimSender) initClient() (*wksdk.Client, error) {
	client := wksdk.NewClient(x.Config.Server,
		wksdk.WithConnectTimeout(time.Duration(x.Config.ConnectTimeout)*time.Second),
		wksdk.WithProtoVersion(x.Config.ProtoVersion),
		wksdk.WithUID(x.Config.UID),
		wksdk.WithToken(x.Config.Token),
		wksdk.WithPingInterval(time.Duration(x.Config.PingInterval)*time.Second),
		wksdk.WithReconnect(x.Config.Reconnect),
		wksdk.WithAutoAck(x.Config.AutoAck),
	)
	err := client.Connect()
	return client, err
}
