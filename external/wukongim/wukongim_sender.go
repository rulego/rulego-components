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

// Register the node
func init() {
	_ = rulego.Registry.Register(&WukongimSender{})
}

// WukongimSenderConfiguration node configuration
type WukongimSenderConfiguration struct {
	Server         string `json:"server" label:"Server" desc:"WuKongIM server address, format: tcp://host:port" required:"true" ref:"primary"`
	UID            string `json:"uid" label:"UID" desc:"Login user UID" required:"true"`
	Token          string `json:"token" label:"Token" desc:"Login authentication token" required:"true" ref:"shared"`
	ChannelType    string `json:"channelType" label:"Channel Type" desc:"Channel type: 1(personal), 2(group), 3(customer service)"`
	ChannelID      string `json:"channelId" label:"Channel ID" desc:"Target channel ID, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	ConnectTimeout int64  `json:"connectTimeout" label:"Connect Timeout (s)" desc:"Connection timeout in seconds"`
	ProtoVersion   int    `json:"protoVersion" label:"Proto Version" desc:"Communication protocol version"`
	PingInterval   int64  `json:"pingInterval" label:"Ping Interval (s)" desc:"Heartbeat interval in seconds"`
	Reconnect      bool   `json:"reconnect" label:"Reconnect" desc:"Auto-reconnect on disconnect"`
	AutoAck        bool   `json:"autoAck" label:"Auto Ack" desc:"Auto-acknowledge messages"`
	NoPersist      bool   `json:"noPersist" label:"No Persist" desc:"Do not persist messages"`
	SyncOnce       bool   `json:"syncOnce" label:"Sync Once" desc:"Sync send, only online users receive"`
	RedDot         bool   `json:"redDot" label:"Red Dot" desc:"Show red dot on receiver side"`
	NoEncrypt      bool   `json:"noEncrypt" label:"No Encrypt" desc:"Do not encrypt messages"`
}

// WukongimSender wksdk.Client client node,
// Success: Switch to the Success chain, send the message execution result, and store it in msg.Data
// Failure: Switch to the Failure chain
type WukongimSender struct {
	base.SharedNode[*wksdk.Client]
	//Node configuration
	Config              WukongimSenderConfiguration
	channelIdTemplate   el.Template
	channelTypeTemplate el.Template
	hasVar              bool
}

// Type returns the component type
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

// Init initializes the component
func (x *WukongimSender) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//Initialize the client
		err = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*wksdk.Client, error) {
			return x.initClient()
		}, func(client *wksdk.Client) error {
			// Cleanup callback function
			return client.Disconnect()
		})
	}
	// Initialize the channel ID template
	channelIdTemplate, err := el.NewTemplate(x.Config.ChannelID)
	if err != nil {
		return err
	}
	x.channelIdTemplate = channelIdTemplate

	// Initialize the channel type template
	channelTypeTemplate, err := el.NewTemplate(x.Config.ChannelType)
	if err != nil {
		return err
	}
	x.channelTypeTemplate = channelTypeTemplate

	// Set a unified hasVar variable
	x.hasVar = channelIdTemplate.HasVar() || channelTypeTemplate.HasVar()
	return nil
}

// OnMsg processes a message
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

// Desc returns the component description
func (x *WukongimSender) Desc() string {
	return "WuKongIM client for sending messages. Routes to Success/Failure"
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
