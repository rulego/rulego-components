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
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/json"
	"github.com/rulego/rulego/utils/maps"
	"strconv"
	"strings"
)

func init() {
	_ = rulego.Registry.Register(&WriteNode{})
}

// WriteConfig defines the OpenGemini client configuration
type WriteConfig struct {
	Server   string `json:"server" label:"Server" desc:"OpenGemini server address, format: http://host:port" required:"true" ref:"primary"`
	Database string `json:"database" label:"Database" desc:"Database name" required:"true"`
	Username string `json:"username" label:"Username" desc:"Authentication username" ref:"shared"`
	Password string `json:"password" label:"Password" desc:"Authentication password" ref:"shared"`
	Token    string `json:"token" label:"Token" desc:"Authentication token" ref:"shared"`
}

// WriteNode opengemini writes nodes
type WriteNode struct {
	base.SharedNode[opengemini.Client]
	Config           WriteConfig
	opengeminiConfig *opengemini.Config
	// databaseTemplate: A database template used to parse dynamic database names
	// databaseTemplate template for resolving dynamic database name
	databaseTemplate el.Template
}

// New Implement the Node interface and create a new instance
func (x *WriteNode) New() types.Node {
	return &WriteNode{
		Config: WriteConfig{
			Server:   "127.0.0.1:8086",
			Database: "db0",
		},
	}
}

// Type implements the Node interface and returns the component type
func (x *WriteNode) Type() string {
	return "x/opengeminiWrite"
}

// Init initializes the OpenGemini client
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
	x.databaseTemplate, err = el.NewTemplate(x.Config.Database)
	if err != nil {
		return err
	}
	_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (opengemini.Client, error) {
		return x.initClient()
	}, func(client opengemini.Client) error {
		return client.Close()
	})
	return nil
}

// OnMsg implements the Node interface to process messages
func (x *WriteNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {

	if client, err := x.SharedNode.GetSafely(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		var database string
		if x.databaseTemplate.HasVar() {
			database = x.databaseTemplate.ExecuteAsString(base.NodeUtils.GetEvnAndMetadata(ctx, msg))
		} else {
			database = x.Config.Database
		}
		var points []*opengemini.Point
		if msg.DataType == types.JSON {
			var point opengemini.Point
			//First, let's analyze whether there are multiple entries
			if err := json.Unmarshal([]byte(msg.GetData()), &points); err != nil {
				//If not an array, it is parsed as a single entry
				if err := json.Unmarshal([]byte(msg.GetData()), &point); err != nil {
					ctx.TellFailure(msg, err)
					return
				} else {
					points = append(points, &point)
				}
			}
		} else {
			//Parse Line Protocol
			if points, err = parseMultiLineProtocol(msg.GetData()); err != nil {
				ctx.TellFailure(msg, err)
				return
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

// Desc returns the component description
func (x *WriteNode) Desc() string {
	return "OpenGemini client for writing time-series data. Routes to Success/Failure"
}

// initClient initializes the client
func (x *WriteNode) initClient() (opengemini.Client, error) {
	// Create the OpenGemini client
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
