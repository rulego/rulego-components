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
	"errors"

	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

func init() {
	_ = rulego.Registry.Register(&QueryNode{})
}

// QueryConfig defines the OpenGemini client configuration
type QueryConfig struct {
	Server   string `json:"server" label:"Server" desc:"OpenGemini server address, format: http://host:port" required:"true" ref:"primary"`
	Database string `json:"database" label:"Database" desc:"Database name" required:"true"`
	Username string `json:"username" label:"Username" desc:"Authentication username" ref:"shared"`
	Password string `json:"password" label:"Password" desc:"Authentication password" ref:"shared"`
	Token    string `json:"token" label:"Token" desc:"Authentication token" ref:"shared"`
	Command  string `json:"command" label:"Query" desc:"SQL query, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
}

// QueryNode opengemini query node
type QueryNode struct {
	*WriteNode
	Config          QueryConfig
	commandTemplate el.Template
	// Whether the identification template contains variables for performance optimization
	commandHasVar bool
}

// New Implement the Node interface and create a new instance
func (x *QueryNode) New() types.Node {
	return &QueryNode{
		Config: QueryConfig{
			Server:   "127.0.0.1:8086",
			Database: "db0",
			Command:  "select * from cpu_load",
		},
	}
}

// Type implements the Node interface and returns the component type
func (x *QueryNode) Type() string {
	return "x/opengeminiQuery"
}

// Init initializes the OpenGemini client
func (x *QueryNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	x.WriteNode = &WriteNode{}
	if err = x.WriteNode.Init(ruleConfig, configuration); err != nil {
		return err
	}
	// Initialize the command template
	commandTemplate, err := el.NewTemplate(x.Config.Command)
	if err != nil {
		return err
	}
	x.commandTemplate = commandTemplate
	x.commandHasVar = commandTemplate.HasVar()
	return nil
}

// OnMsg implements the Node interface to process messages
func (x *QueryNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	database := x.Config.Database
	command := x.Config.Command
	if x.databaseTemplate.HasVar() || x.commandHasVar {
		evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		database = x.databaseTemplate.ExecuteAsString(evn)
		command = x.commandTemplate.ExecuteAsString(evn)
	}

	q := opengemini.Query{
		Database: database,
		Command:  command,
	}
	if client, err := x.SharedNode.GetSafely(); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		if res, err := client.Query(q); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.DataType = types.JSON
			msg.SetData(str.ToString(res))
			if err := hasError(res); err != nil {
				ctx.TellFailure(msg, err)
			} else {
				ctx.TellSuccess(msg)
			}
		}
	}

}

func (x *QueryNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *QueryNode) Desc() string {
	return "OpenGemini client for querying time-series data. Routes to Success/Failure"
}

func hasError(result *opengemini.QueryResult) error {
	if len(result.Error) > 0 {
		return errors.New(result.Error)
	}
	for _, res := range result.Results {
		if len(res.Error) > 0 {
			return errors.New(res.Error)
		}
	}
	return nil
}
