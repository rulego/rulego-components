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

package main

import (
	"fmt"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"

	_ "github.com/rulego/rulego-components/external/wukongim"
)

// 测试x/wukongimClient
func main() {

	config := rulego.NewConfig()

	//初始化规则引擎实例
	ruleEngine, err := rulego.New("rule01", []byte(chainJsonFile), rulego.WithConfig(config))
	if err != nil {
		panic(err)
	}

	msg := types.NewMsg(0, "", types.JSON, types.NewMetadata(), "wukong,hello world!")

	ruleEngine.OnMsg(msg, types.WithEndFunc(func(ctx types.RuleContext, msg types.RuleMsg, err error) {
		fmt.Println("msg处理结果=====")
		//得到规则链处理结果
		fmt.Println(msg, err)
	}))
	time.Sleep(time.Second * 1)
}

var chainJsonFile = `
{
	"ruleChain": {
		"id": "j-VTV0NZgtgA",
		"name": "悟空IM发送测试",
		"root": true,
		"additionalInfo": {
			"description": "",
			"layoutX": "670",
			"layoutY": "330"
		},
		"configuration": {},
		"disabled": false
	},
	"metadata": {
		"endpoints": [],
		"nodes": [
			{
				"id": "node_2",
				"type": "x/wukongimClient",
				"name": "发送节点",
				"configuration": {
					"server": "tcp://127.0.0.1:5100",
					"uID": "test1",
					"token": "test1",
					"connectTimeout": "50",
					"protoVersion": 3,
					"pingInterval": "300",
					"reconnect": true,
					"autoAck": true,
					"channelID": "test2",
					"channelType": 1,
					"redDot": true
				},
				"debugMode": true,
				"additionalInfo": {
					"layoutX": 990,
					"layoutY": 330
				}
			}
		],
		"connections": []
	}
}
 `
