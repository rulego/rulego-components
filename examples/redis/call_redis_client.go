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
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"time"

	_ "github.com/rulego/rulego-components/external/redis"
)

//测试x/redisClient组件
func main() {

	config := rulego.NewConfig()

	//初始化规则引擎实例
	ruleEngine, err := rulego.New("rule01", []byte(chainJsonFile), rulego.WithConfig(config))
	if err != nil {
		panic(err)
	}

	metaData := types.NewMetadata()
	metaData.PutValue("key", "test01")
	metaData.PutValue("value", "test01_value")

	msg := types.NewMsg(0, "TEST_MSG_TYPE1", types.JSON, metaData, "{\"temperature\":41}")

	ruleEngine.OnMsg(msg, types.WithEndFunc(func(ctx types.RuleContext, msg types.RuleMsg, err error) {
		fmt.Println("1.msg处理结果=====")
		//得到规则链处理结果
		fmt.Println(msg, err)
	}))

	time.Sleep(time.Second * 1)

	metaData = types.NewMetadata()
	metaData.PutValue("key", "test02")

	msg = types.NewMsg(0, "TEST_MSG_TYPE2", types.JSON, metaData, "{\"temperature\":42}")

	ruleEngine.OnMsg(msg, types.WithEndFunc(func(ctx types.RuleContext, msg types.RuleMsg, err error) {
		fmt.Println("2.msg处理结果=====")
		//得到规则链处理结果
		fmt.Println(msg, err)
	}))

	time.Sleep(time.Second * 1)
}

var chainJsonFile = `
{
  "ruleChain": {
    "id":"chain_msg_type_switch",
    "name": "测试规则链-msgTypeSwitch",
    "root": false,
    "debugMode": false
  },
  "metadata": {
    "nodes": [
      {
        "id": "s1",
        "type": "msgTypeSwitch",
        "name": "过滤",
        "debugMode": true
      },
      {
        "id": "s2",
        "type": "log",
        "name": "记录日志1",
        "debugMode": true,
        "configuration": {
          "jsScript": "return msgType+':s2--'+JSON.stringify(msg);"
        }
      },
      {
        "id": "s3",
        "type": "log",
        "name": "记录日志2",
        "debugMode": true,
        "configuration": {
          "jsScript": "return msgType+':s3--'+JSON.stringify(msg);"
        }
      },
      {
        "id": "s5",
        "type": "x/redisClient",
        "name": "保存到redis",
        "debugMode": true,
        "configuration": {
          "cmd": "SET",
          "params": ["${key}", "${msg.data}"],
          "poolSize": 10,
          "Server": "192.168.1.1:6379"
        }
      },
	{
        "id": "s6",
        "type": "x/redisClient",
        "name": "保存到redis",
        "debugMode": true,
        "configuration": {
          "cmd": "SET",
          "params": ["${key}", "${value}"],
          "poolSize": 10,
          "Server": "192.168.1.1:6379"
        }
      }
    ],
    "connections": [
      {
        "fromId": "s1",
        "toId": "s2",
        "type": "TEST_MSG_TYPE1"
      },
      {
        "fromId": "s1",
        "toId": "s3",
        "type": "TEST_MSG_TYPE2"
      },
      {
        "fromId": "s3",
        "toId": "s5",
        "type": "Success"
      },
  		{
        "fromId": "s2",
        "toId": "s6",
        "type": "Success"
      }
    ]
  }
}
`
