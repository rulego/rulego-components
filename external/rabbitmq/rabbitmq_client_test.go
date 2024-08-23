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

package rabbitmq

import (
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"testing"
	"time"
)

func TestClientNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/rabbitmqClient"

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &ClientNode{}, types.Configuration{
			"server":       "amqp://guest:guest@127.0.0.1:5672/",
			"exchange":     "rulego",
			"exchangeType": "topic",
			"durable":      true,
			"autoDelete":   true,
			"key":          "device.msg.request",
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"server":       "amqp://guest:guest@127.0.0.1:5672/",
			"exchange":     "rulego.topic.test",
			"exchangeType": "topic",
			"durable":      true,
			"autoDelete":   true,
			"key":          "${metadata.key}",
		}, types.Configuration{
			"server":       "amqp://guest:guest@127.0.0.1:5672/",
			"exchange":     "rulego.topic.test",
			"exchangeType": "topic",
			"durable":      true,
			"autoDelete":   true,
			"key":          "${metadata.key}",
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":       "amqp://guest:guest@127.0.0.1:5672/",
			"exchange":     "rulego.topic.test",
			"exchangeType": "topic",
			"durable":      true,
			"autoDelete":   true,
			"forceDelete":  true,
			"key":          "${metadata.key}",
		}, Registry)
		assert.Nil(t, err)
		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":       "amqp://guest:guest@127.0.0.1:5672/",
			"exchange":     "rulego.topic.test",
			"exchangeType": "topic",
			"durable":      false,
			"autoDelete":   false,
			"key":          "${metadata.key}",
		}, Registry)
		assert.Nil(t, err)
		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":       "amqp://guest:guest@127.0.0.1:5672/",
			"exchange":     "rulego.topic.test",
			"exchangeType": "topic",
			"durable":      true,
			"autoDelete":   false,
			"forceDelete":  true,
			"key":          "${metadata.key}",
		}, Registry)
		assert.Nil(t, err)
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("key", "/device/msg")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT2",
				Data:     "{\"temperature\":60}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node3,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 13)
	})
}
