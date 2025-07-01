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

package grpc

import (
	"os"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestClientNode(t *testing.T) {
	// 如果设置了跳过 gRPC 测试，则跳过
	if os.Getenv("SKIP_GRPC_TESTS") == "true" {
		t.Skip("Skipping gRPC tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/grpcClient"

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &ClientNode{}, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"request": `{"name2": "helloWorld2"}`,
		}, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"request": `{"name2": "helloWorld2"}`,
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"request": `{"name": "lulu","groupName":"app01"}`,
			"headers": map[string]string{
				"key1": "value1",
			},
		}, Registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"headers": map[string]string{
				"key1": "${fromId}",
			},
		}, Registry)
		assert.Nil(t, err)

		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "${metadata.method}",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("service", "helloworld.Greeter")
		metaData.PutValue("method", "SayHello")
		metaData.PutValue("fromId", "aa")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT1",
				Data:     "{\"name\": \"lala\"}",
			},
		}
		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, "{\"message\":\"Hello lulu\",\"groupName\":\"app01\"}\n", msg.GetData())
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, "{\"message\":\"Hello lala\"}\n", msg.GetData())
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node3,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, "{\"message\":\"Hello lala\"}\n", msg.GetData())
					assert.Equal(t, types.Success, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 3)
	})
}
