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

package filter

import (
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"testing"
	"time"
)

func TestLuaFilter(t *testing.T) {
	var targetNodeType = "x/luaFilter"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaFilter{})

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &LuaFilter{}, types.Configuration{}, registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"script": "return msg.temperature > 60",
		}, types.Configuration{
			"script": "return msg.temperature > 60",
		}, registry)
	})

	t.Run("DefaultConfig", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"script": "return msg.temperature > 50",
		}, types.Configuration{
			"script": "return msg.temperature > 50",
		}, registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": "return msg.temperature > 50",
		}, registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": "return msg.temperature > 50",
		}, registry)

		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": "return string.upper(msg) == 'AA'",
		}, registry)

		node4, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": "testdata/script.lua",
		}, registry)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "test")

		msg1 := test.Msg{
			MetaData:   metaData,
			MsgType:    "ACTIVITY_EVENT",
			Data:       "{\"name\":\"aa\",\"temperature\":60,\"humidity\":30}",
			AfterSleep: time.Millisecond * 200,
		}
		msg2 := test.Msg{
			MetaData:   metaData,
			DataType:   types.TEXT,
			MsgType:    "ACTIVITY_EVENT",
			Data:       "aa",
			AfterSleep: time.Millisecond * 200,
		}
		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node2,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Failure, relationType)
				},
			},
			{
				Node:    node3,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node4,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}
