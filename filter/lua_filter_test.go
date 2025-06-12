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

func TestLuaFilterWithArray(t *testing.T) {
	// Test with JSON array data
	var targetNodeType = "x/luaFilter"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaFilter{})

	// Test case 1: Filter JSON array - check if array length > 2
	t.Run("FilterJSONArrayByLength", func(t *testing.T) {
		var configuration = LuaFilterConfiguration{
			Script: `
				-- Filter array: return true if array length > 2
				return #msg > 2
			`,
		}

		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with array length > 2 (should pass filter)
		arrayData := `["apple", "banana", "cherry", "date"]`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "fruit")
		msg1 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       arrayData,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with array length <= 2 (should not pass filter)
		shortArrayData := `["apple", "banana"]`
		msg2 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       shortArrayData,
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
				Node:    node1,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.False, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 2: Filter JSON array - check if contains specific value
	t.Run("FilterJSONArrayByContent", func(t *testing.T) {
		var configuration = LuaFilterConfiguration{
			Script: `
				-- Filter array: return true if array contains "banana"
				for i = 1, #msg do
					if msg[i] == "banana" then
						return true
					end
				end
				return false
			`,
		}

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with array containing "banana" (should pass filter)
		arrayWithBanana := `["apple", "banana", "cherry"]`
		metaData := types.BuildMetadata(make(map[string]string))
		msg3 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       arrayWithBanana,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with array not containing "banana" (should not pass filter)
		arrayWithoutBanana := `["apple", "cherry", "date"]`
		msg4 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       arrayWithoutBanana,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node2,
				MsgList: []test.Msg{msg3},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node2,
				MsgList: []test.Msg{msg4},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.False, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 3: Filter JSON object (existing functionality)
	t.Run("FilterJSONObject", func(t *testing.T) {
		var configuration = LuaFilterConfiguration{
			Script: `
				-- Filter object: return true if temperature > 25
				return msg.temperature > 25
			`,
		}

		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with temperature > 25 (should pass filter)
		highTempData := `{"temperature": 30, "humidity": 60}`
		metaData := types.BuildMetadata(make(map[string]string))
		msg5 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       highTempData,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with temperature <= 25 (should not pass filter)
		lowTempData := `{"temperature": 20, "humidity": 60}`
		msg6 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       lowTempData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node3,
				MsgList: []test.Msg{msg5},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node3,
				MsgList: []test.Msg{msg6},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.False, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 4: Filter complex nested array
	t.Run("FilterNestedArray", func(t *testing.T) {
		var configuration = LuaFilterConfiguration{
			Script: `
				-- Filter nested array: return true if any sub-array has sum > 10
				for i = 1, #msg do
					local sum = 0
					for j = 1, #msg[i] do
						sum = sum + msg[i][j]
					end
					if sum > 10 then
						return true
					end
				end
				return false
			`,
		}

		node4, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with nested array where sum > 10 (should pass filter)
		highSumArray := `[[1, 2, 3], [4, 5, 6]]` // sums: 6, 15 (15 > 10)
		metaData := types.BuildMetadata(make(map[string]string))
		msg7 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       highSumArray,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with nested array where all sums <= 10 (should not pass filter)
		lowSumArray := `[[1, 2], [3, 4]]` // sums: 3, 7 (both <= 10)
		msg8 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       lowSumArray,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node4,
				MsgList: []test.Msg{msg7},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node4,
				MsgList: []test.Msg{msg8},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.False, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}

func TestLuaFilterWithNonJSONData(t *testing.T) {
	// Test with non-JSON data (should be passed as string)
	var targetNodeType = "x/luaFilter"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaFilter{})

	t.Run("FilterStringData", func(t *testing.T) {
		var configuration = LuaFilterConfiguration{
			Script: `
				-- Filter string data: return true if string length > 5
				return string.len(msg) > 5
			`,
		}

		node5, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with long string (should pass filter)
		longString := "hello world"
		metaData := types.BuildMetadata(make(map[string]string))
		msg9 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       longString,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with short string (should not pass filter)
		shortString := "hi"
		msg10 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       shortString,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node5,
				MsgList: []test.Msg{msg9},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node5,
				MsgList: []test.Msg{msg10},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.False, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}

func TestLuaFilterArrayWithMetadata(t *testing.T) {
	// Test filtering array with metadata access
	var targetNodeType = "x/luaFilter"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaFilter{})

	t.Run("FilterArrayWithMetadata", func(t *testing.T) {
		var configuration = LuaFilterConfiguration{
			Script: `
				-- Filter array: return true if array length matches metadata threshold
				local threshold = tonumber(metadata.threshold) or 0
				return #msg >= threshold
			`,
		}

		node6, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with array length >= threshold (should pass filter)
		arrayData := `["a", "b", "c", "d"]`
		metaData := types.BuildMetadata(map[string]string{"threshold": "3"})
		msg11 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       arrayData,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with array length < threshold (should not pass filter)
		shortArrayData := `["a", "b"]`
		metaData2 := types.BuildMetadata(map[string]string{"threshold": "5"})
		msg12 := test.Msg{
			MetaData:   metaData2,
			MsgType:    "TEST_MSG_TYPE",
			Data:       shortArrayData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node6,
				MsgList: []test.Msg{msg11},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.True, relationType)
				},
			},
			{
				Node:    node6,
				MsgList: []test.Msg{msg12},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.False, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}
