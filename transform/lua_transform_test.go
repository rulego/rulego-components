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

package transform

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	luaEngine "github.com/rulego/rulego-components/pkg/lua_engine"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/transform"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	lua "github.com/yuin/gopher-lua"
)

func TestLuaTransform(t *testing.T) {
	var targetNodeType = "x/luaTransform"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaTransform{})

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &LuaTransform{}, types.Configuration{}, registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"script": "return msg, metadata, msgType",
		}, types.Configuration{
			"script": "return msg, metadata, msgType",
		}, registry)
	})

	t.Run("DefaultConfig", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"script": "return msg, metadata, msgType",
		}, types.Configuration{
			"script": "return msg, metadata, msgType",
		}, registry)
	})

	config := types.NewConfig()
	config.Properties.PutValue("from", "test")
	// 定义一个 Go 函数，接受两个数字参数，返回它们的和
	config.RegisterUdf("add", types.Script{Type: types.Lua, Content: func(L *lua.LState) int {
		a := L.CheckNumber(1)
		b := L.CheckNumber(2)
		L.Push(lua.LNumber(a + b))
		return 1
	}})

	config.RegisterUdf("add", types.Script{Type: types.Js, Content: func(a, b int) int {
		return a + b
	}})

	//注册第三方lua工具库
	config.Properties.PutValue(luaEngine.LoadLuaLibs, "true")
	//luaEngine.Preloader.Register(func(state *lua.LState) {
	//	libs.Preload(state)
	//})

	factory := &LuaTransform{}

	t.Run("OnMsg", func(t *testing.T) {
		//测试自定义函数是否影响js运行时
		jsFactory := &transform.JsTransformNode{}
		jsNode := jsFactory.New()
		err := jsNode.Init(config, types.Configuration{
			"jsScript": `
				metadata.from = global.from
				metadata.add = add(5,4)
				return {'msg':msg, 'metadata':metadata, 'msgType':msgType}
		    `,
		})
		assert.Nil(t, err)

		node1 := factory.New()
		err = node1.Init(config, types.Configuration{
			"script": `
			   	-- 将温度值从摄氏度转换为华氏度
			   	msg.temperature = msg.temperature * 1.8 + 32
				-- 在 metadata 中添加一个字段，表示温度单位
				metadata.unit = "F"
				metadata.from = global.from
				metadata.add = add(5,4)
				return msg, metadata, msgType
             `,
		})
		assert.Nil(t, err)

		node2 := factory.New()
		err = node2.Init(config, types.Configuration{
			"script": "return string.upper(msg), metadata, msgType",
		})

		node3 := factory.New()
		err = node3.Init(config, types.Configuration{
			"script": "testdata/script.lua",
		})
		node4 := factory.New()
		err = node4.Init(config, types.Configuration{
			"script": "testdata/libs_script.lua",
		})

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
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "F", msg.Metadata.GetValue("unit"))
					assert.Equal(t, "test", msg.Metadata.GetValue("from"))
					assert.Equal(t, "9", msg.Metadata.GetValue("add"))
					assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":140}", msg.GetData())
				},
			},
			{
				Node:    node2,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "AA", msg.GetData())
				},
			},
			{
				Node:    node3,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "F", msg.Metadata.GetValue("unit"))
					assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":140}", msg.GetData())
				},
			},
			{
				Node:    node4,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "b026324c6904b2a9cb4b88d6d61c81d1", msg.Metadata.GetValue("md5"))
				},
			},
			{
				Node:    jsNode,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "test", msg.Metadata.GetValue("from"))
					assert.Equal(t, "9", msg.Metadata.GetValue("add"))
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	t.Run("OnMsgConcurrency", func(t *testing.T) {
		node1 := factory.New()
		err := node1.Init(config, types.Configuration{
			"script": `
			   	-- 将温度值从摄氏度转换为华氏度
			   	msg.temperature = msg.temperature * 1.8 + 32
				-- 在 metadata 中添加一个字段，表示温度单位
				metadata.unit = "F"
				metadata.from = global.from
				metadata.add = add(5,4)
				return msg, metadata, msgType
             `,
		})
		assert.Nil(t, err)
		var i = 0
		msg1 := types.NewMsg(time.Now().UnixMilli(), "ACTIVITY_EVENT", types.JSON, types.NewMetadata(), "{\"name\":\"aa\",\"temperature\":60,\"humidity\":30}")
		msg2 := types.NewMsg(time.Now().UnixMilli(), "ACTIVITY_EVENT", types.JSON, types.NewMetadata(), "{\"name\":\"aa\",\"temperature\":70,\"humidity\":30}")

		var wg = sync.WaitGroup{}
		wg.Add(100)
		ctx1 := test.NewRuleContextFull(config, node1, nil, func(msg types.RuleMsg, relationType string, err error) {
			assert.Equal(t, types.Success, relationType)
			assert.Equal(t, "F", msg.Metadata.GetValue("unit"))
			assert.Equal(t, "test", msg.Metadata.GetValue("from"))
			assert.Equal(t, "9", msg.Metadata.GetValue("add"))
			assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":140}", msg.GetData())
			wg.Done()
		})
		ctx2 := test.NewRuleContextFull(config, node1, nil, func(msg types.RuleMsg, relationType string, err error) {
			assert.Equal(t, types.Success, relationType)
			assert.Equal(t, "F", msg.Metadata.GetValue("unit"))
			assert.Equal(t, "test", msg.Metadata.GetValue("from"))
			assert.Equal(t, "9", msg.Metadata.GetValue("add"))
			assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":158}", msg.GetData())
			wg.Done()
		})
		for i < 100 {
			if i%2 == 0 {
				go func(msgCopy types.RuleMsg) {
					node1.OnMsg(ctx1, msgCopy)
				}(msg1.Copy())

			} else {
				go func(msgCopy types.RuleMsg) {
					node1.OnMsg(ctx2, msgCopy)
				}(msg2.Copy())
			}
			i++
		}
		wg.Wait()
	})
}

func TestLuaTransformWithArray(t *testing.T) {
	// Test with JSON array data
	var targetNodeType = "x/luaTransform"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaTransform{})

	// Test case 1: Transform JSON array
	t.Run("TransformJSONArray", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				-- Transform array: add index to each element
				local result = {}
				for i = 1, #msg do
					result[i] = {
						index = i,
						value = msg[i],
						processed = true
					}
				end
				return result, metadata, msgType
			`,
		}

		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with JSON array
		arrayData := `["apple", "banana", "cherry"]`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "fruit")
		msg1 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       arrayData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result []map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)
					assert.Equal(t, 3, len(result))

					// Check first element
					assert.Equal(t, float64(1), result[0]["index"])
					assert.Equal(t, "apple", result[0]["value"])
					assert.Equal(t, true, result[0]["processed"])

					// Check second element
					assert.Equal(t, float64(2), result[1]["index"])
					assert.Equal(t, "banana", result[1]["value"])
					assert.Equal(t, true, result[1]["processed"])
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 2: Transform JSON object (existing functionality)
	t.Run("TransformJSONObject", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				msg.processed = true
				msg.timestamp = os.time()
				return msg, metadata, msgType
			`,
		}

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with JSON object
		objectData := `{"name": "test", "value": 123}`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "data")
		msg2 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       objectData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node2,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)
					assert.Equal(t, "test", result["name"])
					assert.Equal(t, true, result["processed"])
					assert.NotNil(t, result["timestamp"])
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 3: Transform complex nested array
	t.Run("TransformNestedArray", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				-- Transform nested array: calculate sum for each sub-array
				local result = {}
				for i = 1, #msg do
					local sum = 0
					for j = 1, #msg[i] do
						sum = sum + msg[i][j]
					end
					result[i] = {
						original = msg[i],
						sum = sum
					}
				end
				return result, metadata, msgType
			`,
		}

		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with nested JSON array
		nestedArrayData := `[[1, 2, 3], [4, 5, 6]]`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("dataType", "matrix")
		msg3 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST_MSG_TYPE",
			Data:       nestedArrayData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node3,
				MsgList: []test.Msg{msg3},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result []map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)
					assert.Equal(t, 2, len(result))

					// Check first sub-array result
					assert.Equal(t, float64(6), result[0]["sum"]) // 1+2+3=6
					// Check second sub-array result
					assert.Equal(t, float64(15), result[1]["sum"]) // 4+5+6=15
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}

func TestLuaTransformWithMapReturn(t *testing.T) {
	// Test Lua transform that returns map/table
	var targetNodeType = "x/luaTransform"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaTransform{})

	// Test case 1: Transform returns map with additional fields
	t.Run("TransformWithMapReturnEnrichment", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				-- Return enriched map with original data plus computed fields
				local result = {
					original = msg,
					timestamp = os.time(),
					processed = true,
					version = "1.0"
				}
				
				-- Add computed fields based on original data
				if msg.temperature then
					result.temperatureStatus = msg.temperature > 25 and "hot" or "normal"
				end
				
				return result, metadata, msgType
			`,
		}

		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with temperature data
		tempData := `{"temperature": 30, "humidity": 60}`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("sensorType", "environmental")
		msg1 := test.Msg{
			MetaData:   metaData,
			MsgType:    "SENSOR_DATA",
			Data:       tempData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)

					// Check enriched fields
					assert.Equal(t, true, result["processed"])
					assert.Equal(t, "1.0", result["version"])
					assert.Equal(t, "hot", result["temperatureStatus"])
					assert.NotNil(t, result["timestamp"])
					assert.NotNil(t, result["original"])
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 2: Transform array to map with statistics
	t.Run("TransformArrayToMapWithStats", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				-- Transform array to map with statistical information
				local result = {
					originalArray = msg,
					count = #msg,
					items = {},
					stats = {}
				}
				
				-- Process each item and collect stats
				local totalLength = 0
				for i = 1, #msg do
					local item = msg[i]
					result.items[i] = {
						index = i,
						value = item,
						length = string.len(tostring(item))
					}
					totalLength = totalLength + string.len(tostring(item))
				end
				
				result.stats.totalLength = totalLength
				result.stats.averageLength = totalLength / #msg
				result.stats.processedAt = os.time()
				
				return result, metadata, msgType
			`,
		}

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with string array
		arrayData := `["apple", "banana", "cherry"]`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("dataType", "fruits")
		msg2 := test.Msg{
			MetaData:   metaData,
			MsgType:    "ARRAY_DATA",
			Data:       arrayData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node2,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)

					// Check basic fields
					assert.Equal(t, float64(3), result["count"])
					assert.NotNil(t, result["originalArray"])
					assert.NotNil(t, result["items"])
					assert.NotNil(t, result["stats"])

					// Check stats
					stats := result["stats"].(map[string]interface{})
					assert.Equal(t, float64(17), stats["totalLength"]) // apple(5) + banana(6) + cherry(6) = 17
					assert.NotNil(t, stats["processedAt"])
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	// Test case 3: Transform with complex map structure
	t.Run("TransformWithComplexMapStructure", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				-- Create complex map structure with nested data
				local result = {
					header = {
						timestamp = os.time(),
						version = "2.0",
						processor = "lua-transform"
					},
					payload = {
						original = msg,
						processed = true
					},
					metrics = {
						processingTime = 0.1,
						dataSize = string.len(tostring(msg))
					}
				}
				
				-- Add conditional fields based on data type
				if type(msg) == "table" then
					if #msg > 0 then
						result.payload.type = "array"
						result.payload.length = #msg
					else
						result.payload.type = "object"
						local count = 0
						for k, v in pairs(msg) do
							count = count + 1
						end
						result.payload.fieldCount = count
					end
				else
					result.payload.type = "primitive"
				end
				
				return result, metadata, msgType
			`,
		}

		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with object data
		objectData := `{"name": "test", "value": 123, "active": true}`
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("source", "api")
		msg3 := test.Msg{
			MetaData:   metaData,
			MsgType:    "OBJECT_DATA",
			Data:       objectData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node3,
				MsgList: []test.Msg{msg3},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)

					// Check header structure
					header := result["header"].(map[string]interface{})
					assert.Equal(t, "2.0", header["version"])
					assert.Equal(t, "lua-transform", header["processor"])
					assert.NotNil(t, header["timestamp"])

					// Check payload structure
					payload := result["payload"].(map[string]interface{})
					assert.Equal(t, "object", payload["type"])
					assert.Equal(t, true, payload["processed"])
					assert.Equal(t, float64(3), payload["fieldCount"])
					assert.NotNil(t, payload["original"])

					// Check metrics structure
					metrics := result["metrics"].(map[string]interface{})
					assert.Equal(t, 0.1, metrics["processingTime"])
					assert.NotNil(t, metrics["dataSize"])
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}

func TestLuaTransformWithNonJSONData(t *testing.T) {
	// Test with non-JSON data (should be passed as string)
	var targetNodeType = "x/luaTransform"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaTransform{})

	t.Run("TransformStringData", func(t *testing.T) {
		var configuration = LuaTransformConfiguration{
			Script: `
				-- Transform string data: convert to uppercase and wrap in object
				local result = {
					original = msg,
					upper = string.upper(msg),
					length = string.len(msg)
				}
				return result, metadata, msgType
			`,
		}

		node4, err := test.CreateAndInitNode(targetNodeType, types.Configuration{"script": configuration.Script}, registry)
		assert.Nil(t, err)

		// Test with string data
		stringData := "hello world"
		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("dataType", "text")
		msg4 := test.Msg{
			MetaData:   metaData,
			DataType:   types.TEXT, // Non-JSON data type
			MsgType:    "TEST_MSG_TYPE",
			Data:       stringData,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node4,
				MsgList: []test.Msg{msg4},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)

					// Parse the transformed result
					var result map[string]interface{}
					err = json.Unmarshal([]byte(msg.GetData()), &result)
					assert.Nil(t, err)
					assert.Equal(t, "hello world", result["original"])
					assert.Equal(t, "HELLO WORLD", result["upper"])
					assert.Equal(t, float64(11), result["length"])
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}

// Simple test for dataType parameter support
func TestLuaTransformWithDataType(t *testing.T) {
	var targetNodeType = "x/luaTransform"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaTransform{})

	t.Run("DataTypeTest", func(t *testing.T) {
		// Test dataType parameter and binary processing
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": `
				metadata.originalType = dataType
				if dataType == "BINARY" then
					local result = {0xAA, 0xBB}
					for i = 1, #msg do
						table.insert(result, msg[i])
					end
					return result, metadata, msgType
				end
				return msg, metadata, msgType
			`,
		}, registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))

		// Test BINARY message - add header
		msg1 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEST",
			Data:       string([]byte{0x01, 0x02}),
			DataType:   types.BINARY,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "BINARY", msg.Metadata.GetValue("originalType"))
					result := []byte(msg.GetData())
					expected := []byte{0xAA, 0xBB, 0x01, 0x02}
					assert.Equal(t, expected, result)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}

// Test binary data header addition
func TestLuaTransformBinaryHeader(t *testing.T) {
	var targetNodeType = "x/luaTransform"
	var registry = &types.SafeComponentSlice{}
	registry.Add(&LuaTransform{})

	t.Run("BinaryHeaderAddition", func(t *testing.T) {
		// Test adding 4-byte header to binary data
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": `
				-- 示例：添加4字节头部
				if dataType == "BINARY" then
					local result = {0xAA, 0xBB, 0xCC, 0xDD}  -- 新头部
					for i = 1, #msg do
						table.insert(result, msg[i])          -- 追加原始数据
					end
					return result, metadata, msgType
				end
				-- 非二进制数据直接返回
				return msg, metadata, msgType
			`,
		}, registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("device", "sensor01")

		// Test with binary data - should add header
		msg1 := test.Msg{
			MetaData:   metaData,
			MsgType:    "SENSOR_DATA",
			Data:       string([]byte{0x01, 0x02, 0x03}), // 原始数据
			DataType:   types.BINARY,
			AfterSleep: time.Millisecond * 200,
		}

		// Test with non-binary data - should pass through unchanged
		msg2 := test.Msg{
			MetaData:   metaData,
			MsgType:    "TEXT_DATA",
			Data:       "hello world",
			DataType:   types.TEXT,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					result := []byte(msg.GetData())
					// 期望结果：4字节头部 + 3字节原始数据
					expected := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0x01, 0x02, 0x03}
					assert.Equal(t, expected, result)
					assert.Equal(t, 7, len(result)) // 验证总长度
				},
			},
			{
				Node:    node1,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					// 非二进制数据应该原样返回
					assert.Equal(t, "hello world", msg.GetData())
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})

	t.Run("BinaryDataProtocolProcessing", func(t *testing.T) {
		// Test more complex binary protocol processing
		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"script": `
				if dataType == "BINARY" then
					-- 协议格式：[长度][命令][数据][校验和]
					local dataLen = #msg
					local command = 0x10  -- 命令码
					local checksum = 0
					
					-- 构建协议包
					local result = {dataLen, command}
					
					-- 添加原始数据
					for i = 1, #msg do
						table.insert(result, msg[i])
						checksum = checksum + msg[i]
					end
					
					-- 添加校验和（简单累加）
					table.insert(result, checksum % 256)
					
					metadata.protocol = "custom"
					metadata.originalLength = tostring(dataLen)
					
					return result, metadata, msgType
				end
				return msg, metadata, msgType
			`,
		}, registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("device", "controller")

		// Test protocol encapsulation
		msg := test.Msg{
			MetaData:   metaData,
			MsgType:    "PROTOCOL_DATA",
			Data:       string([]byte{0x01, 0x02, 0x03}),
			DataType:   types.BINARY,
			AfterSleep: time.Millisecond * 200,
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node2,
				MsgList: []test.Msg{msg},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					result := []byte(msg.GetData())

					// 期望格式：[长度=3][命令=0x10][0x01,0x02,0x03][校验和=(1+2+3)%256=6]
					expected := []byte{0x03, 0x10, 0x01, 0x02, 0x03, 0x06}
					assert.Equal(t, expected, result)

					// 验证元数据
					assert.Equal(t, "custom", msg.Metadata.GetValue("protocol"))
					assert.Equal(t, "3", msg.Metadata.GetValue("originalLength"))
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 20)
	})
}
