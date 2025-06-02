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
	luaEngine "github.com/rulego/rulego-components/pkg/lua_engine"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/transform"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	lua "github.com/yuin/gopher-lua"
	"sync"
	"testing"
	"time"
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
					assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":140}", msg.Data)
				},
			},
			{
				Node:    node2,
				MsgList: []test.Msg{msg2},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "AA", msg.Data)
				},
			},
			{
				Node:    node3,
				MsgList: []test.Msg{msg1},
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
					assert.Equal(t, "F", msg.Metadata.GetValue("unit"))
					assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":140}", msg.Data)
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
			assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":140}", msg.Data)
			wg.Done()
		})
		ctx2 := test.NewRuleContextFull(config, node1, nil, func(msg types.RuleMsg, relationType string, err error) {
			assert.Equal(t, types.Success, relationType)
			assert.Equal(t, "F", msg.Metadata.GetValue("unit"))
			assert.Equal(t, "test", msg.Metadata.GetValue("from"))
			assert.Equal(t, "9", msg.Metadata.GetValue("add"))
			assert.Equal(t, "{\"humidity\":30,\"name\":\"aa\",\"temperature\":158}", msg.Data)
			wg.Done()
		})
		for i < 100 {
			if i%2 == 0 {
				go node1.OnMsg(ctx1, msg1)

			} else {
				go node1.OnMsg(ctx2, msg2)
			}
			i++
		}
		wg.Wait()
	})
}
