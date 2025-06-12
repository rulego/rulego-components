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

package filter

import (
	"encoding/json"
	"fmt"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego-components/pkg/lua_engine"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	lua "github.com/yuin/gopher-lua"
	"strings"
)

// init registers the component to rulego
func init() {
	_ = rulego.Registry.Register(&LuaFilter{})
}

// LuaFilterConfiguration node configuration
type LuaFilterConfiguration struct {
	//Script configures the function body content or the script file path with `.lua` as the suffix
	//Only need to provide the function body content, if it is a file path, then need to provide the complete script function:
	//function Filter(msg, metadata, msgType) ${Script} \n end
	//return bool
	//The parameter msg, if the data type of msg is JSON, then it will be converted to the Lua table type before calling the function
	Script string
}

// LuaFilter is a component that filters messages based on Lua scripts.
type LuaFilter struct {
	Config LuaFilterConfiguration
	// pool is a sync.Pool of *lua.LState
	pool *luaEngine.LStatePool
}

// New creates a new instance of LuaFilter
func (x *LuaFilter) New() types.Node {
	return &LuaFilter{Config: LuaFilterConfiguration{
		Script: "return msg.temperature > 50",
	}}
}

// Type returns the type of the component
func (x *LuaFilter) Type() string {
	return "x/luaFilter"
}

// Init initializes the component
func (x *LuaFilter) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {

		if strings.HasSuffix(x.Config.Script, ".lua") {
			if err = luaEngine.ValidateLua(x.Config.Script); err != nil {
				return err
			}
			// create a new LStatePool from file
			x.pool = luaEngine.NewFileLStatePool(ruleConfig, x.Config.Script, configuration)
		} else {
			script := fmt.Sprintf("function Filter(msg, metadata, msgType) %s \nend", x.Config.Script)
			if err = luaEngine.ValidateLua(script); err != nil {
				return err
			}
			// create a new LStatePool from script
			x.pool = luaEngine.NewStringLStatePool(ruleConfig, script, configuration)
		}

	}
	return err
}

// OnMsg handles the message
func (x *LuaFilter) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	// get a *lua.LState from the pool
	L := x.pool.Get()
	if L == nil {
		// if there is no available *lua.LState, tell the next node to fail
		ctx.TellFailure(msg, fmt.Errorf("x/luaFilter lua.LState nil error"))
		return
	}
	// defer putting back the *lua.LState to the pool
	defer x.pool.Put(L)

	//var data interface{} = msg.Data
	var dataMap map[string]interface{}
	var dataSlice []interface{}
	var isArray bool
	if msg.DataType == types.JSON {
		// Try to unmarshal as object first
		if err := json.Unmarshal([]byte(msg.GetData()), &dataMap); err != nil {
			// If object unmarshal fails, try as array
			if err := json.Unmarshal([]byte(msg.GetData()), &dataSlice); err == nil {
				isArray = true
			}
		}
	}
	var err error
	filter := L.GetGlobal("Filter")
	p := lua.P{
		Fn:      filter,
		NRet:    1,
		Protect: true,
	}
	if isArray {
		// Call the Filter function with array data
		err = L.CallByParam(p, luaEngine.SliceToLTable(L, dataSlice), luaEngine.StringMapToLTable(L, msg.Metadata.Values()), lua.LString(msg.Type))
	} else if dataMap != nil {
		// Call the Filter function, passing in msg, metadata, msgType as arguments.
		err = L.CallByParam(p, luaEngine.MapToLTable(L, dataMap), luaEngine.StringMapToLTable(L, msg.Metadata.Values()), lua.LString(msg.Type))
	} else {
		// Call the Filter function, passing in msg, metadata, msgType as arguments.
		err = L.CallByParam(p, lua.LString(msg.GetData()), luaEngine.StringMapToLTable(L, msg.Metadata.Values()), lua.LString(msg.Type))
	}

	if err != nil {
		// if there is an error, tell the next node to fail
		ctx.TellFailure(msg, err)
		return
	}
	// get the return value from the script
	ret := L.Get(-1)
	// pop the value from the stack
	L.Pop(1)
	// check if the return value is a boolean
	if ret.Type() == lua.LTBool && ret == lua.LTrue {
		ctx.TellNext(msg, types.True)
	} else {
		ctx.TellNext(msg, types.False)
	}
}

// Destroy releases the resources of the component
func (x *LuaFilter) Destroy() {
	if x.pool != nil {
		x.pool.Shutdown()
	}
}
