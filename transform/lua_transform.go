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

package transform

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rulego/rulego"
	luaEngine "github.com/rulego/rulego-components/pkg/lua_engine"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/maps"
	lua "github.com/yuin/gopher-lua"
)

// init registers the component to rulego
func init() {
	_ = rulego.Registry.Register(&LuaTransform{})
}

// FunctionNameTransform is the name of the function to be called in the script
const FunctionNameTransform = "Transform"

// LuaTransformConfiguration node configuration
type LuaTransformConfiguration struct {
	//Script configures the function body content or the script file path with `.lua` as the suffix
	//Only need to provide the function body content, if it is a file path, then need to provide the complete script function:
	//function Transform(msg, metadata, msgType, dataType) ${Script} \n end
	//return msg, metadata, msgType
	//The parameter msg, if the data type of msg is JSON, then it will be converted to the Lua table type before calling the function
	//If the data type of msg is BINARY, then it will be converted to a Lua table (byte array) before calling the function
	Script string
}

// LuaTransform is a component that transforms messages based on Lua scripts
type LuaTransform struct {
	Config LuaTransformConfiguration
	// pool is a sync.Pool of *lua.LState
	pool *luaEngine.LStatePool
}

// New creates a new instance of LuaFilter
func (x *LuaTransform) New() types.Node {
	return &LuaTransform{Config: LuaTransformConfiguration{
		Script: "return msg, metadata, msgType",
	}}
}

// Type returns the type of the component
func (x *LuaTransform) Type() string {
	return "x/luaTransform"
}

// Init initializes the component
func (x *LuaTransform) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {

		if strings.HasSuffix(x.Config.Script, ".lua") {
			if err = luaEngine.ValidateLua(x.Config.Script); err != nil {
				return err
			}
			// create a new LStatePool from file
			x.pool = luaEngine.NewFileLStatePool(ruleConfig, x.Config.Script, configuration)
		} else {
			script := fmt.Sprintf("function Transform(msg, metadata, msgType, dataType) %s \nend", x.Config.Script)
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
func (x *LuaTransform) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	// get a *lua.LState from the pool
	L := x.pool.Get()
	if L == nil {
		// if there is no available *lua.LState, tell the next node to fail
		ctx.TellFailure(msg, fmt.Errorf("x/luaTransform lua.LState nil error"))
		return
	}
	// defer putting back the *lua.LState to the pool
	defer x.pool.Put(L)

	// Use common function to prepare message data
	msgData := luaEngine.PrepareMessageData(L, msg)

	// Call the Transform function with msg, metadata, msgType, dataType
	transform := L.GetGlobal(FunctionNameTransform)
	p := lua.P{
		Fn:      transform,
		NRet:    3, // Specify the number of return values
		Protect: true,
	}
	err := L.CallByParam(p, msgData, luaEngine.StringMapToLTable(L, msg.Metadata.Values()), lua.LString(msg.Type), lua.LString(string(msg.DataType)))

	if err != nil {
		// if there is an error, tell the next node to fail
		ctx.TellFailure(msg, err)
		return
	}
	// get the return values from the script
	ret1 := L.Get(-3) // msg
	ret2 := L.Get(-2) // metadata
	ret3 := L.Get(-1) // msgType
	// pop the values from the stack
	L.Pop(3)

	// update the msg fields with the new values
	// if newMsg is a lua.LTable type value, it could be JSON object/array or binary data
	if newMsg, ok := ret1.(*lua.LTable); ok {
		// Check if it's an array-like table
		if luaEngine.IsLuaArray(newMsg) {
			// Check if this is a byte array (all values are numbers 0-255)
			isByteArray := true
			maxN := newMsg.MaxN()
			for i := 1; i <= maxN; i++ {
				value := newMsg.RawGetInt(i)
				if num, ok := value.(lua.LNumber); ok {
					if num < 0 || num > 255 {
						isByteArray = false
						break
					}
				} else {
					isByteArray = false
					break
				}
			}

			if isByteArray && msg.DataType == types.BINARY {
				// Handle as binary data using common function
				bytes := luaEngine.LuaTableToBytes(newMsg)
				msg.SetBytes(bytes)
			} else {
				// Handle as JSON array
				newMsgSlice := luaEngine.LuaTableToSlice(newMsg)
				if b, err := json.Marshal(newMsgSlice); err != nil {
					ctx.TellFailure(msg, err)
					return
				} else {
					msg.SetBytes(b)
				}
			}
		} else {
			// Handle as JSON object
			newMsgMap := luaEngine.LTableToMap(newMsg)
			if b, err := json.Marshal(newMsgMap); err != nil {
				ctx.TellFailure(msg, err)
				return
			} else {
				msg.SetBytes(b)
			}
		}
	} else if newMsgString, ok := ret1.(lua.LString); ok {
		// If newMsg is not a lua.LTable type value, it means a normal string
		// Directly convert newMsg to a string type value and assign it to msg.Data
		msg.SetData(string(newMsgString))
	} else {
		// Handle primitive data types (number, boolean, etc.)
		convertedValue := luaEngine.LuaToGo(ret1)
		if b, err := json.Marshal(convertedValue); err != nil {
			ctx.TellFailure(msg, err)
			return
		} else {
			msg.SetBytes(b)
		}
	}

	// If newMetadata is a lua.LTable type value, it means a metadata table
	if newMetadata, ok := ret2.(*lua.LTable); ok {
		// Convert newMetadata to a map[string]string type value and assign it to msg.Metadata
		msg.Metadata.ReplaceAll(luaEngine.LTableToStringMap(newMetadata))
	} else {
		// If newMetadata is not a lua.LTable type value, it means a nil value
		// Do not modify the value of msg.Metadata
	}
	// If newMsgType is a lua.LString type value, it means a message type string
	if newMsgType, ok := ret3.(lua.LString); ok {
		// Convert newMsgType to a string type value and assign it to msg.Type
		msg.Type = string(newMsgType)
	} else {
		// If newMsgType is not a lua.LString type value, it means a nil value
		// Do not modify the value of msg.Type
	}

	ctx.TellSuccess(msg)
}

// Destroy releases the resources of the component
func (x *LuaTransform) Destroy() {
	if x.pool != nil {
		x.pool.Shutdown()
	}
}
