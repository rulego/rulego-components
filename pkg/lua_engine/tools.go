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

package luaEngine

import (
	"reflect"
	"strings"

	"github.com/rulego/rulego/api/types"
	lua "github.com/yuin/gopher-lua"
)

// StringMapToLTable converts a map to a lua.LTable
func StringMapToLTable(L *lua.LState, m map[string]string) *lua.LTable {
	// create a new lua.LTable
	table := L.NewTable()
	// iterate over the map
	for k, v := range m {
		// convert the key to a lua.LString
		lk := lua.LString(k)
		// convert the value to a lua.LValue
		lv := lua.LString(v)
		// set the key-value pair to the table
		table.RawSet(lk, lv)
	}
	return table
}

// MapToLTable converts a map to a lua.LTable
func MapToLTable(L *lua.LState, m map[string]interface{}) *lua.LTable {
	// create a new lua.LTable
	table := L.NewTable()
	// iterate over the map
	for k, v := range m {
		// convert the key to a lua.LString
		lk := lua.LString(k)
		// convert the value to a lua.LValue
		lv := GoToLua(L, v)
		// set the key-value pair to the table
		table.RawSet(lk, lv)
	}
	return table
}

// SliceToLTable converts a slice to a lua.LTable
func SliceToLTable(L *lua.LState, slice interface{}) *lua.LTable {
	// create a new lua.LTable
	table := L.NewTable()
	// get the slice value using reflection
	v := reflect.ValueOf(slice)
	// iterate over the slice
	for i := 0; i < v.Len(); i++ {
		// get the element at index i
		elem := v.Index(i).Interface()
		// convert the element to a lua.LValue
		lv := GoToLua(L, elem)
		// set the element to the table (Lua arrays are 1-indexed)
		table.RawSetInt(i+1, lv)
	}
	return table
}

// GoToLua converts a Go value to a lua.LValue
func GoToLua(L *lua.LState, v interface{}) lua.LValue {
	// get the value's type and kind
	t := reflect.TypeOf(v)
	k := t.Kind()
	// switch on the kind
	switch k {
	case reflect.String:
		// convert string to lua.LString
		return lua.LString(v.(string))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// convert int to lua.LNumber
		return lua.LNumber(v.(int))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// convert uint to lua.LNumber
		return lua.LNumber(v.(uint))
	case reflect.Float32, reflect.Float64:
		// convert float to lua.LNumber
		return lua.LNumber(v.(float64))
	case reflect.Bool:
		// convert bool to lua.LBool
		return lua.LBool(v.(bool))
	case reflect.Slice, reflect.Array:
		// convert slice/array to lua.LTable
		return SliceToLTable(L, v)
	case reflect.Map:
		// convert map to lua.LTable
		return MapToLTable(L, v.(map[string]interface{}))
	//case reflect.Struct:
	//	// convert struct to lua.LTable
	//	return StructToLTable(L, v)
	default:
		// return nil for unsupported types
		return lua.LNil
	}
}

// LTableToStringMap converts a lua.LTable to a map[string]string
func LTableToStringMap(table *lua.LTable) map[string]string {
	// create a new map[string]string
	m := make(map[string]string)
	// iterate over the table
	table.ForEach(func(key lua.LValue, value lua.LValue) {
		// convert the key and value to string
		k := key.String()
		v := value.String()
		// set the key-value pair to the map
		m[k] = v
	})
	return m
}

// LTableToMap converts a lua.LTable to a map[string]interface{}
func LTableToMap(table *lua.LTable) map[string]interface{} {
	// create a new map[string]interface{}
	m := make(map[string]interface{})
	// iterate over the table
	table.ForEach(func(key lua.LValue, value lua.LValue) {
		// convert the key to string
		k := key.String()
		// convert the value to interface{}
		v := LuaToGo(value)
		// set the key-value pair to the map
		m[k] = v
	})
	return m
}

// LuaToGo converts a lua.LValue to an interface{}
func LuaToGo(value lua.LValue) interface{} {
	// switch on the value type
	switch value.Type() {
	case lua.LTNil:
		// return nil for nil values
		return nil
	case lua.LTBool:
		// return bool for boolean values
		return bool(value.(lua.LBool))
	case lua.LTNumber:
		// return float64 for number values
		return float64(value.(lua.LNumber))
	case lua.LTString:
		// return string for string values
		return string(value.(lua.LString))
	case lua.LTTable:
		// return map[string]interface{} for table values
		return LTableToMap(value.(*lua.LTable))
	default:
		// return nil for unsupported types
		return nil
	}
}

// IsLuaArray checks if a lua table is array-like
func IsLuaArray(table *lua.LTable) bool {
	maxN := table.MaxN()
	if maxN == 0 {
		return false
	}

	// Check if all keys are consecutive numbers
	for i := 1; i <= maxN; i++ {
		if table.RawGetInt(i) == lua.LNil {
			return false
		}
	}
	return true
}

// LuaTableToSlice converts a lua table to a slice
func LuaTableToSlice(table *lua.LTable) []interface{} {
	maxN := table.MaxN()
	slice := make([]interface{}, maxN)

	for i := 1; i <= maxN; i++ {
		value := table.RawGetInt(i)
		slice[i-1] = LuaToGo(value)
	}

	return slice
}

// ValidateLua 验证脚本是否正确
func ValidateLua(script string) error {
	L := lua.NewState()
	if strings.HasSuffix(script, ".lua") {
		return L.DoFile(script)
	} else {
		return L.DoString(script)
	}
}

// BytesToLuaTable converts byte array to Lua table where each element is a byte value (0-255)
// Lua arrays are 1-indexed, so byte at index 0 in Go becomes index 1 in Lua
func BytesToLuaTable(L *lua.LState, data []byte) *lua.LTable {
	table := L.NewTable()
	for i, b := range data {
		table.RawSetInt(i+1, lua.LNumber(b)) // Lua arrays are 1-indexed
	}
	return table
}

// LuaTableToBytes converts Lua table (byte array) back to []byte
// Assumes the Lua table contains numeric values representing bytes (0-255)
func LuaTableToBytes(table *lua.LTable) []byte {
	maxN := table.MaxN()
	bytes := make([]byte, maxN)
	for i := 1; i <= maxN; i++ {
		value := table.RawGetInt(i)
		if num, ok := value.(lua.LNumber); ok {
			bytes[i-1] = byte(num)
		}
	}
	return bytes
}

// PrepareMessageData prepares message data for Lua processing based on the message data type
// This function handles the conversion from Go message data to appropriate Lua types:
// - JSON: Converts to Lua table (object or array)
// - BINARY: Converts to Lua table representing byte array
// - Others: Keeps as Lua string
func PrepareMessageData(L *lua.LState, msg types.RuleMsg) lua.LValue {
	switch string(msg.DataType) {
	case "JSON":
		// Use GetJsonData method to get cached JSON data (supports both objects and arrays)
		if jsonData, err := msg.GetJsonData(); err == nil {
			// Check the type of parsed JSON data
			switch data := jsonData.(type) {
			case map[string]interface{}:
				// JSON object
				return MapToLTable(L, data)
			case []interface{}:
				// JSON array
				return SliceToLTable(L, data)
			default:
				// Other types (string, number, boolean, null)
				return lua.LString(msg.GetData())
			}
		} else {
			// JSON parsing failed, treat as string
			return lua.LString(msg.GetData())
		}
	case "BINARY":
		// Convert binary data to Lua table (byte array)
		return BytesToLuaTable(L, msg.GetBytes())
	default:
		// For TEXT and other types, use string
		return lua.LString(msg.GetData())
	}
}
