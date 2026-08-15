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
	"testing"

	"github.com/rulego/rulego/test/assert"
	lua "github.com/yuin/gopher-lua"
)

// JSON null 字段和缓存未命中都会以 nil 进入 GoToLua，曾在此 panic。
func TestGoToLuaNil(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	assert.Equal(t, lua.LNil, GoToLua(L, nil))

	m := map[string]interface{}{"a": nil, "b": float64(1)}
	table := GoToLua(L, m).(*lua.LTable)
	assert.Equal(t, lua.LNil, table.RawGetString("a"))
	assert.Equal(t, lua.LNumber(1), table.RawGetString("b"))

	s := []interface{}{nil, "x"}
	arr := GoToLua(L, s).(*lua.LTable)
	assert.Equal(t, lua.LNil, arr.RawGetInt(1))
	assert.Equal(t, lua.LString("x"), arr.RawGetInt(2))
}

func TestGoToLuaNumericWidths(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	assert.Equal(t, lua.LNumber(8), GoToLua(L, int8(8)))
	assert.Equal(t, lua.LNumber(9), GoToLua(L, uint8(9)))
	assert.Equal(t, lua.LNumber(1.5), GoToLua(L, float32(1.5)))
}

func TestGoToLuaNonStringKeyMap(t *testing.T) {
	L := lua.NewState()
	defer L.Close()
	m := map[int]string{1: "a"}
	table := GoToLua(L, m).(*lua.LTable)
	assert.Equal(t, lua.LString("a"), table.RawGetString("1"))
}
