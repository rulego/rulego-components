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
	"strings"
	"sync"

	"github.com/rulego/rulego/utils/cache"

	"github.com/rulego/rulego/components/base"

	"github.com/rulego/rulego/api/types"
	libs "github.com/vadv/gopher-lua-libs"
	lua "github.com/yuin/gopher-lua"
)

// LoadLuaLibs If config.Properties sets this key, then it means to load the built-in lua third-party tool library,
// otherwise do not load
const LoadLuaLibs = "load_lua_libs"

var Preloader = &preloader{}

type preloader struct {
	m                 sync.Mutex
	preloaderFuncList []func(state *lua.LState)
}

// Register add register lua third-party tool library function
func (p *preloader) Register(f func(state *lua.LState)) {
	p.m.Lock()
	defer p.m.Unlock()
	p.preloaderFuncList = append(p.preloaderFuncList, f)
}

// Execute preload lua third-party tool library
func (p *preloader) Execute(state *lua.LState) {
	for _, item := range p.preloaderFuncList {
		item(state)
	}
}

type LStatePool struct {
	m       sync.Mutex
	saved   []*lua.LState
	config  types.Config
	script  string
	path    string
	vars    map[string]interface{}
	chainId string //规则链ID
}

func NewStringLStatePool(config types.Config, script string, configuration types.Configuration) *LStatePool {
	fromVars := base.NodeUtils.GetVars(configuration)
	chainCtx := base.NodeUtils.GetChainCtx(configuration)
	var chainId string
	if chainCtx != nil {
		chainId = chainCtx.GetNodeId().Id
	}
	return &LStatePool{
		saved:   make([]*lua.LState, 0, 20),
		script:  strings.TrimSpace(script),
		config:  config,
		vars:    fromVars,
		chainId: chainId,
	}
}

func NewFileLStatePool(config types.Config, path string, configuration types.Configuration) *LStatePool {
	fromVars := base.NodeUtils.GetVars(configuration)
	chainCtx := base.NodeUtils.GetChainCtx(configuration)
	var chainId string
	if chainCtx != nil {
		chainId = chainCtx.GetNodeId().Id
	}
	return &LStatePool{
		saved:   make([]*lua.LState, 0, 20),
		path:    strings.TrimSpace(path),
		config:  config,
		vars:    fromVars,
		chainId: chainId,
	}
}

func (pl *LStatePool) Get() *lua.LState {
	pl.m.Lock()
	defer pl.m.Unlock()
	n := len(pl.saved)
	if n == 0 {
		return pl.New()
	}
	x := pl.saved[n-1]
	pl.saved = pl.saved[0 : n-1]
	return x
}

func (pl *LStatePool) New() *lua.LState {
	L := lua.NewState()
	globalVarTable := L.NewTable()
	// load modules
	if pl.config.Properties.Has(LoadLuaLibs) {
		loadLuaLibs(L)
	}
	Preloader.Execute(L)
	// Register context-specific methods like GlobalCache and GetChainCache
	RegisterContextMethods(L, pl.config, pl.chainId)

	if len(pl.config.Properties.Values()) != 0 {
		for k, v := range pl.config.Properties {
			L.SetField(globalVarTable, k, lua.LString(v))
		}
	}
	//set global table from config.Properties
	L.SetGlobal(types.Global, globalVarTable)

	//set global vars
	for itemK, itemV := range pl.vars {
		if v, ok := itemV.(map[string]string); ok {
			varTable := L.NewTable()
			for varK, varV := range v {
				L.SetField(varTable, varK, lua.LString(varV))
			}
			L.SetGlobal(itemK, varTable)
		}
	}

	//set global udf
	for k, v := range pl.config.Udf {
		if globalScript, scriptOk := v.(types.Script); scriptOk {
			if globalScript.Type == types.Lua {
				funcName := strings.Replace(k, types.Lua+types.ScriptFuncSeparator, "", 1)
				L.SetGlobal(funcName, L.NewFunction(globalScript.Content.(func(*lua.LState) int)))
			}
		}
	}
	if pl.script != "" {
		if L.DoString(pl.script) != nil {
			return nil
		}
	} else if pl.path != "" {
		if L.DoFile(pl.path) != nil {
			return nil
		}
	}
	return L
}

func (pl *LStatePool) Put(L *lua.LState) {
	pl.m.Lock()
	defer pl.m.Unlock()
	pl.saved = append(pl.saved, L)
}

func (pl *LStatePool) Shutdown() {
	for _, L := range pl.saved {
		L.Close()
	}
}

// createLuaCacheBinding is a helper function to create Lua bindings for a types.Cache instance.
// It maps cache operations (Set, Get, Has, Delete, GetByPrefix, DeleteByPrefix) to Lua functions.
func createLuaCacheBinding(L *lua.LState, cacheGoInstance types.Cache) *lua.LTable {
	cacheTable := L.NewTable()

	// Set function: key, value, [ttl_string] -> bool, [error_string]
	L.SetField(cacheTable, "Set", L.NewFunction(func(ls *lua.LState) int {
		argOffset := 0
		if ls.GetTop() > 0 && ls.Get(1).Type() == lua.LTTable && ls.ToTable(1) == cacheTable {
			argOffset = 1
		}

		key := ls.CheckString(1 + argOffset)
		// Using LuaToGo from the same package (tools.go)
		value := LuaToGo(ls.CheckAny(2 + argOffset))
		ttl := ""
		if ls.GetTop() >= (3 + argOffset) {
			if ls.Get(3+argOffset).Type() != lua.LTNil {
				ttl = ls.CheckString(3 + argOffset)
			}
		}
		err := cacheGoInstance.Set(key, value, ttl)
		if err != nil {
			ls.Push(lua.LString(err.Error()))
			return 1
		}
		ls.Push(lua.LString(""))
		return 1
	}))

	// Get function: key -> value
	L.SetField(cacheTable, "Get", L.NewFunction(func(ls *lua.LState) int {
		argOffset := 0
		if ls.GetTop() > 0 && ls.Get(1).Type() == lua.LTTable && ls.ToTable(1) == cacheTable {
			argOffset = 1
		}

		key := ls.CheckString(1 + argOffset)
		value := cacheGoInstance.Get(key)
		// Using GoToLua from the same package (tools.go)
		ls.Push(GoToLua(ls, value))
		return 1
	}))

	// Has function: key -> bool
	L.SetField(cacheTable, "Has", L.NewFunction(func(ls *lua.LState) int {
		argOffset := 0
		if ls.GetTop() > 0 && ls.Get(1).Type() == lua.LTTable && ls.ToTable(1) == cacheTable {
			argOffset = 1
		}

		key := ls.CheckString(1 + argOffset)
		has := cacheGoInstance.Has(key)
		ls.Push(lua.LBool(has))
		return 1
	}))

	// Delete function: key -> bool, [error_string]
	L.SetField(cacheTable, "Delete", L.NewFunction(func(ls *lua.LState) int {
		argOffset := 0
		if ls.GetTop() > 0 && ls.Get(1).Type() == lua.LTTable && ls.ToTable(1) == cacheTable {
			argOffset = 1
		}

		key := ls.CheckString(1 + argOffset)
		err := cacheGoInstance.Delete(key)
		if err != nil {
			ls.Push(lua.LString(err.Error()))
			return 1
		}
		ls.Push(lua.LString(""))
		return 1
	}))

	// GetByPrefix function: prefix -> table
	L.SetField(cacheTable, "GetByPrefix", L.NewFunction(func(ls *lua.LState) int {
		argOffset := 0
		if ls.GetTop() > 0 && ls.Get(1).Type() == lua.LTTable && ls.ToTable(1) == cacheTable {
			argOffset = 1
		}

		prefix := ls.CheckString(1 + argOffset)
		resultMap := cacheGoInstance.GetByPrefix(prefix)
		// Using MapToLTable from the same package (tools.go)
		ls.Push(MapToLTable(ls, resultMap))
		return 1
	}))

	// DeleteByPrefix function: prefix -> bool, [error_string]
	L.SetField(cacheTable, "DeleteByPrefix", L.NewFunction(func(ls *lua.LState) int {
		argOffset := 0
		if ls.GetTop() > 0 && ls.Get(1).Type() == lua.LTTable && ls.ToTable(1) == cacheTable {
			argOffset = 1
		}

		prefix := ls.CheckString(1 + argOffset)
		err := cacheGoInstance.DeleteByPrefix(prefix)
		if err != nil {
			ls.Push(lua.LString(err.Error()))
			return 1
		}
		ls.Push(lua.LString(""))
		return 1
	}))

	return cacheTable
}

// RegisterContextMethods 将全局缓存和链缓存功能注册到 Lua 状态中。
// GlobalCache: 直接绑定到引擎的全局缓存。
// ChainCache: 如果 config.Cache 和 chainId 不为空，则会根据 chainId 创建一个命名空间缓存实例，并将其作为 Lua 全局对象 ChainCache。否则 ChainCache 将为 nil。
func RegisterContextMethods(L *lua.LState, config types.Config, chainId string) {
	// Register GlobalCache
	if config.Cache != nil {
		globalCacheInstance := config.Cache
		globalLuaCache := createLuaCacheBinding(L, globalCacheInstance)
		L.SetGlobal("GlobalCache", globalLuaCache)
	} else {
		// If config.Cache is nil, GlobalCache will not be set.
		// Lua scripts attempting to use it will get a nil value.
	}

	// Register ChainCache object
	if config.Cache != nil && chainId != "" {
		chainCacheInstance := cache.NewNamespaceCache(config.Cache, chainId+types.NamespaceSeparator)
		if chainCacheInstance != nil {
			chainLuaCache := createLuaCacheBinding(L, chainCacheInstance)
			L.SetGlobal("ChainCache", chainLuaCache)
		} else {
			// Failed to create namespaced cache, ChainCache will be nil in Lua
			L.SetGlobal("ChainCache", lua.LNil)
		}
	} else {
		// If config.Cache is nil or chainId is empty, ChainCache will be nil in Lua.
		L.SetGlobal("ChainCache", lua.LNil)
	}
}
func loadLuaLibs(state *lua.LState) {
	libs.Preload(state)
}
