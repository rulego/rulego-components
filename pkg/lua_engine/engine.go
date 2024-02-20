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
	"github.com/rulego/rulego/api/types"
	libs "github.com/vadv/gopher-lua-libs"
	"github.com/yuin/gopher-lua"
	"strings"
	"sync"
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
	m      sync.Mutex
	saved  []*lua.LState
	config types.Config
	script string
	path   string
}

func NewStringLStatePool(config types.Config, script string) *LStatePool {
	return &LStatePool{
		saved:  make([]*lua.LState, 0, 20),
		script: strings.TrimSpace(script),
		config: config,
	}
}

func NewFileLStatePool(config types.Config, path string) *LStatePool {
	return &LStatePool{
		saved:  make([]*lua.LState, 0, 20),
		path:   strings.TrimSpace(path),
		config: config,
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
	t := L.NewTable()
	// load modules
	if pl.config.Properties.Has(LoadLuaLibs) {
		loadLuaLibs(L)
	}
	Preloader.Execute(L)

	if len(pl.config.Properties.Values()) != 0 {
		for k, v := range pl.config.Properties {
			L.SetField(t, k, lua.LString(v))
		}
	}
	//set global table from config.Properties
	L.SetGlobal("global", t)

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

func loadLuaLibs(state *lua.LState) {
	libs.Preload(state)
}
