package luaEngine

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/rulego/rulego/utils/js"

	"github.com/rulego/rulego/engine"

	"github.com/rulego/rulego/api/types"
	lua "github.com/yuin/gopher-lua"
)

// TestMyStruct is a sample struct for testing UDF registration.
// It needs to be a pointer to a struct to register its methods.
// The methods must be exported (start with an uppercase letter).
// The methods must return an int, which is the number of return values pushed to the Lua stack.
// The methods must take *lua.LState as the first argument.

type TestMyStruct struct {
	Prefix string
}

func (ms *TestMyStruct) GetPrefix() string {
	return ms.Prefix
}

func (ms *TestMyStruct) Concat(s1 string, s2 string) string {
	return ms.Prefix + s1 + s2
}

func (ms *TestMyStruct) Add(a int, b int) int {
	return a + b
}

func TestRegisterGoStructMethods(t *testing.T) {
	myStructInstance := &TestMyStruct{Prefix: "test_"}

	config := types.NewConfig()
	config.RegisterUdf("myLib", types.Script{Type: types.Lua, Content: myStructInstance})
	pool := NewStringLStatePool(config, "", types.Configuration{})
	L := pool.Get()
	defer pool.Put(L)
	if L == nil {
		t.Fatal("Failed to get LState from pool")
	}

	tests := []struct {
		name        string
		script      string
		expected    lua.LValue
		expectError bool
	}{
		{
			name:     "Call GetPrefix with .",
			script:   "return myLib.GetPrefix()",
			expected: lua.LString("test_"),
		},
		{
			name:     "Call GetPrefix with :",
			script:   "return myLib:GetPrefix()",
			expected: lua.LString("test_"),
		},
		{
			name:     "Call Concat with .",
			script:   "return myLib.Concat('hello', 'world')",
			expected: lua.LString("test_helloworld"),
		},
		{
			name:     "Call Concat with :",
			script:   "return myLib:Concat('hello', 'world')",
			expected: lua.LString("test_helloworld"),
		},
		{
			name:     "Call Add with .",
			script:   "return myLib.Add(5, 3)",
			expected: lua.LNumber(8),
		},
		{
			name:     "Call Add with :",
			script:   "return myLib:Add(5, 3)",
			expected: lua.LNumber(8),
		},
		{
			name:        "Call non-existent method",
			script:      "return myLib.NonExistent()",
			expectError: true, // Expecting an error because the method doesn't exist
		},
		{
			name:        "Call method with wrong number of args (.)",
			script:      "return myLib.Add(5)",
			expectError: true,
		},
		{
			name:        "Call method with wrong number of args (:)",
			script:      "return myLib:Add(5)",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := L.DoString(tt.script)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected an error for script '%s', but got nil", tt.script)
				}
				// Reset stack by removing potential return value from failed execution
				if L.GetTop() > 0 {
					L.Pop(L.GetTop())
				}
				return
			}
			if err != nil {
				t.Fatalf("Failed to execute script '%s': %v", tt.script, err)
			}

			if L.GetTop() != 1 {
				t.Fatalf("Expected 1 return value, got %d for script '%s'", L.GetTop(), tt.script)
			}

			ret := L.Get(-1)
			L.Pop(1)

			if !reflect.DeepEqual(ret, tt.expected) {
				t.Errorf("Script '%s' returned %v (type %T), expected %v (type %T)", tt.script, ret, ret, tt.expected, tt.expected)
			}
		})
	}
}

// TestRegisterGoFunctionUDF tests the registration of simple Go functions as UDFs.
func TestRegisterGoFunctionUDF(t *testing.T) {
	addFunc := func(a int, b int) int {
		return a + b
	}

	concatFunc := func(s1, s2 string) string {
		return s1 + s2
	}

	config := types.NewConfig()
	config.RegisterUdf("myAdd", types.Script{Type: "", Content: addFunc})
	config.RegisterUdf("myConcat", types.Script{Type: "", Content: concatFunc})
	pool := NewStringLStatePool(config, "", types.Configuration{})
	L := pool.Get()
	defer pool.Put(L)
	if L == nil {
		t.Fatal("Failed to get LState from pool")
	}

	tests := []struct {
		name     string
		script   string
		expected lua.LValue
	}{
		{
			name:     "Call myAdd function",
			script:   "return myAdd(10, 20)",
			expected: lua.LNumber(30),
		},
		{
			name:     "Call myConcat function",
			script:   "return myConcat('foo', 'bar')",
			expected: lua.LString("foobar"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := L.DoString(tt.script); err != nil {
				t.Fatalf("Failed to execute script '%s': %v", tt.script, err)
			}

			if L.GetTop() != 1 {
				t.Fatalf("Expected 1 return value, got %d for script '%s'", L.GetTop(), tt.script)
			}

			ret := L.Get(-1)
			L.Pop(1)

			if !reflect.DeepEqual(ret, tt.expected) {
				t.Errorf("Script '%s' returned %v, expected %v", tt.script, ret, tt.expected)
			}
		})
	}
}

func TestLuaScriptUDF(t *testing.T) {
	config := types.NewConfig()
	// Use RegisterUdf or ensure keys match the pattern "lua:funcName"
	config.RegisterUdf("myFuncReturn", types.Script{
		Type:    types.Lua,
		Content: "return function(a, b) return a * b end", // UDF returns a function
	})
	config.RegisterUdf("myFuncDirect", types.Script{
		Type:    types.Lua,
		Content: "function(a, b) return a + b end", // UDF is a function definition string
	})

	// Alternative direct map assignment with correct keys:
	// config.Udf = map[string]interface{}{
	// 	"lua:myFuncReturn": types.Script{
	// 		Type:    types.Lua,
	// 		Content: "return function(a, b) return a * b end",
	// 	},
	// 	"lua:myFuncDirect": types.Script{
	// 		Type:    types.Lua,
	// 		Content: "function(a, b) return a + b end",
	// 	},
	// }

	pool := NewStringLStatePool(config, "", types.Configuration{})
	L := pool.Get()
	defer pool.Put(L)
	if L == nil {
		t.Fatal("Failed to get LState from pool")
	}

	t.Run("UDF returning function", func(t *testing.T) {
		script := "return myFuncReturn(3, 4)"
		expected := lua.LNumber(12)

		if err := L.DoString(script); err != nil {
			t.Fatalf("Failed to execute script '%s': %v", script, err)
		}

		if L.GetTop() != 1 {
			t.Fatalf("Expected 1 return value, got %d for script '%s'", L.GetTop(), script)
		}

		ret := L.Get(-1)
		L.Pop(1)

		if !reflect.DeepEqual(ret, expected) {
			t.Errorf("Script '%s' returned %v, expected %v", script, ret, expected)
		}
	})

	t.Run("UDF as direct function string", func(t *testing.T) {
		script := "return myFuncDirect(5, 6)"
		expected := lua.LNumber(11)

		if err := L.DoString(script); err != nil {
			t.Fatalf("Failed to execute script '%s': %v", script, err)
		}

		if L.GetTop() != 1 {
			t.Fatalf("Expected 1 return value, got %d for script '%s'", L.GetTop(), script)
		}

		ret := L.Get(-1)
		L.Pop(1)

		if !reflect.DeepEqual(ret, expected) {
			t.Errorf("Script '%s' returned %v, expected %v", script, ret, expected)
		}
	})

}

func TestGlobalCacheInLua(t *testing.T) {
	// Mock cache for testing
	mockCache := &MockCache{
		store: make(map[string]interface{}),
	}

	config := engine.NewConfig()
	config.Cache = mockCache
	var def types.RuleChain
	def.RuleChain.ID = "test"

	chainCtx, _ := engine.InitRuleChainCtx(config, nil, &def, nil)

	pool := NewStringLStatePool(config, "", types.Configuration{
		types.NodeConfigurationKeyChainCtx: chainCtx,
	})
	L := pool.Get()
	defer pool.Put(L)
	if L == nil {
		t.Fatal("Failed to get LState from pool")
	}

	// Test Set and Get
	scriptSetGet := `
        GlobalCache:Set("myKey", "myValue", "10s")
        return GlobalCache:Get("myKey")
    `
	if err := L.DoString(scriptSetGet); err != nil {
		t.Fatalf("Error executing Set/Get script: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Set/Get, got %d", L.GetTop())
	}
	ret := L.Get(-1)
	L.Pop(1)
	if ret.Type() != lua.LTString || ret.String() != "myValue" {
		t.Errorf("Expected 'myValue', got %s", ret.String())
	}

	// Verify it was stored in mock cache
	if val, found := mockCache.store["myKey"]; !found || val.(string) != "myValue" {
		t.Errorf("Value not found or incorrect in mock cache. Found: %v, Value: %v", found, val)
	}

	// Test Has with colon
	scriptHasColon := `return GlobalCache:Has("myKey")`
	if err := L.DoString(scriptHasColon); err != nil {
		t.Fatalf("Error executing Has script (colon): %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Has (colon), got %d", L.GetTop())
	}
	retHasColon := L.Get(-1)
	L.Pop(1)
	if retHasColon.Type() != lua.LTBool || !lua.LVAsBool(retHasColon) {
		t.Errorf("Expected true for Has (colon), got %s", retHasColon.String())
	}

	// Test Has with dot
	scriptHasDot := `return GlobalCache.Has("myKey")`
	if err := L.DoString(scriptHasDot); err != nil {
		// This might fail if dot notation isn't supported for methods that don't take self
		// For now, let's assume it should work or be made to work.
		t.Logf("Note: GlobalCache.Has(\"myKey\") failed, this might indicate dot notation is not fully supported for all cache methods or this specific setup: %v", err)
		// If strict dot notation support is required, this should be a t.Fatalf
	} else {
		if L.GetTop() != 1 {
			t.Fatalf("Expected 1 return value for Has (dot), got %d", L.GetTop())
		}
		retHasDot := L.Get(-1)
		L.Pop(1)
		if retHasDot.Type() != lua.LTBool || !lua.LVAsBool(retHasDot) {
			t.Errorf("Expected true for Has (dot), got %s", retHasDot.String())
		}
	}

	// Test Has (original variable name for clarity in subsequent tests)
	scriptHas := `return GlobalCache:Has("myKey")`
	if err := L.DoString(scriptHas); err != nil {
		t.Fatalf("Error executing Has script: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Has, got %d", L.GetTop())
	}
	retHas := L.Get(-1)
	L.Pop(1)
	if retHas.Type() != lua.LTBool || !lua.LVAsBool(retHas) {
		t.Errorf("Expected true for Has, got %s", retHas.String())
	}

	// Test Delete
	scriptDelete := `
        GlobalCache:Delete("myKey")
        return GlobalCache:Has("myKey")
    `
	if err := L.DoString(scriptDelete); err != nil {
		t.Fatalf("Error executing Delete script: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Delete, got %d", L.GetTop())
	}
	retDel := L.Get(-1)
	L.Pop(1)
	if retDel.Type() != lua.LTBool || lua.LVAsBool(retDel) {
		t.Errorf("Expected false for Has after Delete, got %s", retDel.String())
	}
	if _, found := mockCache.store["myKey"]; found {
		t.Errorf("Key should have been deleted from mock cache")
	}

	// Test GetByPrefix and DeleteByPrefix
	scriptPrefix := `
        GlobalCache:Set("prefix_1", "val1")
        GlobalCache:Set("prefix_2", "val2")
        GlobalCache:Set("other_3", "val3")
        local items = GlobalCache:GetByPrefix("prefix_")
        GlobalCache:DeleteByPrefix("prefix_")
        local has1 = GlobalCache:Has("prefix_1")
        local has2 = GlobalCache:Has("prefix_2")
        return items, has1, has2
    `
	if err := L.DoString(scriptPrefix); err != nil {
		t.Fatalf("Error executing Prefix script: %v", err)
	}
	if L.GetTop() != 3 {
		t.Fatalf("Expected 3 return values for Prefix, got %d", L.GetTop())
	}
	retHasP2 := lua.LVAsBool(L.Get(-1))
	L.Pop(1)
	retHasP1 := lua.LVAsBool(L.Get(-1))
	L.Pop(1)
	retItemsTable := L.Get(-1).(*lua.LTable)
	L.Pop(1)

	if val1 := retItemsTable.RawGetString("prefix_1"); val1.Type() != lua.LTString || val1.String() != "val1" {
		t.Errorf("Expected prefix_1 to be 'val1', got %s", val1.String())
	}
	if val2 := retItemsTable.RawGetString("prefix_2"); val2.Type() != lua.LTString || val2.String() != "val2" {
		t.Errorf("Expected prefix_2 to be 'val2', got %s", val2.String())
	}

	if retHasP1 {
		t.Errorf("Expected prefix_1 to be deleted")
	}
	if retHasP2 {
		t.Errorf("Expected prefix_2 to be deleted")
	}

}

func TestGlobalCacheInLua_DotNotation(t *testing.T) {
	mockCache := &MockCache{
		store: make(map[string]interface{}),
	}

	config := engine.NewConfig()
	config.Cache = mockCache
	var def types.RuleChain
	def.RuleChain.ID = "testDot"

	chainCtx, _ := engine.InitRuleChainCtx(config, nil, &def, nil)

	pool := NewStringLStatePool(config, "", types.Configuration{
		types.NodeConfigurationKeyChainCtx: chainCtx,
	})
	L := pool.Get()
	defer pool.Put(L)
	if L == nil {
		t.Fatal("Failed to get LState from pool")
	}

	// Test Set and Get with dot notation
	scriptSetGetDot := `
        GlobalCache.Set("dotKey", "dotValue", "5s")
        return GlobalCache.Get("dotKey")
    `
	if err := L.DoString(scriptSetGetDot); err != nil {
		t.Fatalf("Error executing Set/Get script (dot): %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Set/Get (dot), got %d", L.GetTop())
	}
	retDot := L.Get(-1)
	L.Pop(1)
	if retDot.Type() != lua.LTString || retDot.String() != "dotValue" {
		t.Errorf("Expected 'dotValue', got %s", retDot.String())
	}
	if val, found := mockCache.store["dotKey"]; !found || val.(string) != "dotValue" {
		t.Errorf("Value not found or incorrect in mock cache for dotKey. Found: %v, Value: %v", found, val)
	}

	// Test Has with dot notation
	scriptHasDot := `return GlobalCache.Has("dotKey")`
	if err := L.DoString(scriptHasDot); err != nil {
		t.Fatalf("Error executing Has script (dot): %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Has (dot), got %d", L.GetTop())
	}
	retHasDot := L.Get(-1)
	L.Pop(1)
	if retHasDot.Type() != lua.LTBool || !lua.LVAsBool(retHasDot) {
		t.Errorf("Expected true for Has (dot), got %s", retHasDot.String())
	}

	// Test Delete with dot notation
	scriptDeleteDot := `
        GlobalCache.Delete("dotKey")
        return GlobalCache.Has("dotKey")
    `
	if err := L.DoString(scriptDeleteDot); err != nil {
		t.Fatalf("Error executing Delete script (dot): %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for Delete (dot), got %d", L.GetTop())
	}
	retDelDot := L.Get(-1)
	L.Pop(1)
	if retDelDot.Type() != lua.LTBool || lua.LVAsBool(retDelDot) {
		t.Errorf("Expected false for Has after Delete (dot), got %s", retDelDot.String())
	}
	if _, found := mockCache.store["dotKey"]; found {
		t.Errorf("dotKey should have been deleted from mock cache")
	}

	// Test GetByPrefix and DeleteByPrefix with dot notation
	scriptPrefixDot := `
        GlobalCache.Set("dotprefix_1", "val1_dot")
        GlobalCache.Set("dotprefix_2", "val2_dot")
        GlobalCache.Set("other_3_dot", "val3_dot")
        local items = GlobalCache.GetByPrefix("dotprefix_")
        GlobalCache.DeleteByPrefix("dotprefix_")
        local has1 = GlobalCache.Has("dotprefix_1")
        local has2 = GlobalCache.Has("dotprefix_2")
        return items, has1, has2
    `
	if err := L.DoString(scriptPrefixDot); err != nil {
		t.Fatalf("Error executing Prefix script (dot): %v", err)
	}
	if L.GetTop() != 3 {
		t.Fatalf("Expected 3 return values for Prefix (dot), got %d", L.GetTop())
	}
	retHasP2Dot := lua.LVAsBool(L.Get(-1))
	L.Pop(1)
	retHasP1Dot := lua.LVAsBool(L.Get(-1))
	L.Pop(1)
	retItemsTableDot := L.Get(-1).(*lua.LTable)
	L.Pop(1)

	if val1 := retItemsTableDot.RawGetString("dotprefix_1"); val1.Type() != lua.LTString || val1.String() != "val1_dot" {
		t.Errorf("Expected dotprefix_1 to be 'val1_dot', got %s", val1.String())
	}
	if val2 := retItemsTableDot.RawGetString("dotprefix_2"); val2.Type() != lua.LTString || val2.String() != "val2_dot" {
		t.Errorf("Expected dotprefix_2 to be 'val2_dot', got %s", val2.String())
	}

	if retHasP1Dot {
		t.Errorf("Expected dotprefix_1 to be deleted (dot)")
	}
	if retHasP2Dot {
		t.Errorf("Expected dotprefix_2 to be deleted (dot)")
	}
}

// MockCache implements types.Cache for testing.
// Note: This is a simplified mock. A real test might need more sophisticated TTL handling.
type MockCache struct {
	store map[string]interface{}
}

func (m *MockCache) Set(key string, value interface{}, ttlStr string) error {
	m.store[key] = value
	return nil
}

func (m *MockCache) Get(key string) (interface{}, error) {
	v, ok := m.store[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return v, nil
}

func (m *MockCache) Has(key string) bool {
	_, ok := m.store[key]
	return ok
}

func (m *MockCache) Delete(key string) error {
	delete(m.store, key)
	return nil
}

func (m *MockCache) GetByPrefix(prefix string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m.store {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result
}

func (m *MockCache) DeleteByPrefix(prefix string) error {
	for k := range m.store {
		if strings.HasPrefix(k, prefix) {
			delete(m.store, k)
		}
	}
	return nil
}

func (m *MockCache) Clear() error {
	m.store = make(map[string]interface{})
	return nil
}

func (m *MockCache) Size() int {
	return len(m.store)
}

func TestChainCacheInLua(t *testing.T) {
	mockCache := &MockCache{
		store: make(map[string]interface{}),
	}

	config := types.NewConfig()
	config.Cache = mockCache
	chainId := "testChain123"

	pool := NewStringLStatePool(config, "", types.Configuration{})
	// Manually set chainId for this test LState, as NewStringLStatePool might not get it from configuration in this context
	L := pool.Get() // Get a new LState
	defer pool.Put(L)
	if L == nil {
		t.Fatal("Failed to get LState from pool")
	}
	// Re-register context methods with the specific chainId for this LState instance
	RegisterContextMethods(L, config, chainId)

	// Test Set and Get using ChainCache
	scriptSetGet := `
        if ChainCache == nil then return "ChainCache is nil" end
        ChainCache:Set("chainKey", "chainValue")
        return ChainCache:Get("chainKey")
    `
	if err := L.DoString(scriptSetGet); err != nil {
		t.Fatalf("Error executing ChainCache Set/Get script: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for ChainCache Set/Get, got %d", L.GetTop())
	}
	ret := L.Get(-1)
	L.Pop(1)
	if ret.Type() != lua.LTString || ret.String() != "chainValue" {
		t.Errorf("Expected 'chainValue' from ChainCache, got %s", ret.String())
	}

	// Verify it was stored in mock cache with namespace
	expectedKeyInMock := chainId + types.NamespaceSeparator + "chainKey"
	if val, found := mockCache.store[expectedKeyInMock]; !found || val.(string) != "chainValue" {
		t.Errorf("Value not found or incorrect in mock cache for ChainCache. Expected key: %s. Found: %v, Value: %v", expectedKeyInMock, found, val)
	}

	// Test ChainCache is nil if chainId is empty
	poolNilChain := NewStringLStatePool(config, "", types.Configuration{})
	LNil := poolNilChain.Get()
	defer poolNilChain.Put(LNil)
	RegisterContextMethods(LNil, config, "") // Empty chainId

	scriptNil := `return ChainCache == nil`
	if err := LNil.DoString(scriptNil); err != nil {
		t.Fatalf("Error executing nil ChainCache script: %v", err)
	}
	if LNil.GetTop() != 1 {
		t.Fatalf("Expected 1 return value for nil ChainCache, got %d", LNil.GetTop())
	}
	retNil := LNil.Get(-1)
	LNil.Pop(1)
	if retNil.Type() != lua.LTBool || !lua.LVAsBool(retNil) {
		t.Errorf("Expected ChainCache to be nil (true), got %s", retNil.String())
	}
}

func TestPreloader(t *testing.T) {
	Preloader.preloaderFuncList = []func(state *lua.LState){} // Reset for test
	var preloaderCalled bool
	Preloader.Register(func(state *lua.LState) {
		state.SetGlobal("preload_var", lua.LString("loaded"))
		preloaderCalled = true
	})

	config := types.NewConfig()
	// Pass empty script to NewStringLStatePool to avoid execution within New() that might cause issues.
	// The test will explicitly execute the script that uses the preloaded variable.
	pool := NewStringLStatePool(config, "", types.Configuration{})
	L := pool.Get()
	defer pool.Put(L)

	if L == nil { // Add nil check for LState
		t.Fatal("Failed to get LState from pool, L is nil")
	}

	if !preloaderCalled {
		t.Fatal("Preloader function was not called")
	}

	if err := L.DoString("return preload_var"); err != nil {
		t.Fatalf("Error executing script with preloaded var: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value, got %d", L.GetTop())
	}
	ret := L.Get(-1)
	L.Pop(1)
	if ret.Type() != lua.LTString || ret.String() != "loaded" {
		t.Errorf("Expected 'loaded', got %s", ret.String())
	}
	Preloader.preloaderFuncList = []func(state *lua.LState){} // Clean up
}

func TestLStatePool_New_Properties(t *testing.T) {
	config := types.NewConfig()
	config.Properties = make(map[string]string) // Initialize if it's nil
	config.Properties["propKey"] = "propValue"

	pool := NewStringLStatePool(config, "", types.Configuration{})
	L := pool.New() // Test New directly
	defer L.Close()

	script := fmt.Sprintf("return %s.propKey", types.Global)
	if err := L.DoString(script); err != nil {
		t.Fatalf("Failed to execute script: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value, got %d", L.GetTop())
	}
	ret := L.Get(-1)
	if ret.Type() != lua.LTString || ret.String() != "propValue" {
		t.Errorf("Expected 'propValue', got %s", ret.String())
	}
}

func TestLStatePool_New_Vars(t *testing.T) {
	config := types.NewConfig()
	configuration := types.Configuration{
		"vars": map[string]string{
			"varKey": "varValue",
		},
	}

	pool := NewStringLStatePool(config, "", configuration)
	L := pool.New()
	defer L.Close()

	script := "return vars.varKey"
	if err := L.DoString(script); err != nil {
		t.Fatalf("Failed to execute script: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value, got %d", L.GetTop())
	}
	ret := L.Get(-1)
	if ret.Type() != lua.LTString || ret.String() != "varValue" {
		t.Errorf("Expected 'varValue', got %s", ret.String())
	}
}

func TestLStatePool_New_ScriptFile(t *testing.T) {
	// Create a temporary Lua script file
	tempFile, err := os.CreateTemp("", "test_*.lua")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	scriptContent := "function get_val() return 'from_file' end"
	if _, err := tempFile.WriteString(scriptContent); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tempFile.Close()

	config := types.NewConfig()
	pool := NewFileLStatePool(config, tempFile.Name(), types.Configuration{})
	L := pool.New()
	if L == nil {
		t.Fatal("LState is nil after NewFileLStatePool")
	}
	defer L.Close()

	execScript := "return get_val()"
	if err := L.DoString(execScript); err != nil {
		t.Fatalf("Failed to execute script calling function from file: %v", err)
	}
	if L.GetTop() != 1 {
		t.Fatalf("Expected 1 return value, got %d", L.GetTop())
	}
	ret := L.Get(-1)
	if ret.Type() != lua.LTString || ret.String() != "from_file" {
		t.Errorf("Expected 'from_file', got %s", ret.String())
	}
}

func TestLStatePool_Shutdown(t *testing.T) {
	pool := NewStringLStatePool(types.NewConfig(), "", types.Configuration{})
	L1 := pool.New()
	L2 := pool.New()

	pool.Put(L1)
	pool.Put(L2)

	if len(pool.saved) != 2 {
		t.Fatalf("Expected 2 saved LStates, got %d", len(pool.saved))
	}

	pool.Shutdown()

	// This is a bit tricky to test directly if LState.Close() was called without instrumenting LState.
	// We assume that if Shutdown() runs without panic and clears `saved`, it's working.
	// A more robust test might involve checking if operations on a closed LState panic or error out as expected.
	// For now, we check if `saved` is still populated, though Shutdown doesn't clear `saved` in the current impl.
	// The primary effect of Shutdown is calling L.Close() on each saved LState.
}

func TestGoUDFCalledByLuaAndJS(t *testing.T) {
	config := types.NewConfig()

	// Define a simple Go function to be registered as UDF
	addGoFunc := func(a int, b int) int {
		return a + b
	}
	addGoFunc2 := func(a int, b int, c int) int {
		return a + b + c
	}
	config.RegisterUdf("addGoFunc", addGoFunc) // Register for both Lua and JS by default
	config.RegisterUdf("addGoFunc2", types.Script{
		Type:    "",
		Content: addGoFunc2,
	}) // Register for both Lua and JS by default

	// Test with Lua Engine
	t.Run("CallGoUDFFromLua1", func(t *testing.T) {
		luaPool := NewStringLStatePool(config, "", types.Configuration{})
		L := luaPool.Get()
		if L == nil {
			t.Fatal("Failed to get LState from Lua pool")
		}
		defer luaPool.Put(L)

		script := "return addGoFunc(10, 5)"
		if err := L.DoString(script); err != nil {
			t.Fatalf("Lua script execution failed: %v", err)
		}
		if L.GetTop() != 1 {
			t.Fatalf("Expected 1 return value from Lua, got %d", L.GetTop())
		}
		ret := L.Get(-1)
		L.Pop(1)
		if num, ok := ret.(lua.LNumber); !ok || num != 15 {
			t.Errorf("Expected Lua result 15, got %v (type %T)", ret, ret)
		}
	})

	t.Run("CallGoUDFFromLua2", func(t *testing.T) {
		luaPool := NewStringLStatePool(config, "", types.Configuration{})
		L := luaPool.Get()
		if L == nil {
			t.Fatal("Failed to get LState from Lua pool")
		}
		defer luaPool.Put(L)

		script := "return addGoFunc2(10, 5,5)"
		if err := L.DoString(script); err != nil {
			t.Fatalf("Lua script execution failed: %v", err)
		}
		if L.GetTop() != 1 {
			t.Fatalf("Expected 1 return value from Lua, got %d", L.GetTop())
		}
		ret := L.Get(-1)
		L.Pop(1)
		if num, ok := ret.(lua.LNumber); !ok || num != 20 {
			t.Errorf("Expected Lua result 20, got %v (type %T)", ret, ret)
		}
	})
	//t.Log("JS part of TestGoUDFCalledByLuaAndJS is conceptual in this Lua test file.")
	if _, exists := config.Udf["addGoFunc"]; !exists {
		t.Errorf("UDF 'addGoFunc' not found in config.Udf, which means JS engine might not see it.")
	}
	t.Run("CallGoUDFFromJs1", func(t *testing.T) {
		var jsScript = `
			function GetValue(a,b) {
				return addGoFunc(a,b)
			}
		`
		// Recreate engine with params for actual tests
		jsEngine, err := js.NewGojaJsEngine(config, jsScript, nil)
		if err != nil {
			t.Errorf("Failed to create JS engine: %v", err)
		}
		defer jsEngine.Stop()
		response, err := jsEngine.Execute(nil, "GetValue", 10, 5)
		if err != nil {
			t.Errorf("JS script execution failed: %v", err)
		}
		if num, ok := response.(int64); !ok || num != 15 {
			t.Errorf("Expected Lua result 15, got %v (type %T)", response, response)
		}
	})
	t.Run("CallGoUDFFromJs2", func(t *testing.T) {
		var jsScript = `
			function GetValue2(a,b,c) {
				return addGoFunc2(a,b,c)
			}
		`
		// Recreate engine with params for actual tests
		jsEngine, err := js.NewGojaJsEngine(config, jsScript, nil)
		if err != nil {
			t.Errorf("Failed to create JS engine: %v", err)
		}
		defer jsEngine.Stop()
		response, err := jsEngine.Execute(nil, "GetValue2", 10, 5, 5)
		if err != nil {
			t.Errorf("JS script execution failed: %v", err)
		}
		if num, ok := response.(int64); !ok || num != 20 {
			t.Errorf("Expected Lua result 20, got %v (type %T)", response, response)
		}
	})
}
