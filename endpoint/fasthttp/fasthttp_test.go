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

package fasthttp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	endpointApi "github.com/rulego/rulego/api/types/endpoint"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/maps"
)

var testdataFolder = "../../testdata/rule"
var testServer = ":9092"
var testConfigServer = ":9093"

// Test request/response messages
func TestFastHttpMessage(t *testing.T) {
	t.Run("Request", func(t *testing.T) {
		var request = &RequestMessage{}
		test.EndpointMessage(t, request)
	})
	t.Run("Response", func(t *testing.T) {
		var response = &ResponseMessage{}
		test.EndpointMessage(t, response)
	})
}

func TestRouterId(t *testing.T) {
	config := types.NewConfig()
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server: testServer,
	}, &nodeConfig)
	var ep = &Endpoint{}
	err := ep.Init(config, nodeConfig)
	assert.Nil(t, err)
	assert.Equal(t, testServer, ep.Id())
	router := impl.NewRouter().SetId("r1").From("/device/info").End()
	routerId, _ := ep.AddRouter(router, "GET")
	assert.Equal(t, "r1", routerId)

	router = impl.NewRouter().From("/device/info").End()
	routerId, _ = ep.AddRouter(router, "POST")
	assert.Equal(t, "POST:/device/info", routerId)

	err = ep.RemoveRouter("r1")
	assert.Nil(t, err)
	err = ep.RemoveRouter("POST:/device/info")
	assert.Nil(t, err)
	err = ep.RemoveRouter("GET:/device/info")
	assert.Equal(t, fmt.Sprintf("router: %s not found", "GET:/device/info"), err.Error())
}

func TestFastHttpEndpointConfig(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())
	//Create the fastHTTP endpoint service
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server: testConfigServer,
	}, &nodeConfig)
	var epStarted = &Endpoint{}
	err := epStarted.Init(config, nodeConfig)

	assert.Equal(t, testConfigServer, epStarted.Id())
	err = epStarted.Start()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 200)

	var epErr = &Endpoint{}
	err = epErr.Init(config, nodeConfig)

	_, err = epErr.AddRouter(nil, "POST")
	assert.Equal(t, "router can not nil", err.Error())

	fasthttpEndpoint := &Endpoint{}
	err = fasthttpEndpoint.Init(config, nodeConfig)

	assert.Equal(t, testConfigServer, fasthttpEndpoint.Id())
	testUrl := "/api/test"
	router := impl.NewRouter().From(testUrl).End()
	_, err = fasthttpEndpoint.AddRouter(router)
	assert.Equal(t, "need to specify HTTP method", err.Error())

	router = impl.NewRouter().From(testUrl).End()
	routerId, err := fasthttpEndpoint.AddRouter(router, "POST")
	assert.Equal(t, "POST:/api/test", routerId)

	assert.Nil(t, err)
	assert.True(t, fasthttpEndpoint.HasRouter(routerId))

	epStarted.Destroy()
}

func TestFastHttpEndpoint(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())
	//Create the fastHTTP endpoint service
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server: ":9094",
	}, &nodeConfig)
	fasthttpEndpoint := &Endpoint{}
	err := fasthttpEndpoint.Init(config, nodeConfig)
	assert.Nil(t, err)

	//Start the server
	err = fasthttpEndpoint.Start()
	assert.Nil(t, err)
	time.Sleep(time.Millisecond * 200)

	//GET the test
	testGetUrl := "/api/v1/test/:id"
	getRouter := impl.NewRouter().From(testGetUrl).Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
		msg.SetData(fmt.Sprintf(`{"method":"%s","data":%s}`, "GET", msg.GetData()))
		return msg
	})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
		exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
		return true
	}).End()
	_, err = fasthttpEndpoint.AddRouter(getRouter, "GET")
	assert.Nil(t, err)

	//POST testing
	testPostUrl := "/api/v1/test"
	postRouter := impl.NewRouter().From(testPostUrl).Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
		msg.SetData(fmt.Sprintf(`{"method":"%s","data":%s}`, "POST", msg.GetData()))
		return msg
	})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
		exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
		return true
	}).End()
	_, err = fasthttpEndpoint.AddRouter(postRouter, "POST")
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 200)

	// Test the GET request
	resp, err := http.Get("http://localhost:9094/api/v1/test/123?name=test")
	assert.Nil(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	assert.Nil(t, err)
	t.Logf("GET response: %s", string(body))

	// Test POST requests
	postData := `{"name":"test","value":123}`
	resp, err = http.Post("http://localhost:9094/api/v1/test", "application/json", bytes.NewBufferString(postData))
	assert.Nil(t, err)
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	assert.Nil(t, err)
	t.Logf("POST response: %s", string(body))

	fasthttpEndpoint.Destroy()
}

// Testing the static file service functionality
func TestFastHttpStaticFiles(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server: ":9100",
	}, &nodeConfig)
	fasthttpEndpoint := &Endpoint{}
	err := fasthttpEndpoint.Init(config, nodeConfig)
	assert.Nil(t, err)

	// Start the server
	err = fasthttpEndpoint.Start()
	assert.Nil(t, err)
	time.Sleep(time.Millisecond * 200)

	// Configure static file mappings - using relative paths
	resourceMapping := "/static=testdata/static,/assets=testdata/static/css"
	fasthttpEndpoint.RegisterStaticFiles(resourceMapping)

	time.Sleep(time.Millisecond * 100)

	// Test the JSON file
	t.Run("JSON File", func(t *testing.T) {
		resp, err := http.Get("http://localhost:9100/static/data.json")
		assert.Nil(t, err)
		defer resp.Body.Close()

		assert.Equal(t, 200, resp.StatusCode)
		assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

		body, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)

		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		assert.Nil(t, err)
		assert.Equal(t, "Hello from static JSON file", data["message"])
		assert.Equal(t, "success", data["status"])
		//t.Logf("JSON response: %s", string(body))
	})

	// Test documents that don't exist
	t.Run("Not Found File", func(t *testing.T) {
		resp, err := http.Get("http://localhost:9100/static/nonexistent.txt")
		assert.Nil(t, err)
		defer resp.Body.Close()

		assert.Equal(t, 404, resp.StatusCode)
		//t.Logf("404 response status: %d", resp.StatusCode)
	})

	// Test directory access (should return index.html or directory list)
	t.Run("Directory Access", func(t *testing.T) {
		resp, err := http.Get("http://localhost:9100/static/")
		assert.Nil(t, err)
		defer resp.Body.Close()

		// It should return index.html or a list of directories
		assert.True(t, resp.StatusCode == 200 || resp.StatusCode == 404)
		t.Logf("Directory access status: %d", resp.StatusCode)
	})

	fasthttpEndpoint.Destroy()
}

// Testing CORS functionality
func TestFastHttpCORS(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server:    ":9098",
		AllowCors: true,
	}, &nodeConfig)
	fasthttpEndpoint := &Endpoint{}
	err := fasthttpEndpoint.Init(config, nodeConfig)
	assert.Nil(t, err)

	// Add test routes
	router := impl.NewRouter().From("/cors").Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
		msg.SetData(`{"cors":"enabled"}`)
		return msg
	})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
		exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
		return true
	}).End()
	_, err = fasthttpEndpoint.AddRouter(router, "POST")
	assert.Nil(t, err)

	err = fasthttpEndpoint.Start()
	assert.Nil(t, err)
	time.Sleep(time.Millisecond * 200)

	// Test the OPTIONS request
	client := &http.Client{}
	req, _ := http.NewRequest("OPTIONS", "http://localhost:9098/cors", nil)
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := client.Do(req)
	assert.Nil(t, err)
	defer resp.Body.Close()

	// Verify the CORS head
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Headers"))

	// Test the actual POST request
	resp, err = http.Post("http://localhost:9098/cors", "application/json", bytes.NewBufferString(`{"test":"cors"}`))
	assert.Nil(t, err)
	defer resp.Body.Close()

	// Verify the CORS head in the response
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))

	fasthttpEndpoint.Destroy()
}

// Test configuration parameters
func TestFastHttpConfig(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server:           ":9097",
		ReadTimeout:      5,    // 5 seconds
		WriteTimeout:     5,    // 5 seconds
		IdleTimeout:      30,   // 30 seconds
		MaxRequestSize:   "2M", // 2MB
		Concurrency:      512,
		DisableKeepalive: false,
	}, &nodeConfig)
	fasthttpEndpoint := &Endpoint{}
	err := fasthttpEndpoint.Init(config, nodeConfig)
	assert.Nil(t, err)

	// Verify configuration
	assert.Equal(t, ":9097", fasthttpEndpoint.Config.Server)
	assert.Equal(t, 5, fasthttpEndpoint.Config.ReadTimeout)
	assert.Equal(t, 5, fasthttpEndpoint.Config.WriteTimeout)
	assert.Equal(t, 30, fasthttpEndpoint.Config.IdleTimeout)
	assert.Equal(t, "2M", fasthttpEndpoint.Config.MaxRequestSize)
	assert.Equal(t, 512, fasthttpEndpoint.Config.Concurrency)
	assert.Equal(t, false, fasthttpEndpoint.Config.DisableKeepalive)

	err = fasthttpEndpoint.Start()
	assert.Nil(t, err)

	// Verify server configuration
	server := fasthttpEndpoint.GetServer()
	assert.NotNil(t, server)
	assert.Equal(t, 5*time.Second, server.ReadTimeout)
	assert.Equal(t, 5*time.Second, server.WriteTimeout)
	assert.Equal(t, 30*time.Second, server.IdleTimeout)
	assert.Equal(t, 2*1024*1024, server.MaxRequestBodySize)
	assert.Equal(t, 512, server.Concurrency)
	assert.Equal(t, false, server.DisableKeepalive)

	fasthttpEndpoint.Destroy()
}

// Test the hot update (Restart) feature
func TestFastHttpRestart(t *testing.T) {
	config := engine.NewConfig(types.WithDefaultPool())
	var nodeConfig = make(types.Configuration)
	_ = maps.Map2Struct(&Config{
		Server: ":9099",
	}, &nodeConfig)
	fasthttpEndpoint := &Endpoint{}
	err := fasthttpEndpoint.Init(config, nodeConfig)
	assert.Nil(t, err)

	// Start the server
	err = fasthttpEndpoint.Start()
	assert.Nil(t, err)
	time.Sleep(time.Millisecond * 200)

	// Add the first route
	testUrl1 := "/api/v1/test1"
	router1 := impl.NewRouter().From(testUrl1).Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
		msg.SetData(`{"route":"test1","data":"` + msg.GetData() + `"}`)
		return msg
	})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
		exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
		return true
	}).End()
	_, err = fasthttpEndpoint.AddRouter(router1, "GET")
	assert.Nil(t, err)

	// Add a second route
	testUrl2 := "/api/v1/test2"
	router2 := impl.NewRouter().From(testUrl2).Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
		msg.SetData(`{"route":"test2","data":"` + msg.GetData() + `"}`)
		return msg
	})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
		exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
		return true
	}).End()
	_, err = fasthttpEndpoint.AddRouter(router2, "POST")
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 200)

	// Test whether the route before the restart is working properly
	t.Run("BeforeRestart", func(t *testing.T) {
		// Test GET routing
		resp, err := http.Get("http://localhost:9099/api/v1/test1?param=value1")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, 200, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)
		t.Logf("GET response before restart: %s", string(body))
		assert.True(t, strings.Contains(string(body), "test1"))

		// Test POST routing
		postData := `{"test":"data"}`
		resp, err = http.Post("http://localhost:9099/api/v1/test2", "application/json", bytes.NewBufferString(postData))
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, 200, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		assert.Nil(t, err)
		t.Logf("POST response before restart: %s", string(body))
		assert.True(t, strings.Contains(string(body), "test2"))
	})

	// Perform a hot update restart
	t.Run("Restart", func(t *testing.T) {
		err = fasthttpEndpoint.Restart()
		assert.Nil(t, err)
		time.Sleep(time.Millisecond * 500) // Wait for the restart to complete
	})

	// Test whether the route is still working properly after restarting
	t.Run("AfterRestart", func(t *testing.T) {
		// Test GET routing
		resp, err := http.Get("http://localhost:9099/api/v1/test1?param=value2")
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, 200, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)
		t.Logf("GET response after restart: %s", string(body))
		assert.True(t, strings.Contains(string(body), "test1"))

		// Test POST routing
		postData := `{"test":"data_after_restart"}`
		resp, err = http.Post("http://localhost:9099/api/v1/test2", "application/json", bytes.NewBufferString(postData))
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, 200, resp.StatusCode)
		body, err = io.ReadAll(resp.Body)
		assert.Nil(t, err)
		t.Logf("POST response after restart: %s", string(body))
		assert.True(t, strings.Contains(string(body), "test2"))
	})

	// Test adding new routes after reboot
	t.Run("AddNewRouterAfterRestart", func(t *testing.T) {
		testUrl3 := "/api/v1/test3"
		router3 := impl.NewRouter().From(testUrl3).Transform(transformMsg(func(ctx types.RuleContext, msg types.RuleMsg) types.RuleMsg {
			msg.SetData(`{"route":"test3_new","data":"` + msg.GetData() + `"}`)
			return msg
		})).Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
			exchange.Out.Headers().Set(ContentTypeKey, JsonContextType)
			exchange.Out.SetBody([]byte(exchange.In.GetMsg().GetData()))
			return true
		}).End()
		_, err = fasthttpEndpoint.AddRouter(router3, "PUT")
		assert.Nil(t, err)

		time.Sleep(time.Millisecond * 200)

		// Test newly added routes
		client := &http.Client{}
		req, _ := http.NewRequest("PUT", "http://localhost:9099/api/v1/test3", bytes.NewBufferString(`{"new":"route"}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		assert.Nil(t, err)
		defer resp.Body.Close()
		assert.Equal(t, 200, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		assert.Nil(t, err)
		t.Logf("PUT response for new router: %s", string(body))
		assert.True(t, strings.Contains(string(body), "test3_new"))
	})

	// Verify the number of routes
	t.Run("VerifyRouterCount", func(t *testing.T) {
		// There should be three routes: GET /api/v1/test1, POST /api/v1/test2, PUT /api/v1/test3
		assert.True(t, fasthttpEndpoint.HasRouter("GET:/api/v1/test1"))
		assert.True(t, fasthttpEndpoint.HasRouter("POST:/api/v1/test2"))
		assert.True(t, fasthttpEndpoint.HasRouter("PUT:/api/v1/test3"))
	})

	fasthttpEndpoint.Destroy()
}

// Test the parseSize function
func TestParseSize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
		hasError bool
	}{
		// Normal situation
		{"空字符串", "", 4 * 1024 * 1024, false},
		{"纯空格", "   ", 4 * 1024 * 1024, false},
		{"字节单位", "1024", 1024, false},
		{"字节单位B", "1024B", 1024, false},
		{"KB单位", "1K", 1024, false},
		{"KB单位完整", "1KB", 1024, false},
		{"MB单位", "1M", 1024 * 1024, false},
		{"MB单位完整", "1MB", 1024 * 1024, false},
		{"GB单位", "1G", 1024 * 1024 * 1024, false},
		{"GB单位完整", "1GB", 1024 * 1024 * 1024, false},
		{"小数KB", "1.5K", int(1.5 * 1024), false},
		{"小数MB", "2.5M", int(2.5 * 1024 * 1024), false},
		{"小写单位", "4m", 4 * 1024 * 1024, false},
		{"混合大小写", "8Mb", 8 * 1024 * 1024, false},
		{"带空格", " 512 K ", 512 * 1024, false},
		{"零值", "0", 0, false},
		{"零值KB", "0K", 0, false},

		// Error case
		{"无效格式", "abc", 0, true},
		{"无效数字", "abc123K", 0, true},
		{"无效单位", "123X", 0, true},
		{"无效单位TB", "1TB", 0, true},
		{"只有单位", "K", 0, true},
		{"负数", "-1K", 0, true},
		{"多个小数点", "1.2.3K", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseSize(tt.input)

			if tt.hasError {
				assert.NotNil(t, err, "期望有错误，但没有错误")
			} else {
				assert.Nil(t, err, "不期望有错误，但出现了错误: %v", err)
				assert.Equal(t, tt.expected, result, "解析结果不匹配")
			}
		})
	}
}

// Test the parseSize boundary situation
func TestParseSizeBoundary(t *testing.T) {
	// Test the maximum value
	maxGB := "4G"
	result, err := parseSize(maxGB)
	assert.Nil(t, err)
	assert.Equal(t, 4*1024*1024*1024, result)

	// Testing decimal accuracy
	precision := "1.999M"
	result, err = parseSize(precision)
	assert.Nil(t, err)
	expected := 2096103 // int(1.999 * 1024 * 1024)
	assert.Equal(t, expected, result)

	// Test compatibility with various formats
	formats := map[string]int{
		"1024": 1024,
		"1K":   1024,
		"1KB":  1024,
		"1k":   1024,
		"1kb":  1024,
		"1M":   1024 * 1024,
		"1MB":  1024 * 1024,
		"1m":   1024 * 1024,
		"1mb":  1024 * 1024,
	}

	for input, expected := range formats {
		t.Run(fmt.Sprintf("格式_%s", input), func(t *testing.T) {
			result, err := parseSize(input)
			assert.Nil(t, err, "解析 %s 时出错: %v", input, err)
			assert.Equal(t, expected, result, "解析 %s 的结果不正确", input)
		})
	}
}
