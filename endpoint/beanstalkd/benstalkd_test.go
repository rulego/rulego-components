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

package beanstalkd

import (
	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

var testdataFolder = "../../testdata"

func TestBeanstalkdEndpoint(t *testing.T) {
	beanstalkdURL := os.Getenv("BEANSTALKD_URL")
	if beanstalkdURL == "" {
		beanstalkdURL = "127.0.0.1:11300"
	}

	if os.Getenv("SKIP_BEANSTALKD_TESTS") == "true" {
		t.Skip("Skipping beanstalkd tests")
	}

	conn, err := beanstalk.Dial("tcp", beanstalkdURL)
	if err != nil {
		t.Skipf("beanstalkd server not available, skip this test: %v", err)
		return
	}
	defer conn.Close()

	buf, err := os.ReadFile(testdataFolder + "/chain_msg_type_switch.json")
	assert.Nil(t, err)
	config := rulego.NewConfig(types.WithDefaultPool())
	_, err = rulego.New("default", buf, rulego.WithConfig(config))
	assert.Nil(t, err)

	ep, err := endpoint.Registry.New(Type, config, TubesetConfig{
		Server:   beanstalkdURL,
		Tubesets: []string{"test_tube"},
		Timeout:  2,
	},
	)
	assert.Nil(t, err)
	beanstalkdEndpoint := ep.(endpointApi.Endpoint)

	count := int32(0)
	router1 := endpoint.NewRouter().From("").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		assert.Equal(t, "test message", exchange.In.GetMsg().GetData())
		atomic.AddInt32(&count, 1)
		return true
	}).To("chain:default").End()
	router1.SetId("router1")
	router1Id, err := beanstalkdEndpoint.AddRouter(router1)
	assert.Nil(t, err)
	assert.Equal(t, "router1", router1Id)

	// test add duplicate router
	router2 := endpoint.NewRouter().SetId("router2")
	_, err = beanstalkdEndpoint.AddRouter(router2)
	assert.NotNil(t, err)

	err = beanstalkdEndpoint.Start()
	assert.Nil(t, err)

	// Use a beanstalkd client to put a job
	tube := &beanstalk.Tube{Conn: conn, Name: "test_tube"}
	_, err = tube.Put([]byte("test message"), 1, 0, time.Minute)
	assert.Nil(t, err)

	time.Sleep(3 * time.Second)

	assert.Equal(t, int32(1), atomic.LoadInt32(&count))

	// test remove router
	atomic.StoreInt32(&count, 0)
	err = beanstalkdEndpoint.RemoveRouter(router1Id)
	assert.Nil(t, err)

	_, err = tube.Put([]byte("test message"), 1, 0, time.Minute)
	assert.Nil(t, err)

	time.Sleep(3 * time.Second)
	// should not process
	assert.Equal(t, int32(0), atomic.LoadInt32(&count))

	beanstalkdEndpoint.Destroy()
}
