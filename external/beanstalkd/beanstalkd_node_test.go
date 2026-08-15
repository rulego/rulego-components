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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func beanstalkdTestServer(t *testing.T) string {
	if os.Getenv("SKIP_BEANSTALKD_TESTS") == "true" {
		t.Skip("Skipping beanstalkd tests")
	}
	server := os.Getenv("BEANSTALKD_URL")
	if server == "" {
		server = "127.0.0.1:11300"
	}
	conn, err := beanstalk.Dial("tcp", server)
	if err != nil {
		t.Skipf("beanstalkd server not available, skip this test: %v", err)
	}
	_ = conn.Close()
	return server
}

// 本地模式下取连接曾与 GetSafely 的内部锁死锁，此测试回归该问题。
func TestBeanstalkdNodeNoDeadlock(t *testing.T) {
	server := beanstalkdTestServer(t)

	node := &TubeNode{}
	err := node.Init(types.NewConfig(), types.Configuration{
		"server": server,
		"tube":   fmt.Sprintf("test_node_%d", time.Now().UnixNano()),
		"cmd":    Put,
		"body":   "hello",
	})
	assert.Nil(t, err)

	done := make(chan struct{})
	msgList := []test.Msg{{DataType: types.JSON, MetaData: types.NewMetadata(), MsgType: "TEST", Data: "hello"}}
	go func() {
		defer close(done)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			assert.Nil(t, err)
			assert.Equal(t, types.Success, relationType)
		})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("OnMsg deadlock: GetSafely blocked by node-level lock")
	}
}

func TestWorkerNodeStaticJobId(t *testing.T) {
	server := beanstalkdTestServer(t)

	tube := fmt.Sprintf("test_worker_%d", time.Now().UnixNano())
	putNode := &TubeNode{}
	err := putNode.Init(types.NewConfig(), types.Configuration{
		"server": server,
		"tube":   tube,
		"cmd":    Put,
		"body":   "hello",
	})
	assert.Nil(t, err)

	var jobId string
	putDone := make(chan struct{})
	msgList := []test.Msg{{DataType: types.JSON, MetaData: types.NewMetadata(), MsgType: "TEST", Data: "hello"}}
	test.NodeOnMsg(t, putNode, msgList, func(msg types.RuleMsg, relationType string, err error) {
		assert.Nil(t, err)
		jobId = msg.Metadata.GetValue("id")
		close(putDone)
	})
	<-putDone
	assert.True(t, jobId != "")

	worker := &WorkerNode{}
	err = worker.Init(types.NewConfig(), types.Configuration{
		"server": server,
		"cmd":    StatsJob,
		"jobId":  jobId,
	})
	assert.Nil(t, err)

	workerDone := make(chan struct{})
	test.NodeOnMsg(t, worker, msgList, func(msg types.RuleMsg, relationType string, err error) {
		assert.Nil(t, err)
		assert.Equal(t, types.Success, relationType)
		close(workerDone)
	})
	<-workerDone
}
