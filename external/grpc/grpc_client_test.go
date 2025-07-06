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

package grpc

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

var (
	testdataFolder = "testdata"
	serverCmd      *exec.Cmd
)

// TestMain package-level entry point
func TestMain(m *testing.M) {
	// Skip tests if the skip flag is set
	if os.Getenv("SKIP_GRPC_TESTS") == "true" {
		fmt.Println("Skipping gRPC tests")
		os.Exit(0)
	}

	// Set up and start the gRPC server before running tests
	if err := setupTestServer(); err != nil {
		fmt.Printf("Failed to set up test server: %v\n", err)
		os.Exit(1)
	}

	// Run the tests
	exitCode := m.Run()

	// Stop the server after tests
	teardownTestServer()

	os.Exit(exitCode)
}

// setupTestServer compiles and starts the gRPC test server.
func setupTestServer() error {
	fmt.Println("Setting up gRPC test server for grpc_client_test...")
	serverPath := "server.go"

	// Check if protoc is installed
	if _, err := exec.LookPath("protoc"); err != nil {
		return fmt.Errorf("protoc is not installed or not in PATH: %w", err)
	}
	// Generate protobuf code
	protoCmd := exec.Command("protoc",
		"--go_out=.",
		"--go_opt=paths=source_relative",
		"--go-grpc_out=.",
		"--go-grpc_opt=paths=source_relative",
		"helloworld/helloworld.proto",
	)
	protoCmd.Dir = testdataFolder
	output, err := protoCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to generate protobuf code: %v\n%s", err, string(output))
	}
	// Build the server
	serverBin := "helloworld_server"
	if runtime.GOOS == "windows" {
		serverBin += ".exe"
	}
	buildCmd := exec.Command("go", "build", "-o", serverBin, serverPath)
	buildCmd.Dir = testdataFolder
	output, err = buildCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to build gRPC server: %v\n%s", err, string(output))
	}

	// Start the server
	serverCmd = exec.Command(filepath.Join(testdataFolder, serverBin))
	if err := serverCmd.Start(); err != nil {
		return fmt.Errorf("failed to start gRPC server: %v", err)
	}
	// Give server time to start
	time.Sleep(3 * time.Second)
	fmt.Println("gRPC test server started for grpc_client_test.")
	return nil
}

// teardownTestServer stops the gRPC test server.
func teardownTestServer() {
	if serverCmd != nil && serverCmd.Process != nil {
		fmt.Println("Stopping gRPC test server for grpc_client_test...")
		if err := serverCmd.Process.Kill(); err != nil {
			fmt.Printf("Failed to kill server process: %v\n", err)
		}
	}
}

func TestClientNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/grpcClient"

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &ClientNode{}, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"request": `{"name2": "helloWorld2"}`,
		}, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"request": `{"name2": "helloWorld2"}`,
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"request": `{"name": "lulu","groupName":"app01"}`,
			"headers": map[string]string{
				"key1": "value1",
			},
		}, Registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "SayHello",
			"headers": map[string]string{
				"key1": "${fromId}",
			},
		}, Registry)
		assert.Nil(t, err)

		node3, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  "127.0.0.1:50051",
			"service": "helloworld.Greeter",
			"method":  "${metadata.method}",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("service", "helloworld.Greeter")
		metaData.PutValue("method", "SayHello")
		metaData.PutValue("fromId", "aa")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT1",
				Data:     "{\"name\": \"lala\"}",
			},
		}
		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, "{\"message\":\"Hello lulu\",\"groupName\":\"app01\"}\n", msg.GetData())
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, "{\"message\":\"Hello lala\"}\n", msg.GetData())
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node3,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, "{\"message\":\"Hello lala\"}\n", msg.GetData())
					assert.Equal(t, types.Success, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Second * 3)
	})
}
