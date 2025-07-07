package grpcstream

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/test/assert"
)

var (
	testdataFolder = "testdata"
	serverCmd      *exec.Cmd
)

// TestMain package-level entry point
func TestMain(m *testing.M) {
	// Skip tests if running in CI without a specific flag
	if os.Getenv("SKIP_GRPC_TESTS") == "true" {
		fmt.Println("Skipping gRPC tests")
		os.Exit(0)
	}

	// Build and start the gRPC server before running tests
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
	fmt.Println("Setting up gRPC test server...")
	//buildPath := filepath.Join(testdataFolder, "api/ble/v1")
	// Generate protobuf code
	// Check if protoc is installed
	if _, err := exec.LookPath("protoc"); err != nil {
		return fmt.Errorf("protoc is not installed or not in PATH: %w", err)
	}
	protoCmd := exec.Command("protoc",
		"--go_out=.",
		"--go_opt=paths=source_relative",
		"--go-grpc_out=.",
		"--go-grpc_opt=paths=source_relative",
		"api/ble/v1/ble.proto",
	)
	protoCmd.Dir = testdataFolder
	output, err := protoCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to generate protobuf code: %v\n%s", err, string(output))
	}
	// Build the server
	serverBin := "ble_server"
	if runtime.GOOS == "windows" {
		serverBin += ".exe"
	}
	buildCmd := exec.Command("go", "build", "-o", serverBin, "main.go")
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
	time.Sleep(2 * time.Second)
	fmt.Println("gRPC test server started.")
	return nil
}

// teardownTestServer stops the gRPC test server.
func teardownTestServer() {
	if serverCmd != nil && serverCmd.Process != nil {
		fmt.Println("Stopping gRPC test server...")
		if err := serverCmd.Process.Kill(); err != nil {
			fmt.Printf("Failed to kill server process: %v\n", err)
		}
	}
}

func TestGrpcStreamEndpoint(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool())
	// Create an endpoint instance
	grpcEndpoint, err := endpoint.Registry.New(Type, config, map[string]interface{}{
		"server":  "127.0.0.1:9000",
		"service": "ble.DataService",
		"method":  "StreamData",
	})
	assert.Nil(t, err)

	// Define a counter for received messages
	var messageCount int32

	// Create a router to handle incoming messages
	router := endpoint.NewRouter().From("*").Process(func(router endpointApi.Router, exchange *endpointApi.Exchange) bool {
		atomic.AddInt32(&messageCount, 1)
		msg := exchange.In.GetMsg()
		t.Logf("Received message: %s", msg.Data)

		var data map[string]interface{}
		err := json.Unmarshal([]byte(msg.GetData()), &data)
		assert.Nil(t, err)

		// Check message content based on type
		msgType, ok := data["type"].(string)
		assert.True(t, ok)
		if msgType == "temperature" {
			assert.Equal(t, "temperature", data["type"])
		} else if msgType == "humidity" {
			assert.Equal(t, "humidity", data["type"])
		}
		return true
	}).End()

	// Add router to the endpoint
	_, err = grpcEndpoint.AddRouter(router)
	assert.Nil(t, err)

	// Start the endpoint
	err = grpcEndpoint.Start()
	assert.Nil(t, err)

	// Wait for messages to be processed
	time.Sleep(3 * time.Second)

	// Verify that messages were received
	assert.True(t, atomic.LoadInt32(&messageCount) > 0)
	t.Logf("Total messages received: %d", atomic.LoadInt32(&messageCount))
	// Destroy the endpoint to test lifecycle
	grpcEndpoint.Destroy()
	// Wait a bit
	time.Sleep(1 * time.Second)
	// Check if router is cleared
	grpcStream, ok := grpcEndpoint.(*GrpcStream)
	assert.True(t, ok)
	assert.Nil(t, grpcStream.Router)
}

func TestGrpcStreamLifecycle(t *testing.T) {
	config := rulego.NewConfig(types.WithDefaultPool())
	configuration := types.Configuration{
		"server":  "127.0.0.1:9000",
		"service": "ble.DataService",
		"method":  "StreamData",
	}

	// Create a new GrpcStream endpoint
	grpcEndpoint := &GrpcStream{}
	err := grpcEndpoint.Init(config, configuration)
	assert.Nil(t, err)

	// Check if initialization is correct
	assert.Equal(t, "127.0.0.1:9000", grpcEndpoint.Config.Server)

	// Add a dummy router
	router := endpoint.NewRouter().From("*").End()
	_, err = grpcEndpoint.AddRouter(router)
	assert.Nil(t, err)
	assert.NotNil(t, grpcEndpoint.Router)

	// Start the endpoint
	err = grpcEndpoint.Start()
	assert.Nil(t, err)

	// Wait a moment
	time.Sleep(500 * time.Millisecond)

	// Destroy the endpoint
	grpcEndpoint.Destroy()

	// Check if resources are released
	assert.Nil(t, grpcEndpoint.Router)
	// After destroy, getting a client should return an error or nil
	_, err = grpcEndpoint.SharedNode.GetSafely()
	assert.NotNil(t, err)
}
