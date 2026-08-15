package redis

import (
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

const statusTestServer = "127.0.0.1:6379"

func TestClientNodeConnectionStatus(t *testing.T) {
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"Server": statusTestServer,
		"Cmd":    "GET",
		"Params": []interface{}{"k"},
	})
	assert.Nil(t, err)
	if _, err := node.SharedNode.GetSafely(); err != nil {
		t.Skipf("redis server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, node.ConnectionStatus().Status)
	node.Destroy()
	assert.Equal(t, types.StatusDisconnected, node.ConnectionStatus().Status)
}

func TestPublisherNodeConnectionStatus(t *testing.T) {
	var node PublisherNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"Server":  statusTestServer,
		"Channel": "test",
	})
	assert.Nil(t, err)
	if _, err := node.SharedNode.GetSafely(); err != nil {
		t.Skipf("redis server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, node.ConnectionStatus().Status)
	node.Destroy()
	assert.Equal(t, types.StatusDisconnected, node.ConnectionStatus().Status)
}

func TestClientNodeConnectionStatusUnreachable(t *testing.T) {
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"Server": "127.0.0.1:1",
		"Cmd":    "GET",
		"Params": []interface{}{"k"},
	})
	assert.Nil(t, err)
	_, err = node.SharedNode.GetSafely()
	assert.NotNil(t, err)
	info := node.ConnectionStatus()
	assert.Equal(t, types.StatusReconnecting, info.Status)
	assert.True(t, info.Message != "")
}
