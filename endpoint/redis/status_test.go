package redis

import (
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

func TestEndpointConnectionStatus(t *testing.T) {
	endpoint := &Redis{}
	config := types.NewConfig()
	err := endpoint.Init(config, types.Configuration{
		"Server": redisServer,
	})
	assert.Nil(t, err)
	if _, err := endpoint.SharedNode.GetSafely(); err != nil {
		t.Skipf("redis server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, endpoint.ConnectionStatus().Status)
	endpoint.Destroy()
	assert.Equal(t, types.StatusDisconnected, endpoint.ConnectionStatus().Status)
}
