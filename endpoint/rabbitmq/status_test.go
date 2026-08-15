package rabbitmq

import (
	"os"
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

func TestEndpointConnectionStatus(t *testing.T) {
	if os.Getenv("SKIP_RABBITMQ_TESTS") == "true" {
		t.Skip("Skipping RabbitMQ tests")
	}
	server := os.Getenv("RABBITMQ_URL")
	if server == "" {
		server = "amqp://guest:guest@localhost:5672/"
	}
	endpoint := &RabbitMQ{}
	config := types.NewConfig()
	err := endpoint.Init(config, types.Configuration{
		"Server": server,
	})
	assert.Nil(t, err)
	conn, err := endpoint.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("rabbitmq server not available: %v", err)
	}
	assert.Equal(t, types.StatusConnected, endpoint.ConnectionStatus().Status)

	err = conn.Close()
	assert.Nil(t, err)
	info := endpoint.ConnectionStatus()
	assert.Equal(t, types.StatusReconnecting, info.Status)
}
