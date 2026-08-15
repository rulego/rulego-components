package nsq

import (
	"os"
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

func TestClientNodeConnectionStatus(t *testing.T) {
	if os.Getenv("SKIP_NSQ_TESTS") == "true" {
		t.Skip("Skipping NSQ tests")
	}
	nsqdAddress := os.Getenv("NSQD_ADDRESS")
	if nsqdAddress == "" {
		nsqdAddress = "127.0.0.1:4150"
	}
	var node ClientNode
	config := types.NewConfig()
	err := node.Init(config, types.Configuration{
		"server": nsqdAddress,
		"topic":  "status_test",
	})
	assert.Nil(t, err)
	producer, err := node.SharedNode.GetSafely()
	if err != nil {
		t.Skipf("nsqd not available: %v", err)
	}
	// Producer 连接是懒建立的，先确认 nsqd 可达
	if err := producer.Ping(); err != nil {
		t.Skipf("nsqd not responding: %v", err)
	}
	assert.Equal(t, types.StatusConnected, node.ConnectionStatus().Status)
	node.Destroy()
	assert.Equal(t, types.StatusDisconnected, node.ConnectionStatus().Status)
}
