package redis

import (
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestPublisherNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&PublisherNode{})
	var targetNodeType = "x/redisPub"

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &PublisherNode{}, types.Configuration{
			"server":   "127.0.0.1:6379",
			"password": "",
			"poolSize": 0,
			"db":       0,
			"channel":  "default",
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"server":   "127.0.0.1:6379",
			"channel":  "${metadata.channel}",
			"password": "",
			"poolSize": 10,
			"db":       0,
		}, types.Configuration{
			"server":   "127.0.0.1:6379",
			"channel":  "${metadata.channel}",
			"password": "",
			"poolSize": 10,
			"db":       0,
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		var server = "127.0.0.1:6379"
		node1, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  server,
			"channel": "${metadata.channel}",
		}, Registry)
		assert.Nil(t, err)

		node2, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"server":  server,
			"channel": "test-channel",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("channel", "device/msg")
		msgList := []test.Msg{
			{
				MetaData:   metaData,
				Data:       "test message",
				AfterSleep: time.Millisecond * 100,
			},
		}
		var nodeList = []test.NodeAndCallback{
			{
				Node:    node1,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			},
			{
				Node:    node2,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					assert.Equal(t, types.Success, relationType)
				},
			},
		}
		for _, item := range nodeList {
			test.NodeOnMsgWithChildren(t, item.Node, item.MsgList, item.ChildrenNodes, item.Callback)
		}
		time.Sleep(time.Millisecond * 100)
	})
}
