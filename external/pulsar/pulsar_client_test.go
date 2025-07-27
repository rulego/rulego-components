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

package pulsar

import (
	"os"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
)

func TestClientNode(t *testing.T) {
	// 如果设置了跳过 Pulsar 测试，则跳过
	if os.Getenv("SKIP_PULSAR_TESTS") == "true" {
		t.Skip("Skipping Pulsar tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/pulsarClient"

	pulsarURL := os.Getenv("PULSAR_URL")
	if pulsarURL == "" {
		pulsarURL = "pulsar://localhost:6650"
	}

	t.Run("NewNode", func(t *testing.T) {
		test.NodeNew(t, targetNodeType, &ClientNode{}, types.Configuration{
			"topic":  "/device/msg",
			"server": pulsarURL,
		}, Registry)
	})

	t.Run("InitNode", func(t *testing.T) {
		test.NodeInit(t, targetNodeType, types.Configuration{
			"topic":  "/device/msg",
			"server": pulsarURL,
		}, types.Configuration{
			"topic":  "/device/msg",
			"server": pulsarURL,
		}, Registry)
	})

	t.Run("OnMsg", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "/device/msg",
			"server": pulsarURL,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create Pulsar client node (Pulsar may not be available): %v", err)
			return
		}

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "test")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT1",
				Data:     "AA",
			},
			{
				MetaData: metaData,
				MsgType:  "ACTIVITY_EVENT2",
				Data:     "{\"temperature\":60}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("Pulsar publish failed (Pulsar may not be available): %v", err)
						return
					}
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

func TestClientNodeWithTemplate(t *testing.T) {
	// 如果设置了跳过 Pulsar 测试，则跳过
	if os.Getenv("SKIP_PULSAR_TESTS") == "true" {
		t.Skip("Skipping Pulsar tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/pulsarClient"

	pulsarURL := os.Getenv("PULSAR_URL")
	if pulsarURL == "" {
		pulsarURL = "pulsar://localhost:6650"
	}

	t.Run("OnMsgWithTemplate", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "/device/${productType}/msg",
			"server": pulsarURL,
			"key":    "${deviceId}-${msgType}",
			"headers": map[string]string{
				"source": "${productType}",
				"type":   "${msgType}",
			},
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create Pulsar client node (Pulsar may not be available): %v", err)
			return
		}

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("productType", "sensor")
		metaData.PutValue("deviceId", "device123")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "TELEMETRY",
				Data:     "{\"temperature\":25.5}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("Pulsar publish failed (Pulsar may not be available): %v", err)
						return
					}
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

func TestClientNodeWithProducerOptions(t *testing.T) {
	// 如果设置了跳过 Pulsar 测试，则跳过
	if os.Getenv("SKIP_PULSAR_TESTS") == "true" {
		t.Skip("Skipping Pulsar tests")
	}

	Registry := &types.SafeComponentSlice{}
	Registry.Add(&ClientNode{})
	var targetNodeType = "x/pulsarClient"

	pulsarURL := os.Getenv("PULSAR_URL")
	if pulsarURL == "" {
		pulsarURL = "pulsar://localhost:6650"
	}

	t.Run("OnMsgWithProducerOptions", func(t *testing.T) {
		node, err := test.CreateAndInitNode(targetNodeType, types.Configuration{
			"topic":  "test-topic-with-options",
			"server": pulsarURL,
		}, Registry)
		if err != nil {
			t.Skipf("Failed to create Pulsar client node (Pulsar may not be available): %v", err)
			return
		}

		metaData := types.BuildMetadata(make(map[string]string))
		metaData.PutValue("source", "test")
		msgList := []test.Msg{
			{
				MetaData: metaData,
				MsgType:  "TEST_EVENT",
				Data:     "{\"message\":\"test with options\"}",
			},
		}

		var nodeList = []test.NodeAndCallback{
			{
				Node:    node,
				MsgList: msgList,
				Callback: func(msg types.RuleMsg, relationType string, err error) {
					if err != nil {
						t.Logf("Pulsar publish failed (Pulsar may not be available): %v", err)
						return
					}
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
