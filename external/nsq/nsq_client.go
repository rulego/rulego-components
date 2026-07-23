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

package nsq

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/str"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
)

// Register the component
func init() {
	_ = rulego.Registry.Register(&ClientNode{})
}

// ClientNodeConfiguration NSQ client node configuration
type ClientNodeConfiguration struct {
	// NSQ server address
	Server string `json:"server" label:"Server" desc:"NSQ server address, comma-separated for multiple" required:"true" ref:"primary"`
	// Publish topics, support ${} variables
	Topic string `json:"topic" label:"Topic" desc:"Publish topic. Supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	// Authority and token of authority
	AuthToken string `json:"authToken" label:"Auth Token" desc:"NSQ authentication token" ref:"shared"`
	// TLS certificate file
	CertFile string `json:"certFile" label:"Cert File" desc:"TLS certificate file path" ref:"shared"`
	// TLS private key file
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path" ref:"shared"`
}

// ClientNode NSQ client node
type ClientNode struct {
	base.SharedNode[*nsq.Producer]
	// Node configuration
	Config ClientNodeConfiguration
	//topic template
	topicTemplate el.Template
}

// Type returns the component type
func (x *ClientNode) Type() string {
	return "x/nsqClient"
}

// New creates an instance
func (x *ClientNode) New() types.Node {
	return &ClientNode{Config: ClientNodeConfiguration{
		Server: "127.0.0.1:4150",
		Topic:  "devices_msg",
	}}
}

// Init initializes the component
func (x *ClientNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	// Remove all preceding and following spaces for all string values in the configuration
	base.NodeUtils.TrimStrings(configuration)
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		_ = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, ruleConfig.NodeClientInitNow, func() (*nsq.Producer, error) {
			return x.initClient()
		}, func(client *nsq.Producer) error {
			// Cleanup callback function
			client.Stop()
			return nil
		})
		if x.Config.Topic == "" {
			return errors.New("topic cannot be empty")
		}
		x.topicTemplate, err = el.NewTemplate(x.Config.Topic)
	}
	return err
}

// OnMsg processes a message
func (x *ClientNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {

	var evn map[string]interface{}
	if x.topicTemplate.HasVar() {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	topic, err := x.topicTemplate.Execute(evn)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	if err := client.Publish(str.ToString(topic), []byte(msg.GetData())); err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

// Destroy releases resources
func (x *ClientNode) Destroy() {
	_ = x.SharedNode.Close()
}

// Desc returns the component description
func (x *ClientNode) Desc() string {
	return "NSQ client for publishing messages. Topic supports ${metadata.key} and ${msg.key} substitution. Routes to Success/Failure"
}

// parseAddresses parses addresses in the Server field
// Supported formats:
// 1. Single nsqd: "127.0.0.1:4150"
// 2. Multiple nsqd: "127.0.0.1:4150,127.0.0.1:4151"
// 3. lookupd address: "http://127.0.0.1:4161,http://127.0.0.1:4162"
func (x *ClientNode) parseAddresses() (nsqdAddrs []string, lookupdAddrs []string) {
	if x.Config.Server == "" {
		return
	}

	addresses := strings.Split(x.Config.Server, ",")
	for _, addr := range addresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		// Determine whether it is a lookupd address (including http:// or https://)
		if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
			lookupdAddrs = append(lookupdAddrs, addr)
		} else {
			// A regular nsqd address
			nsqdAddrs = append(nsqdAddrs, addr)
		}
	}
	return
}

// initClient initializes the NSQ producer client
func (x *ClientNode) initClient() (*nsq.Producer, error) {
	config := nsq.NewConfig()

	// Set up authentication tokens
	if x.Config.AuthToken != "" {
		config.AuthSecret = x.Config.AuthToken
	}

	// Set up TLS configuration
	if x.Config.CertFile != "" && x.Config.CertKeyFile != "" {
		config.TlsV1 = true
		config.TlsConfig = &tls.Config{
			InsecureSkipVerify: false,
		}
		// Load certificates
		cert, err := tls.LoadX509KeyPair(x.Config.CertFile, x.Config.CertKeyFile)
		if err != nil {
			return nil, err
		}
		config.TlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Parse address configuration
	nsqdAddrs, lookupdAddrs := x.parseAddresses()

	// NSQ producers can only connect to a single NSQD and do not support lookupd
	// If you have configured a lookupd address, you need to first use lookupd to find the nsqd address
	var targetAddr string
	if len(nsqdAddrs) > 0 {
		// Use the first nsqd address
		targetAddr = nsqdAddrs[0]
	} else if len(lookupdAddrs) > 0 {
		// Discover nsqd addresses through the lookupd API
		nsqdAddr, err := x.discoverNsqdFromLookupd(lookupdAddrs[0])
		if err != nil {
			return nil, fmt.Errorf("failed to discover nsqd from lookupd %s: %w", lookupdAddrs[0], err)
		}
		targetAddr = nsqdAddr
	} else {
		// Use the original Server configuration
		targetAddr = x.Config.Server
	}

	client, err := nsq.NewProducer(targetAddr, config)
	return client, err
}

// discoverNsqdFromLookupd discovers available nsqd addresses through the lookupd API
func (x *ClientNode) discoverNsqdFromLookupd(lookupdAddr string) (string, error) {
	// Build lookupd API URL
	apiURL := fmt.Sprintf("%s/nodes", strings.TrimSuffix(lookupdAddr, "/"))

	// Create an HTTP client and set timeouts
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	// Send a GET request to lookupd
	resp, err := client.Get(apiURL)
	if err != nil {
		return "", fmt.Errorf("failed to query lookupd API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("lookupd API returned status %d", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read lookupd response: %w", err)
	}

	// Parsing JSON responses
	var response struct {
		Producers []struct {
			RemoteAddress    string `json:"remote_address"`
			Hostname         string `json:"hostname"`
			BroadcastAddress string `json:"broadcast_address"`
			TCPPort          int    `json:"tcp_port"`
			HTTPPort         int    `json:"http_port"`
			Version          string `json:"version"`
		} `json:"producers"`
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", fmt.Errorf("failed to parse lookupd response: %w", err)
	}

	// Check if there are available nsqd nodes
	if len(response.Producers) == 0 {
		return "", errors.New("no nsqd nodes found from lookupd")
	}

	// Returns the first available nsqd address
	producer := response.Producers[0]
	var nsqdAddr string
	if producer.BroadcastAddress != "" {
		nsqdAddr = fmt.Sprintf("%s:%d", producer.BroadcastAddress, producer.TCPPort)
	} else {
		nsqdAddr = fmt.Sprintf("%s:%d", producer.RemoteAddress, producer.TCPPort)
	}

	return nsqdAddr, nil
}
