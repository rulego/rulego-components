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
	"context"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type returns the component type
const Type = types.EndpointTypePrefix + "pulsar"

// KeyResponseTopic: Response topic metadataKey
const KeyResponseTopic = "responseTopic"

// Endpoint alias
type Endpoint = Pulsar

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// Register the component
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage
type RequestMessage struct {
	message pulsar.Message
	msg     *types.RuleMsg
	err     error
}

// Body
func (r *RequestMessage) Body() []byte {
	return r.message.Payload()
}

// Headers: Get the message header
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.message.Topic())
	header.Set("messageId", r.message.ID().String())
	header.Set("publishTime", r.message.PublishTime().Format(time.RFC3339))
	header.Set("eventTime", r.message.EventTime().Format(time.RFC3339))
	if r.message.Key() != "" {
		header.Set("key", r.message.Key())
	}
	// Add custom properties
	for k, v := range r.message.Properties() {
		header.Set(k, v)
	}
	return header
}

// Source: Source
func (r *RequestMessage) From() string {
	return r.message.Topic()
}

// GetParam to get the parameters
func (r *RequestMessage) GetParam(key string) string {
	return r.message.Properties()[key]
}

// SetMsg sets the rule message
func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

// GetMsg obtains rule messages
func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		// The default specification is JSON format. If it is not this type, please modify it in the process function
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))
		ruleMsg.Metadata.PutValue("topic", r.message.Topic())
		ruleMsg.Metadata.PutValue("messageId", r.message.ID().String())
		ruleMsg.Metadata.PutValue("publishTime", r.message.PublishTime().Format(time.RFC3339))
		ruleMsg.Metadata.PutValue("eventTime", r.message.EventTime().Format(time.RFC3339))
		if r.message.Key() != "" {
			ruleMsg.Metadata.PutValue("key", r.message.Key())
		}
		// Add custom properties
		for k, v := range r.message.Properties() {
			ruleMsg.Metadata.PutValue(k, v)
		}
		r.msg = &ruleMsg
	}
	return r.msg
}

// SetStatusCode sets the status code
func (r *RequestMessage) SetStatusCode(statusCode int) {
}

// SetBody sets the message body
func (r *RequestMessage) SetBody(body []byte) {
}

// SetError is set incorrectly
func (r *RequestMessage) SetError(err error) {
	r.err = err
}

// GetError retrieves an error
func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage
type ResponseMessage struct {
	message   pulsar.Message
	producers *sync.Map
	client    *base.SharedNode[pulsar.Client]
	body      []byte
	msg       *types.RuleMsg
	headers   textproto.MIMEHeader
	err       error
}

// Body acquires the response body
func (r *ResponseMessage) Body() []byte {
	return r.body
}

// Headers: Get the response head
func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

// Source: Source
func (r *ResponseMessage) From() string {
	return r.message.Topic()
}

// GetParam to get the parameters
func (r *ResponseMessage) GetParam(key string) string {
	return r.message.Properties()[key]
}

// SetMsg sets the rule message
func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

// GetMsg obtains rule messages
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

// SetStatusCode sets the status code
func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

// getMetadataValue from msg.Metadata or response header to obtain values
func (r *ResponseMessage) getMetadataValue(metadataName, headerName string) string {
	var v string
	if r.GetMsg() != nil {
		metadata := r.GetMsg().Metadata
		v = metadata.GetValue(metadataName)
	}
	if v == "" {
		return r.Headers().Get(headerName)
	} else {
		return v
	}
}

// SetBody sets the response body
func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.getMetadataValue(KeyResponseTopic, KeyResponseTopic)
	if topic != "" && r.producers != nil && r.client != nil {
		// Get the client
		client, err := r.client.GetSafely()
		if err != nil {
			r.SetError(err)
			return
		}

		// Obtain or create the producer for the corresponding topic (using sync.Map's lock-free operation)
		var producer pulsar.Producer
		if value, exists := r.producers.Load(topic); exists {
			producer = value.(pulsar.Producer)
		} else {
			// Create new producers
			producerOptions := pulsar.ProducerOptions{
				Topic: topic,
			}

			newProducer, err := client.CreateProducer(producerOptions)
			if err != nil {
				r.SetError(err)
				return
			}

			// Using LoadOrStore ensures that only one producer is created and stored
			if actual, loaded := r.producers.LoadOrStore(topic, newProducer); loaded {
				// If it already exists, close the newly created producer and use the existing one
				newProducer.Close()
				producer = actual.(pulsar.Producer)
			} else {
				// Use newly created producers
				producer = newProducer
			}
		}

		// Build message attributes
		properties := make(map[string]string)
		for k, v := range r.Headers() {
			if len(v) > 0 {
				properties[k] = v[0]
			}
		}

		// Send a response message
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:    r.body,
			Properties: properties,
		})
		if err != nil {
			r.SetError(err)
		}
	}
}

// SetError is set incorrectly
func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

// GetError retrieves an error
func (r *ResponseMessage) GetError() error {
	return r.err
}

// Config Pulsar configuration
type Config struct {
	// Pulsar server address
	Server string `json:"server" label:"Server" desc:"Pulsar server address, format: pulsar://host:port" required:"true" ref:"primary"`
	// Default subscription name
	SubName string `json:"subName" label:"Subscription Name" desc:"Subscription name, used as default when AddRouter does not specify one" required:"true"`
	// Subscription type
	SubType string `json:"subType" label:"Subscription Type" desc:"Subscription type: Exclusive, Shared, Failover, KeyShared" required:"true"`
	// Message channel buffer pool size
	PoolSize int `json:"poolSize" label:"Pool Size" desc:"Message channel buffer size, default is 100"`
	// Authority and token of authority
	AuthToken string `json:"authToken" label:"Auth Token" desc:"Pulsar JWT authentication token" ref:"shared"`
	// TLS certificate file
	CertFile string `json:"certFile" label:"Cert File" desc:"TLS certificate file path" ref:"shared"`
	// TLS private key file
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path" ref:"shared"`
}

// parseSubscriptionType parses the subscription type string as pulsar.SubType (case-insensitive)
func parseSubscriptionType(subscriptionType string) pulsar.SubscriptionType {
	switch strings.ToLower(subscriptionType) {
	case "exclusive":
		return pulsar.Exclusive
	case "shared":
		return pulsar.Shared
	case "failover":
		return pulsar.Failover
	case "keyshared":
		return pulsar.KeyShared
	default:
		return pulsar.Shared // The Shared type is used by default
	}
}

// Pulsar Pulsar Receiving Endpoint
type Pulsar struct {
	impl.BaseEndpoint
	base.SharedNode[pulsar.Client]
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown offers an elegant shutdown function
	base.GracefulShutdown
	RuleConfig types.Config
	//Config configuration
	Config Config
	// Consumer mapping relationships, used to stop consumption
	consumers map[string]pulsar.Consumer
	// topic+subscription combined mapping for checking duplicate subscriptions
	subscriptions map[string]string // key: topic+subscription, value: routerId
	// Producer mapping: key is topic, value is the corresponding producer
	producers sync.Map
	// Mutually exclusive locks
	mu sync.RWMutex
}

// Type returns the component type
func (x *Pulsar) Type() string {
	return Type
}

// ID to obtain the component ID
func (x *Pulsar) Id() string {
	return x.Config.Server
}

// New creates an instance
func (x *Pulsar) New() types.Node {
	return &Pulsar{
		Config: Config{
			Server:   "pulsar://localhost:6650",
			SubName:  "default",
			SubType:  "Shared",
			PoolSize: 100,
		},
	}
}

func (x *Pulsar) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "Pulsar consumer endpoint for subscribing to topics and processing messages",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Topic",
					Desc:     "Pulsar topic to subscribe, e.g. persistent://public/default/orders",
					Required: true,
				},
			},
		},
	}
}

// Init initializes the component
func (x *Pulsar) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	x.consumers = make(map[string]pulsar.Consumer)
	x.subscriptions = make(map[string]string)

	// Initialize the elegant shutdown function
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	// Initialize the shared client
	_ = x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (pulsar.Client, error) {
		return x.initClient()
	}, func(client pulsar.Client) error {
		if client != nil {
			client.Close()
		}
		return nil
	})

	return err
}

// Destroy releases resources
func (x *Pulsar) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop Graceful Stop
func (x *Pulsar) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// Close Close closes the connection
func (x *Pulsar) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	// Stop all consumers
	for _, consumer := range x.consumers {
		consumer.Close()
	}
	x.consumers = make(map[string]pulsar.Consumer)
	x.subscriptions = make(map[string]string)

	// Stop all producers (using sync.Map's lock-free operation)
	x.producers.Range(func(key, value interface{}) bool {
		if producer, ok := value.(pulsar.Producer); ok && producer != nil {
			producer.Close()
		}
		return true
	})
	// Clear sync.Map
	x.producers = sync.Map{}

	// Close the shared client
	_ = x.SharedNode.Close()
	x.BaseEndpoint.Destroy()
	return nil
}

// AddRouter adds a route
func (x *Pulsar) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}

	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return "", err
	}

	routerId := router.GetId()
	if routerId == "" {
		routerId = router.GetFrom().ToString()
		router.SetId(routerId)
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	if _, ok := x.consumers[routerId]; ok {
		return routerId, fmt.Errorf("routerId %s already exists", routerId)
	}

	// Parse topics and subscriptions
	from := router.FromToString()
	topic := from
	subscription := x.Config.SubName
	if subscription == "" {
		subscription = "default"
	}

	// If there are parameters, the first parameter is called subscription
	if len(params) > 0 {
		if sub, ok := params[0].(string); ok {
			subscription = sub
		}
	}

	// Check whether the topic+subscription combination already exists
	subscriptionKey := topic + "|" + subscription
	if existingRouterId, exists := x.subscriptions[subscriptionKey]; exists {
		return routerId, fmt.Errorf("topic '%s' with subscription '%s' already exists for routerId '%s'", topic, subscription, existingRouterId)
	}

	// Resolve subscription types
	subscriptionType := parseSubscriptionType(x.Config.SubType)

	// Create consumer profiles
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscription,
		Type:             subscriptionType,
	}

	// Use a simplified configuration
	consumerOptions.Topic = topic
	consumerOptions.SubscriptionName = subscription

	// Set up the message processor
	poolSize := x.Config.PoolSize
	if poolSize <= 0 {
		poolSize = 100 // Default values
	}
	consumerOptions.MessageChannel = make(chan pulsar.ConsumerMessage, poolSize)

	// Create consumers
	consumer, err := client.Subscribe(consumerOptions)
	if err != nil {
		return "", err
	}

	// Start the message processing coroutine
	go func() {
		for {
			select {
			case msg, ok := <-consumer.Chan():
				if !ok {
					return
				}
				// Use thread pools or coroutines to process messages to avoid blocking message reception loops
				if x.RuleConfig.Pool != nil {
					_ = x.RuleConfig.Pool.Submit(func() {
						x.handleMessage(msg, router)
					})
				} else {
					// Enable coroutine message processing to ensure message reception is not blocked
					go x.handleMessage(msg, router)
				}
			}
		}
	}()

	x.consumers[routerId] = consumer
	x.subscriptions[subscriptionKey] = routerId
	return routerId, nil
}

// handleMessage handles individual messages
// Processes Pulsar messages, creates Exchanges, and executes rule chain processing
func (x *Pulsar) handleMessage(msg pulsar.ConsumerMessage, router endpointApi.Router) {
	defer func() {
		if e := recover(); e != nil {
			x.Printf("pulsar endpoint handler err :\n%v", runtime.Stack())
		}
	}()

	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			message: msg.Message,
		},
		Out: &ResponseMessage{
			message:   msg.Message,
			producers: &x.producers,
			client:    &x.SharedNode,
		},
	}
	x.DoProcess(context.Background(), router, exchange)
	// Confirm the news
	_ = msg.Ack(msg.Message)
}

// RemoveRouter removes the route
func (x *Pulsar) RemoveRouter(routerId string, params ...interface{}) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	if consumer, ok := x.consumers[routerId]; ok {
		consumer.Close()
		delete(x.consumers, routerId)

		// Delete the corresponding subscription mapping record
		for key, value := range x.subscriptions {
			if value == routerId {
				delete(x.subscriptions, key)
				break
			}
		}
		return nil
	}
	return errors.New("router not found")
}

// Start the service
func (x *Pulsar) Start() error {
	if !x.SharedNode.IsInit() {
		return x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (pulsar.Client, error) {
			return x.initClient()
		}, func(client pulsar.Client) error {
			if client != nil {
				client.Close()
			}
			return nil
		})
	}

	// Producers will dynamically create these as needed
	return nil
}

// Printf prints logs
func (x *Pulsar) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// initClient Initializes the Pulsar client
func (x *Pulsar) initClient() (pulsar.Client, error) {
	clientOptions := pulsar.ClientOptions{
		URL: x.Config.Server,
	}

	// Set JWT Token authentication
	if x.Config.AuthToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(x.Config.AuthToken)
	}

	// Set up TLS configuration
	if x.Config.CertFile != "" {
		clientOptions.TLSCertificateFile = x.Config.CertFile
	}
	if x.Config.CertKeyFile != "" {
		clientOptions.TLSKeyFilePath = x.Config.CertKeyFile
	}

	client, err := pulsar.NewClient(clientOptions)
	return client, err
}
