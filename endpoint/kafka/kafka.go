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

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type returns the component type
const Type = types.EndpointTypePrefix + "kafka"
const (
	//Topic: message topic
	Topic = "topic"
	//Key message: key
	Key = "key"
	//Partition (consumption partition).
	Partition = "partition"
)
const (
	// KeyResponseTopic: Response topic metadataKey
	KeyResponseTopic = "responseTopic"
	// KeyResponseKey Response key metadataKey
	KeyResponseKey = "key"
	// KeyResponsePartition Response: Consumed partition metadataKey
	KeyResponsePartition = "partition"
)

// Endpoint alias
type Endpoint = Kafka

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// Register the component
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage http requests messages
type RequestMessage struct {
	request *sarama.ConsumerMessage
	msg     *types.RuleMsg
	err     error
}

func (r *RequestMessage) Body() []byte {
	return r.request.Value
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set(Topic, r.request.Topic)
	return header
}

func (r *RequestMessage) From() string {
	return r.request.Topic
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//The default specification is JSON format. If it is not this type, please modify it in the process function
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))

		ruleMsg.Metadata.PutValue(Topic, r.From())

		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

// ResponseMessage http Response message
type ResponseMessage struct {
	request  *sarama.ConsumerMessage
	response sarama.SyncProducer
	body     []byte
	msg      *types.RuleMsg
	headers  textproto.MIMEHeader
	err      error
	log      func(format string, v ...interface{})
}

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return r.request.Topic
}

func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

// From msg.Metadata or response header access
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
func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
	topic := r.getMetadataValue(KeyResponseTopic, KeyResponseTopic)
	if topic != "" {
		key := r.getMetadataValue(KeyResponseKey, KeyResponseKey)
		partitionStr := r.getMetadataValue(KeyResponsePartition, KeyResponsePartition)
		var partition = int32(0)
		if partitionStr != "" {
			if num, err := strconv.ParseInt(partitionStr, 10, 32); err == nil {
				partition = int32(num)
			}
		}
		message := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(r.body),
		}
		_, _, err := r.response.SendMessage(message)
		if err != nil {

		}
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

type Config struct {
	Server  string     `json:"server" label:"Server" desc:"Kafka broker addresses, comma-separated for multiple" required:"true" ref:"primary"`
	GroupId string     `json:"groupId" label:"Group ID" desc:"Kafka consumer group ID"`
	SASL    SASLConfig `json:"sasl" label:"SASL Auth" desc:"SASL authentication configuration"`
	TLS     TLSConfig  `json:"tls" label:"TLS" desc:"TLS encryption configuration"`
}

type SASLConfig struct {
	Enable    bool   `json:"enable" label:"Enable" desc:"Enable SASL authentication"`
	Mechanism string `json:"mechanism" label:"Mechanism" desc:"SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512"`
	Username  string `json:"username" label:"Username" desc:"SASL authentication username" ref:"shared"`
	Password  string `json:"password" label:"Password" desc:"SASL authentication password" ref:"shared"`
}

type TLSConfig struct {
	Enable             bool `json:"enable" label:"Enable" desc:"Enable TLS encryption"`
	InsecureSkipVerify bool `json:"insecureSkipVerify" label:"Skip Verify" desc:"Skip server certificate verification, disable in production"`
}

// Kafka Kafka receiver endpoint
type Kafka struct {
	impl.BaseEndpoint
	RuleConfig types.Config
	//Config configuration
	Config Config
	// Brokers Kafka server address list
	brokers []string
	//Message producer, used for response
	producer sarama.SyncProducer
	// Themes and themed consumer mapping relationships are used to unsubscribe
	handlers map[string]sarama.ConsumerGroup
	closed   bool
	// Gracefully closed state
	isShuttingDown int32 // Atomic operations are used
	// Active message processing counter
	activeMessages int64 // Atomic operations are used
	// Waiting for all messages to be processed in the channel
	shutdownComplete chan struct{}
	// Turn off timeout
	shutdownTimeout time.Duration
}

// Type returns the component type
func (x *Kafka) Type() string {
	return Type
}

func (x *Kafka) New() types.Node {
	return &Kafka{
		Config: Config{
			Server:  "127.0.0.1:9092",
			GroupId: "rulego",
			SASL: SASLConfig{
				Mechanism: "PLAIN",
			},
		},
		shutdownComplete: make(chan struct{}),
		shutdownTimeout:  30 * time.Second,
	}
}

func (x *Kafka) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "Kafka consumer endpoint for subscribing to topics and processing messages",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Topic",
					Desc:     "Kafka topic to subscribe, supports multiple topics separated by comma, e.g. topic1,topic2",
					Required: true,
				},
			},
		},
	}
}

func (x *Kafka) getBrokerFromOldVersion(configuration types.Configuration) []string {
	if v, ok := configuration["brokers"]; ok {
		return v.([]string)
	} else {
		return nil
	}
}

// Init initializes the component
func (x *Kafka) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.Config.GroupId = strings.TrimSpace(x.Config.GroupId)
	if x.Config.GroupId == "" {
		x.Config.GroupId = "rulego"
	}
	x.brokers = x.getBrokerFromOldVersion(configuration)
	if len(x.brokers) == 0 && x.Config.Server != "" {
		x.brokers = strings.Split(x.Config.Server, ",")
	}
	if len(x.brokers) == 0 {
		return errors.New("brokers is empty")
	}
	x.RuleConfig = ruleConfig
	return err
}

// Destroy releases resources
func (x *Kafka) Destroy() {
	_ = x.Close()
}

func (x *Kafka) Close() error {
	x.Lock()

	// Prevent repeated closing
	if x.closed {
		x.Unlock()
		return nil
	}

	// Set the state to off, preventing new consumers from starting and processing new messages
	x.closed = true
	atomic.StoreInt32(&x.isShuttingDown, 1)

	// Retrieve a copy of the current handler to avoid modifying the map during closing
	handlersToClose := make(map[string]sarama.ConsumerGroup)
	for k, v := range x.handlers {
		handlersToClose[k] = v
	}
	x.Unlock()

	// Stage 1: Stop receiving new messages – turn off all consumers
	for routerId, consumer := range handlersToClose {
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				x.Printf("[ERROR] Error closing consumer %s: %v", routerId, err)
			}
		}
	}

	// Stage 2: Wait for active message processing to complete
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(x.shutdownTimeout)
	defer timeout.Stop()

	for {
		activeCount := atomic.LoadInt64(&x.activeMessages)
		if activeCount == 0 {
			break
		}

		select {
		case <-timeout.C:
			goto forceClose
		case <-ticker.C:
			// Keep waiting
		}
	}

forceClose:
	// Stage 3: Close the producer
	x.Lock()
	x.handlers = nil

	var err error
	if x.producer != nil {
		err = x.producer.Close()
		if err != nil {
			x.Printf("[ERROR] Error closing Kafka producer: %v", err)
		}
		x.producer = nil
	}
	x.Unlock()

	// After releasing the lock, BaseEndpoint.Destroy() is called to avoid deadlocks
	x.BaseEndpoint.Destroy()

	// Notification of closure completed
	select {
	case <-x.shutdownComplete:
		// It has been closed
	default:
		close(x.shutdownComplete)
	}

	return err
}

func (x *Kafka) Id() string {
	if len(x.brokers) > 0 {
		return x.brokers[0]
	} else {
		return ""
	}
}

func (x *Kafka) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router can not nil")
	}
	//Initialize the Kafka client
	if err := x.initKafkaProducer(); err != nil {
		x.Printf("[ERROR] Failed to initialize Kafka producer: %v", err)
		return "", err
	}

	if id := router.GetId(); id == "" {
		router.SetId(router.GetFrom().ToString())
	}
	if err := x.createTopicConsumer(router); err != nil {
		x.Printf("[ERROR] Failed to create topic consumer for %s: %v", router.GetFrom().ToString(), err)
		return "", err
	}
	return router.GetId(), nil
}

func (x *Kafka) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	//Delete the subscription
	if v, ok := x.handlers[routerId]; ok {
		delete(x.handlers, routerId)
		err := v.Close()
		if err != nil {
			x.Printf("[ERROR] Error closing consumer for router %s: %v", routerId, err)
		}
		return err
	}
	return nil
}

func (x *Kafka) Start() error {
	return x.initKafkaProducer()
}

// initKafkaProducer initializes the kafka producer for response
func (x *Kafka) initKafkaProducer() error {
	x.Lock()
	defer x.Unlock()
	if x.producer != nil {
		return nil
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // Sync mode needs to be set to true

	// Configure SASL certification
	if x.Config.SASL.Enable {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = x.Config.SASL.Username
		config.Net.SASL.Password = x.Config.SASL.Password

		switch strings.ToUpper(x.Config.SASL.Mechanism) {
		case "PLAIN":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	// Configure TLS
	if x.Config.TLS.Enable {
		config.Net.TLS.Enable = true
		if x.Config.TLS.InsecureSkipVerify {
			config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
		}
	}

	producer, err := sarama.NewSyncProducer(x.brokers, config)
	if err != nil {
		x.Printf("[ERROR] Failed to create Kafka producer: %v", err)
		return err
	}
	x.producer = producer

	return nil
}

// Create Kafka consumers
func (x *Kafka) createTopicConsumer(router endpointApi.Router) error {
	if form := router.GetFrom(); form != nil {
		routerId := router.GetId()
		if routerId == "" {
			routerId = router.GetFrom().ToString()
			router.SetId(routerId)
		}
		x.Lock()
		defer x.Unlock()
		if x.handlers == nil {
			x.handlers = make(map[string]sarama.ConsumerGroup)
		}
		if _, ok := x.handlers[routerId]; ok {
			x.Printf("[ERROR] RouterId %s already exists", routerId)
			return fmt.Errorf("routerId %s already exists", routerId)
		}
		config := sarama.NewConfig()
		// Set the reconnection configuration settings
		config.Consumer.Return.Errors = true
		config.Metadata.Retry.Max = 3
		config.Metadata.Retry.Backoff = 250 * 1000000 // 250ms
		config.Consumer.Offsets.Initial = sarama.OffsetNewest

		// Configure SASL certification
		if x.Config.SASL.Enable {
			config.Net.SASL.Enable = true
			config.Net.SASL.User = x.Config.SASL.Username
			config.Net.SASL.Password = x.Config.SASL.Password

			switch strings.ToUpper(x.Config.SASL.Mechanism) {
			case "PLAIN":
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			case "SCRAM-SHA-256":
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			case "SCRAM-SHA-512":
				config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			default:
				config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
			}
		}

		// Configure TLS
		if x.Config.TLS.Enable {
			config.Net.TLS.Enable = true
			if x.Config.TLS.InsecureSkipVerify {
				config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
			}
		}

		consumer, err := sarama.NewConsumerGroup(x.brokers, x.Config.GroupId, config)
		if err != nil {
			x.Printf("[ERROR] Failed to create consumer group for topic %s: %v", form.ToString(), err)
			return err
		}
		x.handlers[routerId] = consumer

		topics := []string{form.ToString()}                                          // Subscribe to the topic list
		handler := &consumerHandler{router: router, ep: x, ruleConfig: x.RuleConfig} // Custom consumer handlers

		// Launch consumer goroutine with multi-link mechanism
		go x.startConsumerWithRetry(consumer, topics, handler, routerId)

	}
	return nil
}

// Custom consumer handlers
type consumerHandler struct {
	ep         *Kafka
	router     endpointApi.Router
	ruleConfig types.Config
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		// Handling message logic
		if h.ruleConfig.Pool != nil {
			err := h.ruleConfig.Pool.Submit(func() {
				h.handlerMsg(session, msg)
			})
			if err != nil {
				h.ep.Printf("kafka consumer handler err :%v", err)
			}
			// Do not immediately return the error; continue processing the next message
		} else {
			go h.handlerMsg(session, msg)
		}
	}
	return nil
}

func (h *consumerHandler) handlerMsg(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	defer func() {
		// Reduce active message counts
		atomic.AddInt64(&h.ep.activeMessages, -1)

		if e := recover(); e != nil {
			h.ep.Printf("[ERROR] kafka endpoint handler panic: %v\n%v", e, runtime.Stack())
		}
	}()

	// Increase active message count
	atomic.AddInt64(&h.ep.activeMessages, 1)

	// Check if it is being closed; if so, refuse to process new messages
	if h.ep.IsShuttingDown() {
		session.MarkMessage(msg, "") // Still mark messages as processed to avoid duplication
		return
	}

	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			request: msg,
		},
		Out: &ResponseMessage{
			request:  msg,
			response: h.ep.producer,
			log: func(format string, v ...interface{}) {
				h.ep.Printf(format, v...)
			},
		},
	}
	metadata := exchange.In.GetMsg().Metadata
	metadata.PutValue(Key, string(msg.Key))
	metadata.PutValue(Partition, strconv.Itoa(int(msg.Partition)))

	h.ep.DoProcess(context.Background(), h.router, exchange)
	session.MarkMessage(msg, "") // The message was marked as processed
}

// startConsumerWithRetry is a consumer startup function with reconnection mechanism
func (x *Kafka) startConsumerWithRetry(consumer sarama.ConsumerGroup, topics []string, handler *consumerHandler, routerId string) {
	defer func() {
		if consumer != nil {
			_ = consumer.Close()
		}
		// Remove closed consumers from handlers using a secure cleanup method
		x.Lock()
		if x.handlers != nil {
			// Only delete if the current consumer is indeed the one we want to delete
			if currentConsumer, exists := x.handlers[routerId]; exists && currentConsumer == consumer {
				delete(x.handlers, routerId)
			}
		}
		x.Unlock()
	}()

	ctx := context.Background()
	for {
		// Check if the consumer has been turned off
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Check if the consumer group is still in handlers (used to determine if it was manually removed)
		x.Lock()
		_, exists := x.handlers[routerId]
		closed := x.closed
		x.Unlock()
		if !exists || closed {
			return
		}

		err := consumer.Consume(ctx, topics, handler)
		if err != nil {
			x.Printf("[ERROR] Failed to consume for topic %s: %v", topics[0], err)
			// If it's a fatal mistake, recreate the consumer
			if err == sarama.ErrClosedConsumerGroup {
				// Recreate consumers using complete configurations
				config := sarama.NewConfig()
				config.Consumer.Return.Errors = true
				config.Metadata.Retry.Max = 3
				config.Metadata.Retry.Backoff = 250 * 1000000 // 250ms
				config.Consumer.Offsets.Initial = sarama.OffsetNewest

				// Configure SASL certification
				if x.Config.SASL.Enable {
					config.Net.SASL.Enable = true
					config.Net.SASL.User = x.Config.SASL.Username
					config.Net.SASL.Password = x.Config.SASL.Password

					switch strings.ToUpper(x.Config.SASL.Mechanism) {
					case "PLAIN":
						config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
					case "SCRAM-SHA-256":
						config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
					case "SCRAM-SHA-512":
						config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
					default:
						config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
					}
				}

				// Configure TLS
				if x.Config.TLS.Enable {
					config.Net.TLS.Enable = true
					if x.Config.TLS.InsecureSkipVerify {
						config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}
					}
				}

				newConsumer, createErr := sarama.NewConsumerGroup(x.brokers, x.Config.GroupId, config)
				if createErr != nil {
					x.Printf("[ERROR] Failed to recreate consumer for topic %s: %v", topics[0], createErr)
					return
				}
				// Update consumer references in handlers
				x.Lock()
				oldConsumer := consumer
				if x.handlers != nil {
					x.handlers[routerId] = newConsumer
					consumer = newConsumer
				}
				x.Unlock()
				// Closing the lock after the old consumer is released
				_ = oldConsumer.Close()
			} else {
				// Other errors, wait a while and try again
				time.Sleep(5 * time.Second)
			}
		} else {
			// Finish as usual, wait for a while, then try the connection again
			time.Sleep(1 * time.Second)
		}
	}
}

// BeginShutdown implements the GracefulShutdown interface and begins the graceful shutdown process
func (x *Kafka) BeginShutdown(ctx context.Context) error {
	// Set it to a closed state to prevent new connections and messages from being received
	atomic.StoreInt32(&x.isShuttingDown, 1)

	// Instead of immediately shutting down resources, mark the status so that the message being processed is completed
	// The actual resource shutdown is done in Destroy().
	return nil
}

// IsShuttingDown implements the GracefulShutdown interface to check if it is closing
func (x *Kafka) IsShuttingDown() bool {
	return atomic.LoadInt32(&x.isShuttingDown) == 1
}

// GetShutdownTimeout implements the ShutdownTimeout interface and returns the shutdown timeout
func (x *Kafka) GetShutdownTimeout() time.Duration {
	// Kafka components take longer to gracefully shut down all consumers and producers
	return 30 * time.Second
}

func (x *Kafka) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}
