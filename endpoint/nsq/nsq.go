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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

// Type returns the component type
const Type = types.EndpointTypePrefix + "nsq"

// KeyResponseTopic: Response topic metadataKey
const KeyResponseTopic = "responseTopic"

// Endpoint alias
type Endpoint = Nsq

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// Register the component
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// RequestMessage
type RequestMessage struct {
	topic   string
	message *nsq.Message
	msg     *types.RuleMsg
	err     error
}

// Body
func (r *RequestMessage) Body() []byte {
	return r.message.Body
}

// Headers: Get the message header
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(textproto.MIMEHeader)
	header.Set("topic", r.topic)
	header.Set("attempts", fmt.Sprintf("%d", r.message.Attempts))
	header.Set("timestamp", fmt.Sprintf("%d", r.message.Timestamp))
	return header
}

// Source: Source
func (r *RequestMessage) From() string {
	return string(r.topic)
}

// GetParam to get the parameters
func (r *RequestMessage) GetParam(key string) string {
	return ""
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
		ruleMsg.Metadata.PutValue("messageId", string(r.message.ID[:]))
		ruleMsg.Metadata.PutValue("attempts", fmt.Sprintf("%d", r.message.Attempts))
		ruleMsg.Metadata.PutValue("timestamp", fmt.Sprintf("%d", r.message.Timestamp))
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

// nsqPublisher polls publish abstraction for single or multiple nsqd polls, facilitating runtime load balancing
type nsqPublisher interface {
	Publish(topic string, body []byte) error
	Stop()
}

// roundRobinProducers Publish round-robin messages across multiple * nsq.Producer;
// If a single Publish fails, it will try the remaining nodes one by one, balancing load balancing and fault tolerance when a single node is temporarily unavailable.
type roundRobinProducers struct {
	prods []*nsq.Producer
	rr    uint32
}

func (p *roundRobinProducers) Publish(topic string, body []byte) error {
	n := len(p.prods)
	if n == 0 {
		return errors.New("no nsqd producer in pool")
	}
	// Each time starts from polling cursors, dispersing traffic between nodes
	start := int(atomic.AddUint32(&p.rr, 1)-1) % n
	var lastErr error
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		err := p.prods[idx].Publish(topic, body)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func (p *roundRobinProducers) Stop() {
	for _, pr := range p.prods {
		if pr != nil {
			pr.Stop()
		}
	}
}

// ResponseMessage
type ResponseMessage struct {
	topic     string
	message   *nsq.Message
	publisher nsqPublisher
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
	return r.topic
}

// GetParam to get the parameters
func (r *ResponseMessage) GetParam(key string) string {
	return ""
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
	if topic != "" && r.publisher != nil {
		err := r.publisher.Publish(topic, r.body)
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

// Config NSQ configuration
type Config struct {
	// NSQ server address, supports multiple formats:
	// 1. Single nsqd: "127.0.0.1:4150"
	// 2. Multiple nsqd: "127.0.0.1:4150,127.0.0.1:4151" (establish a connection for all reachable nodes, runtime is announced by message polling, see README.md)
	// 3. lookupd address: "http://127.0.0.1:4161,http://127.0.0.1:4162" (sequentially try the /nodes of each lookupd, link and poll the returned nsqd for publishing)
	// Instructions and examples are in the same directory README.md
	Server string `json:"server" label:"Server" desc:"NSQ server address. Supports nsqd 'host:port' (single or comma-separated multiple) or lookupd 'http://host:port' (comma-separated), e.g. 127.0.0.1:4150 or http://127.0.0.1:4161" required:"true" ref:"primary"`
	// The default channel name, which is used if not specified during AddRouter
	Channel string `json:"channel" label:"Channel" desc:"Default channel name, used when AddRouter does not specify one"`
	// Authority and token of authority
	AuthToken string `json:"authToken" label:"Auth Token" desc:"NSQ authentication token"`
	// TLS certificate file
	CertFile string `json:"certFile" label:"Cert File" desc:"TLS certificate file path"`
	// TLS private key file
	CertKeyFile string `json:"certKeyFile" label:"Cert Key File" desc:"TLS private key file path"`
}

// NSQ NSQ receiving endpoint
type Nsq struct {
	impl.BaseEndpoint
	// GracefulShutdown provides graceful shutdown capabilities
	// GracefulShutdown offers an elegant shutdown function
	base.GracefulShutdown
	RuleConfig types.Config
	//Config configuration
	Config Config
	// Consumer mapping relationship, used to stop consumption, key is routerId
	consumers map[string]*nsq.Consumer
	// Publisher (single-node or multi-node polling)
	publisher nsqPublisher
	// Mutually exclusive locks
	mu sync.RWMutex
}

// Type returns the component type
func (x *Nsq) Type() string {
	return Type
}

// parseAddresses parses addresses in the Server field
// Supported formats:
// 1. Single nsqd: "127.0.0.1:4150"
// 2. Multiple nsqd: "127.0.0.1:4150,127.0.0.1:4151"
// 3. lookupd address: "http://127.0.0.1:4161,http://127.0.0.1:4162"
func (x *Nsq) parseAddresses() (nsqdAddrs []string, lookupdAddrs []string) {
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

// ID to obtain the component ID
func (x *Nsq) Id() string {
	return x.Config.Server
}

// New creates an instance
func (x *Nsq) New() types.Node {
	return &Nsq{
		Config: Config{
			Server:  "127.0.0.1:4150",
			Channel: "default",
		},
	}
}

func (x *Nsq) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "NSQ consumer endpoint for subscribing to topics and processing messages",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name:     "path",
					Type:     "string",
					Label:    "Topic",
					Desc:     "NSQ topic to subscribe, e.g. orders",
					Required: true,
				},
			},
		},
	}
}

// Init initializes the component
func (x *Nsq) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	x.consumers = make(map[string]*nsq.Consumer)

	// Initialize the elegant shutdown function
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 0)

	// Initialize the producer
	if x.Config.Server != "" {
		// Parse address configuration
		nsqdAddrs, lookupdAddrs := x.parseAddresses()

		// For multiple addresses, a Producer is created for all accessible nsqd, and runtime is polled and released by roundRobinProducers
		producerConfig := nsq.NewConfig()
		if x.Config.AuthToken != "" {
			producerConfig.AuthSecret = x.Config.AuthToken
		}

		var nsqdCandidates []string
		if len(nsqdAddrs) > 0 {
			nsqdCandidates = nsqdAddrs
		} else if len(lookupdAddrs) > 0 {
			discovered, discoverErr := discoverNsqdProducersFromLookupds(lookupdAddrs)
			if discoverErr != nil {
				return discoverErr
			}
			nsqdCandidates = discovered
		} else {
			nsqdCandidates = []string{strings.TrimSpace(x.Config.Server)}
		}

		if len(nsqdCandidates) > 0 {
			nsqdCandidates = dedupeAddrsStable(nsqdCandidates)
			prods, connectErr := buildReachableProducers(nsqdCandidates, producerConfig)
			if connectErr != nil {
				return connectErr
			}
			x.publisher = &roundRobinProducers{prods: prods}
		}
	}

	return err
}

// Destroy releases resources
func (x *Nsq) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// GracefulStop Graceful Stop
func (x *Nsq) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

// Close Close closes the connection
func (x *Nsq) Close() error {
	x.mu.Lock()
	defer x.mu.Unlock()

	// Stop all consumers
	for _, consumer := range x.consumers {
		consumer.Stop()
	}
	x.consumers = make(map[string]*nsq.Consumer)

	// Stop publishing the platform
	if x.publisher != nil {
		x.publisher.Stop()
		x.publisher = nil
	}

	x.BaseEndpoint.Destroy()
	return nil
}

// AddRouter adds a route
// Create independent consumers for each route, and if the route already exists, it will directly throw an error
func (x *Nsq) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}

	routerId := router.GetId()
	if routerId == "" {
		routerId = router.GetFrom().ToString()
		router.SetId(routerId)
	}

	x.mu.Lock()
	defer x.mu.Unlock()

	// Check whether the routerId already exists; if it does, it will directly send an error
	if _, exists := x.consumers[routerId]; exists {
		return "", fmt.Errorf("routerId %s already exists", routerId)
	}

	// Parse topics and channels
	from := strings.TrimSpace(router.FromToString())
	topic := from
	channel := strings.TrimSpace(x.Config.Channel)
	if channel == "" {
		channel = "default"
	}

	// If there are parameters, the first parameter is treated as a channel and has higher priority than the configuration
	if len(params) > 0 {
		if ch, ok := params[0].(string); ok && ch != "" {
			channel = ch
		}
	}

	// Create new consumer configurations
	consumerConfig := nsq.NewConfig()
	// Set authentication configurations
	if x.Config.AuthToken != "" {
		consumerConfig.AuthSecret = x.Config.AuthToken
	}

	// Create consumers
	consumer, err := nsq.NewConsumer(topic, channel, consumerConfig)
	if err != nil {
		return "", err
	}

	// Disable NSQ internal log output
	consumer.SetLoggerLevel(nsq.LogLevelError)

	// Set the message handler and directly pass the router parameters
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		return x.handleMessage(message, router, topic)
	}))

	// Connect to lookupd or nsqd
	nsqdAddrs, lookupdAddrs := x.parseAddresses()
	if len(lookupdAddrs) > 0 {
		err = consumer.ConnectToNSQLookupds(lookupdAddrs)
	} else if len(nsqdAddrs) > 0 {
		if len(nsqdAddrs) == 1 {
			err = consumer.ConnectToNSQD(nsqdAddrs[0])
		} else {
			err = consumer.ConnectToNSQDs(nsqdAddrs)
		}
	} else {
		return "", errors.New("no NSQ address configured")
	}

	if err != nil {
		consumer.Stop()
		return "", err
	}

	// Preserve consumers
	x.consumers[routerId] = consumer
	return routerId, nil
}

// RemoveRouter removes the route
// Stop and delete consumers on specified routes
func (x *Nsq) RemoveRouter(routerId string, params ...interface{}) error {
	x.mu.Lock()
	defer x.mu.Unlock()

	consumer, ok := x.consumers[routerId]
	if !ok {
		return errors.New("router not found")
	}

	// Stop the consumer
	consumer.Stop()
	// Removing consumers
	delete(x.consumers, routerId)
	return nil
}

// handleMessage handles individual messages
// Handles NSQ messages, creates Exchanges, and executes rule chain processing for specified routes
func (x *Nsq) handleMessage(message *nsq.Message, router endpointApi.Router, topic string) error {
	defer func() {
		if e := recover(); e != nil {
			x.Printf("nsq endpoint handler err :\n%v", runtime.Stack())
		}
	}()

	exchange := &endpointApi.Exchange{
		In: &RequestMessage{
			message: message,
			topic:   topic,
		},
		Out: &ResponseMessage{
			message:   message,
			publisher: x.publisher,
			topic:     topic,
		},
	}
	x.DoProcess(context.Background(), router, exchange)
	return nil
}

// Start the service
func (x *Nsq) Start() error {
	return nil
}

// lookupdNodesProducer corresponds to a single producer in lookupd/nodes
type lookupdNodesProducer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

func nsqdAddrFromLookupdProducer(p lookupdNodesProducer) (string, bool) {
	if p.TCPPort <= 0 || p.TCPPort > 65535 {
		return "", false
	}
	// Consistent with historical behavior: Broadcast + tcp_port takes priority, otherwise remote + tcp_port
	if p.BroadcastAddress != "" {
		return fmt.Sprintf("%s:%d", p.BroadcastAddress, p.TCPPort), true
	}
	if p.RemoteAddress != "" {
		return fmt.Sprintf("%s:%d", p.RemoteAddress, p.TCPPort), true
	}
	return "", false
}

// dedupeAddrsStable removes deduplications in the order they first appear
func dedupeAddrsStable(addrs []string) []string {
	seen := make(map[string]struct{}, len(addrs))
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}

// fetchNsqdProducersFromLookupd requests /nodes for a single lookupd and returns a list of dialable nsqd addresses
func fetchNsqdProducersFromLookupd(lookupdAddr string) ([]string, error) {
	apiURL := fmt.Sprintf("%s/nodes", strings.TrimSuffix(lookupdAddr, "/"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build lookupd request: %w", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query lookupd API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("lookupd API status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read lookupd response: %w", err)
	}
	var response struct {
		Producers []lookupdNodesProducer `json:"producers"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("parse lookupd response: %w", err)
	}
	if len(response.Producers) == 0 {
		return nil, nil
	}
	candidates := make([]string, 0, len(response.Producers))
	for _, pr := range response.Producers {
		if addr, ok := nsqdAddrFromLookupdProducer(pr); ok {
			candidates = append(candidates, addr)
		}
	}
	return dedupeAddrsStable(candidates), nil
}

// discoverNsqdProducersFromLookupds Attempt multiple lookupds in order, and after the first successful attempt, return a non-empty result
// The NSQD list is used for this list. Aggregation error returns when all fail.
func discoverNsqdProducersFromLookupds(lookupdAddrs []string) ([]string, error) {
	if len(lookupdAddrs) == 0 {
		return nil, errors.New("no lookupd address configured")
	}
	var errs []error
	for _, u := range lookupdAddrs {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		addrs, err := fetchNsqdProducersFromLookupd(u)
		if err != nil {
			errs = append(errs, fmt.Errorf("lookupd %s: %w", u, err))
			continue
		}
		if len(addrs) == 0 {
			errs = append(errs, fmt.Errorf("lookupd %s: no nsqd nodes in /nodes response", u))
			continue
		}
		return addrs, nil
	}
	if len(errs) == 0 {
		return nil, errors.New("no non-empty lookupd address in configuration")
	}
	return nil, fmt.Errorf("all lookupd failed: %w", errors.Join(errs...))
}

// buildReachableProducers creates and pings a Producer for each candidate address, keeping all successful instances; Unreachable Meeting Stop and Skip.
// Multiple instances allow roundRobinProducers to retry load balancing during runtime and retry with other nodes if release fails.
func buildReachableProducers(candidates []string, cfg *nsq.Config) ([]*nsq.Producer, error) {
	if len(candidates) == 0 {
		return nil, errors.New("no nsqd address candidates")
	}
	var out []*nsq.Producer
	var lastErr error
	for _, addr := range candidates {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		p, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			lastErr = err
			continue
		}
		if err := p.Ping(); err != nil {
			p.Stop()
			lastErr = err
			continue
		}
		p.SetLoggerLevel(nsq.LogLevelError)
		out = append(out, p)
	}
	if len(out) == 0 {
		if lastErr == nil {
			lastErr = errors.New("no valid non-empty address in nsqd candidate list")
		}
		return nil, fmt.Errorf("no reachable nsqd: %w", lastErr)
	}
	return out, nil
}

// Printf prints logs
func (x *Nsq) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}
