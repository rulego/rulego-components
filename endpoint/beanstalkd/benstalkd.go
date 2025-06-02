package beanstalkd

import (
	"context"
	"encoding/json"
	"log"
	"net/textproto"
	"time"

	"errors"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
)

const (
	Type                     = types.EndpointTypePrefix + "beanstalkdTubeset"
	BEANSTALKD_DATA_MSG_TYPE = "BEANSTALKD_DATA"
	DefaultTube              = "default"
)

// Endpoint 别名
type Endpoint = BeanstalkdTubeSet

var _ endpointApi.Endpoint = (*Endpoint)(nil)

// 注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

// beanstalk Tubeset 配置
type TubesetConfig struct {
	// 服务器地址
	Server string
	// tube 列表
	Tubesets []string
	// 超时参数
	Timeout int64
}

type BeanstalkdTubeSet struct {
	impl.BaseEndpoint
	base.SharedNode[*beanstalk.Conn]
	RuleConfig types.Config
	// beanstalk Tubeset 相关配置
	Config TubesetConfig
	// 路由实例
	Router endpointApi.Router
	// beanstalk 连接实例
	conn *beanstalk.Conn
	// beanstalk Tubesett实例
	tubeset *beanstalk.TubeSet
}

// Type 组件类型
func (x *BeanstalkdTubeSet) Type() string {
	return Type
}

// New 创建组件实例
func (x *BeanstalkdTubeSet) New() types.Node {
	return &BeanstalkdTubeSet{
		Config: TubesetConfig{
			Server:   "127.0.0.1:11300",
			Tubesets: []string{DefaultTube},
			Timeout:  300,
		},
	}
}

// Init 初始化
func (x *BeanstalkdTubeSet) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	x.RuleConfig = ruleConfig
	_ = x.SharedNode.Init(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*beanstalk.Conn, error) {
		return x.initClient()
	})
	return err
}

// Destroy 销毁
func (x *BeanstalkdTubeSet) Destroy() {
	_ = x.Close()
}

func (x *BeanstalkdTubeSet) Close() error {
	if x.tubeset != nil && x.tubeset.Conn != nil {
		_ = x.conn.Close()
		x.tubeset = nil
	}
	return nil
}

// Id 获取组件id
func (x *BeanstalkdTubeSet) Id() string {
	return x.Config.Server
}

// AddRouter 添加路由
func (x *BeanstalkdTubeSet) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router cannot be nil")
	}
	if x.Router != nil {
		return "", errors.New("duplicate router")
	}
	x.Router = router
	return router.GetId(), nil
}

// RemoveRouter 移除路由
func (x *BeanstalkdTubeSet) RemoveRouter(routerId string, params ...interface{}) error {
	x.Lock()
	defer x.Unlock()
	x.Router = nil
	return nil
}

// Start 启动
func (x *BeanstalkdTubeSet) Start() error {
	var err error
	if !x.SharedNode.IsInit() {
		err = x.SharedNode.Init(x.RuleConfig, x.Type(), x.Config.Server, false, func() (*beanstalk.Conn, error) {
			return x.initClient()
		})
		if err != nil {
			return err
		}
	}
	go func(router endpointApi.Router) {
		for {
			_ = x.reserve(x.Router)
			x.Printf("reserve job err: %s", err)
		}
	}(x.Router)
	return nil
}

// pop job： Remove a job from a queue and pass it to next node with job stat as meta.
func (x *BeanstalkdTubeSet) reserve(router endpointApi.Router) error {
	x.Lock()
	defer x.Unlock()
	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer func() {
		cancel()
	}()

	var err error
	timeout := time.Duration(x.Config.Timeout) * time.Second
	x.conn, err = x.SharedNode.Get()
	if err != nil {
		return err
	}
	x.tubeset = beanstalk.NewTubeSet(x.conn, x.Config.Tubesets...)
	id, data, err := x.tubeset.Reserve(timeout)
	if err != nil {
		x.Printf("reserve job error %v ", err)
		return err
	}
	stat, err := x.conn.StatsJob(id)
	if err != nil {
		x.Printf("get job stats error %v ", err)
		return err
	}
	if err != nil {
		x.Printf("delete job error %v ", err)
		return err
	}
	exchange := &endpoint.Exchange{
		In: &RequestMessage{
			body:  data,
			stats: stat,
		},
		Out: &ResponseMessage{
			body:  data,
			stats: stat,
		}}
	x.DoProcess(ctx, router, exchange)
	return nil
}

// Printf 打印日志
func (x *BeanstalkdTubeSet) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// initClient 初始化客户端
func (x *BeanstalkdTubeSet) initClient() (*beanstalk.Conn, error) {
	if x.conn != nil {
		return x.conn, nil
	} else {
		_, cancel := context.WithTimeout(context.TODO(), 4*time.Second)
		x.Lock()
		defer func() {
			cancel()
			x.Unlock()
		}()
		if x.conn != nil {
			return x.conn, nil
		}
		var err error
		x.conn, err = beanstalk.Dial("tcp", x.Config.Server)
		if err != nil {
			return nil, err
		}
		x.tubeset = beanstalk.NewTubeSet(x.conn, x.Config.Tubesets...)
		return x.conn, err
	}
}

type RequestMessage struct {
	headers    textproto.MIMEHeader
	body       []byte
	stats      map[string]string
	msg        *types.RuleMsg
	statusCode int
	err        error
}

func (r *RequestMessage) Body() []byte {
	return r.body
}

func (r *RequestMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *RequestMessage) From() string {
	return ""
}

// GetParam 不提供获取参数
func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, BEANSTALKD_DATA_MSG_TYPE, types.JSON, types.BuildMetadata(r.stats), string(r.Body()))
		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
	r.statusCode = statusCode
}
func (r *RequestMessage) SetBody(body []byte) {
	r.body = body
}

// SetError set error
func (r *RequestMessage) SetError(err error) {
	r.err = err
}

// GetError get error
func (r *RequestMessage) GetError() error {
	return r.err
}

type ResponseMessage struct {
	headers    textproto.MIMEHeader
	body       []byte
	stats      map[string]string
	msg        *types.RuleMsg
	statusCode int
	err        error
}

func (r *ResponseMessage) Body() []byte {
	b, err := json.Marshal(r.body)
	if err != nil {
		log.Println(err)
	}
	return b
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return ""
}

// GetParam 不提供获取参数
func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, BEANSTALKD_DATA_MSG_TYPE, types.JSON, types.BuildMetadata(r.stats), string(r.Body()))
		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
	r.statusCode = statusCode
}
func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body
}
func (r *ResponseMessage) getBody() []byte {
	return r.body
}

// SetError set error
func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

// GetError get error
func (r *ResponseMessage) GetError() error {
	return r.err
}
