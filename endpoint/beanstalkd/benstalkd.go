package beanstalkd

import (
	"context"
	"encoding/json"
	"log"
	"net/textproto"
	"sync/atomic"
	"time"

	"errors"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
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
	base.GracefulShutdown
	RuleConfig types.Config
	// beanstalk Tubeset 相关配置
	Config TubesetConfig
	// 路由实例
	Router endpointApi.Router
	// beanstalk Tubesett实例
	// tubeset *beanstalk.TubeSet
	started int32
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
	x.GracefulShutdown.InitGracefulShutdown(x.RuleConfig.Logger, 10*time.Second)
	_ = x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, true, func() (*beanstalk.Conn, error) {
		return x.initClient()
	}, func(conn *beanstalk.Conn) error {
		if conn != nil {
			return conn.Close()
		}
		return nil
	})
	return err
}

// Destroy 销毁
func (x *BeanstalkdTubeSet) Destroy() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
}

func (x *BeanstalkdTubeSet) Close() error {
	// SharedNode 会通过 InitWithClose 中的清理函数来管理客户端的关闭
	// SharedNode manages client closure through the cleanup function in InitWithClose
	_ = x.SharedNode.Close()
	x.BaseEndpoint.Destroy()
	return nil
}

// GracefulStop provides graceful shutdown for the beanstalkd endpoint
func (x *BeanstalkdTubeSet) GracefulStop() {
	x.GracefulShutdown.GracefulStop(func() {
		_ = x.Close()
	})
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
	if atomic.LoadInt32(&x.started) == 1 {
		return nil
	}
	var err error
	if !x.SharedNode.IsInit() {
		err = x.SharedNode.InitWithClose(x.RuleConfig, x.Type(), x.Config.Server, false, func() (*beanstalk.Conn, error) {
			return x.initClient()
		}, func(conn *beanstalk.Conn) error {
			if conn != nil {
				return conn.Close()
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	atomic.StoreInt32(&x.started, 1)

	go func() {
		defer func() {
			atomic.StoreInt32(&x.started, 0)
			if e := recover(); e != nil {
				x.Printf("beanstalkd endpoint reserve err :\n%v", runtime.Stack())
			}
		}()
		for {
			if x.GracefulShutdown.IsShuttingDown() {
				return
			}
			// 增加活跃操作计数
			x.GracefulShutdown.IncrementActiveOperations()

			reserveErr := x.reserve()

			x.GracefulShutdown.DecrementActiveOperations()

			if reserveErr != nil {
				if x.GracefulShutdown.IsShuttingDown() {
					return
				}

				// Ignore timeout errors, they are expected when no job is available
				var connErr beanstalk.ConnError
				if errors.As(reserveErr, &connErr) && connErr.Err == beanstalk.ErrTimeout {
					continue
				}
				x.Printf("reserve error: %v, retrying after 5 seconds", reserveErr)
				select {
				case <-time.After(5 * time.Second):
				case <-x.GracefulShutdown.GetShutdownContext().Done():
					return
				}
			}
		}
	}()
	return nil
}

// pop job： Remove a job from a queue and pass it to next node with job stat as meta.
func (x *BeanstalkdTubeSet) reserve() error {
	conn, err := x.SharedNode.GetSafely()
	if err != nil {
		return err
	}
	// Use a local tubeset to avoid race conditions with Close
	tubeset := beanstalk.NewTubeSet(conn, x.Config.Tubesets...)

	timeout := time.Duration(x.Config.Timeout) * time.Second
	id, data, err := tubeset.Reserve(timeout)
	if err != nil {
		return err
	}

	// Lock to get the router, then unlock to avoid holding lock during processing
	x.RLock()
	router := x.Router
	x.RUnlock()

	// If router is nil, delete the job to prevent it from being stuck
	if router == nil {
		// Try to delete the job. If it fails, we can't do much more.
		_ = conn.Delete(id)
		return nil
	}
	stat, err := conn.StatsJob(id)
	if err != nil {
		// Also delete job if we can't get its stats
		_ = conn.Delete(id)
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
	x.DoProcess(context.Background(), router, exchange)
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
	conn, err := beanstalk.Dial("tcp", x.Config.Server)
	return conn, err
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
