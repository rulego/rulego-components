package beanstalkd

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	Put         = "Put"
	PeekReady   = "PeekReady"
	PeekDelayed = "PeekDelayed"
	PeekBuried  = "PeekBuried"
	Kick        = "Kick"
	Stat        = "Stat"
	Pause       = "Pause"

	//优先级
	PriHigh   uint32 = 1
	PriNormal uint32 = 2
	PriLow    uint32 = 3

	DefaultPri   = PriLow
	DefaultDelay = time.Second * 0
	DefaultTime  = time.Second * 5
	DefaultBound = 10
	DefaultTube  = "default"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&TubeNode{})
}

type TubeMsgParams struct {
	Tube  string
	Body  string
	Pri   uint32
	Delay time.Duration
	Ttr   time.Duration
	Pause time.Duration
	Bound int
}

// TubeConfiguration 节点配置
type TubeConfiguration struct {
	// 服务器地址
	Server string
	// Tube名称 允许使用 ${} 占位符变量
	Tube string
	// 命令名称，支持Put、PeekReady、PeekDelayed、PeekBuried、Kick、Stat、Pause
	Cmd string
	// 消息内容：body 允许使用 ${} 占位符变量
	Body string
	// 优先级：pri 允许使用 ${} 占位符变量
	Pri string
	// 延迟时间：delay 允许使用 ${} 占位符变量
	Delay string
	// 最大执行秒数:ttr 允许使用 ${} 占位符变量
	Ttr string
	// Kick命令参数bound 允许使用 ${} 占位符变量
	KickBound string
	// Pause命令参数time 允许使用 ${} 占位符变量
	PauseTime string
}

// TubeNode 客户端节点，
// 成功：转向Success链，发送消息执行结果存放在msg.Data
// 失败：转向Failure链
type TubeNode struct {
	base.SharedNode[*beanstalk.Conn]
	//节点配置
	Config TubeConfiguration
	// tubeTemplate Tube名称模板，用于解析动态Tube名称
	// tubeTemplate template for resolving dynamic tube names
	tubeTemplate el.Template
	// putBodyTemplate 消息内容模板，用于解析动态消息内容
	// putBodyTemplate template for resolving dynamic message body
	putBodyTemplate el.Template
	// putPriTemplate 优先级模板，用于解析动态优先级
	// putPriTemplate template for resolving dynamic priority
	putPriTemplate el.Template
	// putDelayTemplate 延迟时间模板，用于解析动态延迟时间
	// putDelayTemplate template for resolving dynamic delay time
	putDelayTemplate el.Template
	// putTTRTemplate TTR模板，用于解析动态TTR时间
	// putTTRTemplate template for resolving dynamic TTR time
	putTTRTemplate el.Template
	// kickBoundTemplate Kick边界模板，用于解析动态Kick边界
	// kickBoundTemplate template for resolving dynamic kick bound
	kickBoundTemplate el.Template
	// pauseTimeTemplate 暂停时间模板，用于解析动态暂停时间
	// pauseTimeTemplate template for resolving dynamic pause time
	pauseTimeTemplate el.Template
	// hasVar 标识模板是否包含变量，用于优化性能
	// hasVar indicates whether the template contains variables for performance optimization
	hasVar bool
}

// Type 返回组件类型
func (x *TubeNode) Type() string {
	return "x/beanstalkdTube"
}

// New 默认参数
func (x *TubeNode) New() types.Node {
	return &TubeNode{Config: TubeConfiguration{
		Server: "127.0.0.1:11300",
		Tube:   "default",
		Cmd:    Stat,
	}}
}

// Init 初始化组件
func (x *TubeNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//初始化客户端
		err = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, false, func() (*beanstalk.Conn, error) {
			return x.initClient()
		}, func(conn *beanstalk.Conn) error {
			return conn.Close()
		})
	}
	//初始化模板
	x.tubeTemplate, err = el.NewTemplate(x.Config.Tube)
	if err != nil {
		return err
	}
	x.putBodyTemplate, err = el.NewTemplate(x.Config.Body)
	if err != nil {
		return err
	}
	x.putPriTemplate, err = el.NewTemplate(x.Config.Pri)
	if err != nil {
		return err
	}
	x.putDelayTemplate, err = el.NewTemplate(x.Config.Delay)
	if err != nil {
		return err
	}
	x.putTTRTemplate, err = el.NewTemplate(x.Config.Ttr)
	if err != nil {
		return err
	}
	x.kickBoundTemplate, err = el.NewTemplate(x.Config.KickBound)
	if err != nil {
		return err
	}
	x.pauseTimeTemplate, err = el.NewTemplate(x.Config.PauseTime)
	if err != nil {
		return err
	}
	// 检查是否有任何模板包含变量
	x.hasVar = x.tubeTemplate.HasVar() || x.putBodyTemplate.HasVar() || x.putPriTemplate.HasVar() || x.putDelayTemplate.HasVar() || x.putTTRTemplate.HasVar() || x.kickBoundTemplate.HasVar() || x.pauseTimeTemplate.HasVar()
	return err
}

// OnMsg 处理消息
func (x *TubeNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	x.Locker.Lock()
	defer x.Locker.Unlock()
	var (
		err    error
		id     uint64
		body   []byte
		count  int
		stat   map[string]string
		data   map[string]any = make(map[string]any)
		params *TubeMsgParams
	)
	params, err = x.getParams(ctx, msg)

	// use tube
	conn, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	x.Printf("conn :%v ", conn)
	tube := beanstalk.NewTube(conn, params.Tube)
	conn.Tube.Name = params.Tube
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	switch x.Config.Cmd {
	case Put:
		id, err = tube.Put([]byte(params.Body), params.Pri, params.Delay, params.Ttr)
		if err != nil {
			x.Printf("put job with err: %s", err)
			break
		}
		data["id"] = id
		x.Printf("put job id:%d to %s ", id, tube.Conn.Tube.Name)
	case PeekReady:
		id, body, err = tube.PeekReady()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
		x.Printf("peek ready job id:%d  with err: %s", id, err)
	case PeekDelayed:
		id, body, err = tube.PeekDelayed()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
		x.Printf("peek delayed job id:%d  with err: %s", id, err)
	case PeekBuried:
		id, body, err = tube.PeekBuried()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
		x.Printf("peek bury job id:%d  with err: %s", id, err)
	case Kick:
		count, err = tube.Kick(params.Bound)
		if err != nil {
			break
		}
		data["count"] = count
		x.Printf("kicked with err: %s", err)
	case Stat:
		stat, err = tube.Stats()
		for k, v := range stat {
			data[k] = v
		}
		x.Printf("tube stats:%v, err: %s", stat, err)
	case Pause:
		err = tube.Pause(params.Pause)
		x.Printf("pause with  err: %s", err)
	default:
		err = errors.New("Unknown Command")
	}
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		bytes, err := json.Marshal(data)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		msg.SetData(str.ToString(bytes))
		if id > 0 {
			stat, err = tube.Conn.StatsJob(id)
			if err != nil {
				x.Printf("get job stats error %v ", err)
				ctx.TellFailure(msg, err)
				return
			}
			msg.Metadata.ReplaceAll(stat)
		}
		ctx.TellSuccess(msg)
	}
}

func (x *TubeNode) getParams(ctx types.RuleContext, msg types.RuleMsg) (*TubeMsgParams, error) {
	var (
		err    error
		tube   string        = DefaultTube
		body   string        = ""
		pri    uint32        = DefaultPri
		delay  time.Duration = DefaultDelay
		ttr    time.Duration = DefaultTime
		pause  time.Duration = DefaultTime
		bound  int           = DefaultBound
		params               = TubeMsgParams{
			Tube:  tube,
			Body:  body,
			Pri:   DefaultPri,
			Bound: DefaultBound,
			Delay: DefaultDelay,
			Ttr:   DefaultTime,
			Pause: DefaultTime,
		}
	)
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}
	// 获取tube参数
	if !x.tubeTemplate.IsNotVar() {
		tube = x.tubeTemplate.ExecuteAsString(evn)
	} else if len(x.Config.Tube) > 0 {
		tube = x.Config.Tube
	}
	// 获取body参数
	if !x.putBodyTemplate.IsNotVar() {
		body = x.putBodyTemplate.ExecuteAsString(evn)
	} else if len(x.Config.Body) > 0 {
		body = x.Config.Body
	} else {
		body = msg.GetData()
	}
	// 获取优先级参数
	var ti int
	if !x.putPriTemplate.IsNotVar() {
		tmp := x.putPriTemplate.ExecuteAsString(evn)
		ti, err = strconv.Atoi(tmp)
		pri = uint32(ti)
	} else if len(x.Config.Pri) > 0 {
		ti, err = strconv.Atoi(x.Config.Pri)
		pri = uint32(ti)
	}
	if err != nil {
		return nil, err
	}
	// 获取延迟参数
	if !x.putDelayTemplate.IsNotVar() {
		tmp := x.putDelayTemplate.ExecuteAsString(evn)
		delay, err = time.ParseDuration(tmp)
	} else if len(x.Config.Delay) > 0 {
		delay, err = time.ParseDuration(x.Config.Delay)
	}
	if err != nil {
		return nil, err
	}
	// 获取TTR参数
	if !x.putTTRTemplate.IsNotVar() {
		tmp := x.putTTRTemplate.ExecuteAsString(evn)
		ttr, err = time.ParseDuration(tmp)
	} else if len(x.Config.Ttr) > 0 {
		ttr, err = time.ParseDuration(x.Config.Ttr)
	}
	if err != nil {
		return nil, err
	}
	// 获取Bound数量参数
	if !x.kickBoundTemplate.IsNotVar() {
		tmp := x.kickBoundTemplate.ExecuteAsString(evn)
		bound, err = strconv.Atoi(tmp)
	} else if len(x.Config.KickBound) > 0 {
		bound, err = strconv.Atoi(x.Config.KickBound)
	}
	if err != nil {
		return nil, err
	}
	// 获取暂停时间参数
	if !x.pauseTimeTemplate.IsNotVar() {
		tmp := x.pauseTimeTemplate.ExecuteAsString(evn)
		pause, err = time.ParseDuration(tmp)
	} else if len(x.Config.PauseTime) > 0 {
		pause, err = time.ParseDuration(x.Config.PauseTime)
	}
	if err != nil {
		return nil, err
	}
	params.Tube = tube
	params.Body = body
	params.Pri = pri
	params.Bound = bound
	params.Delay = delay
	params.Ttr = ttr
	params.Pause = pause
	return &params, nil
}

// Printf 打印日志
func (x *TubeNode) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// 初始化连接
func (x *TubeNode) initClient() (*beanstalk.Conn, error) {
	conn, err := beanstalk.Dial("tcp", x.Config.Server)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (x *TubeNode) Destroy() {
	_ = x.SharedNode.Close()
}
