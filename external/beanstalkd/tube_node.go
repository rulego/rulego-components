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
	TTR string
	// Kick命令参数bound 允许使用 ${} 占位符变量
	KickBound string
	// Pause命令参数time 允许使用 ${} 占位符变量
	PauseTime string
}

// TubeNode 客户端节点，
// 成功：转向Success链，发送消息执行结果存放在msg.Data
// 失败：转向Failure链
type TubeNode struct {
	base.SharedNode[*beanstalk.Tube]
	//节点配置
	Config            TubeConfiguration
	tube              *beanstalk.Tube
	tubeTemplate      str.Template
	putBodyTemplate   str.Template
	putPriTemplate    str.Template
	putDelayTemplate  str.Template
	putTTRTemplate    str.Template
	kickBoundTemplate str.Template
	pauseTimeTemplate str.Template
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
		err = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*beanstalk.Tube, error) {
			return x.initClient()
		})
	}
	//初始化模板
	x.tubeTemplate = str.NewTemplate(x.Config.Tube)
	x.putBodyTemplate = str.NewTemplate(x.Config.Body)
	x.putPriTemplate = str.NewTemplate(x.Config.Pri)
	x.putDelayTemplate = str.NewTemplate(x.Config.Delay)
	x.putTTRTemplate = str.NewTemplate(x.Config.TTR)
	x.kickBoundTemplate = str.NewTemplate(x.Config.KickBound)
	x.pauseTimeTemplate = str.NewTemplate(x.Config.PauseTime)
	return err
}

// OnMsg 处理消息
func (x *TubeNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
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
	x.Use(params.Tube)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	switch x.Config.Cmd {
	case Put:
		id, err = x.tube.Put([]byte(params.Body), params.Pri, params.Delay, params.Ttr)
		if err != nil {
			x.Printf("put job with err: %s", err)
			break
		}
		data["id"] = id
		x.Printf("put job id:%d to %s ", id, x.tube.Conn.Tube.Name)
	case PeekReady:
		id, body, err = x.tube.PeekReady()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
		x.Printf("peek ready job id:%d  with err: %s", id, err)
	case PeekDelayed:
		id, body, err = x.tube.PeekDelayed()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
		x.Printf("peek delayed job id:%d  with err: %s", id, err)
	case PeekBuried:
		id, body, err = x.tube.PeekBuried()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
		x.Printf("peek bury job id:%d  with err: %s", id, err)
	case Kick:
		count, err = x.tube.Kick(params.Bound)
		if err != nil {
			break
		}
		data["count"] = count
		x.Printf("kicked with err: %s", err)
	case Stat:
		stat, err = x.tube.Stats()
		for k, v := range stat {
			data[k] = v
		}
		x.Printf("tube stats:%v, err: %s", stat, err)
	case Pause:
		err = x.tube.Pause(params.Pause)
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
		msg.Data = str.ToString(bytes)
		if id > 0 {
			stat, err = x.tube.Conn.StatsJob(id)
			if err != nil {
				x.Printf("get job stats error %v ", err)
				ctx.TellFailure(msg, err)
				return
			}
			msg.Metadata = stat
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
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	// 获取tube参数
	if !x.tubeTemplate.IsNotVar() {
		tube = x.tubeTemplate.Execute(evn)
	} else if len(x.Config.Tube) > 0 {
		tube = x.Config.Tube
	}
	// 获取body参数
	if !x.putBodyTemplate.IsNotVar() {
		body = x.putBodyTemplate.Execute(evn)
	} else if len(x.Config.Body) > 0 {
		body = x.Config.Body
	} else {
		body = msg.Data
	}
	// 获取优先级参数
	var ti int
	if !x.putPriTemplate.IsNotVar() {
		tmp := x.putPriTemplate.Execute(evn)
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
		tmp := x.putDelayTemplate.Execute(evn)
		delay, err = time.ParseDuration(tmp)
	} else if len(x.Config.Delay) > 0 {
		delay, err = time.ParseDuration(x.Config.Delay)
	}
	if err != nil {
		return nil, err
	}
	// 获取TTR参数
	if !x.putTTRTemplate.IsNotVar() {
		tmp := x.putTTRTemplate.Execute(evn)
		ttr, err = time.ParseDuration(tmp)
	} else if len(x.Config.TTR) > 0 {
		ttr, err = time.ParseDuration(x.Config.TTR)
	}
	if err != nil {
		return nil, err
	}
	// 获取Bound数量参数
	if !x.kickBoundTemplate.IsNotVar() {
		tmp := x.kickBoundTemplate.Execute(evn)
		bound, err = strconv.Atoi(tmp)
	} else if len(x.Config.KickBound) > 0 {
		bound, err = strconv.Atoi(x.Config.KickBound)
	}
	if err != nil {
		return nil, err
	}
	// 获取暂停时间参数
	if !x.pauseTimeTemplate.IsNotVar() {
		tmp := x.pauseTimeTemplate.Execute(evn)
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

// Destroy 销毁组件
func (x *TubeNode) Destroy() {
	if x.tube != nil {
		_ = x.tube.Conn.Close()
	}
}

// Printf 打印日志
func (x *TubeNode) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// 初始化连接
func (x *TubeNode) initClient() (*beanstalk.Tube, error) {
	if x.tube != nil {
		return x.tube, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.tube != nil {
			return x.tube, nil
		}
		var err error
		conn, err := beanstalk.Dial("tcp", x.Config.Server)
		x.tube = beanstalk.NewTube(conn, DefaultTube)
		return x.tube, err
	}
}

// use tube
func (x *TubeNode) Use(tube string) {
	x.tube.Name = tube
	x.tube.Conn.Tube.Name = tube
}
