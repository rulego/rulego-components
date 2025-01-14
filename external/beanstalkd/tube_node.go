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
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&TubeNode{})
}

// TubeConfiguration 节点配置
type TubeConfiguration struct {
	// 服务器地址
	Server string
	// Tube名称
	Tube string
	// 命令：Put、PeekReady、PeekDelayed、PeekBuried、Kick、Stat、Pause
	Cmd string
	// Put命令参数pri 允许使用 ${} 占位符变量
	PutPri string
	// Put命令参数delay 允许使用 ${} 占位符变量
	PutDelay string
	// Put命令参数ttr 允许使用 ${} 占位符变量
	PutTTR string
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
	//初始化模板
	x.putPriTemplate = str.NewTemplate(x.Config.PutPri)
	x.putDelayTemplate = str.NewTemplate(x.Config.PutDelay)
	x.putTTRTemplate = str.NewTemplate(x.Config.PutTTR)
	x.kickBoundTemplate = str.NewTemplate(x.Config.KickBound)
	x.pauseTimeTemplate = str.NewTemplate(x.Config.PauseTime)
	if err == nil {
		//初始化客户端
		err = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*beanstalk.Tube, error) {
			return x.initClient()
		})
	}
	return err
}

// OnMsg 处理消息
func (x *TubeNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var (
		err   error
		id    uint64
		body  []byte
		count int
		data  map[string]any = make(map[string]any)
		pri   uint32         = DefaultPri
		delay time.Duration  = DefaultDelay
		ttr   time.Duration  = DefaultTime
		pause time.Duration  = DefaultTime
		bound int            = DefaultBound
	)
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	switch x.Config.Cmd {
	case Put:
		if !x.putPriTemplate.IsNotVar() {
			tmp := x.putPriTemplate.Execute(evn)
			ti, err := strconv.Atoi(tmp)
			if err != nil {
				break
			}
			pri = uint32(ti)
		} else if len(x.Config.PutPri) > 0 {
			ti, err := strconv.Atoi(x.Config.PutPri)
			if err != nil {
				break
			}
			pri = uint32(ti)
		}

		if !x.putDelayTemplate.IsNotVar() {
			tmp := x.putDelayTemplate.Execute(evn)
			delay, err = time.ParseDuration(tmp)
		} else if len(x.Config.PutDelay) > 0 {
			delay, err = time.ParseDuration(x.Config.PutDelay)
		}
		if err != nil {
			break
		}
		if !x.putTTRTemplate.IsNotVar() {
			tmp := x.putTTRTemplate.Execute(evn)
			ttr, err = time.ParseDuration(tmp)
		} else if len(x.Config.PutTTR) > 0 {
			ttr, err = time.ParseDuration(x.Config.PutTTR)
		}
		if err != nil {
			break
		}
		id, err = x.tube.Put([]byte(msg.Data), pri, delay, ttr)
		if err != nil {
			break
		}
		data["id"] = id
	case PeekReady:
		id, body, err = x.tube.PeekReady()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
	case PeekDelayed:
		id, body, err = x.tube.PeekDelayed()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
	case PeekBuried:
		id, body, err = x.tube.PeekBuried()
		if err != nil {
			break
		}
		data["id"] = id
		data["body"] = string(body)
	case Kick:
		if !x.kickBoundTemplate.IsNotVar() {
			tmp := x.kickBoundTemplate.Execute(evn)
			bound, err = strconv.Atoi(tmp)
		} else if len(x.Config.KickBound) > 0 {
			bound, err = strconv.Atoi(x.Config.KickBound)
		}
		if err != nil {
			break
		}
		count, err = x.tube.Kick(bound)
		if err != nil {
			break
		}
		data["count"] = count
	case Stat:
		if !x.pauseTimeTemplate.IsNotVar() {
			tmp := x.pauseTimeTemplate.Execute(evn)
			pause, err = time.ParseDuration(tmp)
		} else if len(x.Config.PauseTime) > 0 {
			pause, err = time.ParseDuration(x.Config.PauseTime)
		}
		if err != nil {
			break
		}
		var stat map[string]string
		stat, err = x.tube.Stats()
		for k, v := range stat {
			data[k] = v
		}
	case Pause:
		err = x.tube.Pause(pause)
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
		ctx.TellSuccess(msg)
	}
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
		x.tube = beanstalk.NewTube(conn, x.Config.Tube)
		return x.tube, err
	}
}
