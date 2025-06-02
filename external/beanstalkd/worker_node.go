package beanstalkd

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	Delete     = "Delete"
	Release    = "Release"
	Bury       = "Bury"
	KickJob    = "KickJob"
	Touch      = "Touch"
	Peek       = "Peek"
	ReserveJob = "ReserveJob"
	StatsJob   = "StatsJob"
	Stats      = "Stats"
	ListTubes  = "ListTubes"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&WorkerNode{})
}

type WorkerMsgParams struct {
	Id    uint64
	Tube  string
	Pri   uint32
	Delay time.Duration
	Ttr   time.Duration
	Pause time.Duration
	Bound int
}

// WorkerConfiguration 节点配置
type WorkerConfiguration struct {
	// 服务器地址
	Server string
	// Tube名称 允许使用 ${} 占位符变量
	Tube string
	// 命令名称，支持Delete Release Bury KickJob Touch Peek ReserveJob  StatsJob Stats  ListTubes
	Cmd string
	// JobId  允许使用 ${} 占位符变量
	JobId string
	// 优先级: pri 允许使用 ${} 占位符变量
	Pri string
	// 延迟时间: delay 允许使用 ${} 占位符变量
	Delay string
}

// WorkerNode 客户端节点，
// 成功：转向Success链，发送消息执行结果存放在msg.Data
// 失败：转向Failure链
type WorkerNode struct {
	base.SharedNode[*beanstalk.Conn]
	//节点配置
	Config           WorkerConfiguration
	conn             *beanstalk.Conn
	tubeTemplate     str.Template
	jobIdTemplate    str.Template
	putPriTemplate   str.Template
	putDelayTemplate str.Template
}

// Type 返回组件类型
func (x *WorkerNode) Type() string {
	return "x/beanstalkdWorker"
}

// New 默认参数
func (x *WorkerNode) New() types.Node {
	return &WorkerNode{Config: WorkerConfiguration{
		Server: "127.0.0.1:11300",
		Tube:   "default",
		Cmd:    Stats,
	}}
}

// Init 初始化组件
func (x *WorkerNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//初始化客户端
		err = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, false, func() (*beanstalk.Conn, error) {
			return x.initClient()
		})
	}
	//初始化模板
	x.tubeTemplate = str.NewTemplate(x.Config.Tube)
	x.putPriTemplate = str.NewTemplate(x.Config.Pri)
	x.putDelayTemplate = str.NewTemplate(x.Config.Delay)
	x.jobIdTemplate = str.NewTemplate(x.Config.JobId)
	return err
}

// OnMsg 处理消息
func (x *WorkerNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	x.Locker.Lock()
	defer x.Locker.Unlock()
	var (
		err    error
		body   []byte
		tubes  []string          = make([]string, 0)
		data   map[string]string = make(map[string]string)
		params *WorkerMsgParams
		stat   map[string]string
	)
	params, err = x.getParams(ctx, msg)
	// use tube
	x.conn, err = x.SharedNode.Get()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	x.Printf("conn :%v ", x.conn)
	x.conn.Tube.Name = params.Tube
	switch x.Config.Cmd {
	case Delete:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.Delete(params.Id)
		x.Printf("delete job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case Release:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.Release(params.Id, params.Pri, params.Delay)
		x.Printf("release job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case Bury:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.Bury(params.Id, params.Pri)
		x.Printf("bury job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case KickJob:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.KickJob(params.Id)
		x.Printf("kick job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case Touch:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.Touch(params.Id)
		x.Printf("touch job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case Peek:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		body, err = x.conn.Peek(params.Id)
		data["body"] = string(body)
		x.Printf("peek job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case ReserveJob:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		body, err = x.conn.ReserveJob(params.Id)
		data["body"] = string(body)
		x.Printf("reserve job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case StatsJob:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		data, err = x.conn.StatsJob(params.Id)
		x.Printf("stats job id:%d tube:%s with err: %s", params.Id, x.conn.Tube.Name, err)
	case Stats:
		data, err = x.conn.Stats()
		x.Printf("stats :%v  with err: %s", data, err)
	case ListTubes:
		tubes, err = x.conn.ListTubes()
		data["tubes"] = strings.Join(tubes, ",")
		x.Printf("tubes :%v  with err: %s", tubes, err)
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
		if params.Id > 0 {
			stat, err = x.conn.StatsJob(params.Id)
			if err == nil {
				msg.Metadata.ReplaceAll(stat)
			}

		}
		ctx.TellSuccess(msg)
	}
}

// getParams 获取参数
func (x *WorkerNode) getParams(ctx types.RuleContext, msg types.RuleMsg) (*WorkerMsgParams, error) {
	var (
		err    error
		id     uint64        = 0
		tube   string        = DefaultTube
		pri    uint32        = DefaultPri
		delay  time.Duration = DefaultDelay
		params               = WorkerMsgParams{
			Id:    id,
			Tube:  tube,
			Pri:   DefaultPri,
			Delay: DefaultDelay,
		}
	)
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	// 获取tube参数
	if !x.tubeTemplate.IsNotVar() {
		tube = x.tubeTemplate.Execute(evn)
	} else if len(x.Config.Tube) > 0 {
		tube = x.Config.Tube
	}
	// 获取jobId参数
	if !x.jobIdTemplate.IsNotVar() {
		tmp := x.jobIdTemplate.Execute(evn)
		id, err = strconv.ParseUint(tmp, 10, 64)
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
	// 更新参数
	params.Id = id
	params.Tube = tube
	params.Pri = pri
	params.Delay = delay
	return &params, nil
}

// Destroy 销毁组件
func (x *WorkerNode) Destroy() {
	if x.conn != nil {
		_ = x.conn.Close()
	}
}

// Printf 打印日志
func (x *WorkerNode) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// 初始化连接
func (x *WorkerNode) initClient() (*beanstalk.Conn, error) {
	if x.conn != nil {
		return x.conn, nil
	} else {
		x.Locker.Lock()
		defer x.Locker.Unlock()
		if x.conn != nil {
			return x.conn, nil
		}
		var err error
		x.conn, err = beanstalk.Dial("tcp", x.Config.Server)
		x.conn.Tube = *beanstalk.NewTube(x.conn, DefaultTube)
		return x.conn, err
	}
}
