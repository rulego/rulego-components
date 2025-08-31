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
	"github.com/rulego/rulego/utils/el"
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
	Config WorkerConfiguration
	// tubeTemplate Tube名称模板，用于解析动态Tube名称
	// tubeTemplate template for resolving dynamic tube names
	tubeTemplate el.Template
	// jobIdTemplate 作业ID模板，用于解析动态作业ID
	// jobIdTemplate template for resolving dynamic job IDs
	jobIdTemplate el.Template
	// putPriTemplate 优先级模板，用于解析动态优先级
	// putPriTemplate template for resolving dynamic priority
	putPriTemplate el.Template
	// putDelayTemplate 延迟时间模板，用于解析动态延迟时间
	// putDelayTemplate template for resolving dynamic delay time
	putDelayTemplate el.Template
	// hasVar 标识模板是否包含变量，用于优化性能
	// hasVar indicates whether the template contains variables for performance optimization
	hasVar bool
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
	x.putPriTemplate, err = el.NewTemplate(x.Config.Pri)
	if err != nil {
		return err
	}
	x.putDelayTemplate, err = el.NewTemplate(x.Config.Delay)
	if err != nil {
		return err
	}
	x.jobIdTemplate, err = el.NewTemplate(x.Config.JobId)
	if err != nil {
		return err
	}
	// 检查是否有任何模板包含变量
	x.hasVar = x.tubeTemplate.HasVar() || x.putPriTemplate.HasVar() || x.putDelayTemplate.HasVar() || x.jobIdTemplate.HasVar()
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
	conn, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	x.Printf("conn :%v ", conn)
	conn.Tube.Name = params.Tube
	switch x.Config.Cmd {
	case Delete:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = conn.Delete(params.Id)
		x.Printf("delete job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case Release:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = conn.Release(params.Id, params.Pri, params.Delay)
		x.Printf("release job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case Bury:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = conn.Bury(params.Id, params.Pri)
		x.Printf("bury job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case KickJob:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = conn.KickJob(params.Id)
		x.Printf("kick job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case Touch:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = conn.Touch(params.Id)
		x.Printf("touch job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case Peek:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		body, err = conn.Peek(params.Id)
		data["body"] = string(body)
		x.Printf("peek job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case ReserveJob:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		body, err = conn.ReserveJob(params.Id)
		data["body"] = string(body)
		x.Printf("reserve job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case StatsJob:
		if params.Id == 0 {
			err = errors.New("id is empty")
			break
		}
		data, err = conn.StatsJob(params.Id)
		x.Printf("stats job id:%d tube:%s with err: %s", params.Id, conn.Tube.Name, err)
	case Stats:
		data, err = conn.Stats()
		x.Printf("stats :%v  with err: %s", data, err)
	case ListTubes:
		tubes, err = conn.ListTubes()
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
			stat, err = conn.StatsJob(params.Id)
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
	// 获取jobId参数
	if !x.jobIdTemplate.IsNotVar() {
		tmp := x.jobIdTemplate.ExecuteAsString(evn)
		id, err = strconv.ParseUint(tmp, 10, 64)
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
	// 更新参数
	params.Id = id
	params.Tube = tube
	params.Pri = pri
	params.Delay = delay
	return &params, nil
}

// Printf 打印日志
func (x *WorkerNode) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// 初始化连接
func (x *WorkerNode) initClient() (*beanstalk.Conn, error) {
	conn, err := beanstalk.Dial("tcp", x.Config.Server)
	if err != nil {
		return nil, err
	}
	conn.Tube = *beanstalk.NewTube(conn, DefaultTube)
	return conn, nil
}

func (x *WorkerNode) Destroy() {
	_ = x.SharedNode.Close()
}
