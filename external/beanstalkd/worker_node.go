package beanstalkd

import (
	"encoding/json"
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
	"strconv"
	"strings"
	"time"
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

// WorkerConfiguration 节点配置
type WorkerConfiguration struct {
	// 服务器地址
	Server string
	// Tube名称
	Tube string
	// 命令：Delete Release Bury KickJob Touch Peek ReserveJob  StatsJob Stats  ListTubes
	Cmd string
	// JobId  允许使用 ${} 占位符变量
	JobId string
	// Put命令参数pri 允许使用 ${} 占位符变量
	PutPri string
	// Put命令参数delay 允许使用 ${} 占位符变量
	PutDelay string
}

// WorkerNode 客户端节点，
// 成功：转向Success链，发送消息执行结果存放在msg.Data
// 失败：转向Failure链
type WorkerNode struct {
	base.SharedNode[*beanstalk.Conn]
	//节点配置
	Config           WorkerConfiguration
	conn             *beanstalk.Conn
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
	//初始化模板
	x.putPriTemplate = str.NewTemplate(x.Config.PutPri)
	x.putDelayTemplate = str.NewTemplate(x.Config.PutDelay)
	x.jobIdTemplate = str.NewTemplate(x.Config.JobId)
	if err == nil {
		//初始化客户端
		err = x.SharedNode.Init(ruleConfig, x.Type(), x.Config.Server, true, func() (*beanstalk.Conn, error) {
			return x.initClient()
		})
	}
	return err
}

// OnMsg 处理消息
func (x *WorkerNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	var (
		err   error
		id    uint64 = 0
		body  []byte
		tubes []string          = make([]string, 0)
		data  map[string]string = make(map[string]string)
		pri                     = DefaultPri
		delay time.Duration     = DefaultDelay
	)
	evn := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	if !x.jobIdTemplate.IsNotVar() {
		tmp := x.jobIdTemplate.Execute(evn)
		id, err = strconv.ParseUint(tmp, 10, 64)
	}

	switch x.Config.Cmd {
	case Delete:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.Delete(id)
	case Release:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
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
		err = x.conn.Release(id, pri, delay)
	case Bury:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
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
		err = x.conn.Bury(id, pri)
	case KickJob:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.KickJob(id)
	case Touch:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
		err = x.conn.Touch(id)
	case Peek:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
		body, err = x.conn.Peek(id)
		data["body"] = string(body)
	case ReserveJob:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
		body, err = x.conn.ReserveJob(id)
		data["body"] = string(body)
	case StatsJob:
		if id == 0 {
			err = errors.New("id is empty")
			break
		}
		data, err = x.conn.StatsJob(id)
	case Stats:
		data, err = x.conn.Stats()
	case ListTubes:
		tubes, err = x.conn.ListTubes()
		data["tubes"] = strings.Join(tubes, ",")
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
		conn, err := beanstalk.Dial("tcp", x.Config.Server)
		conn.Tube = *beanstalk.NewTube(conn, x.Config.Tube)
		x.conn = conn
		return x.conn, err
	}
}
