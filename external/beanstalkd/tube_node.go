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

	//Priority
	PriHigh   uint32 = 1
	PriNormal uint32 = 2
	PriLow    uint32 = 3

	DefaultPri   = PriLow
	DefaultDelay = time.Second * 0
	DefaultTime  = time.Second * 5
	DefaultBound = 10
	DefaultTube  = "default"
)

// Register the node
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

// TubeConfiguration node configuration
type TubeConfiguration struct {
	Server    string `json:"server" label:"Server" desc:"Beanstalkd server address, format: host:port" required:"true" ref:"primary"`
	Tube      string `json:"tube" label:"Tube" desc:"Tube name, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	Cmd       string `json:"cmd" label:"Command" desc:"Command: put, kick, kickBound, pause, peek, peekReady, peekDelayed, peekBuried" required:"true"`
	Body      string `json:"body" label:"Body" desc:"Message body for put command, supports ${metadata.key} and ${msg.key} substitution"`
	Pri       string `json:"pri" label:"Priority" desc:"Message priority, lower number means higher priority"`
	Delay     string `json:"delay" label:"Delay (s)" desc:"Delay before message becomes ready, in seconds"`
	Ttr       string `json:"ttr" label:"TTR (s)" desc:"Time to run, max time for worker to process the job before re-queueing"`
	KickBound string `json:"kickBound" label:"Kick Bound" desc:"Number of messages to kick for kickBound command"`
	PauseTime string `json:"pauseTime" label:"Pause Time (s)" desc:"Seconds to pause the tube for pause command"`
}

// TubeNode client node,
// Success: Switch to the Success chain, send the message execution result, and store it in msg.Data
// Failure: Switch to the Failure chain
type TubeNode struct {
	base.SharedNode[*beanstalk.Conn]
	//Node configuration
	Config TubeConfiguration
	// tubeTemplate Tube name template, used to parse dynamic tube names
	// tubeTemplate template for resolving dynamic tube names
	tubeTemplate el.Template
	// putBodyTemplate message content template, used to parse dynamic message content
	// putBodyTemplate template for resolving dynamic message body
	putBodyTemplate el.Template
	// putPriTemplate priority template, used to resolve dynamic priorities
	// putPriTemplate template for resolving dynamic priority
	putPriTemplate el.Template
	// putDelayTemplate is used to resolve dynamic delay times
	// putDelayTemplate template for resolving dynamic delay time
	putDelayTemplate el.Template
	// putTTRTemplate TTR template, used to parse dynamic TTR time
	// putTTRTemplate template for resolving dynamic TTR time
	putTTRTemplate el.Template
	// kickBoundTemplate is used to parse dynamic Kick boundaries
	// kickBoundTemplate template for resolving dynamic kick bound
	kickBoundTemplate el.Template
	// pauseTimeTemplate pause time template, used to resolve dynamic pause times
	// pauseTimeTemplate template for resolving dynamic pause time
	pauseTimeTemplate el.Template
	// hasVar identifies whether the template contains variables used to optimize performance
	// hasVar indicates whether the template contains variables for performance optimization
	hasVar bool
}

// Type returns the component type
func (x *TubeNode) Type() string {
	return "x/beanstalkdTube"
}

// New default parameters
func (x *TubeNode) New() types.Node {
	return &TubeNode{Config: TubeConfiguration{
		Server: "127.0.0.1:11300",
		Tube:   "default",
		Cmd:    Stat,
	}}
}

// Init initializes the component
func (x *TubeNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err == nil {
		//Initialize the client
		err = x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.Server, false, func() (*beanstalk.Conn, error) {
			return x.initClient()
		}, func(conn *beanstalk.Conn) error {
			return conn.Close()
		})
	}
	//Initialize the template
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
	// Check if any templates contain variables
	x.hasVar = x.tubeTemplate.HasVar() || x.putBodyTemplate.HasVar() || x.putPriTemplate.HasVar() || x.putDelayTemplate.HasVar() || x.putTTRTemplate.HasVar() || x.kickBoundTemplate.HasVar() || x.pauseTimeTemplate.HasVar()
	return err
}

// OnMsg processes a message
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
	// Get the tube parameters
	if !x.tubeTemplate.IsNotVar() {
		tube = x.tubeTemplate.ExecuteAsString(evn)
	} else if len(x.Config.Tube) > 0 {
		tube = x.Config.Tube
	}
	// Get the body parameter
	if !x.putBodyTemplate.IsNotVar() {
		body = x.putBodyTemplate.ExecuteAsString(evn)
	} else if len(x.Config.Body) > 0 {
		body = x.Config.Body
	} else {
		body = msg.GetData()
	}
	// Obtain priority parameters
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
	// Obtain delay parameters
	if !x.putDelayTemplate.IsNotVar() {
		tmp := x.putDelayTemplate.ExecuteAsString(evn)
		delay, err = time.ParseDuration(tmp)
	} else if len(x.Config.Delay) > 0 {
		delay, err = time.ParseDuration(x.Config.Delay)
	}
	if err != nil {
		return nil, err
	}
	// Get TTR parameters
	if !x.putTTRTemplate.IsNotVar() {
		tmp := x.putTTRTemplate.ExecuteAsString(evn)
		ttr, err = time.ParseDuration(tmp)
	} else if len(x.Config.Ttr) > 0 {
		ttr, err = time.ParseDuration(x.Config.Ttr)
	}
	if err != nil {
		return nil, err
	}
	// Retrieve the Bound quantity parameter
	if !x.kickBoundTemplate.IsNotVar() {
		tmp := x.kickBoundTemplate.ExecuteAsString(evn)
		bound, err = strconv.Atoi(tmp)
	} else if len(x.Config.KickBound) > 0 {
		bound, err = strconv.Atoi(x.Config.KickBound)
	}
	if err != nil {
		return nil, err
	}
	// Obtain pause time parameters
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

// Printf prints logs
func (x *TubeNode) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// Initialize the connection
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

// Desc returns the component description
func (x *TubeNode) Desc() string {
	return "Beanstalkd tube for publishing jobs. Routes to Success/Failure"
}
