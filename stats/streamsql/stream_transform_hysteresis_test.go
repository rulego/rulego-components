/*
 * Copyright 2025 The RuleGo Authors.
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

package streamsql

import (
	"testing"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
)

// TestStreamTransformNode_Hysteresis 验证 streamTransform 节点能消费 streamsql 的 hysteresis
// 死区状态机（依赖 streamsql v1.1.3+）。采用与 acc_sum/changed_col 等跨消息状态测试一致的
// 单节点 + sendNodeMsg 同步范式（节点内 EmitSync 同步、保序），避免多节点串联时 TellNext 跨节点
// 异步在多核 CI 上的时序抖动。
//
// 死区核心：≥80 进 on、≤78 才 off，中间迟滞区保持当前态——挡住边界抖动。
// 死区+边沿的两节点串联（hysteresis → changed_col）用法见 rulego-doc「IoT 场景示例 · 场景十一」。
func TestStreamTransformNode_Hysteresis(t *testing.T) {
	eng, cleanup := newJoinEngine(t, "SELECT hysteresis(temp, 80, 78) AS alarm FROM stream", nil)
	defer cleanup()

	r1, rt1, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 79.0})
	assert.Equal(t, types.Success, rt1)
	assert.Equal(t, false, r1["alarm"], "79 未达上限 = off")

	r2, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 80.0})
	assert.Equal(t, true, r2["alarm"], "80 达上限 = on")

	r3, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 79.0})
	assert.Equal(t, true, r3["alarm"], "79 落在迟滞区(78~80)，保持 on（死区防抖核心）")

	r4, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 77.0})
	assert.Equal(t, false, r4["alarm"], "77 跌破恢复线(78) = off")

	r5, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 79.0})
	assert.Equal(t, false, r5["alarm"], "79 回升未达上限，保持 off")

	r6, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 80.0})
	assert.Equal(t, true, r6["alarm"], "80 再次达上限 = on")
}

// TestStreamTransformNode_HysteresisLowerLimit 下越限：enter<exit 自动判方向（≤10 进、≥15 出）。
func TestStreamTransformNode_HysteresisLowerLimit(t *testing.T) {
	eng, cleanup := newJoinEngine(t, "SELECT hysteresis(temp, 10, 15) AS alarm FROM stream", nil)
	defer cleanup()

	r1, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 12.0})
	assert.Equal(t, false, r1["alarm"], "12 未达下限 = off")

	r2, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 10.0})
	assert.Equal(t, true, r2["alarm"], "10 跌入下限 = on")

	r3, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 12.0})
	assert.Equal(t, true, r3["alarm"], "12 迟滞区保持 on")

	r4, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"temp": 15.0})
	assert.Equal(t, false, r4["alarm"], "15 升破恢复线 = off")
}

// TestStreamTransformNode_Latch 验证 streamTransform 节点能消费 latch SR/RS 锁存。
func TestStreamTransformNode_Latch(t *testing.T) {
	eng, cleanup := newJoinEngine(t, "SELECT latch(set, reset) AS q FROM stream", nil)
	defer cleanup()

	r1, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"set": false, "reset": false})
	assert.Equal(t, false, r1["q"], "初始 off，皆假保持")

	r2, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"set": true, "reset": false})
	assert.Equal(t, true, r2["q"], "set 置位")

	r3, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"set": false, "reset": false})
	assert.Equal(t, true, r3["q"], "皆假保持 on（锁存）")

	r4, _, _ := sendNodeMsg(t, eng, map[string]interface{}{"set": false, "reset": true})
	assert.Equal(t, false, r4["q"], "reset 复位")
}
