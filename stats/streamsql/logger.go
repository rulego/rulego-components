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
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/streamsql/logger"
)

// rulegoLogger 把 rulego 的 types.Logger 适配为 streamsql 的 logger.Logger，
// 使 streamsql 内部日志统一走 rulego 日志体系（级别、格式、输出目标一致）。
type rulegoLogger struct {
	l types.Logger
}

func (a *rulegoLogger) Debug(format string, args ...any) { a.l.Debugf(format, args...) }
func (a *rulegoLogger) Info(format string, args ...any)  { a.l.Infof(format, args...) }
func (a *rulegoLogger) Warn(format string, args ...any)  { a.l.Warnf(format, args...) }
func (a *rulegoLogger) Error(format string, args ...any) { a.l.Errorf(format, args...) }

// SetLevel 级别由 rulego 日志器自管，此处空实现。
func (a *rulegoLogger) SetLevel(logger.Level) {}

// newRulegoLogger 用 rulego 日志器构造 streamsql logger.Logger；nil 回退默认。
func newRulegoLogger(l types.Logger) logger.Logger {
	if l == nil {
		l = types.DefaultLogger()
	}
	return &rulegoLogger{l: l}
}
