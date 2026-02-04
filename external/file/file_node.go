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

package file

import (
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/fs"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	// KeyFilePathWhitelist 文件路径白名单的配置键
	KeyFilePathWhitelist = "filePathWhitelist"
	// KeyDeletedCount 删除文件计数的配置键
	KeyDeletedCount = "deletedCount"
	// ValueOne 1
	ValueOne = "1"
	// KeyWorkDir 工作目录的配置键
	KeyWorkDir = "workDir"

	// DataTypeText 文本格式
	DataTypeText = "text"
	// DataTypeBase64 base64格式
	DataTypeBase64 = "base64"
)

// ErrPathNotAllowed 路径不在白名单中的错误
var ErrPathNotAllowed = errors.New("path not allowed error")

// ErrPathEmpty path is empty error
var ErrPathEmpty = errors.New("path is empty")

const (
	defaultPath     = "/tmp/data.txt"
	defaultGlobPath = "/tmp/*.txt"
	globChars       = "*?[]"
)

// 注册节点
func init() {
	_ = rulego.Registry.Register(&FileReadNode{})
	_ = rulego.Registry.Register(&FileWriteNode{})
	_ = rulego.Registry.Register(&FileDeleteNode{})
	_ = rulego.Registry.Register(&FileListNode{})
}

// checkPath checks if the path is allowed by the whitelist.
// checkPath 检查路径是否在白名单中允许。
func checkPath(ctx types.RuleContext, path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	// Security Check: If workDir is set in context, the path MUST be within workDir
	// 安全检查：如果上下文中设置了 workDir，则路径必须在 workDir 内部
	var workDir string
	if ctx.GetContext() != nil {
		if v := ctx.GetContext().Value(KeyWorkDir); v != nil {
			workDir = str.ToString(v)
		}
	}
	if workDir != "" {
		absWorkDir, err := filepath.Abs(workDir)
		if err != nil {
			return err
		}
		// Use strict prefix check to ensure path is inside workDir
		// 使用严格的前缀检查以确保路径在 workDir 内部
		// Clean paths to handle OS separators consistently
		// 清理路径以一致地处理操作系统分隔符
		cleanWorkDir := filepath.Clean(absWorkDir)
		cleanPath := filepath.Clean(absPath)

		// Check if path is outside workDir
		// We allow if it IS workDir, or is a subdirectory
		// 检查路径是否在 workDir 外部
		// 如果是 workDir 本身或者是其子目录，我们允许访问
		if cleanPath != cleanWorkDir && !strings.HasPrefix(cleanPath, cleanWorkDir+string(filepath.Separator)) {
			return ErrPathNotAllowed
		}
	}

	properties := ctx.Config().Properties
	if properties == nil {
		return nil
	}
	whitelistStr := properties.GetValue(KeyFilePathWhitelist)
	if whitelistStr == "" {
		return nil
	}

	whitelists := strings.Split(whitelistStr, ",")
	for _, whitelist := range whitelists {
		whitelist = strings.TrimSpace(whitelist)
		if whitelist == "" {
			continue
		}

		// Check if whitelist item contains glob characters
		// 检查白名单项是否包含 glob 通配符
		if strings.ContainsAny(whitelist, globChars) {
			// Convert whitelist pattern to absolute path pattern
			// 将白名单模式转换为绝对路径模式
			// We use filepath.Abs to ensure it matches the format of absPath (separators, etc.)
			// 我们使用 filepath.Abs 来确保它与 absPath 的格式匹配（分隔符等）
			// Note: filepath.Abs works on paths with glob chars on most OSes (it just treats them as chars)
			// 注意：filepath.Abs 在大多数操作系统上都可以处理包含 glob 字符的路径（它只是将它们视为普通字符）
			absWhitelistPattern, err := filepath.Abs(whitelist)
			if err != nil {
				// Fallback to original if Abs fails
				// 如果 Abs 失败，则回退到原始路径
				absWhitelistPattern = whitelist
			}

			// Try to match the path or any of its parents against the pattern
			// 尝试将路径或其任何父目录与模式匹配
			currentPath := absPath
			for {
				matched, err := filepath.Match(absWhitelistPattern, currentPath)
				if err == nil && matched {
					return nil
				}

				// Move to parent
				// 移动到父目录
				parent := filepath.Dir(currentPath)
				// Check if we reached the root
				// 检查是否到达根目录
				if parent == currentPath || parent == "." || (len(parent) > 0 && parent[len(parent)-1] == filepath.Separator) {
					// On Windows, filepath.Dir("C:\\") returns "C:\\", so parent == currentPath
					// 在 Windows 上，filepath.Dir("C:\\") 返回 "C:\\"，因此 parent == currentPath
					// On Unix, filepath.Dir("/") returns "/"
					// 在 Unix 上，filepath.Dir("/") 返回 "/"
					break
				}
				// Additional check for root on some systems/edge cases
				// 针对某些系统/边缘情况的额外根目录检查
				if len(parent) <= 1 && os.IsPathSeparator(parent[0]) {
					break
				}
				currentPath = parent
			}
		} else {
			// Standard prefix matching
			// 标准前缀匹配
			absWhitelist, err := filepath.Abs(whitelist)
			if err != nil {
				continue
			}
			if strings.HasPrefix(absPath, absWhitelist) {
				return nil
			}
		}
	}
	return ErrPathNotAllowed
}

// getAbsPath gets the absolute path, handling workDir from context.
// getAbsPath 获取绝对路径，处理上下文中的 workDir。
func getAbsPath(ctx types.RuleContext, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	var workDir string
	if ctx.GetContext() != nil {
		if v := ctx.GetContext().Value(KeyWorkDir); v != nil {
			workDir = str.ToString(v)
		}
	}
	if workDir != "" {
		return filepath.Join(workDir, path)
	}
	return path
}

// resolvePath resolves the path using the template and context.
// resolvePath 使用模板和上下文解析路径。
func resolvePath(ctx types.RuleContext, msg types.RuleMsg, pathTemplate el.Template) (string, map[string]interface{}, error) {
	env := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	path := pathTemplate.ExecuteAsString(env)
	if path == "" {
		return "", env, ErrPathEmpty
	}
	return getAbsPath(ctx, path), env, nil
}

// FileReadNodeConfiguration 文件读取节点配置
type FileReadNodeConfiguration struct {
	// Path 文件路径，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
	Path string `json:"path"`
	// DataType 编码格式，支持 text, base64
	DataType string `json:"dataType"`
	// Recursive 是否递归查找子目录，默认false
	Recursive bool `json:"recursive"`
}

// FileReadNode read file content
// 读取文件内容
//
// Configuration:
// 配置说明：
//
//	{
//		"path": "/tmp/data.txt",  // File path or glob pattern, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.  文件路径或glob模式，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
//		"dataType": "text",       // DataType format: text, base64  编码格式：text, base64
//		"recursive": false        // Whether to search recursively. Default is false. 是否递归查找子目录，默认false
//	}
type FileReadNode struct {
	//节点配置
	Config FileReadNodeConfiguration
	//path模板
	pathTemplate el.Template
}

func (x *FileReadNode) Type() string {
	return "x/fileRead"
}

func (x *FileReadNode) New() types.Node {
	return &FileReadNode{Config: FileReadNodeConfiguration{
		Path:      defaultPath,
		DataType:  DataTypeText,
		Recursive: false,
	}}
}

func (x *FileReadNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	x.pathTemplate, err = el.NewTemplate(x.Config.Path)
	return err
}

func (x *FileReadNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	path, _, err := resolvePath(ctx, msg, x.pathTemplate)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// For security, check the directory part of the path
	// path can be a glob pattern like /tmp/*.txt
	// We check the directory containing the pattern
	// 为了安全起见，检查路径的目录部分
	// 路径可以是 glob 模式，如 /tmp/*.txt
	// 我们检查包含该模式的目录
	dir := filepath.Dir(path)
	if err := checkPath(ctx, dir); err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Check if path contains glob characters
	// 检查路径是否包含 glob 通配符
	if strings.ContainsAny(path, globChars) {
		var paths []string
		var err error
		if x.Config.Recursive {
			paths, err = fs.GetFilePaths(path)
		} else {
			// Use filepath.Glob for non-recursive search which is more efficient
			// 使用 filepath.Glob 进行非递归搜索，这更高效
			paths, err = filepath.Glob(path)
		}
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}

		var b strings.Builder
		count := 0
		for _, p := range paths {
			data, err := fs.DefaultFile.Get(p)
			if err != nil {
				// Skip file read errors in batch mode
				// 在批处理模式下跳过文件读取错误
				continue
			}

			if count > 0 {
				b.WriteString("\n")
			}
			if x.Config.DataType == DataTypeBase64 {
				b.WriteString(base64.StdEncoding.EncodeToString(data))
			} else {
				b.Write(data)
			}
			count++
		}

		msg.SetData(b.String())
		ctx.TellSuccess(msg)

	} else {
		// Single file read
		// 单个文件读取
		if err := checkPath(ctx, path); err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		data, err := fs.DefaultFile.Get(path)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		if x.Config.DataType == DataTypeBase64 {
			msg.SetData(base64.StdEncoding.EncodeToString(data))
		} else {
			msg.SetBytes(data)
		}
		ctx.TellSuccess(msg)
	}
}

func (x *FileReadNode) Destroy() {
}

// FileWriteNodeConfiguration 文件写入节点配置
type FileWriteNodeConfiguration struct {
	// Path 文件路径，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
	Path string `json:"path"`
	// Content 文件内容，支持变量替换。如果是base64字符串，会自动解码写入
	Content string `json:"content"`
	// Append 是否追加写入，默认false
	Append bool `json:"append"`
}

// FileWriteNode write data to file
// 写入数据到文件
//
// Configuration:
// 配置说明：
//
//	{
//		"path": "/tmp/data.txt",     // File path, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.  文件路径，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
//		"content": "${data}",    // Content to write, supports variable substitution  写入内容，支持变量替换
//		"append": false              // Whether to append to file, default is false  是否追加写入，默认false
//	}
type FileWriteNode struct {
	//节点配置
	Config FileWriteNodeConfiguration
	//path模板
	pathTemplate el.Template
	//content模板
	contentTemplate el.Template
}

func (x *FileWriteNode) Type() string {
	return "x/fileWrite"
}

func (x *FileWriteNode) New() types.Node {
	return &FileWriteNode{Config: FileWriteNodeConfiguration{
		Path:    defaultPath,
		Content: "${data}",
		Append:  false,
	}}
}

func (x *FileWriteNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	if strings.TrimSpace(x.Config.Path) == "" {
		return errors.New("path is empty")
	}
	x.pathTemplate, err = el.NewTemplate(x.Config.Path)
	if err != nil {
		return err
	}
	if strings.TrimSpace(x.Config.Content) != "" {
		x.contentTemplate, err = el.NewTemplate(x.Config.Content)
	}

	return err
}

func (x *FileWriteNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	path, env, err := resolvePath(ctx, msg, x.pathTemplate)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	if err := checkPath(ctx, path); err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	var content interface{}
	if x.contentTemplate == nil {
		content = msg.GetData()
	} else {
		content, err = x.contentTemplate.Execute(env)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
	}

	var data []byte
	if strContent, ok := content.(string); ok {
		data = []byte(strContent)
	} else if byteContent, ok := content.([]byte); ok {
		data = byteContent
	} else {
		data = []byte(str.ToString(content))
	}

	if x.Config.Append {
		err = fs.DefaultFile.SaveAppend(path, data)
	} else {
		err = fs.DefaultFile.Save(path, data)
	}
	if err != nil {
		ctx.TellFailure(msg, err)
	} else {
		ctx.TellSuccess(msg)
	}
}

func (x *FileWriteNode) Destroy() {
}

// FileDeleteNodeConfiguration 文件删除节点配置
type FileDeleteNodeConfiguration struct {
	// Path 文件路径，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
	Path string `json:"path"`
}

// FileDeleteNode delete file
// 删除文件
//
// Configuration:
// 配置说明：
//
//	{
//		"path": "/tmp/data.txt"  // File path or glob pattern, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.  文件路径或glob模式，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
//	}
type FileDeleteNode struct {
	//节点配置
	Config FileDeleteNodeConfiguration
	//path模板
	pathTemplate el.Template
}

func (x *FileDeleteNode) Type() string {
	return "x/fileDelete"
}

func (x *FileDeleteNode) New() types.Node {
	return &FileDeleteNode{Config: FileDeleteNodeConfiguration{
		Path: defaultPath,
	}}
}

func (x *FileDeleteNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	x.pathTemplate, err = el.NewTemplate(x.Config.Path)
	return err
}

func (x *FileDeleteNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	path, _, err := resolvePath(ctx, msg, x.pathTemplate)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// For security, check the directory part of the path
	// path can be a glob pattern like /tmp/*.txt
	// We check the directory containing the pattern
	// 为了安全起见，检查路径的目录部分
	// 路径可以是 glob 模式，如 /tmp/*.txt
	// 我们检查包含该模式的目录
	dir := filepath.Dir(path)
	if err := checkPath(ctx, dir); err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Check if path contains glob characters
	// 检查路径是否包含 glob 通配符
	if strings.ContainsAny(path, globChars) {
		paths, err := fs.GetFilePaths(path)
		if err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		var deletedCount int
		var lastErr error
		for _, p := range paths {
			if err := fs.DefaultFile.Delete(p); err != nil {
				lastErr = err
			} else {
				deletedCount++
			}
		}
		if lastErr != nil && deletedCount == 0 {
			ctx.TellFailure(msg, lastErr)
		} else {
			// If at least one file deleted or no files matched (success case), return success
			// You might want to return details about deleted files in metadata
			// 如果至少删除了一个文件或没有匹配的文件（成功情况），则返回成功
			// 你可能希望在元数据中返回有关已删除文件的详细信息
			msg.Metadata.PutValue(KeyDeletedCount, str.ToString(deletedCount))
			ctx.TellSuccess(msg)
		}
	} else {
		// Single file delete
		// 单个文件删除
		if err := checkPath(ctx, path); err != nil {
			ctx.TellFailure(msg, err)
			return
		}
		if err := fs.DefaultFile.Delete(path); err != nil {
			ctx.TellFailure(msg, err)
		} else {
			msg.Metadata.PutValue(KeyDeletedCount, ValueOne)
			ctx.TellSuccess(msg)
		}
	}
}

func (x *FileDeleteNode) Destroy() {
}

// FileListNodeConfiguration 文件列表节点配置
type FileListNodeConfiguration struct {
	// Path 文件路径模式，支持通配符 and 变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
	Path string `json:"path"`
	// Recursive 是否递归查找子目录，默认false
	Recursive bool `json:"recursive"`
}

// FileListNode list files
// 列出文件
//
// Configuration:
// 配置说明：
//
//	{
//		"path": "/tmp/*.txt", // File path pattern, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.  文件路径模式，支持变量替换。如果是相对路径，优先相对于 context 中的 workDir，如果未配置，则相对于当前进程工作目录。
//		"recursive": false    // Whether to search recursively. Default is false. 是否递归查找子目录，默认false
//	}
type FileListNode struct {
	//节点配置
	Config FileListNodeConfiguration
	//path模板
	pathTemplate el.Template
}

func (x *FileListNode) Type() string {
	return "x/fileList"
}

func (x *FileListNode) New() types.Node {
	return &FileListNode{Config: FileListNodeConfiguration{
		Path:      defaultGlobPath,
		Recursive: false,
	}}
}

func (x *FileListNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}
	x.pathTemplate, err = el.NewTemplate(x.Config.Path)
	return err
}

func (x *FileListNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	path, _, err := resolvePath(ctx, msg, x.pathTemplate)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// For security, check the directory part of the path
	// path can be a glob pattern like /tmp/*.txt
	// We check the directory containing the pattern
	// 为了安全起见，检查路径的目录部分
	// 路径可以是 glob 模式，如 /tmp/*.txt
	// 我们检查包含该模式的目录
	dir := filepath.Dir(path)
	if err := checkPath(ctx, dir); err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	var paths []string
	if x.Config.Recursive {
		paths, err = fs.GetFilePaths(path)
	} else {
		// Use filepath.Glob for non-recursive search which is more efficient
		// 使用 filepath.Glob 进行非递归搜索，这更高效
		paths, err = filepath.Glob(path)
	}

	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Convert to interface array for JSON serialization
	// 转换为接口数组以进行 JSON 序列化
	var result []interface{}
	for _, p := range paths {
		result = append(result, p)
	}

	msg.SetData(str.ToString(result))
	ctx.TellSuccess(msg)
}

func (x *FileListNode) Destroy() {
}
