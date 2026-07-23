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
	// KeyFilePathWhitelist configuration key for the file path whitelist
	KeyFilePathWhitelist = "filePathWhitelist"
	// KeyDeletedCount is the configuration key for deleting file counts
	KeyDeletedCount = "deletedCount"
	// ValueOne 1
	ValueOne = "1"
	// KeyWorkDir is the configuration key for the working directory
	KeyWorkDir = "workDir"

	// DataTypeText text format
	DataTypeText = "text"
	// DataTypeBase64 base64 format
	DataTypeBase64 = "base64"
)

// ErrPathNotAllowed Path Not on the Whitelist Error
var ErrPathNotAllowed = errors.New("path not allowed error")

// ErrPathEmpty path is empty error
var ErrPathEmpty = errors.New("path is empty")

const (
	defaultPath     = "/tmp/data.txt"
	defaultGlobPath = "/tmp/*.txt"
	globChars       = "*?[]"
)

// Register the node
func init() {
	_ = rulego.Registry.Register(&FileReadNode{})
	_ = rulego.Registry.Register(&FileWriteNode{})
	_ = rulego.Registry.Register(&FileDeleteNode{})
	_ = rulego.Registry.Register(&FileListNode{})
}

// checkPath checks if the path is allowed by the whitelist.
// checkPath checks whether the path is allowed on the whitelist.
func checkPath(ctx types.RuleContext, path string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}

	// Security Check: If workDir is set in context, the path MUST be within workDir
	// Security check: If workDir is set in context, the path must be inside the workDir
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
		// Use strict prefix checks to ensure the path is inside the workDir
		// Clean paths to handle OS separators consistently
		// Clean paths to consistently handle operating system delimiters
		cleanWorkDir := filepath.Clean(absWorkDir)
		cleanPath := filepath.Clean(absPath)

		// Check if path is outside workDir
		// We allow if it IS workDir, or is a subdirectory
		// Check if the path is outside the workDir
		// If it is the workDir itself or its subdirectories, we allow access
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
		// Check whether the whitelist items contain glob wildcards
		if strings.ContainsAny(whitelist, globChars) {
			// Convert whitelist pattern to absolute path pattern
			// Convert whitelist mode to absolute path mode
			// We use filepath.Abs to ensure it matches the format of absPath (separators, etc.)
			// We use filepath.Abs to ensure it matches the format of absPath (such as delimiters)
			// Note: filepath.Abs works on paths with glob chars on most OSes (it just treats them as chars)
			// Note: filepath.Abs can handle paths containing glob characters on most operating systems (it simply treats them as regular characters)
			absWhitelistPattern, err := filepath.Abs(whitelist)
			if err != nil {
				// Fallback to original if Abs fails
				// If ABS fails, it rolls back to the original path
				absWhitelistPattern = whitelist
			}

			// Try to match the path or any of its parents against the pattern
			// Try matching the path or any of its parent directories to the pattern
			currentPath := absPath
			for {
				matched, err := filepath.Match(absWhitelistPattern, currentPath)
				if err == nil && matched {
					return nil
				}

				// Move to parent
				// Move to the parent directory
				parent := filepath.Dir(currentPath)
				// Check if we reached the root
				// Check if you have reached the root directory
				if parent == currentPath || parent == "." || (len(parent) > 0 && parent[len(parent)-1] == filepath.Separator) {
					// On Windows, filepath.Dir("C:\\") returns "C:\\", so parent == currentPath
					// On Windows, filepath.Dir("C:\\") returns "C:\\", so parent == currentPath
					// On Unix, filepath.Dir("/") returns "/"
					// On Unix, filepath.Dir("/") returns "/"
					break
				}
				// Additional check for root on some systems/edge cases
				// Additional root directory checks for certain system/edge situations
				if len(parent) <= 1 && os.IsPathSeparator(parent[0]) {
					break
				}
				currentPath = parent
			}
		} else {
			// Standard prefix matching
			// Standard prefix matching
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
// getAbsPath obtains the absolute path and processes the workDir in context.
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
// resolvePath uses a template and contextual parsing path.
func resolvePath(ctx types.RuleContext, msg types.RuleMsg, pathTemplate el.Template) (string, map[string]interface{}, error) {
	env := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	path := pathTemplate.ExecuteAsString(env)
	if path == "" {
		return "", env, ErrPathEmpty
	}
	return getAbsPath(ctx, path), env, nil
}

// FileReadNodeConfiguration The file reads node configuration
type FileReadNodeConfiguration struct {
	Path      string `json:"path" label:"Path" desc:"File or directory path, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	DataType  string `json:"dataType" label:"Data Type" desc:"Read data type: TEXT, JSON, BINARY, default TEXT"`
	Recursive bool   `json:"recursive" label:"Recursive" desc:"Recursively read subdirectories when path is a directory"`
}

// FileReadNode read file content
// Read the file content
//
// Configuration:
// Configuration:
//
//	{
//		"path": "/tmp/data.txt",  // File path or glob pattern, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.
//		"dataType": "text",       // DataType format: text, base64
//		"recursive": false // Whether to search recursively. Default is false. Whether to recursively search subdirectories is false by default
//	}
type FileReadNode struct {
	//Node configuration
	Config FileReadNodeConfiguration
	//path template
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
	// For safety, check the directory section of the path
	// The path can be glob mode, such as /tmp/*.txt
	// We check the directories that contain this pattern
	dir := filepath.Dir(path)
	if err := checkPath(ctx, dir); err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Check if path contains glob characters
	// Check if the path contains glob wildcards
	if strings.ContainsAny(path, globChars) {
		var paths []string
		var err error
		if x.Config.Recursive {
			paths, err = fs.GetFilePaths(path)
		} else {
			// Use filepath.Glob for non-recursive search which is more efficient
			// Use filepath.Glob performs non-recursive search, which is more efficient
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
				// Skip file read errors in batch mode
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
		// Individual file reading
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

// Desc returns the component description
func (x *FileReadNode) Desc() string {
	return "Read file content. Path supports ${metadata.key} substitution. Routes to Success/Failure"
}

// FileWriteNodeConfiguration Writes node configuration to the file
type FileWriteNodeConfiguration struct {
	Path    string `json:"path" label:"Path" desc:"File path, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	Content string `json:"content" label:"Content" desc:"Content to write, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	Append  bool   `json:"append" label:"Append" desc:"true=append to file, false=overwrite file"`
}

// FileWriteNode write data to file
// Write data to files
//
// Configuration:
// Configuration:
//
//	{
//		"path": "/tmp/data.txt",     // File path, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.
//		"content": "${data}",    // Content to write, supports variable substitution
//		"append": false              // Whether to append to file, default is false
//	}
type FileWriteNode struct {
	//Node configuration
	Config FileWriteNodeConfiguration
	//path template
	pathTemplate el.Template
	//content template
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

// Desc returns the component description
func (x *FileWriteNode) Desc() string {
	return "Write content to file. Path and content support ${metadata.key} substitution. Routes to Success/Failure"
}

// FileDeleteNodeConfiguration File-deleted node configuration
type FileDeleteNodeConfiguration struct {
	Path string `json:"path" label:"Path" desc:"File path to delete, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
}

// FileDeleteNode delete file
// Delete the file
//
// Configuration:
// Configuration:
//
//	{
//		"path": "/tmp/data.txt"  // File path or glob pattern, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.
//	}
type FileDeleteNode struct {
	//Node configuration
	Config FileDeleteNodeConfiguration
	//path template
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
	// For safety, check the directory section of the path
	// The path can be glob mode, such as /tmp/*.txt
	// We check the directories that contain this pattern
	dir := filepath.Dir(path)
	if err := checkPath(ctx, dir); err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Check if path contains glob characters
	// Check if the path contains glob wildcards
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
			// If at least one file is deleted or there is no matching file (a success status), it returns a success
			// You may want to return detailed information about deleted files in the metadata
			msg.Metadata.PutValue(KeyDeletedCount, str.ToString(deletedCount))
			ctx.TellSuccess(msg)
		}
	} else {
		// Single file delete
		// Individual file deletion
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

// Desc returns the component description
func (x *FileDeleteNode) Desc() string {
	return "Delete file. Path supports ${metadata.key} substitution. Routes to Success/Failure"
}

// FileListNodeConfiguration Configuration of the FileList node
type FileListNodeConfiguration struct {
	Path      string `json:"path" label:"Path" desc:"Directory path, supports ${metadata.key} and ${msg.key} substitution" required:"true"`
	Recursive bool   `json:"recursive" label:"Recursive" desc:"Recursively list files in subdirectories"`
}

// FileListNode list files
// List the files
//
// Configuration:
// Configuration:
//
//	{
//		"path": "/tmp/*.txt", // File path pattern, supports variable substitution. If it is a relative path, it is relative to workDir in context, otherwise it is relative to the process working directory.
//		"recursive": false // Whether to search recursively. Default is false. Whether to recursively search subdirectories is false by default
//	}
type FileListNode struct {
	//Node configuration
	Config FileListNodeConfiguration
	//path template
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
	// For safety, check the directory section of the path
	// The path can be glob mode, such as /tmp/*.txt
	// We check the directories that contain this pattern
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
		// Use filepath.Glob performs non-recursive search, which is more efficient
		paths, err = filepath.Glob(path)
	}

	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// Convert to interface array for JSON serialization
	// Convert to interface arrays for JSON serialization
	var result []interface{}
	for _, p := range paths {
		result = append(result, p)
	}

	msg.SetData(str.ToString(result))
	ctx.TellSuccess(msg)
}

func (x *FileListNode) Destroy() {
}

// Desc returns the component description
func (x *FileListNode) Desc() string {
	return "List files matching path pattern. Supports wildcards and ${metadata.key} substitution. Routes to Success/Failure"
}
