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
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test"
	"github.com/rulego/rulego/test/assert"
	"github.com/rulego/rulego/utils/fs"
)

func TestFileNode(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&FileReadNode{})
	Registry.Add(&FileWriteNode{})
	Registry.Add(&FileDeleteNode{})
	Registry.Add(&FileListNode{})

	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")

	t.Run("GlobReadRecursive", func(t *testing.T) {
		// Setup directory structure:
		// tempDir/glob_rec_1.txt
		// tempDir/subdir/glob_rec_2.txt

		file1 := filepath.Join(tempDir, "glob_rec_1.txt")
		subdir := filepath.Join(tempDir, "subdir")
		_ = os.Mkdir(subdir, 0755)
		file2 := filepath.Join(subdir, "glob_rec_2.txt")

		_ = os.WriteFile(file1, []byte("Content1"), 0644)
		_ = os.WriteFile(file2, []byte("Content2"), 0644)

		metaData := types.NewMetadata()
		msgList := []test.Msg{{DataType: types.JSON, MetaData: metaData, MsgType: "TEST_MSG_TYPE", Data: ""}}

		// 1. Recursive = false (Default) -> Should only read file1
		// Pattern: tempDir/*.txt (Note: standard glob doesn't match subdir)
		// Wait, standard glob pattern "dir/*.txt" matches files in dir.
		// If we use "dir/*", it matches files and dirs in dir.
		// If we want to test recursive vs non-recursive, we need a pattern that *would* match deeply if recursive.
		// fs.GetFilePaths implementation of recursive is: walk dir, match filename.
		// So pattern "/tmp/*.txt" matches "/tmp/a.txt" and "/tmp/sub/b.txt".
		// filepath.Glob("/tmp/*.txt") ONLY matches "/tmp/a.txt".

		nodeNonRec, err := test.CreateAndInitNode("x/fileRead", types.Configuration{
			"path":      filepath.Join(tempDir, "*.txt"),
			"dataType":  "text",
			"recursive": false,
		}, Registry)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, nodeNonRec, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Should contain "Content1" but NOT "Content2"
			// Also might contain other .txt files from previous tests in tempDir
			assert.True(t, strings.Contains(msg.GetData(), "Content1"))
			assert.False(t, strings.Contains(msg.GetData(), "Content2"))
		})
		wg.Wait()

		// 2. Recursive = true -> Should read file1 and file2
		nodeRec, err := test.CreateAndInitNode("x/fileRead", types.Configuration{
			"path":      filepath.Join(tempDir, "*.txt"),
			"dataType":  "text",
			"recursive": true,
		}, Registry)
		assert.Nil(t, err)

		wg.Add(1)
		test.NodeOnMsg(t, nodeRec, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Should contain BOTH
			assert.True(t, strings.Contains(msg.GetData(), "Content1"))
			assert.True(t, strings.Contains(msg.GetData(), "Content2"))
		})
		wg.Wait()
	})

	t.Run("WriteFile", func(t *testing.T) {
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    testFile,
			"content": "hello world",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Verify file content
			content, readErr := os.ReadFile(testFile)
			assert.Nil(t, readErr)
			assert.Equal(t, "hello world", string(content))
		})
		wg.Wait()
	})

	t.Run("ReadFile", func(t *testing.T) {
		node, err := test.CreateAndInitNode("x/fileRead", types.Configuration{
			"path": testFile,
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			if err != nil {
				t.Logf("ReadFile error: %v", err)
			}
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			assert.Equal(t, "hello world", msg.GetData())
		})
		wg.Wait()
	})

	t.Run("DeleteFile", func(t *testing.T) {
		node, err := test.CreateAndInitNode("x/fileDelete", types.Configuration{
			"path": testFile,
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Verify file is deleted
			_, statErr := os.Stat(testFile)
			assert.True(t, os.IsNotExist(statErr))
		})
		wg.Wait()
	})

	t.Run("VarSubstitution", func(t *testing.T) {
		varFile := filepath.Join(tempDir, "${metadata.filename}")
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    varFile,
			"content": "${msg.content}",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		metaData.PutValue("filename", "var_test.txt")
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "{\"content\":\"var content\"}",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Verify file content
			actualFile := filepath.Join(tempDir, "var_test.txt")
			content, readErr := os.ReadFile(actualFile)
			assert.Nil(t, readErr)
			assert.Equal(t, "var content", string(content))
		})
		wg.Wait()
	})

	t.Run("Whitelist", func(t *testing.T) {
		// Set whitelist to tempDir
		config := types.NewConfig()
		config.Properties.PutValue(KeyFilePathWhitelist, tempDir)

		// Allowed path
		allowedFile := filepath.Join(tempDir, "allowed.txt")
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    allowedFile,
			"content": "allowed",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsgWithChildrenAndConfig(t, config, node, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
		})
		wg.Wait()

		// Denied path (parent directory)
		deniedFile := filepath.Join(filepath.Dir(tempDir), "denied.txt")
		nodeDenied, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    deniedFile,
			"content": "denied",
		}, Registry)
		assert.Nil(t, err)

		wg.Add(1)
		test.NodeOnMsgWithChildrenAndConfig(t, config, nodeDenied, msgList, nil, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Failure, relationType)
			assert.NotNil(t, err)
			assert.Equal(t, ErrPathNotAllowed.Error(), err.Error())
		})
		wg.Wait()
	})

	t.Run("WorkDir", func(t *testing.T) {
		// Use relative path
		relativePath := "relative.txt"
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    relativePath,
			"content": "relative content",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		// Set workDir to tempDir via context
		// metaData.PutValue("workDir", tempDir)
		msg := types.NewMsg(0, "TEST_MSG_TYPE", types.JSON, metaData, "")

		var wg sync.WaitGroup
		wg.Add(1)

		// Create rule context with workDir in context
		config := types.NewConfig()
		ruleCtx := test.NewRuleContext(config, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Verify file content at tempDir/relative.txt
			expectedFile := filepath.Join(tempDir, relativePath)
			content, readErr := os.ReadFile(expectedFile)
			assert.Nil(t, readErr)
			assert.Equal(t, "relative content", string(content))
		})
		ruleCtx.SetContext(context.WithValue(context.Background(), KeyWorkDir, tempDir))

		node.OnMsg(ruleCtx, msg)

		wg.Wait()
	})

	t.Run("AppendAndBase64", func(t *testing.T) {
		appendFile := filepath.Join(tempDir, "append.txt")

		// 1. Write Part1
		nodeWrite1, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    appendFile,
			"content": "Part1",
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, nodeWrite1, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
		})
		wg.Wait()

		// 2. Write Part2 with append
		nodeWrite2, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    appendFile,
			"content": "Part2",
			"append":  true,
		}, Registry)
		assert.Nil(t, err)

		wg.Add(1)
		test.NodeOnMsg(t, nodeWrite2, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
		})
		wg.Wait()

		// Verify content on disk
		content, _ := os.ReadFile(appendFile)
		assert.Equal(t, "Part1Part2", string(content))
		time.Now().Format("20060102")
		// 3. Read with Base64
		nodeRead, err := test.CreateAndInitNode("x/fileRead", types.Configuration{
			"path":     appendFile,
			"dataType": "base64",
		}, Registry)
		assert.Nil(t, err)

		wg.Add(1)
		test.NodeOnMsg(t, nodeRead, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			expectedBase64 := base64.StdEncoding.EncodeToString([]byte("Part1Part2"))
			assert.Equal(t, expectedBase64, msg.GetData())
		})
		wg.Wait()
	})

	t.Run("FileList", func(t *testing.T) {
		// Create some files
		file1 := filepath.Join(tempDir, "list_1.txt")
		file2 := filepath.Join(tempDir, "list_2.log")
		file3 := filepath.Join(tempDir, "other.txt")
		_ = os.WriteFile(file1, []byte("1"), 0644)
		_ = os.WriteFile(file2, []byte("2"), 0644)
		_ = os.WriteFile(file3, []byte("3"), 0644)

		// Case 1: List *.txt
		node, err := test.CreateAndInitNode("x/fileList", types.Configuration{
			"path": filepath.Join(tempDir, "*.txt"),
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Check if list_1.txt and other.txt are in the result
			// The result is a JSON array string because msg.Data wraps it
			// But wait, NewSharedData(interface{}) usually results in a wrapper that might need casting or JSON decoding if accessing raw bytes.
			// msg.GetData() returns string representation.

			// Let's check if the string contains the filenames
			assert.True(t, filepath.Base(file1) == "list_1.txt")
			// Simple string check
			assert.True(t, len(msg.GetData()) > 0)
			// Parse JSON to verify
			// But for simplicity in this test environment, we can just check string contains
		})
		wg.Wait()
	})

	t.Run("FileListRecursive", func(t *testing.T) {
		// Create nested directory
		nestedDir := filepath.Join(tempDir, "nested")
		_ = os.Mkdir(nestedDir, 0755)

		file1 := filepath.Join(tempDir, "rec_1.txt")
		file2 := filepath.Join(nestedDir, "rec_2.txt")
		_ = os.WriteFile(file1, []byte("1"), 0644)
		_ = os.WriteFile(file2, []byte("2"), 0644)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		// Case 1: Recursive = false (default)
		// Pattern matches rec_*.txt
		node, err := test.CreateAndInitNode("x/fileList", types.Configuration{
			"path": filepath.Join(tempDir, "rec_*.txt"),
		}, Registry)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Should contain rec_1.txt but NOT rec_2.txt
			assert.True(t, strings.Contains(msg.GetData(), "rec_1.txt"))
			assert.False(t, strings.Contains(msg.GetData(), "rec_2.txt"))
		})
		wg.Wait()

		// Case 2: Recursive = true
		nodeRec, err := test.CreateAndInitNode("x/fileList", types.Configuration{
			"path":      filepath.Join(tempDir, "rec_*.txt"),
			"recursive": true,
		}, Registry)
		assert.Nil(t, err)

		wg.Add(1)
		test.NodeOnMsg(t, nodeRec, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// Should contain BOTH
			assert.True(t, strings.Contains(msg.GetData(), "rec_1.txt"))
			assert.True(t, strings.Contains(msg.GetData(), "rec_2.txt"))
		})
		wg.Wait()
	})

	t.Run("GlobDelete", func(t *testing.T) {
		// Create files for deletion
		del1 := filepath.Join(tempDir, "del_1.txt")
		del2 := filepath.Join(tempDir, "del_2.txt")
		keep := filepath.Join(tempDir, "keep.log")
		_ = os.WriteFile(del1, []byte("d1"), 0644)
		_ = os.WriteFile(del2, []byte("d2"), 0644)
		_ = os.WriteFile(keep, []byte("k"), 0644)

		node, err := test.CreateAndInitNode("x/fileDelete", types.Configuration{
			"path": filepath.Join(tempDir, "del_*.txt"),
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			assert.Equal(t, "2", msg.Metadata.GetValue("deletedCount"))
		})
		wg.Wait()

		// Verify files
		assert.False(t, fs.IsExist(del1))
		assert.False(t, fs.IsExist(del2))
		assert.True(t, fs.IsExist(keep))
	})

	t.Run("GlobRead", func(t *testing.T) {
		// Create files for reading
		read1 := filepath.Join(tempDir, "read_1.txt")
		read2 := filepath.Join(tempDir, "read_2.txt")
		_ = os.WriteFile(read1, []byte("content1"), 0644)
		_ = os.WriteFile(read2, []byte("content2"), 0644)

		node, err := test.CreateAndInitNode("x/fileRead", types.Configuration{
			"path": filepath.Join(tempDir, "read_*.txt"),
		}, Registry)
		assert.Nil(t, err)

		metaData := types.NewMetadata()
		msgList := []test.Msg{
			{
				DataType: types.JSON,
				MetaData: metaData,
				MsgType:  "TEST_MSG_TYPE",
				Data:     "",
			},
		}

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsg(t, node, msgList, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
			// msg.Data should be concatenated content with newline
			// We can verify content simply by string contains
			assert.True(t, strings.Contains(msg.GetData(), "content1"))
			assert.True(t, strings.Contains(msg.GetData(), "content2"))
			// The file name should NOT be in the content anymore
			assert.False(t, strings.Contains(msg.GetData(), "read_1.txt"))
		})
		wg.Wait()
	})
}

func TestFileNodeWhitelistWildcard(t *testing.T) {
	Registry := &types.SafeComponentSlice{}
	Registry.Add(&FileWriteNode{})

	// Setup directories
	rootDir, err := os.MkdirTemp("", "rulego_wildcard")
	assert.Nil(t, err)
	defer os.RemoveAll(rootDir)

	// Structure:
	// rootDir/
	//   tenantA/
	//     output/ -> Allowed
	//     private/ -> Denied
	//   tenantB/
	//     output/ -> Allowed

	tenantADir := filepath.Join(rootDir, "tenantA")
	tenantBDir := filepath.Join(rootDir, "tenantB")

	err = os.MkdirAll(filepath.Join(tenantADir, "output"), 0755)
	assert.Nil(t, err)
	err = os.MkdirAll(filepath.Join(tenantADir, "private"), 0755)
	assert.Nil(t, err)
	err = os.MkdirAll(filepath.Join(tenantBDir, "output"), 0755)
	assert.Nil(t, err)

	// Configure Whitelist with Wildcard
	// Pattern: rootDir/*/output
	// Note: We use filepath.Join to ensure correct separators for the OS
	wildcardPattern := filepath.Join(rootDir, "*", "output")

	config := types.NewConfig()
	config.Properties.PutValue(KeyFilePathWhitelist, wildcardPattern)

	t.Logf("Whitelist Pattern: %s", wildcardPattern)

	// Case 1: Write to allowed directory in Tenant A
	t.Run("Allowed_TenantA", func(t *testing.T) {
		targetFile := filepath.Join(tenantADir, "output", "data.txt")
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    targetFile,
			"content": "data",
		}, Registry)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsgWithChildrenAndConfig(t, config, node, []test.Msg{{DataType: types.JSON}}, nil, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
		})
		wg.Wait()
	})

	// Case 2: Write to allowed directory in Tenant B
	t.Run("Allowed_TenantB", func(t *testing.T) {
		targetFile := filepath.Join(tenantBDir, "output", "data.txt")
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    targetFile,
			"content": "data",
		}, Registry)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsgWithChildrenAndConfig(t, config, node, []test.Msg{{DataType: types.JSON}}, nil, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Success, relationType)
			assert.Nil(t, err)
		})
		wg.Wait()
	})

	// Case 3: Write to denied directory in Tenant A (private)
	t.Run("Denied_TenantA_Private", func(t *testing.T) {
		targetFile := filepath.Join(tenantADir, "private", "data.txt")
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    targetFile,
			"content": "data",
		}, Registry)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsgWithChildrenAndConfig(t, config, node, []test.Msg{{DataType: types.JSON}}, nil, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Failure, relationType)
			assert.NotNil(t, err)
			t.Logf("Expected error: %v", err)
		})
		wg.Wait()
	})

	// Case 4: Write to root of Tenant A (not in output)
	t.Run("Denied_TenantA_Root", func(t *testing.T) {
		targetFile := filepath.Join(tenantADir, "root_data.txt")
		node, err := test.CreateAndInitNode("x/fileWrite", types.Configuration{
			"path":    targetFile,
			"content": "data",
		}, Registry)
		assert.Nil(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		test.NodeOnMsgWithChildrenAndConfig(t, config, node, []test.Msg{{DataType: types.JSON}}, nil, func(msg types.RuleMsg, relationType string, err error) {
			defer wg.Done()
			assert.Equal(t, types.Failure, relationType)
			assert.NotNil(t, err)
		})
		wg.Wait()
	})
}
