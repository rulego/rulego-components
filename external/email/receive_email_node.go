/*
 * Copyright 2026 The RuleGo Authors.
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

package email

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/text/encoding/htmlindex"
	"golang.org/x/text/transform"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/str"
)

const (
	defaultIMAPPort         = 993
	defaultConnectTimeout   = 10
	defaultMailFolder       = "INBOX"
	defaultFetchContentType = "full"
	defaultPostAction       = "none"
	dateLayout              = "2006-01-02"
	nodeType                = "x/receiveEmail"

	// Post action types
	actionMarkRead = "markRead"
	actionDelete   = "delete"
	actionMove     = "move"

	// Content type values
	contentTypeHeaders = "headers"
	contentTypeBody    = "body"

	// Default attachment filename
	defaultAttachmentFilename = "attachment"
)

func init() {
	_ = rulego.Registry.Register(&ReceiveEmailNode{})
}

// SearchConfig 邮件搜索配置
type SearchConfig struct {
	Folder   string `json:"folder" label:"Folder" desc:"Mailbox folder name, e.g. INBOX, Sent"`
	Since    string `json:"since" label:"Since" desc:"Search start date, format: 2006-01-02"`
	Before   string `json:"before" label:"Before" desc:"Search end date, format: 2006-01-02"`
	LastDays int    `json:"lastDays" label:"Last Days" desc:"Search emails from the last N days"`
	From     string `json:"from" label:"From" desc:"Filter by sender address"`
	To       string `json:"to" label:"To" desc:"Filter by recipient address"`
	Subject  string `json:"subject" label:"Subject" desc:"Filter by email subject"`
	Unread   bool   `json:"unread" label:"Unread Only" desc:"Search only unread emails"`
	Limit    int    `json:"limit" label:"Limit" desc:"Maximum number of emails to return"`
}

// FetchConfig 邮件获取选项
type FetchConfig struct {
	ContentType         string `json:"contentType" label:"Content Type" desc:"Email content type: text, html, default text"`
	IncludeAttachments  bool   `json:"includeAttachments" label:"Include Attachments" desc:"Download email attachments"`
	MaxAttachmentSizeMB int    `json:"maxAttachmentSizeMb" label:"Max Attachment Size (MB)" desc:"Max attachment size in MB, skip if exceeded"`
	AttachmentSavePath  string `json:"attachmentSavePath" label:"Attachment Save Path" desc:"Directory path to save attachments"`
}

// PostActionConfig 执行后操作配置
type PostActionConfig struct {
	Action       string `json:"action" label:"Action" desc:"Post-fetch action: markRead, move, delete"`
	TargetFolder string `json:"targetFolder" label:"Target Folder" desc:"Target folder for move action"`
}

// ReceiveEmailConfiguration 接收邮件配置
type ReceiveEmailConfiguration struct {
	Server         string          `json:"server" label:"Server" desc:"IMAP server address" required:"true" ref:"primary"`
	Port           int             `json:"port" label:"Port" desc:"IMAP server port, default 993"`
	Username       string          `json:"username" label:"Username" desc:"Email login username" required:"true" ref:"shared"`
	Password       string          `json:"password" label:"Password" desc:"Email login password" required:"true" ref:"shared"`
	EnableTLS      bool            `json:"enableTls" label:"Enable TLS" desc:"Enable TLS encryption"`
	ConnectTimeout int             `json:"connectTimeout" label:"Connect Timeout (s)" desc:"Connection timeout in seconds"`
	Search         SearchConfig    `json:"search" label:"Search" desc:"Email search criteria configuration"`
	Fetch          FetchConfig     `json:"fetch" label:"Fetch" desc:"Email content fetch configuration"`
	PostAction     PostActionConfig `json:"postAction" label:"Post Action" desc:"Post-fetch action configuration"`
}

// EmailAddress 邮件地址
type EmailAddress struct {
	Name    string `json:"name,omitempty"`
	Address string `json:"address"`
}

// EmailAttachment 邮件附件
type EmailAttachment struct {
	Filename string `json:"filename"`
	// ContentType MIME类型
	ContentType string `json:"contentType,omitempty"`
	// Size 文件大小(字节)
	Size int64 `json:"size"`
	// ContentBase64 Base64编码内容(未保存到文件时)
	ContentBase64 string `json:"contentBase64,omitempty"`
	// Path 保存路径(保存到文件时)
	Path string `json:"path,omitempty"`
}

// EmailMessage 邮件消息
type EmailMessage struct {
	// UID 邮件唯一标识
	UID uint32 `json:"uid"`
	// MessageID 消息ID
	MessageID string `json:"messageId,omitempty"`
	// Subject 主题
	Subject string `json:"subject"`
	// From 发件人
	From EmailAddress `json:"from"`
	// To 收件人列表
	To []EmailAddress `json:"to,omitempty"`
	// Cc 抄送人列表
	Cc []EmailAddress `json:"cc,omitempty"`
	// Date 日期
	Date time.Time `json:"date"`
	// Body 正文内容
	Body string `json:"body"`
	// HtmlBody HTML正文
	HtmlBody string `json:"htmlBody,omitempty"`
	// Headers 头部字段
	Headers map[string]string `json:"headers,omitempty"`
	// Attachments 附件列表
	Attachments []EmailAttachment `json:"attachments,omitempty"`
	// Flags 标记
	Flags []string `json:"flags,omitempty"`
}

// ReceiveEmailResult 接收邮件结果
type ReceiveEmailResult struct {
	Emails []EmailMessage `json:"emails"`
	Total  int            `json:"total"`
}

// MailboxInfo 邮件夹信息
type MailboxInfo struct {
	// Name 邮件夹名称
	Name string `json:"name"`
	// Delim 层级分隔符
	Delim string `json:"delim,omitempty"`
	// Attributes 邮件夹属性
	Attributes []string `json:"attributes,omitempty"`
}

// ListMailboxesResult 获取邮件夹列表结果
type ListMailboxesResult struct {
	Mailboxes []MailboxInfo `json:"mailboxes"`
}

// ReceiveEmailNode 通过IMAP协议接收邮件
// 如果请求成功，发送消息到`Success`链，否则发到`Failure`链
type ReceiveEmailNode struct {
	Config                 ReceiveEmailConfiguration
	ConnectTimeoutDuration time.Duration
	ruleConfig             types.Config

	// 模板字段
	serverTemplate     el.Template
	usernameTemplate   el.Template
	passwordTemplate   el.Template
	sinceTemplate      el.Template
	beforeTemplate     el.Template
	fromTemplate       el.Template
	toTemplate         el.Template
	subjectTemplate    el.Template
	attachmentPathTemp el.Template

	// 是否包含变量
	hasVar bool
}

// Type 组件类型
func (x *ReceiveEmailNode) Type() string {
	return nodeType
}

// New 创建新实例
func (x *ReceiveEmailNode) New() types.Node {
	return &ReceiveEmailNode{
		Config: ReceiveEmailConfiguration{
			Port:           defaultIMAPPort,
			EnableTLS:      true,
			ConnectTimeout: defaultConnectTimeout,
			Search: SearchConfig{
				Folder: defaultMailFolder,
			},
			Fetch: FetchConfig{
				ContentType: defaultFetchContentType,
			},
			PostAction: PostActionConfig{
				Action: defaultPostAction,
			},
		},
	}
}

// Init 初始化
func (x *ReceiveEmailNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	x.ruleConfig = ruleConfig
	err := maps.Map2Struct(configuration, &x.Config)
	if err != nil {
		return err
	}

	// 设置默认值
	if x.Config.Port == 0 {
		x.Config.Port = defaultIMAPPort
	}
	if x.Config.ConnectTimeout <= 0 {
		x.Config.ConnectTimeout = defaultConnectTimeout
	}
	if x.Config.Search.Folder == "" {
		x.Config.Search.Folder = defaultMailFolder
	}
	if x.Config.Fetch.ContentType == "" {
		x.Config.Fetch.ContentType = defaultFetchContentType
	}
	if x.Config.PostAction.Action == "" {
		x.Config.PostAction.Action = defaultPostAction
	}

	x.ConnectTimeoutDuration = time.Duration(x.Config.ConnectTimeout) * time.Second

	// 初始化模板
	if x.serverTemplate, err = el.NewTemplate(x.Config.Server); err != nil {
		return err
	}
	if x.usernameTemplate, err = el.NewTemplate(x.Config.Username); err != nil {
		return err
	}
	if x.passwordTemplate, err = el.NewTemplate(x.Config.Password); err != nil {
		return err
	}
	if x.sinceTemplate, err = el.NewTemplate(x.Config.Search.Since); err != nil {
		return err
	}
	if x.beforeTemplate, err = el.NewTemplate(x.Config.Search.Before); err != nil {
		return err
	}
	if x.fromTemplate, err = el.NewTemplate(x.Config.Search.From); err != nil {
		return err
	}
	if x.toTemplate, err = el.NewTemplate(x.Config.Search.To); err != nil {
		return err
	}
	if x.subjectTemplate, err = el.NewTemplate(x.Config.Search.Subject); err != nil {
		return err
	}
	if x.attachmentPathTemp, err = el.NewTemplate(x.Config.Fetch.AttachmentSavePath); err != nil {
		return err
	}

	// 检查是否包含变量
	x.hasVar = x.serverTemplate.HasVar() || x.usernameTemplate.HasVar() || x.passwordTemplate.HasVar() ||
		x.sinceTemplate.HasVar() || x.beforeTemplate.HasVar() || x.fromTemplate.HasVar() ||
		x.toTemplate.HasVar() || x.subjectTemplate.HasVar() || x.attachmentPathTemp.HasVar()

	return nil
}

// OnMsg 处理消息
func (x *ReceiveEmailNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	// 获取环境变量
	var evn map[string]interface{}
	if x.hasVar {
		evn = base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	}

	// 执行模板
	server := x.serverTemplate.ExecuteAsString(evn)
	username := x.usernameTemplate.ExecuteAsString(evn)
	password := x.passwordTemplate.ExecuteAsString(evn)
	since := x.sinceTemplate.ExecuteAsString(evn)
	before := x.beforeTemplate.ExecuteAsString(evn)
	from := x.fromTemplate.ExecuteAsString(evn)
	to := x.toTemplate.ExecuteAsString(evn)
	subject := x.subjectTemplate.ExecuteAsString(evn)
	attachmentPath := x.attachmentPathTemp.ExecuteAsString(evn)

	// 验证必填字段
	if server == "" {
		ctx.TellFailure(msg, errors.New("server is required"))
		return
	}
	if username == "" {
		ctx.TellFailure(msg, errors.New("username is required"))
		return
	}
	if password == "" {
		ctx.TellFailure(msg, errors.New("password is required"))
		return
	}

	// 执行接收邮件
	result, err := x.receiveEmails(server, username, password, since, before, from, to, subject, attachmentPath)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}

	// 更新消息
	data := str.ToString(result)
	msg.SetData(data)
	ctx.TellSuccess(msg)
}

// Destroy 销毁
func (x *ReceiveEmailNode) Destroy() {
}

// Desc returns the component description
func (x *ReceiveEmailNode) Desc() string {
	return "Receive emails via IMAP protocol. Supports SSL/TLS. Routes to Success/Failure"
}

// receiveEmails 接收邮件
func (x *ReceiveEmailNode) receiveEmails(server, username, password, since, before, from, to, subject, attachmentPath string) (*ReceiveEmailResult, error) {
	addr := fmt.Sprintf("%s:%d", server, x.Config.Port)

	// 连接IMAP服务器
	var c *client.Client
	var err error

	dialer := &net.Dialer{Timeout: x.ConnectTimeoutDuration}

	if x.Config.EnableTLS {
		c, err = client.DialWithDialerTLS(dialer, addr, nil)
	} else {
		c, err = client.DialWithDialer(dialer, addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to IMAP server: %v", err)
	}
	defer c.Logout()

	// 登录
	if err := c.Login(username, password); err != nil {
		return nil, fmt.Errorf("login failed: %v", err)
	}

	// 选择邮件夹
	mbox, err := c.Select(x.Config.Search.Folder, false)
	if err != nil {
		return nil, fmt.Errorf("failed to select folder: %v", err)
	}

	// 如果邮件夹为空
	if mbox.Messages == 0 {
		return &ReceiveEmailResult{Emails: []EmailMessage{}, Total: 0}, nil
	}

	// 构建搜索条件
	criteria := x.buildSearchCriteria(since, before, from, to, subject)

	// 搜索邮件
	uids, err := c.Search(criteria)
	if err != nil {
		return nil, fmt.Errorf("search failed: %v", err)
	}

	if len(uids) == 0 {
		return &ReceiveEmailResult{Emails: []EmailMessage{}, Total: 0}, nil
	}

	// 应用limit限制
	total := len(uids)
	limit := x.Config.Search.Limit
	if limit > 0 && len(uids) > limit {
		// 取最新的N封邮件(从后往前取)
		uids = uids[len(uids)-limit:]
	}

	// 获取邮件
	seqset := new(imap.SeqSet)
	seqset.AddNum(uids...)

	// 获取邮件内容
	messages := make(chan *imap.Message, len(uids))
	fetchItems := x.buildFetchItems()

	err = c.Fetch(seqset, fetchItems, messages)
	if err != nil {
		return nil, fmt.Errorf("fetch failed: %v", err)
	}

	// 解析邮件
	var emails []EmailMessage
	for msg := range messages {
		if msg == nil {
			continue
		}
		email, err := x.parseEmail(msg, attachmentPath)
		if err != nil {
			x.ruleConfig.Logger.Warnf("parseEmail uid=%d failed: %v", msg.Uid, err)
			continue
		}
		emails = append(emails, *email)
	}

	// 执行后置操作
	if x.Config.PostAction.Action != defaultPostAction && len(emails) > 0 {
		x.executePostAction(c, uids)
	}

	return &ReceiveEmailResult{Emails: emails, Total: total}, nil
}

// buildSearchCriteria 构建搜索条件
func (x *ReceiveEmailNode) buildSearchCriteria(since, before, from, to, subject string) *imap.SearchCriteria {
	criteria := &imap.SearchCriteria{
		Header: make(textproto.MIMEHeader),
	}

	// 时间条件
	if x.Config.Search.LastDays > 0 {
		criteria.Since = time.Now().AddDate(0, 0, -x.Config.Search.LastDays)
	} else {
		if since != "" {
			if t, err := time.Parse(dateLayout, since); err == nil {
				criteria.Since = t
			}
		}
		if before != "" {
			if t, err := time.Parse(dateLayout, before); err == nil {
				criteria.Before = t
			}
		}
	}

	// 发件人
	if from != "" {
		criteria.Header.Add("From", from)
	}

	// 收件人
	if to != "" {
		criteria.Header.Add("To", to)
	}

	// 主题
	if subject != "" {
		criteria.Header.Add("Subject", subject)
	}

	// 未读标记
	if x.Config.Search.Unread {
		criteria.WithoutFlags = []string{imap.SeenFlag}
	}

	return criteria
}

// buildFetchItems 构建获取项
func (x *ReceiveEmailNode) buildFetchItems() []imap.FetchItem {
	items := []imap.FetchItem{imap.FetchUid, imap.FetchFlags, imap.FetchInternalDate}

	switch x.Config.Fetch.ContentType {
	case contentTypeHeaders:
		items = append(items, imap.FetchEnvelope)
	case contentTypeBody:
		items = append(items, imap.FetchBody)
	default:
		// full 或其他值默认获取完整内容
		items = append(items, imap.FetchEnvelope, imap.FetchBody)
	}

	return items
}

// parseEmail 解析邮件
func (x *ReceiveEmailNode) parseEmail(msg *imap.Message, attachmentPath string) (*EmailMessage, error) {
	email := &EmailMessage{
		UID:     msg.Uid,
		Date:    msg.InternalDate,
		Flags:   formatFlags(msg.Flags),
		Headers: make(map[string]string),
	}

	// 解析信封
	if msg.Envelope != nil {
		email.MessageID = msg.Envelope.MessageId
		email.Subject = decodeMimeHeader(msg.Envelope.Subject)

		// 发件人
		if len(msg.Envelope.From) > 0 {
			email.From = EmailAddress{
				Name:    decodeMimeHeader(msg.Envelope.From[0].PersonalName),
				Address: msg.Envelope.From[0].Address(),
			}
		}

		// 收件人
		for _, addr := range msg.Envelope.To {
			email.To = append(email.To, EmailAddress{
				Name:    decodeMimeHeader(addr.PersonalName),
				Address: addr.Address(),
			})
		}

		// 抄送人
		for _, addr := range msg.Envelope.Cc {
			email.Cc = append(email.Cc, EmailAddress{
				Name:    decodeMimeHeader(addr.PersonalName),
				Address: addr.Address(),
			})
		}
	}

	// 解析正文和附件
	if x.Config.Fetch.ContentType != contentTypeHeaders {
		if err := x.parseBody(msg, email, attachmentPath); err != nil {
			x.ruleConfig.Logger.Warnf("parseBody uid=%d failed: %v", msg.Uid, err)
		}
	}

	return email, nil
}

// parseBody 解析正文和附件
func (x *ReceiveEmailNode) parseBody(msg *imap.Message, email *EmailMessage, attachmentPath string) error {
	// 获取正文
	section := &imap.BodySectionName{}
	reader := msg.GetBody(section)
	if reader == nil {
		return nil
	}

	// 读取全部内容
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	contentType := "text/plain"
	contentTransferEncoding := ""

	// 从原始内容中分离头部和正文
	var sepLen int
	headerEnd := bytes.Index(bodyBytes, []byte("\r\n\r\n"))
	if headerEnd != -1 {
		sepLen = 4
	} else {
		headerEnd = bytes.Index(bodyBytes, []byte("\n\n"))
		if headerEnd != -1 {
			sepLen = 2
		}
	}

	var headerBytes []byte
	var bodyContent []byte
	if headerEnd != -1 {
		headerBytes = bodyBytes[:headerEnd]
		bodyContent = bodyBytes[headerEnd+sepLen:]
	} else {
		bodyContent = bodyBytes
	}

	// 解析头部
	if len(headerBytes) > 0 {
		headerReader := textproto.NewReader(bufio.NewReader(bytes.NewReader(headerBytes)))
		header, err := headerReader.ReadMIMEHeader()
		if err == nil {
			contentType = header.Get("Content-Type")
			contentTransferEncoding = header.Get("Content-Transfer-Encoding")
		}
	}

	// 解析Content-Type
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		email.Body = string(bodyContent)
		return nil
	}

	// 处理multipart消息
	if strings.HasPrefix(mediaType, "multipart/") {
		boundary := params["boundary"]
		if boundary == "" {
			return nil
		}

		mr := multipart.NewReader(bytes.NewReader(bodyContent), boundary)
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}

			partMediaType, partParams, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
			if err != nil {
				continue
			}

			// 处理附件
			filename := part.FileName()
			if filename != "" || part.Header.Get("Content-Disposition") != "" {
				if x.Config.Fetch.IncludeAttachments {
					x.processAttachment(part, filename, partMediaType, email, attachmentPath)
				}
				continue
			}

			// 处理正文
			partBytes, err := io.ReadAll(part)
			if err != nil {
				continue
			}

			partEncoding := part.Header.Get("Content-Transfer-Encoding")
			decoded := decodeBody(partBytes, partEncoding)
			bodyStr := decodeCharset(decoded, partParams["charset"])

			if strings.HasPrefix(partMediaType, "text/html") {
				email.HtmlBody = bodyStr
			} else if strings.HasPrefix(partMediaType, "text/plain") {
				email.Body = bodyStr
			}
		}
	} else {
		// 单部分消息
		decoded := decodeBody(bodyContent, contentTransferEncoding)
		bodyStr := decodeCharset(decoded, params["charset"])

		if strings.HasPrefix(mediaType, "text/html") {
			email.HtmlBody = bodyStr
		} else {
			email.Body = bodyStr
		}
	}

	return nil
}

// processAttachment 处理附件
func (x *ReceiveEmailNode) processAttachment(part *multipart.Part, filename, contentType string, email *EmailMessage, savePath string) {
	// 解码文件名
	filename = decodeMimeHeader(filename)
	if filename == "" {
		filename = defaultAttachmentFilename
	}

	// 获取附件内容
	content, err := io.ReadAll(part)
	if err != nil {
		x.ruleConfig.Logger.Warnf("read attachment %s failed: %v", filename, err)
		return
	}

	// 检查大小限制
	maxSize := int64(x.Config.Fetch.MaxAttachmentSizeMB) * 1024 * 1024
	if maxSize > 0 && int64(len(content)) > maxSize {
		x.ruleConfig.Logger.Infof("attachment %s size %d exceeds limit %d, skipped", filename, len(content), maxSize)
		return
	}

	attachment := EmailAttachment{
		Filename:    filename,
		ContentType: contentType,
		Size:        int64(len(content)),
	}

	// 保存到文件或嵌入到消息中
	if savePath != "" {
		fullPath := filepath.Join(savePath, filename)
		if err := os.MkdirAll(savePath, 0755); err != nil {
			x.ruleConfig.Logger.Warnf("create dir %s failed: %v", savePath, err)
		} else if err := os.WriteFile(fullPath, content, 0644); err != nil {
			x.ruleConfig.Logger.Warnf("write attachment %s failed: %v", fullPath, err)
		} else {
			attachment.Path = fullPath
		}
	} else {
		// 未指定保存路径，嵌入到消息中
		attachment.ContentBase64 = base64.StdEncoding.EncodeToString(content)
	}

	email.Attachments = append(email.Attachments, attachment)
}

// executePostAction 执行后置操作
func (x *ReceiveEmailNode) executePostAction(c *client.Client, uids []uint32) {
	seqset := new(imap.SeqSet)
	seqset.AddNum(uids...)

	switch x.Config.PostAction.Action {
	case actionMarkRead:
		flags := []interface{}{imap.SeenFlag}
		if err := c.Store(seqset, imap.AddFlags, flags, nil); err != nil {
			x.ruleConfig.Logger.Warnf("markRead failed: %v", err)
		}
	case actionDelete:
		flags := []interface{}{imap.DeletedFlag}
		if err := c.Store(seqset, imap.AddFlags, flags, nil); err != nil {
			x.ruleConfig.Logger.Warnf("delete mark failed: %v", err)
		}
		if err := c.Expunge(nil); err != nil {
			x.ruleConfig.Logger.Warnf("expunge failed: %v", err)
		}
	case actionMove:
		if x.Config.PostAction.TargetFolder != "" {
			if err := c.Move(seqset, x.Config.PostAction.TargetFolder); err != nil {
				x.ruleConfig.Logger.Warnf("move to %s failed: %v", x.Config.PostAction.TargetFolder, err)
			}
		}
	}
}

// decodeMimeHeader 解码MIME头部
func decodeMimeHeader(header string) string {
	if header == "" {
		return ""
	}
	dec := new(mime.WordDecoder)
	decoded, err := dec.DecodeHeader(header)
	if err != nil {
		return header
	}
	return decoded
}

// decodeBody 解码正文传输编码(base64/quoted-printable)，返回解码后的原始字节
func decodeBody(body []byte, encoding string) []byte {
	encoding = strings.ToLower(strings.TrimSpace(encoding))
	switch encoding {
	case "base64":
		// 邮件base64内容每76字符折行，需要去除空白字符
		cleaned := stripBase64Whitespace(body)
		decoded, err := base64.StdEncoding.DecodeString(string(cleaned))
		if err != nil {
			return body
		}
		return decoded
	case "quoted-printable":
		return decodeQuotedPrintable(body)
	default:
		return body
	}
}

// decodeCharset 将字节从指定字符集转换为UTF-8
func decodeCharset(data []byte, charset string) string {
	charset = strings.ToLower(strings.TrimSpace(charset))
	if charset == "" || charset == "utf-8" || charset == "utf8" || charset == "us-ascii" {
		return string(data)
	}
	enc, err := htmlindex.Get(charset)
	if err != nil {
		return string(data)
	}
	decoded, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), enc.NewDecoder()))
	if err != nil {
		return string(data)
	}
	return string(decoded)
}

// stripBase64Whitespace 去除base64内容中的空白字符
func stripBase64Whitespace(data []byte) []byte {
	return bytes.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, data)
}

// decodeQuotedPrintable 解码quoted-printable编码
func decodeQuotedPrintable(data []byte) []byte {
	var buf bytes.Buffer
	i := 0
	for i < len(data) {
		if data[i] == '=' {
			if i+2 < len(data) && isHexChar(data[i+1]) && isHexChar(data[i+2]) {
				buf.WriteByte(hexVal(data[i+1])<<4 | hexVal(data[i+2]))
				i += 3
			} else if bytes.HasPrefix(data[i:], []byte("=\r\n")) {
				i += 3 // soft line break
			} else if bytes.HasPrefix(data[i:], []byte("=\n")) {
				i += 2 // soft line break
			} else {
				buf.WriteByte(data[i])
				i++
			}
		} else {
			buf.WriteByte(data[i])
			i++
		}
	}
	return buf.Bytes()
}

func isHexChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')
}

func hexVal(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	default:
		return c - 'a' + 10
	}
}

// formatFlags 格式化标记
func formatFlags(flags []string) []string {
	var result []string
	for _, flag := range flags {
		// 移除标记前的反斜杠
		flag = strings.TrimPrefix(flag, "\\")
		result = append(result, flag)
	}
	return result
}

// ============================================================
// 工具函数 - 获取邮件夹列表
// ============================================================

// ListMailboxes 获取IMAP服务器上的所有邮件夹列表
// server: IMAP服务器地址 (如: imap.gmail.com)
// port: IMAP端口 (如: 993)
// username: 用户名
// password: 密码
// enableTLS: 是否启用TLS
// 返回邮件夹列表
func ListMailboxes(server string, port int, username, password string, enableTLS bool) (*ListMailboxesResult, error) {
	addr := fmt.Sprintf("%s:%d", server, port)

	// 连接IMAP服务器
	var c *client.Client
	var err error

	dialer := &net.Dialer{Timeout: 10 * time.Second}

	if enableTLS {
		c, err = client.DialWithDialerTLS(dialer, addr, nil)
	} else {
		c, err = client.DialWithDialer(dialer, addr)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to IMAP server: %v", err)
	}
	defer c.Logout()

	// 登录
	if err := c.Login(username, password); err != nil {
		return nil, fmt.Errorf("login failed: %v", err)
	}

	// 获取邮件夹列表
	mailboxes := make(chan *imap.MailboxInfo, 10)
	done := make(chan error, 1)

	go func() {
		done <- c.List("", "*", mailboxes)
	}()

	var result []MailboxInfo
	for m := range mailboxes {
		result = append(result, MailboxInfo{
			Name:       m.Name,
			Delim:      m.Delimiter,
			Attributes: m.Attributes,
		})
	}

	if err := <-done; err != nil {
		return nil, fmt.Errorf("failed to list mailboxes: %v", err)
	}

	return &ListMailboxesResult{Mailboxes: result}, nil
}

// ListMailboxesWithConfig 使用配置对象获取邮件夹列表
func ListMailboxesWithConfig(config ReceiveEmailConfiguration) (*ListMailboxesResult, error) {
	return ListMailboxes(config.Server, config.Port, config.Username, config.Password, config.EnableTLS)
}
