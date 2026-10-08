package email

import (
	"context"
	"errors"
	"strings"
)

// Driver 邮件发送驱动
type Driver interface {
	// Close 关闭驱动
	Close()
	// Send 发送邮件
	Send(ctx context.Context, to, title, body string) error
}

var (
	// ErrChanNotOpen 邮件队列未开启
	ErrChanNotOpen = errors.New("email queue is not started")
	// ErrNoActiveDriver 无可用邮件发送服务
	ErrNoActiveDriver = errors.New("no avaliable email provider")
)

// QQPlaceholderSuffix is the domain of the placeholder address assigned to
// accounts created through a QQ OpenID login. Such an address is not a mailbox
// and rejects mail, so it must never be treated as a deliverable recipient.
const QQPlaceholderSuffix = "@login.qq.com"

// Undeliverable reports whether an address is a placeholder that cannot receive
// mail.
//
// The SMTP pool skips these silently, because a transactional send has no other
// sensible outcome. A bulk send must ask first: counting a skipped address as
// delivered would overstate how many users were actually reached.
func Undeliverable(address string) bool {
	return strings.HasSuffix(address, QQPlaceholderSuffix)
}
