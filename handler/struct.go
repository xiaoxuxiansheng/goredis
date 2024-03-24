package handler

import (
	"context"
	"io"
	"strings"
)

var UnknownErrReplyBytes = []byte("-ERR unknown\r\n")

type Reply interface {
	ToBytes() []byte
}

type MultiReply interface {
	Reply
	Args() [][]byte
}

type Droplet struct {
	Reply Reply
	Err   error
}

func (d *Droplet) Terminated() bool {
	if d.Err == io.EOF || d.Err == io.ErrUnexpectedEOF {
		return true
	}
	return d.Err != nil && strings.Contains(d.Err.Error(), "use of closed network connection")
}

type DB interface {
	Do(ctx context.Context, cmdLine [][]byte) Reply
	Close()
}

// 协议解析器
type Parser interface {
	ParseStream(reader io.Reader) <-chan *Droplet
}
