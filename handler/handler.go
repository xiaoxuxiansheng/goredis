package handler

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/xiaoxuxiansheng/goredis/log"
	"github.com/xiaoxuxiansheng/goredis/server"
)

type Handler struct {
	sync.Once
	mu     sync.RWMutex
	conns  map[net.Conn]struct{}
	closed atomic.Bool

	db     DB
	parser Parser

	logger log.Logger
}

func NewHandler(db DB, parser Parser, logger log.Logger) server.Handler {
	return &Handler{
		conns:  make(map[net.Conn]struct{}),
		logger: logger,
		db:     db,
		parser: parser,
	}
}

func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	h.mu.Lock()
	// 判断 db 是否已经关闭
	if h.closed.Load() {
		h.mu.Unlock()
		return
	}

	// 当前 conn 缓存起来
	h.conns[conn] = struct{}{}
	h.mu.Unlock()

	// 持续处理
	stream := h.parser.ParseStream(conn)
	for {
		select {
		case <-ctx.Done():
			h.logger.Warnf("[handler]handle ctx err: %s", ctx.Err().Error())
			return

		case droplet := <-stream:
			if err := h.handle(ctx, conn, droplet); err != nil {
				h.logger.Errorf("[handler]conn terminated, err: %s", droplet.Err.Error())
				return
			}
		}
	}
}

func (h *Handler) handle(ctx context.Context, conn net.Conn, droplet *Droplet) error {
	if droplet.Terminated() {
		return droplet.Err
	}

	if droplet.Err != nil {
		_, _ = conn.Write(droplet.Reply.ToBytes())
		h.logger.Errorf("[handler]conn request, err: %s", droplet.Err.Error())
		return nil
	}

	if droplet.Reply == nil {
		h.logger.Errorf("[handler]conn empty request")
		return nil
	}

	// 请求参数必须为 multiBulkReply 类型
	multiReply, ok := droplet.Reply.(MultiReply)
	if !ok {
		h.logger.Errorf("[handler]conn invalid request: %s", droplet.Reply.ToBytes())
		return nil
	}

	if reply := h.db.Do(ctx, multiReply.Args()); reply != nil {
		_, _ = conn.Write(reply.ToBytes())
		return nil
	}

	_, _ = conn.Write(UnknownErrReplyBytes)
	return nil
}

func (h *Handler) Close() {
	h.Once.Do(func() {
		h.logger.Warnf("[handler]handler closing...")
		h.closed.Store(true)
		h.mu.RLock()
		defer h.mu.RUnlock()
		for conn := range h.conns {
			if err := conn.Close(); err != nil {
				h.logger.Errorf("[handler]close conn err, local addr: %s, err: %s", conn.LocalAddr().String(), err.Error())
			}
		}
		h.conns = nil
		h.db.Close()
	})
}
