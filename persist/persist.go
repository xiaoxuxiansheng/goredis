package persist

import (
	"context"
	"io"

	"github.com/xiaoxuxiansheng/goredis/handler"
)

type Thinker interface {
	AppendOnly() bool
	AppendFileName() string
	AppendFsync() string
	AutoAofRewriteAfterCmd() int
}

func NewPersister(thinker Thinker) (handler.Persister, error) {
	if !thinker.AppendOnly() {
		return newFakePersister(nil), nil
	}

	return newAofPersister(thinker)
}

type fakeReadCloser struct {
	io.Reader
	closef func() error
}

func readCloserAdapter(reader io.Reader, closef func() error) io.ReadCloser {
	return &fakeReadCloser{Reader: reader, closef: closef}
}

func (f *fakeReadCloser) Close() error {
	return f.closef()
}

func newFakePersister(readCloser io.ReadCloser) handler.Persister {
	f := fakePersister{}
	if readCloser == nil {
		f.readCloser = singleFakeReloader
		return &f
	}
	f.readCloser = readCloser
	return &f
}

type fakePersister struct {
	readCloser io.ReadCloser
}

func (f *fakePersister) Reloader() (io.ReadCloser, error) {
	return f.readCloser, nil
}

func (f *fakePersister) PersistCmd(ctx context.Context, cmd [][]byte) {}

func (f *fakePersister) Close() {}

var singleFakeReloader = &fakeReloader{}

type fakeReloader struct {
}

func (f *fakeReloader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (f *fakeReloader) Close() error {
	return nil
}
