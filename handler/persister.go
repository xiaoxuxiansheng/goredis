package handler

import (
	"context"
	"io"
)

var loadingPersisterPattern int
var ctxKeyLoadingPersisterPattern = &loadingPersisterPattern

func SetLoadingPattern(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeyLoadingPersisterPattern, true)
}

func IsLoadingPattern(ctx context.Context) bool {
	is, _ := ctx.Value(ctxKeyLoadingPersisterPattern).(bool)
	return is
}

type Persister interface {
	Reloader() (io.ReadCloser, error)
	PersistCmd(ctx context.Context, cmd [][]byte)
	Close()
}

type fakeReadWriter struct {
	io.Reader
}

func newFakeReaderWriter(reader io.Reader) io.ReadWriter {
	return &fakeReadWriter{
		Reader: reader,
	}
}

func (f *fakeReadWriter) Write(p []byte) (n int, err error) {
	// log ...
	return 0, nil
}
