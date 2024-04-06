package handler

import "io"

type Persister interface {
	Reloader() (io.ReadCloser, error)
	PersistCmd(cmd [][]byte)
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
