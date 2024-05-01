package backend

import (
	"context"
	"io"
	"net/http"
	"time"
)

// watchdogRoundtriper cancels an http request if an upload or download did not make progress
// within timeout. The time between fully sending the request and receiving an response is also
// limited by this timeout. This ensures that stuck requests are cancel after some time.
//
// The roundtriper makes the assumption that the upload and download happen continuously. In particular,
// the caller must not make long pauses between individual read requests from the response body.
type watchdogRoundtriper struct {
	rt        http.RoundTripper
	timeout   time.Duration
	chunkSize int
}

var _ http.RoundTripper = &watchdogRoundtriper{}

func newWatchdogRoundtriper(rt http.RoundTripper, timeout time.Duration, chunkSize int) *watchdogRoundtriper {
	return &watchdogRoundtriper{
		rt:        rt,
		timeout:   timeout,
		chunkSize: chunkSize,
	}
}

func (w *watchdogRoundtriper) RoundTrip(req *http.Request) (*http.Response, error) {
	timer := time.NewTimer(w.timeout)
	ctx, cancel := context.WithCancel(req.Context())

	// cancel context if timer expires
	go func() {
		defer timer.Stop()
		select {
		case <-timer.C:
			cancel()
		case <-ctx.Done():
		}
	}()

	kick := func() {
		timer.Reset(w.timeout)
	}

	req = req.Clone(ctx)
	if req.Body != nil {
		// kick watchdog timer as long as uploading makes progress
		req.Body = newWatchdogReadCloser(req.Body, w.chunkSize, kick, nil)
	}

	resp, err := w.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// kick watchdog timer as long as downloading makes progress
	// cancel context to stop goroutine once response body is closed
	resp.Body = newWatchdogReadCloser(resp.Body, w.chunkSize, kick, cancel)
	return resp, nil
}

func newWatchdogReadCloser(rc io.ReadCloser, chunkSize int, kick func(), close func()) *watchdogReadCloser {
	return &watchdogReadCloser{
		rc:        rc,
		chunkSize: chunkSize,
		kick:      kick,
		close:     close,
	}
}

type watchdogReadCloser struct {
	rc        io.ReadCloser
	chunkSize int
	kick      func()
	close     func()
}

var _ io.ReadCloser = &watchdogReadCloser{}

func (w *watchdogReadCloser) Read(p []byte) (n int, err error) {
	w.kick()

	if len(p) > w.chunkSize {
		p = p[:w.chunkSize]
	}
	n, err = w.rc.Read(p)
	w.kick()

	return n, err
}

func (w *watchdogReadCloser) Close() error {
	if w.close != nil {
		w.close()
	}
	return w.rc.Close()
}
