package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/jaracil/ei"
	nexus "github.com/nayarsystems/nxgo/nxcore"
	r "gopkg.in/dancannon/gorethink.v2"
	"gopkg.in/dancannon/gorethink.v2/encoding"
)

const (
	PipeMsgError     = -1
	PipeMsgData      = 0
	PipeMsgKeepalive = 1
)

func Term(term r.Term) interface{} {
	t, err := term.Build()
	if err != nil {
		return nil
	}
	return t
}

func WriteResponse(res interface{}) *r.WriteResponse {
	var wres r.WriteResponse
	err := encoding.Decode(&wres, res)
	if err != nil {
		return nil
	}
	return &wres
}

type StreamedResponse struct {
	lock   *sync.Mutex
	pipeRx *nexus.Pipe
	ch     chan interface{}
	closed bool
	err    error
}

func StreamResponse(pipeRx *nexus.Pipe, keepalive int) *StreamedResponse {
	sr := &StreamedResponse{
		lock:   &sync.Mutex{},
		pipeRx: pipeRx,
		ch:     make(chan interface{}),
		closed: false,
		err:    nil,
	}
	go func(sr *StreamedResponse) {
		defer pipeRx.Close()
		defer close(sr.ch)

		msgCh := sr.pipeRx.Listen(nil)
		keepalive += 1
		keepaliveTimeout := time.After(time.Second * time.Duration(keepalive))
		var expected int64 = 1

		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					sr.lock.Lock()
					if !sr.closed {
						sr.err = fmt.Errorf("Error reading from stream pipe: closed")
					}
					sr.closed = true
					sr.lock.Unlock()
					return
				}
				if msg.Count != expected {
					sr.lock.Lock()
					sr.err = fmt.Errorf("Error reading from stream pipe: %d drops", msg.Count-expected)
					sr.closed = true
					sr.lock.Unlock()
					return
				}
				expected += 1
				ty, err := ei.N(msg.Msg).M("type").Int()
				if err == nil {
					if ty == PipeMsgData {
						data, err := ei.N(msg.Msg).M("data").Raw()
						if err == nil {
							sr.ch <- data
						}
					} else if ty == PipeMsgKeepalive {
						keepaliveTimeout = time.After(time.Second * time.Duration(keepalive))
					} else if ty == PipeMsgError {
						errs, err := ei.N(msg.Msg).M("error").String()
						if err == nil {
							sr.lock.Lock()
							sr.err = fmt.Errorf("Stream closed by other peer: %s", errs)
							sr.closed = true
							sr.lock.Unlock()
							return
						}
					}
				}
			case <-keepaliveTimeout:
				sr.lock.Lock()
				sr.err = fmt.Errorf("Stream closed by keepalive")
				sr.closed = true
				sr.lock.Unlock()
				return
			}
		}
	}(sr)
	return sr
}

func (sr *StreamedResponse) Ch() chan interface{} {
	return sr.ch
}

func (sr *StreamedResponse) Next() (interface{}, error) {
	el, ok := <-sr.ch
	if !ok {
		if err := sr.Err(); err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf("Stream closed locally")
		}
	}
	return el, nil
}

func (sr *StreamedResponse) Closed() bool {
	sr.lock.Lock()
	defer sr.lock.Unlock()
	return sr.closed
}

func (sr *StreamedResponse) Err() error {
	sr.lock.Lock()
	defer sr.lock.Unlock()
	return sr.err
}

func (sr *StreamedResponse) Close() {
	sr.lock.Lock()
	defer sr.lock.Unlock()
	if !sr.closed {
		sr.closed = true
		sr.pipeRx.Close()
	}
}
