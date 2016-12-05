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

// Term prepares a gorethink Term to be sent to a rethink-persist service
func Term(term r.Term) interface{} {
	t, err := term.Build()
	if err != nil {
		return nil
	}
	return t
}

// ResIsNil returns wether a result is nil (nil or slice with one nil value)
func ResIsNil(res interface{}) bool {
	if res == nil {
		return true
	}
	if sl, err := ei.N(res).Slice(); err == nil {
		if len(sl) > 0 {
			return sl[0] == nil
		}
		return true
	}
	return false
}

// Write response parses a write operation response into a gorethink WriteResponse
func WriteResponse(res interface{}) *r.WriteResponse {
	wres := r.WriteResponse{}
	err := encoding.Decode(&wres, res)
	if err != nil {
		return &wres
	}
	return &wres
}

// StreamedTerm manages reading results from a rethink query in a streamed way (with a nexus pipe)
// Must be used with changefeeds. Can be used with normal queries to receive results in a stream.
type StreamedTerm struct {
	lock      *sync.Mutex
	term      r.Term
	pipeRx    *nexus.Pipe
	keepalive int64
	ch        chan interface{}
	started   bool
	closed    bool
	err       error
}

// StreamedTermOpts are the options for reading from a StreamedTerm
type StreamedTermOpts struct {
	Keepalive int64 //
	PipeLen   int
}

func NewStreamedTerm(nxconn *nexus.NexusConn, term r.Term, opts ...*StreamedTermOpts) (*StreamedTerm, error) {
	var keepalive int64 = 30
	var pipeLen int = 10000
	if len(opts) > 0 {
		keepalive = opts[0].Keepalive
		pipeLen = opts[0].PipeLen
	}
	pipeRx, err := nxconn.PipeCreate(&nexus.PipeOpts{Length: pipeLen})
	if err != nil {
		return nil, fmt.Errorf("Error creating receive pipe")
	}
	st := &StreamedTerm{
		lock:      &sync.Mutex{},
		term:      term,
		pipeRx:    pipeRx,
		keepalive: keepalive,
		ch:        make(chan interface{}),
		started:   false,
		closed:    false,
		err:       nil,
	}

	return st, nil
}

func (st *StreamedTerm) Params() map[string]interface{} {
	return ei.M{"term": Term(st.term), "pipeid": st.pipeRx.Id(), "keepalive": st.keepalive}
}

func (st *StreamedTerm) Ch() chan interface{} {
	st.start()
	return st.ch
}

func (st *StreamedTerm) Next() (interface{}, error) {
	st.start()
	el, ok := <-st.ch
	if !ok {
		if err := st.Err(); err != nil {
			return nil, err
		} else {
			return nil, fmt.Errorf("Stream closed locally")
		}
	}
	return el, nil
}

func (st *StreamedTerm) Closed() bool {
	st.lock.Lock()
	defer st.lock.Unlock()
	return st.closed
}

func (st *StreamedTerm) Err() error {
	st.lock.Lock()
	defer st.lock.Unlock()
	return st.err
}

func (st *StreamedTerm) Close() {
	st.lock.Lock()
	defer st.lock.Unlock()
	if !st.closed {
		st.closed = true
		st.pipeRx.Close()
	}
}

func (st *StreamedTerm) start() {
	st.lock.Lock()
	if st.started && !st.closed {
		st.lock.Unlock()
		return
	}
	go func() {
		defer st.pipeRx.Close()
		defer close(st.ch)

		msgCh := st.pipeRx.Listen(nil)
		st.keepalive += 1
		keepaliveTimeout := time.After(time.Second * time.Duration(st.keepalive))
		var expected int64 = 1
		st.started = true
		st.lock.Unlock()

		for {
			select {
			case msg, ok := <-msgCh:
				if !ok {
					st.lock.Lock()
					if !st.closed {
						st.err = fmt.Errorf("Error reading from stream pipe: closed")
					}
					st.closed = true
					st.lock.Unlock()
					return
				}
				if msg.Count != expected {
					st.lock.Lock()
					st.err = fmt.Errorf("Error reading from stream pipe: %d drops", msg.Count-expected)
					st.closed = true
					st.lock.Unlock()
					return
				}
				expected += 1
				ty, err := ei.N(msg.Msg).M("type").Int()
				if err == nil {
					if ty == PipeMsgData {
						data, err := ei.N(msg.Msg).M("data").Raw()
						if err == nil {
							st.ch <- data
						}
						keepaliveTimeout = time.After(time.Second * time.Duration(st.keepalive))
					} else if ty == PipeMsgKeepalive {
						keepaliveTimeout = time.After(time.Second * time.Duration(st.keepalive))
					} else if ty == PipeMsgError {
						errs, err := ei.N(msg.Msg).M("error").String()
						if err == nil {
							st.lock.Lock()
							st.err = fmt.Errorf("Stream closed by other peer: %s", errs)
							st.closed = true
							st.lock.Unlock()
							return
						}
					}
				}
			case <-keepaliveTimeout:
				st.lock.Lock()
				st.err = fmt.Errorf("Stream closed by keepalive")
				st.closed = true
				st.lock.Unlock()
				return
			}
		}
	}()
}
