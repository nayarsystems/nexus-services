package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jaracil/ei"
	nexus "github.com/nayarsystems/nxgo/nxcore"
	"github.com/nayarsystems/nxsugar-go"
	r "gopkg.in/dancannon/gorethink.v2"
	p "gopkg.in/dancannon/gorethink.v2/ql2"
)

const (
	ErrInvalidDB    = 1
	ErrRunningQuery = 2
	ErrOnCursor     = 3
	ErrOnPipe       = 4
)

const (
	PipeMsgError     = -1
	PipeMsgData      = 0
	PipeMsgKeepalive = 1
)

type PersistService struct {
	*nxsugar.Service
	db          *r.Session
	rethinkHost string
	rethinkDb   string
	reset       bool
}

var s *PersistService

func main() {
	// Service
	svc, err := nxsugar.NewServiceFromConfig("rethink-persist")
	if err != nil {
		return
	}
	s = &PersistService{Service: svc}

	// Config
	config, err := nxsugar.GetConfig()
	if err != nil {
		return
	}
	s.rethinkHost = ei.N(config).M("services").M("rethink-persist").M("rethink-host").StringZ()
	if s.rethinkHost == "" {
		s.rethinkHost = "localhost:28015"
	}
	s.rethinkDb = ei.N(config).M("services").M("rethink-persist").M("rethink-db").StringZ()
	if s.rethinkDb == "" {
		s.rethinkDb = "persist"
	}
	s.reset = ei.N(config).M("services").M("rethink-persist").M("reset").BoolZ()
	s.Log(nxsugar.InfoLevel, "RethinkDB host=%s db=%s reset=%t", s.rethinkHost, s.rethinkDb, s.reset)

	// Connect with rethink
	s.db, err = r.Connect(r.ConnectOpts{
		Address:  s.rethinkHost,
		Database: s.rethinkDb,
	})
	if err != nil {
		s.Log(nxsugar.ErrorLevel, "Could not connect to rethink: %s", err.Error())
		return
	}

	// Bootstrap DB
	if err = s.bootstrap(); err != nil {
		s.Log(nxsugar.ErrorLevel, "Could not bootstrap rethink db: %s", err.Error())
		return
	}

	// Add methods
	if err = s.AddMethodSchema("query", &nxsugar.Schema{FromFile: "schemas/querySchema.json"},
		func(task *nxsugar.Task) (interface{}, *nxsugar.JsonRpcErr) {
			// Get term param and check
			builtTerm, err := ei.N(task.Params).M("term").Raw()
			if err != nil {
				return nil, &nxsugar.JsonRpcErr{nxsugar.ErrInvalidParams, "Missing term", nil}
			}
			termSlice := ei.N(builtTerm).SliceZ()
			termErr, isChangeFeed := s.checkTermSlice(termSlice)
			if termErr != nil {
				return nil, termErr
			}

			// Get pipeId param and check
			pipeId := ei.N(task.Params).M("pipeId").StringZ()
			if isChangeFeed && pipeId == "" {
				return nil, &nxsugar.JsonRpcErr{nxsugar.ErrInvalidParams, "Missing pipeId for a changefeed term", nil}
			}

			// Get keepalive param
			keepalive := ei.N(task.Params).M("keepalive").IntZ()
			if keepalive == 0 {
				keepalive = 30
			}
			if keepalive < 1 {
				keepalive = 1
			}

			// Execute term
			rq, _ := json.Marshal(builtTerm)
			cur, err := r.RawQuery(rq).Run(s.db)
			if err != nil {
				return nil, &nxsugar.JsonRpcErr{ErrRunningQuery, fmt.Sprintf("Error running query: %s", err.Error()), err}
			}

			// Get results from cursor
			if pipeId == "" {
				if ret, err := cur.Interface(); err != nil {
					return nil, &nxsugar.JsonRpcErr{ErrOnCursor, fmt.Sprintf("Error on cursor: %s", err.Error()), err}
				} else {
					return ret, nil
				}
			} else {
				pipeTx, err := svc.GetConn().PipeOpen(pipeId)
				if err != nil {
					return nil, &nxsugar.JsonRpcErr{ErrOnPipe, fmt.Sprintf("Error opening pipe: %s", err.Error()), nil}
				}
				go func(cur *r.Cursor, pipeTx *nexus.Pipe) {
					defer cur.Close()
					ch := make(chan interface{})
					keepaliveTimer := time.After(time.Second * time.Duration(keepalive))
					cur.Listen(ch)
					for {
						select {
						case data, ok := <-ch:
							if !ok {
								pipeTx.Write(ei.M{"type": PipeMsgError, "error": fmt.Sprintf("Cursor closed: %s", cur.Err().Error())})
								return
							}
							_, err := pipeTx.Write(ei.M{"type": PipeMsgData, "data": data})
							if err != nil {
								return
							}
							keepaliveTimer = time.After(time.Second * time.Duration(keepalive))
						case <-keepaliveTimer:
							_, err := pipeTx.Write(ei.M{"type": PipeMsgKeepalive})
							if err != nil {
								return
							}
							keepaliveTimer = time.After(time.Second * time.Duration(keepalive))
						}
					}
				}(cur, pipeTx)
				return ei.M{"keepalive": keepalive}, nil
			}

		},
	); err != nil {
		return
	}

	// Serve
	s.Serve()
}

func (s *PersistService) bootstrap() error {
	// Create or reset database
	if s.reset {
		r.DBDrop(s.rethinkDb).Exec(s.db)
	}
	cur, err := r.DBList().Run(s.db)
	if err != nil {
		return err
	}
	dblist := make([]string, 0)
	err = cur.All(&dblist)
	cur.Close()
	if err != nil {
		return err
	}
	dbexists := false
	for _, x := range dblist {
		if x == s.rethinkDb {
			dbexists = true
			break
		}
	}
	if !dbexists {
		_, err := r.DBCreate(s.rethinkDb).RunWrite(s.db)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PersistService) checkTermSlice(t []interface{}) (*nxsugar.JsonRpcErr, bool) {
	isChangeFeed := false
	if len(t) >= 1 {
		ty := ei.N(t[0]).IntZ()
		if _, ok := p.Term_TermType_name[int32(ty)]; ok {
			if p.Term_TermType(ty) == p.Term_DB && len(t) >= 2 { // Check DB (only access self db)
				if args, err := ei.N(t[1]).Slice(); err == nil && len(args) >= 1 {
					if dbname, err := ei.N(args[0]).String(); err == nil && dbname != s.rethinkDb {
						return &nxsugar.JsonRpcErr{ErrInvalidDB, fmt.Sprintf("Invalid access to DB `%s`, allowed DB is `%s`", dbname, s.rethinkDb), nil}, isChangeFeed
					}
				}
			} else if p.Term_TermType(ty) == p.Term_CHANGES { // Indicate it's a changefeed request
				isChangeFeed = true
			}
		}
	}
	if len(t) > 1 {
		if args, err := ei.N(t[1]).Slice(); err == nil {
			for _, arg := range args {
				if argn, err := ei.N(arg).Slice(); err == nil {
					if rpcerr, isChFeed := s.checkTermSlice(argn); rpcerr != nil {
						if isChFeed {
							isChangeFeed = true
						}
						return rpcerr, isChangeFeed
					}
				}
			}
		}
	}
	return nil, isChangeFeed
}

func parseTermSlice(t []interface{}, deep int) {
	if len(t) < 1 || len(t) > 3 {
		fmt.Printf("Expecting slice of 1, 2 or 3 entries\n")
		return
	}
	ty := ei.N(t[0]).IntZ()
	if s, ok := p.Term_TermType_name[int32(ty)]; !ok {
		fmt.Printf("Unrecognized term type: %v\n", t[0])
		return
	} else {
		fmt.Printf("TYPE = (%d)%s | ", ty, s)
	}
	if len(t) == 3 {
		opts := ei.N(t[2]).RawZ()
		fmt.Printf("OPTS = %v\n", opts)
	} else {
		fmt.Printf("NO OPTS\n")
	}
	if len(t) > 1 {
		args, err := ei.N(t[1]).Slice()
		if err != nil {
			fmt.Printf("ARGS NOT SLICE: %v\n", t[1])
			return
		}
		for i, arg := range args {
			argn, err := ei.N(arg).Slice()
			if err != nil {
				fmt.Printf("%sARGS[%d] = %v\n", strings.Repeat("\t", deep), i, arg)
			} else {
				fmt.Printf("%sARGS[%d] ", strings.Repeat("\t", deep), i)
				parseTermSlice(argn, deep+1)
			}
		}
	}
}
