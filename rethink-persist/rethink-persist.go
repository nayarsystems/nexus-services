package main

import (
	"encoding/json"
	"fmt"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxsugar-go"
	r "gopkg.in/dancannon/gorethink.v2"
	p "gopkg.in/dancannon/gorethink.v2/ql2"
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
			// Get params
			builtTerm, err := ei.N(task.Params).M("term").Raw()
			if err != nil {
				return nil, nxsugar.NewJsonRpcErr(1, "Missing term", nil)
			}
			//runWrite := ei.N(task.Params).M("runWrite").BoolZ()

			termSlice := ei.N(builtTerm).SliceZ()
			parseTermSlice(termSlice)

			rq, _ := json.Marshal(builtTerm)
			cur, err := r.RawQuery(rq).Run(s.db)

			// Execute query
			//queryOpts := map[string]interface{}{}
			//queryOpts["db"], err = r.DB(s.rethinkDb).Build()
			//if err != nil {
			//	return nil, nxsugar.NewJsonRpcErr(2, "Error building opts", nil)
			//}
			// set more opts?
			//query := r.Query{
			//	Type:      p.Query_START,
			//	Term:      &r.Term{},
			//	Opts:      queryOpts,
			//	BuiltTerm: builtTerm,
			//}
			//cur, err := s.db.Query(query)
			if err != nil {
				return nil, nxsugar.NewJsonRpcErr(3, fmt.Sprintf("Error on query: %s", err.Error()), nil)
			}

			// Get results from cursor
			if ret, err := cur.Interface(); err != nil {
				return nil, nxsugar.NewJsonRpcErr(4, fmt.Sprintf("Error on cursor: %s", err.Error()), nil)
			} else {
				return ret, nil
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

func parseTermSlice(t []interface{}) {
	if len(t) < 2 || len(t) > 3 {
		fmt.Printf("Expecting slice of 2 or 3 entries\n")
		return
	}
	ty := ei.N(t[0]).IntZ()
	if s, ok := p.Term_TermType_name[int32(ty)]; !ok {
		fmt.Printf("Unrecognized term type: %v\n", t[0])
		return
	} else {
		fmt.Printf("Term type %s\n", s)
	}
	args, err := ei.N(t[1]).Slice()
	if err != nil {
		fmt.Printf("Arguments is not a slice: %v\n", t[1])
		return
	}
	for i, arg := range args {
		argn, err := ei.N(arg).Slice()
		if err != nil {
			fmt.Printf("Argument %d is %v\n", i, arg)
		} else {
			fmt.Printf("Argument %d is a term\n", i)
			parseTermSlice(argn)
		}
	}
	if len(t) == 3 {
		opts := ei.N(t[2]).RawZ()
		fmt.Printf("Options are %v\n", opts)
	}
}
