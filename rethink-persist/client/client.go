package main

import (
	"log"
	"time"

	"github.com/jaracil/ei"
	"github.com/nayarsystems/nxgo"
	r "gopkg.in/dancannon/gorethink.v2"
)

type RthTerm struct {
	term      r.Term
	builtTerm interface{}
}

func main() {
	// Connect to nexus
	nxconn, err := nxgo.Dial("tcp://localhost:11717", nil)
	if err != nil {
		log.Fatalf("Error on dial: %s\n", err.Error())
	}
	_, err = nxconn.Login("root", "root")
	if err != nil {
		log.Fatalf("Error on login: %s\n", err.Error())
	}

	// Create rethinkDB terms
	terms := map[string]*RthTerm{}
	terms["tableCreate"], err = NewRthTerm(r.TableCreate("test"))
	if err != nil {
		log.Fatalf("Error building tableCreate term: %s\n", err.Error())
	}
	terms["read"], err = NewRthTerm(r.Table("test"))
	if err != nil {
		log.Fatalf("Error building read term: %s\n", err.Error())
	}
	terms["insert"], err = NewRthTerm(r.Table("test").Insert(ei.M{"hello": "world"}))
	if err != nil {
		log.Fatalf("Error building insert term: %s\n", err.Error())
	}
	terms["update"], err = NewRthTerm(r.Table("test").Filter(r.Row.Field("hello").Eq("world")).Update(ei.M{"hello": "nayar"}))
	if err != nil {
		log.Fatalf("Error building update term: %s\n", err.Error())
	}
	terms["delete"], err = NewRthTerm(r.Table("test").Filter(r.Row.Field("hello").Eq("nayar")).Delete())
	if err != nil {
		log.Fatalf("Error building delete term: %s\n", err.Error())
	}
	terms["tableDelete"], err = NewRthTerm(r.TableDrop("test"))
	if err != nil {
		log.Fatalf("Error building tableDelete term: %s\n", err.Error())
	}

	// Execute some terms
	method := "test.rethink-persist.query"
	timeout := time.Second * 5
	for _, tn := range []string{"tableCreate", "read", "insert", "read", "update", "read", "delete", "read", "tableDelete"} {
		term := terms[tn]
		log.Printf("Executing term %s: %s => %#v\n", tn, term.term.String(), term.builtTerm)

		res, err := nxconn.TaskPush(method, ei.M{"term": term.builtTerm}, timeout)
		if err != nil {
			log.Printf("Error executing term %s: %s\n", tn, err.Error())
			return
		} else {
			log.Printf("Executed term %s: %#v\n", tn, res)
		}
	}

}

func NewRthTerm(r r.Term) (*RthTerm, error) {
	var err error
	rthTerm := &RthTerm{r, nil}
	if rthTerm.builtTerm, err = rthTerm.term.Build(); err != nil {
		return nil, err
	}
	return rthTerm, nil
}
