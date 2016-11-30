package main

import (
	"log"
	"time"

	"sync"

	"github.com/jaracil/ei"
	nxr "github.com/nayarsystems/nexus-services/rethink-persist/client"
	"github.com/nayarsystems/nxgo"
	r "gopkg.in/dancannon/gorethink.v2"
)

var wg sync.WaitGroup = sync.WaitGroup{}

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

	// Nexus push params
	method := "test.rethink-persist.query"
	timeout := time.Second * 5

	// Some terms to execute
	tableCreate := r.TableCreate("test")
	read := r.Table("test")
	insert := r.Table("test").Insert(ei.S{ei.M{"hello": "world"}, ei.M{"hello": "earth"}, ei.M{"hello": "planet"}})
	changes := r.Table("test").Changes()
	update := r.Table("test").Filter(r.Row.Field("hello").Eq("world")).Update(ei.M{"hello": "nayar"})
	del := r.Table("test").Delete()

	// Create test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(tableCreate)}, timeout); err != nil {
		log.Printf("Error table create: %s\n", err.Error())
	} else {
		log.Printf("Table create: %#v", nxr.WriteResponse(res))
	}

	// Changes to test table: A stream with a keepalive of 10 seconds
	st, err := nxr.NewStreamedTerm(nxconn, changes, &nxr.StreamedTermOpts{Keepalive: 10})
	if err != nil {
		log.Fatalf("Error changes: %s\n", err.Error())
	}
	if _, err := nxconn.TaskPush(method, st.Params(), timeout); err != nil {
		log.Fatalf("Error changes: %s\n", err.Error())
	} else {
		// Read from the stream in a goroutine (wait for it before exit)
		wg.Add(1)
		go func() {
			for {
				msg, err := st.Next()
				if err != nil {
					log.Printf("Changes feed stream error: %s\n", err.Error())
					wg.Done()
					return
				}
				log.Printf("Changes feed stream recv: %v\n", msg)
			}
		}()
		// Close the stream after 12 seconds
		go func() {
			time.Sleep(time.Second * 12)
			st.Close()
		}()
	}

	// Read test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(read)}, timeout); err != nil {
		log.Fatalf("Error read: %s\n", err.Error())
	} else {
		log.Printf("Read: %v\n", res)
	}

	// Insert some objects
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(insert)}, timeout); err != nil {
		log.Fatalf("Error insert: %s\n", err.Error())
	} else {
		log.Printf("Insert WriteResponse: %#v\n", nxr.WriteResponse(res))
	}

	// Read test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(read)}, timeout); err != nil {
		log.Fatalf("Error read: %s\n", err.Error())
	} else {
		log.Printf("Read: %v\n", res)
	}

	// Update
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(update)}, timeout); err != nil {
		log.Fatalf("Error update: %s\n", err.Error())
	} else {
		log.Printf("Update WriteResponse: %#v\n", nxr.WriteResponse(res))
	}

	// Read test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(read)}, timeout); err != nil {
		log.Fatalf("Error read: %s\n", err.Error())
	} else {
		log.Printf("Read: %v\n", res)
	}

	// Delete
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(del)}, timeout); err != nil {
		log.Fatal("Error delete: %s\n", err.Error())
	} else {
		wres := nxr.WriteResponse(res)
		log.Printf("Delete WriteResponse: %#v\n", wres)
	}

	// Table Delete: if this was executed, the stream would be automatically closed by the other peer
	//if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(r.TableDrop("test"))}, timeout); err != nil {
	//	log.Printf("Error tableDelete: %s\n", err.Error())
	//} else {
	//	log.Printf("TableDelete WriteResponse: %#v\n", nxr.WriteResponse(res))
	//}

	// Wait for the stream to be closed
	wg.Wait()
}
