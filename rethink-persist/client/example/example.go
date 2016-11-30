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
	//tableDelete := r.TableDrop("test")

	// Create test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(tableCreate)}, timeout); err != nil {
		log.Printf("Error table create: %s", err.Error())
	} else {
		wres := nxr.WriteResponse(res)
		log.Printf("Table create: %#v", wres)
	}

	// Changes (a pipe is needed to read changes)
	pipeRx, err := nxconn.PipeCreate()
	if err != nil {
		log.Fatalf("Error changes pipe create: %s", err.Error())
	}
	keepalive := 5
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(changes), "pipeId": pipeRx.Id(), "keepalive": keepalive}, timeout); err != nil {
		log.Fatalf("Error changes: %s", err.Error())
	} else {
		log.Printf("Changes feed response: %v", res)
		sr := nxr.StreamResponse(pipeRx, keepalive)
		wg.Add(1)
		go func() {
			for {
				msg, err := sr.Next()
				if err != nil {
					log.Printf(err.Error())
					return
				}
				log.Printf("Changes feed stream recv: %v", msg)
			}
		}()
		go func() {
			time.Sleep(time.Second * 12)
			sr.Close()
		}()
	}

	// Read test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(read)}, timeout); err != nil {
		log.Fatalf("Error read: %s", err.Error())
	} else {
		log.Printf("Read: %v\n", res)
	}

	// Insert some objects
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(insert)}, timeout); err != nil {
		log.Fatalf("Error insert: %s", err.Error())
	} else {
		wres := nxr.WriteResponse(res)
		log.Printf("Insert WriteResponse: %#v\n", wres)
	}

	// Read test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(read)}, timeout); err != nil {
		log.Fatalf("Error read: %s", err.Error())
	} else {
		log.Printf("Read: %v\n", res)
	}

	// Update
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(update)}, timeout); err != nil {
		log.Fatalf("Error update: %s", err.Error())
	} else {
		wres := nxr.WriteResponse(res)
		log.Printf("Update WriteResponse: %#v\n", wres)
	}

	// Read test table
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(read)}, timeout); err != nil {
		log.Fatalf("Error read: %s", err.Error())
	} else {
		log.Printf("Read: %v\n", res)
	}

	// Delete
	if res, err := nxconn.TaskPush(method, ei.M{"term": nxr.Term(del)}, timeout); err != nil {
		log.Fatal("Error delete: %s", err.Error())
	} else {
		wres := nxr.WriteResponse(res)
		log.Printf("Delete WriteResponse: %#v\n", wres)
	}

	wg.Wait()
}
