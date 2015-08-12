/*  This file will create a server that provides a
	simple In-Memory Key/Value store */

package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
)

/*  An Entry in the Cache storage, storing state
	of the value as well as the current LockID */
type Entry struct {
	sync.RWMutex
	cond *sync.Cond
	value string
	lockID string
}

/* A helper struct in order to marshal some JSON */
type Response struct {
    LockID  string				   `json:"lock_id,omitempty"`
    Value   string				   `json:"value,omitempty"`
}

/* A lockable cache since map has no concurrent safety */
type Cache struct {
	sync.RWMutex
	storage map[string]*Entry
}

type Server struct {
	cache	*Cache
	router   *mux.Router
	listener net.Listener
}

/* Create new server and instantiate the cache */
func NewServer() (server *Server, err error) {
	server = new(Server)
	router := mux.NewRouter()

	reserveRoute := router.HandleFunc("/reservations/{key}", server.handleReservation)
	reserveRoute.Methods("POST")

	updateRoute := router.HandleFunc("/values/{key}/{lock_id}", server.handleUpdate)
	updateRoute.Methods("POST")

	setRoute := router.HandleFunc("/values/{key}", server.handleSet)
	setRoute.Methods("PUT")

	router.NotFoundHandler = http.HandlerFunc(server.defaultCall)
	server.router = router

	server.cache = new(Cache)
	server.cache.storage = make(map[string]*Entry)
	return server, nil
}

/* Helper error function */
func (self *Server) sendError(res http.ResponseWriter, msg string, code int) {
	http.Error(res, "{\"error\": \"" + msg + "\"}", code)
}

/* Helper 404 function */
func (self *Server) defaultCall(res http.ResponseWriter, req *http.Request) {
	self.sendError(res, "This is not what you are looking for :/", http.StatusNotFound)
}

/* Handle lock acquisition and value read as defined in the documentation */
func (self *Server) handleReservation(res http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	key := vars["key"]

	// Protect multiple go funcs from reading inconsistent map state
	self.cache.RLock()
	entry, ok := self.cache.storage[key]
	self.cache.RUnlock()

	// This is ok here since we do not support deletion of keys,
	// if we were to support deletion of keys then the following
	// wait on the lockID to change would be invalid as the entry
	// may dissapear 

	if !ok {
		self.sendError(res, "Unable to claim lock on non-existent key", http.StatusNotFound)
		return
	}

	entry.Lock()
	for entry.lockID != "" {
		entry.cond.Wait()
	}
	entry.lockID = uuid()
	entry.Unlock()
	data, err := json.Marshal(Response{entry.lockID, entry.value})
	if err != nil {
		self.sendError(res, "Unable to marshal the response", http.StatusInternalServerError)
		return
	}
	res.Write(data)
}

/* Handle update and unlocking as defined in the documentation provided */
func (self *Server) handleUpdate(res http.ResponseWriter, req *http.Request) {

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		self.sendError(res, "Unable to read the body of the set request", http.StatusInternalServerError)
		self.cache.Unlock()
		return
	}

	release, ok := req.URL.Query()["release"]
	if !ok {
		self.sendError(res, "release is a required query parameter [ true || false ]", http.StatusBadRequest)
		return
	}
	vars := mux.Vars(req)
	key := vars["key"]
	lockID := vars["lock_id"]

	// Attain read only lock, since cache won't be mutated here
	self.cache.RLock()
	entry, ok := self.cache.storage[key]
	self.cache.RUnlock()

	if !ok {
		self.sendError(res, "This key hasn't been created", http.StatusNotFound)
		return
	}
	if entry.lockID != lockID {
		self.sendError(res, "Your lock id isn't consistent with the currently held lock", http.StatusUnauthorized)
		return
	}

	entry.value = string(body)

	if len(release) > 0 && release[0] == "true" {
		entry.cond.Broadcast()
		entry.lockID = ""
	}

	res.WriteHeader(http.StatusNoContent)
}

/* Handle set as defined in the documentation provided */
func (self *Server) handleSet(res http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	key := vars["key"]

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		self.sendError(res, "Unable to read the body of the set request", http.StatusInternalServerError)
		self.cache.Unlock()
		return
	}

	// Grab the WriteLock because we may mutate the dictionary
	self.cache.Lock()
	entry, ok := self.cache.storage[key]
	if !ok {
		entry = new(Entry)
		entry.lockID = uuid() // Prevent any other routine from locking this
		entry.cond = sync.NewCond(entry)
		self.cache.storage[key] = entry
	}
	self.cache.Unlock()

	entry.Lock()

	// If this entry already existed, we wait until we can lock it
	for ok && entry.lockID != "" {
		entry.cond.Wait()
	}

	entry.lockID = uuid()
	entry.value = string(body)

	entry.Unlock()

	data, err := json.Marshal(Response{entry.lockID, ""})
	if err != nil {
		self.sendError(res, "Unable to marshal the response", http.StatusInternalServerError)
		return
	}
	res.Write(data)
}

func (self *Server) Start() (err error) {
	listener, err := net.Listen("tcp", ":9999")
	if err != nil { return err }

	log.Println("Starting server at port 9999")
	self.listener = listener
	http.Handle("/", self.router)

	go http.Serve(self.listener, nil)

	forever := make(chan bool)
	<-forever

	return nil
}

func main() {
	server, err := NewServer()
	if err != nil {
		log.Println("Critical failure", err)
		return
	}
	err = server.Start()
	if err != nil {
		log.Println("Critical failure", err)
		return
	}
}

// UUID Functionality taken from google group
// https://groups.google.com/forum/#!topic/golang-nuts/Rn13T6BZpgE
var Random *os.File

func uuid() string {
	var err error
	if Random == nil {
		Random, err = os.Open("/dev/urandom")
		if err != nil {
			log.Fatal(err)
		}
	}

	b := make([]byte, 16)
	Random.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x",
	b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

