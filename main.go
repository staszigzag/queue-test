package main

import (
	"container/list"
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	queues = &queuesSync{m: make(map[string]*queue)}
)

type queuesSync struct {
	mx sync.RWMutex
	m  map[string]*queue
}

func (q *queuesSync) get(key string) (*queue, bool) {
	q.mx.RLock()
	defer q.mx.RUnlock()

	val, ok := q.m[key]
	return val, ok
}

func (q *queuesSync) put(key string, value *queue) {
	q.mx.Lock()
	defer q.mx.Unlock()

	q.m[key] = value
}

type queue struct {
	cond *sync.Cond
	list *list.List //  память выделенная для массива, никогда не возвращается по этому связанный список
}

func main() {
	port := flag.String("port", "8080", "port to listen")
	flag.Parse()

	http.HandleFunc("/", handleRequest)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		getFromQueue(w, r)

	case http.MethodPut:
		putInQueue(w, r)

	default:
		w.WriteHeader(http.StatusNotImplemented)

	}
}

func putInQueue(w http.ResponseWriter, r *http.Request) {
	nameQueue := r.URL.Path[1:] // можно добавить доп.проверку для не валидного url
	valueQueue := r.URL.Query().Get("v")

	if nameQueue == "" || valueQueue == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	q, ok := queues.get(nameQueue)
	if !ok {
		q = &queue{
			list: list.New(),
			cond: sync.NewCond(new(sync.Mutex)),
		}
		queues.put(nameQueue, q)
	}

	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.list.PushBack(valueQueue)
	q.cond.Signal()

	w.WriteHeader(http.StatusOK)
}

func getFromQueue(w http.ResponseWriter, r *http.Request) {
	nameQueue := r.URL.Path[1:] // доп проверка для не валидного url
	if nameQueue == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	timeout := time.Duration(0)
	timeoutRaw := strings.TrimSpace(r.URL.Query().Get("timeout"))
	if timeoutRaw != "" {
		v, err := strconv.Atoi(timeoutRaw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		timeout = time.Duration(v)
	}

	q, ok := queues.get(nameQueue)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	q.cond.L.Lock()

	if q.list.Len() != 0 {
		value := q.list.Front().Value.(string)
		q.list.Remove(q.list.Front())
		q.cond.L.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
		return
	}

	wait := make(chan struct{}, 1)
	go func() {
		q.cond.Wait()
		wait <- struct{}{}
	}()

	select {
	case <-wait:
		value := q.list.Front().Value.(string)
		q.list.Remove(q.list.Front())
		q.cond.L.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
		return
	case <-time.After(timeout * time.Second):
		w.WriteHeader(http.StatusNotFound)
		return
	}
}
