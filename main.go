package main

import (
	"container/list"
	"context"
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

type queue struct {
	cond *sync.Cond
	list *list.List //  память выделенная для массива, никогда не возвращается по этому связанный список
}

type queuesSync struct { // стандартная потокобезопасная мапа
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
	q.list.PushBack(valueQueue)
	q.cond.Signal() // сигнализируем что появились изменения
	q.cond.L.Unlock()

	w.WriteHeader(http.StatusOK)
}

func getFromQueue(w http.ResponseWriter, r *http.Request) {
	nameQueue := r.URL.Path[1:]
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

	if q.list.Len() != 0 { // если в очереди что то есть, то отправляем сразу
		value := q.list.Front().Value.(string)
		q.list.Remove(q.list.Front())
		q.cond.L.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
		return
	}

	if timeout == 0 {
		q.cond.L.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}

	ctx, _ := context.WithTimeout(r.Context(), timeout*time.Second)

	waitCh := make(chan struct{})
	go func() {
		q.cond.Wait() // ждем изменений в очереди и отправляем сигнал в канал, что бы в селекте сработал нужный блок
		if ctx.Err() != nil {
			// самый сложный кусок кода
			// переменные условия срабатывают по порядку как начинали ждать.
			// проверяем был ли закрыт контекс
			// и если был, значит зто уже не нужная горутина и мы сигнализируем следуюей горутине которая ждет
			q.cond.Signal()
			q.cond.L.Unlock()
			close(waitCh)
			return
		}

		waitCh <- struct{}{}
	}()

	select {
	case <-waitCh:
		value := q.list.Front().Value.(string)
		q.list.Remove(q.list.Front())
		q.cond.L.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(value))
		return
	case <-ctx.Done():
		w.WriteHeader(http.StatusNotFound)
		return
	}
}
