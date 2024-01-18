package queue

import (
    "errors"
    "time"
)

type Queue interface {
    Connect(host, port string) error
    Close()
    Publish(queue string, message []byte) error
    Subscribe(queue string, c chan []byte) error
    Unsubscribe(queue string) error
    RegisterRPC(proc string, cb RequestHandler) error
    Request(queue string, message []byte, timeout time.Duration) ([]byte, error)
    LastError() error
    SetName(name string)
}

type Request struct {
    Data  []byte
    Subj  string
    Id    string
    queue Queue
}

func NewRequest(q Queue) Request {
    return Request{
        queue: q,
    }
}

func (r *Request) Reply(reply []byte) error {
    return r.queue.Publish(r.Id, reply)
}

type RequestHandler func(request *Request) []byte

func New(kind string) (Queue, error) {
    switch kind {
    /*case "nsq":
        return newNsqQueue()*/
    case "nats":
        return newNatsQueue()
    }

    return nil, errors.New("Unsupported kind of queue")
}
