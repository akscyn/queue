package queue

import (
	"fmt"
	"time"

	nats "github.com/akscyn/go-nats"
)

type natsQueue struct {
	host string
	name string

	conn *nats.Conn
	stop chan int

	subscription map[string]*nats.Subscription
}

type extConn struct {
	*nats.Conn
}

/*type natsRequest struct {
    data string
    subj string
    id   string

    queue *natsQueue
}*/

func newNatsQueue() (*natsQueue, error) {
	q := &natsQueue{
		subscription: make(map[string]*nats.Subscription),
		stop:         make(chan int, 1),
	}

	return q, nil
}

// Name is an Option to set the verbose state.
func Verbose(state bool) nats.Option {
	return func(o *nats.Options) error {
		o.Verbose = state
		return nil
	}
}

func (q *natsQueue) SetName(name string) {
	q.name = name
}

func (q *natsQueue) Connect(host, port string) error {
	var err error

	addr := fmt.Sprintf("nats://%s:%s", host, port)
	opts := []nats.Option{
		nats.MaxReconnects(-1),
		nats.Name(q.name),
	}

	q.conn, err = nats.Connect(addr, opts...)
	q.host = host

	q.conn.SetDisconnectHandler(func(conn *nats.Conn) {
		if conn.Status() == nats.CLOSED {
			q.conn, err = nats.Connect(addr, nats.MaxReconnects(-1))
			if err != nil {
				println(err.Error())
			}
		}
	})

	return err
}

func (q *natsQueue) Close() {
	if q.conn != nil {
		q.conn.SetDisconnectHandler(nil)

		q.conn.Close()
		q.conn = nil

		q.stop <- 1
	}
}

func (q *natsQueue) LastError() error {
	return q.conn.LastError()
}

func (q *natsQueue) Publish(queue string, message []byte) error {
	err := q.conn.Publish(queue, message)
	return err
}

func (q *natsQueue) Subscribe(queue string, c chan []byte) error {
	ch := make(chan *nats.Msg, 64)

	sub, err := q.conn.ChanSubscribe(queue, ch)
	if err == nil {
		q.subscription[queue] = sub

		go func() {
		loop:
			for {
				select {
				case msg := <-ch:
					c <- msg.Data
				case <-q.stop:
					break loop
				}
			}
		}()
	}

	return err
}

func (q *natsQueue) Unsubscribe(queue string) error {
	if sub, ok := q.subscription[queue]; ok {
		return sub.Unsubscribe()
	}
	return nil
}

func (q *natsQueue) RegisterRPC(proc string, cb RequestHandler) error {
	sub, err := q.conn.Subscribe(proc, func(msg *nats.Msg) {
		request := Request{
			Subj:  msg.Subject,
			Data:  msg.Data,
			Id:    msg.Reply,
			queue: q,
		}
		if reply := cb(&request); len(reply) > 0 {
			request.Reply(reply)
		}
	})

	if err == nil {
		q.subscription[proc] = sub
	}

	return err
}

func (q *natsQueue) Request(queue string, message []byte, timeout time.Duration) ([]byte, error) {
	reply, err := q.conn.Request(queue, message, timeout)
	if err == nil {
		return reply.Data, nil
	}
	return nil, err
}

/*
func (q *natsQueue) trustCheck() bool {
	const DEFAULT_HOST = "0.0.0.0"

	serverId := q.conn.ConnectedServerId()
	if len(serverId) == 0 {
		return false
	}
	host := q.conn.ConnectedServerHost()
	if len(host) == 0 || host == DEFAULT_HOST || host != q.host {
		mac, _ := macAddress()
		if len(mac) == 17 {
			host = strings.Replace(mac, ":", "", -1)[6:]
		}
	}

	key := []byte(host)
	id, err := base62.StdEncoding.DecodeString(serverId + "++")
	if err != nil {
		return false
	}
	decId := fastEncryptDecrypt(id, key)

	serverInfo := strings.Split(string(decId), " ")
	if len(serverInfo) != 2 {
		return false
	}

	return serverInfo[1] == q.conn.ConnectedServerVersion()
}

func fastEncryptDecrypt(data, key []byte) []byte {
	result := data

	for i := 0; i < len(data); i++ {
		result[i] = result[i] ^ key[i%(len(key)/1)]
	}

	return result
}

func macAddress() (string, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		// Failed to get network hardware info
		return "", err
	}

	mac := ""
	for _, ifi := range ifs {
		if (ifi.Flags&net.FlagUp != 0) && len(ifi.HardwareAddr.String()) > 0 {
			mac = ifi.HardwareAddr.String()
			break
		}
	}
	return mac, nil
}
*/
