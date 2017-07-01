package miniflow

import nats "github.com/nats-io/go-nats"

// Worker is method set which defines mandatory methods for Worker.
type Worker interface {
	Topic() string
	Execute(*nats.Msg)
}

// WorkerGroup is master handler/orchestrator of all workers.
type WorkerGroup struct {
	topic string //
	name  string // worker group name(queue name), each worker in this work group will compete for work
	conn  *nats.EncodedConn
}

// Register will subscriber worker and make him ready to receive work.
func (wg *WorkerGroup) Register(worker Worker) {
	wg.conn.QueueSubscribe(worker.Topic(), wg.name, worker.Execute)
}
