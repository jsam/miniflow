package miniflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
)

var (
	errTaskNotFound       = errors.New("Task is not found")
	errTaskNotImplemented = errors.New("Task is not implemented")
	errNatsNoConnection   = errors.New("There is no NATS configuration")
)

// Token is data traveling from one task to task.
type Token struct {
	TaskID    string
	TaskType  string
	ContextID string
	Data      map[string]interface{}
}

// NewToken is constructor for new empty token.
func NewToken() *Token {
	return &Token{
		TaskID:   "",
		TaskType: "",
		Data:     make(map[string]interface{}),
	}
}

// Work is definition for work implementation
type Work func(*Token) *Token

// Workflow is structure which will be used to execute a a workflow
type Workflow struct {
	ID          string
	Name        string
	Description string
	Tasks       map[string]Task
	Subs        []stan.Subscription
	config      *NATSStreamingConfig
}

func (w *Workflow) workCheck() error {
	for _, value := range w.Tasks {
		if value.Fn == nil {
			return errTaskNotImplemented
		}
	}
	return nil
}

// Publish is used to publish information between tasks. Can be used externally to start the flow.
func (w *Workflow) Publish(subject string, token *Token) error {
	if subject == "start" {
		subject = fmt.Sprintf("start:%s", w.ID)
	}
	tokenB, err := json.Marshal(token)
	if err != nil {
		return err
	}
	return w.config.NATSConn.Publish(subject, tokenB)
}

// Register will register function mapping between workflow schema and actual code you wish to execute.
func (w *Workflow) Register(name string, work Work) error {
	task, ok := w.Tasks[name]
	if !ok {
		return errTaskNotFound
	}
	task.Fn = work
	w.Tasks[name] = task
	return nil
}

// ArcTasks is helper method for retrieving from and to tasks of a given arc.
func (w *Workflow) ArcTasks(arc *Arc) (from Task, to Task) {
	from = w.Tasks[arc.FromID]
	to = w.Tasks[arc.ToID]
	return
}

// Teardown will gracefully shutdown workflow.
func (w *Workflow) Teardown(signalChan <-chan time.Time) {
	// signalChan := make(chan time.Time, 1)
	cleanupDone := make(chan bool)
	go func() {
		for _ = range signalChan {
			log.Printf("Received an interrupt, unsubscribing and closing connection...")
			// Do not unsubscribe a durable on exit, except if asked to.
			// for _, sub := range w.Subs {
			// 	sub.Unsubscribe()
			// }
			w.config.NATSConn.Close()
			log.Printf("Closed connection to NATS.")
			w.config.writeConfig()
			log.Printf("Wrote to config .miniflow successfully.")
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}

// Run is method to start execution of the workflow.
func (w *Workflow) Run() error {
	if err := w.workCheck(); err != nil {
		return err
	}

	if w.config == nil {
		return errNatsNoConnection
	}

	startOpt := stan.StartAt(pb.StartPosition_NewOnly)

	if w.config.StartSeq != 0 {
		startOpt = stan.StartAtSequence(w.config.StartSeq)
	} else if w.config.DeliverLast == true {
		startOpt = stan.StartWithLastReceived()
	} else if w.config.DeliverAll == true {
		log.Print("subscribing with DeliverAllAvailable")
		startOpt = stan.DeliverAllAvailable()
	} else if w.config.StartDelta != "" {
		ago, err := time.ParseDuration(w.config.StartDelta)
		if err != nil {
			w.config.NATSConn.Close()
			log.Fatal(err)
		}
		startOpt = stan.StartAtTimeDelta(ago)
	}

	// Subject is queue of name <arc_ID>:<workflow_ID>
	// QueueGroup is task name, cause all same tasks are competing for work.
	for _, task := range w.Tasks {
		// TODO: subscribe to topic name <relation_name>:<workflow_id>
		if len(task.FromArcs) == 0 {
			subj := fmt.Sprintf("%s:%s", "start", w.ID)
			sub, err := w.config.NATSConn.QueueSubscribe(subj, task.ID, task.serveToken, startOpt, stan.DurableName(w.config.Durable))
			w.Subs = append(w.Subs, sub)
			if err != nil {
				w.config.NATSConn.Close()
				log.Fatal(err)
			}
		}
		for _, fromArc := range task.FromArcs {
			subj := fmt.Sprintf("%s:%s", fromArc.ID, w.ID)
			_, err := w.config.NATSConn.QueueSubscribe(subj, task.ID, task.serveToken, startOpt, stan.DurableName(w.config.Durable))
			if err != nil {
				w.config.NATSConn.Close()
				log.Fatal(err)
			}
		}
	}
	return nil
}

// NewWorkflow is constructor which will construct workflow from given schema.
func NewWorkflow(wfs *WorkflowSchema, config *NATSStreamingConfig) (*Workflow, error) {
	wf := &Workflow{
		Name:        wfs.Name,
		Description: wfs.Description,
		Tasks:       make(map[string]Task),

		config: config,
	}
	wf.ID = fmt.Sprintf("%p", wf)

	for _, task := range wfs.Tasks {
		task.workflow = wf
		wf.Tasks[task.ID] = task
	}

	for _, arc := range wfs.Flow {
		refArc := arc
		if fromTask, ok := wf.Tasks[arc.FromID]; ok {
			fromTask.ToArcs = append(fromTask.ToArcs, &refArc)
			wf.Tasks[arc.FromID] = fromTask
		} else {
			return nil, errWrongSchemaDefinition
		}

		if toTask, ok := wf.Tasks[arc.ToID]; ok {
			toTask.FromArcs = append(toTask.FromArcs, &refArc)
			wf.Tasks[arc.ToID] = toTask
		} else {
			return nil, errWrongSchemaDefinition
		}
	}
	// TODO: bootstrap task arcs and you are done
	return wf, nil
}
