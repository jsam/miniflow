package miniflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/go-nats-streaming"
	"log"
	"time"
)

var (
	errTaskNotFound           = errors.New("Task is not found")
	errTaskNotImplemented     = errors.New("Task is not implemented")
	errNatsNoConnection       = errors.New("There is no NATS configuration")
	errWorkflowAlreadyRunning = errors.New("Workflow is already running")
)

// FlowError custom error holder.
type FlowError struct {
	Err  error
	Data interface{}
}

func (fe FlowError) Error() string {
	return fmt.Sprintf("%s \n %+#v", fe.Err, fe.Data)
}

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

// Work is definition for work implementation.
type Work func(*Token) *Token

// Produce is definition for producer implementation.
type Produce func() *Token

// Workflow is structure which will be used to execute a a workflow
type Workflow struct {
	Name        string
	Description string
	Tasks       map[string]Task
	Subs        []stan.Subscription

	EndTokens chan *Token

	producers []*Producer
	running   bool

	config *NATSStreamingConfig
}

func (w *Workflow) workCheck() error {
	for _, value := range w.Tasks {
		if value.Fn == nil {
			return FlowError{errTaskNotImplemented, value}
		}
	}
	return nil
}

// AttachProducer will register function which will produce tokens.
func (w *Workflow) AttachProducer(concurency int, produceFn Produce) error {
	if w.running {
		return errWorkflowAlreadyRunning
	}
	producer := NewProducer(concurency, produceFn)
	w.producers = append(w.producers, producer)
	return nil
}

// Publish is used to publish information between tasks. Can be used externally to start the flow.
func (w *Workflow) Publish(subject string, token *Token) error {
	if subject == "start" {
		subject = fmt.Sprintf("start:%s", w.Name)
	}
	tokenB, err := json.Marshal(token)
	if err != nil {
		return err
	}
	log.Printf("[nflow] Publishing to NATS. %s", subject)
	msg, err := w.config.NATSConn.PublishAsync(subject, tokenB, nil)
	if err != nil {
		log.Printf("[nflow] Publish failed %+v", msg)
		return err
	}
	log.Printf("[nflow] Publish successeed. Msg guid: %+v\n", msg)
	return nil
}

// Register will register function mapping between workflow schema and actual code you wish to execute.
func (w *Workflow) Register(name string, work Work) error {
	if w.running {
		return errWorkflowAlreadyRunning
	}
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
	cleanupDone := make(chan bool)
	go func() {
		for _ = range signalChan {
			for _, producer := range w.producers {
				producer.stop()
			}
			log.Printf("[nflow] Received an interrupt, unsubscribing and closing connection...")

			// Do not unsubscribe a durable on exit, except if asked to.
			if w.config != nil && w.config.Durable == "" {
				for _, sub := range w.Subs {
					sub.Unsubscribe()
				}
			}
			w.config.writeConfig()
			log.Printf("[nflow] Wrote to config .miniflow successfully.")
			cleanupDone <- true
		}
	}()
	<-cleanupDone
	w.running = false
}

// Close will explicitly close connection towards NATS messaging.
func (w *Workflow) Close() {
	log.Printf("[nflow] Closed connection to NATS.")
	w.config.NATSConn.Close()
}

// Run is method to start execution of the workflow.
func (w *Workflow) Run() error {
	if err := w.workCheck(); err != nil {
		return err
	}
	if w.running {
		return errWorkflowAlreadyRunning
	}
	if w.config == nil {
		return errNatsNoConnection
	}
	log.Printf("[nflow] ID: %s %s\n", w.Name, w.config.ClientID)

	if len(w.Tasks) > 0 {
		w.running = true
	}

	// Subject is queue of name <arc_ID>:<workflow_ID>
	// QueueGroup is task name, cause all same tasks are competing for work.
	for _, task := range w.Tasks {
		if len(task.FromArcs) == 0 {
			subj := fmt.Sprintf("%s:%s", "start", w.Name)
			log.Printf("[nflow] Subscribing to %s", subj)
			sub, err := w.config.NATSConn.QueueSubscribe(subj, task.ID, task.serveToken, stan.StartAtTime(time.Now()), stan.DurableName(w.config.Durable))
			if err != nil {
				w.config.NATSConn.Close()
				log.Fatal(err)
			}

			w.Subs = append(w.Subs, sub)
		}
		for _, fromArc := range task.FromArcs {
			subj := fmt.Sprintf("%s:%s", fromArc.ID, w.Name)
			log.Printf("[nflow] Subscribing to %s", subj)
			sub, err := w.config.NATSConn.QueueSubscribe(subj, task.ID, task.serveToken, stan.StartAtTime(time.Now()), stan.DurableName(w.config.Durable))
			if err != nil {
				w.config.NATSConn.Close()
				log.Fatal(err)
			}

			w.Subs = append(w.Subs, sub)
		}
	}

	for _, producer := range w.producers {
		producer.start(w)
	}
	return nil
}

// NewWorkflow is constructor which will construct workflow from given schema.
func NewWorkflow(wfs *WorkflowSchema, config *NATSStreamingConfig) (*Workflow, error) {
	wf := &Workflow{
		Name:        wfs.Name,
		Description: wfs.Description,
		Tasks:       make(map[string]Task),

		EndTokens: make(chan *Token, 1000000),

		config:  config,
		running: false,
	}

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

	return wf, nil
}
