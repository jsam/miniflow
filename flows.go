package miniflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/Knetic/govaluate"
	"github.com/nats-io/go-nats-streaming"
	yaml "gopkg.in/yaml.v2"
)

var (
	errWrongSchemaDefinition = errors.New("Wrong schema definition. Aborting process. Check your vertex definitions")
	errCannotLoadSchema      = errors.New("Cannot load schema")
	errCannotMarshalSchema   = errors.New("Cannot marshal schema")
)

// Arc is graph element which represents a relation between two Tasks.
type Arc struct {
	ID        string `yaml:"id"`
	Condition string `yaml:"condition,omitempty"`

	FromID string `yaml:"from"`
	ToID   string `yaml:"to"`

	FromTask *Task `yaml:"-"`
	ToTask   *Task `yaml:"-"`
}

func (a *Arc) isUsable(params map[string]interface{}) (bool, error) {
	if len(a.Condition) == 0 {
		return true, nil
	}

	expression, err := govaluate.NewEvaluableExpression(a.Condition)
	if err != nil {
		return false, err
	}
	result, err := expression.Evaluate(params)
	if err != nil {
		return false, err
	}
	return result.(bool), nil
}

// Task is graph element which represents task which is getting executed. Each task can have n in channels and m out channels.
// Depending on the type of the task, we are going to start their receivers and senders.
type Task struct {
	ID   string `yaml:"id"`
	Type string `yaml:"type"`
	Fn   Work   `yaml:"-"`

	FromArcs []*Arc
	ToArcs   []*Arc

	workflow *Workflow `yaml:"-"`
}

func (t Task) serveToken(msg *stan.Msg) {
	log.Printf("[task] Received message.")

	recvToken := NewToken()
	err := json.Unmarshal(msg.Data, &recvToken)
	if err != nil {
		log.Println(err)
	}
	recvToken.TaskID = t.ID
	recvToken.TaskType = t.Type

	if t.workflow.Ctx != nil {
		recvToken.Ctx = t.workflow.Ctx
	}

	token := t.Fn(recvToken)

	if token == nil || len(t.ToArcs) == 0 {
		// NOTE: If you returned nil that means that you want to stop that token from progressing through flow.
		log.Printf("[task] Ending token.")
		t.workflow.EndTokens <- recvToken
		log.Printf("[task] token ended")
		return
	}

	for _, toArc := range t.ToArcs {
		log.Printf("[task] Searching for usable arcs.")
		if usable, err := toArc.isUsable(token.Data); usable && err == nil {
			subj := fmt.Sprintf("%s:%s", toArc.ID, t.workflow.Name)
			log.Printf("[task] Usable Arcs found! %s", subj)
			t.workflow.Publish(subj, token)
		}
	}

}

// WorkflowSchema is structure which will be used to parse YAML defined schemas.
type WorkflowSchema struct {
	Name        string
	Description string
	Flow        []Arc
	Tasks       []Task
}

// NewWorkflowSchema constructor will source yaml file schema from provided schemaPath and return Workflow spec.
func NewWorkflowSchema(schemaPath string) (*WorkflowSchema, error) {
	wfs := &WorkflowSchema{}

	filename, err := filepath.Abs(schemaPath)
	if err != nil {
		return nil, errCannotLoadSchema
	}
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, errCannotLoadSchema
	}

	err = yaml.Unmarshal(yamlFile, wfs)
	if err != nil {
		return nil, errCannotMarshalSchema
	}

	return wfs, nil
}

// Producer is responsible for producing tokens.
type Producer struct {
	*sync.RWMutex
	concurency int
	isRunning  bool
	producer   Produce
}

func (p *Producer) stop() {
	p.Lock()
	p.isRunning = false
	p.Unlock()
}

func (p *Producer) start(w *Workflow) {
	p.Lock()
	p.isRunning = true
	p.Unlock()

	for i := 0; i < p.concurency; i++ {
		go func() {
			log.Printf("[producer] Publisher %s", w.Name)
			for { // control loop
				token := p.producer()
				w.Publish("start", token)
				if !p.isRunning {
					break
				}
				time.Sleep(time.Nanosecond)
			}
		}()
	}
}

// NewProducer will create new producer object.
func NewProducer(concurency int, producer Produce) *Producer {
	return &Producer{
		&sync.RWMutex{},
		concurency,
		false,
		producer,
	}
}
