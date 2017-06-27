package miniflow

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"

	nats "github.com/nats-io/go-nats"

	yaml "gopkg.in/yaml.v2"
)

const (
	// Collector is constant representing descriptor of collector.
	Collector = "collector"
	// Processor is constant representing descriptor of processor.
	Processor = "processor"
	// Exporter is constant representing descriptor of exporter.
	Exporter = "exporter"
)

var (
	errWrongSchemaDefinition = errors.New("Wrong schema definition. Aborting process. Check your vertex definitions")
	errCannotLoadSchema      = errors.New("Cannot load schema")
	errCannotMarshalSchema   = errors.New("Cannot marshal schema")
	errTaskNotInSchema       = errors.New("Specified task name cannot be find in schema")
	errNoStartTasks          = errors.New("There is no start tasks defined")
	errAlreadyRunning        = errors.New("Workflow has already started")
)

// Condition is template which holds information about conditional routing.
type Condition struct {
	rhs      interface{}
	operator int
	lhs      interface{}
}

func (c *Condition) isAllowed() bool {
	return true
}

// Channel is a communication mechanism of Arc in the graph.
type Channel struct {
	ch    chan *Token
	route *Condition
}

func (c *Channel) send(t *Token) {
	c.ch <- t
}

func (c *Channel) recv() *Token {
	return <-c.ch
}

// NewChannel is constructor for Channel object.
func NewChannel() *Channel {
	return &Channel{
		ch:    make(chan *Token),
		route: &Condition{},
	}
}

// Token represent items which flows through the workflow.
type Token struct {
	Ctx  context.Context        // workflow meta information
	Data map[string]interface{} // data processed from the last step.
}

// NewToken is constructor for new empty token.
func NewToken() *Token {
	return &Token{
		context.Background(),
		make(map[string]interface{}),
	}
}

// Arc is graph element which represents a relation between two Tasks.
type Arc struct {
	ID        string `yaml:"id"`
	Condition string `yaml:"condition,omitempty"`
	//Weight    int64  `yaml:"weight,omitempty"`
	FromID string `yaml:"from"`
	ToID   string `yaml:"to"`

	FromTask *Task
	ToTask   *Task
}

// Task is graph element which represents task which is getting executed. Each task can have n in channels and m out channels. Depending on the type of the task,
// we are going to start their receivers and senders.
// Type:
//   * Collector: has zero in channels so there are no receivers.
//   * Processor: has 1 or more ins channel and 1 or more outs channels.
//   * Exporter: has 1 or more in channels.
type Task struct {
	ID       string      `yaml:"id"`
	Type     string      `yaml:"type"`
	ExecFunc interface{} `yaml:"-"`

	// Local Flow
	ins  []*Channel // Fan-in
	fin  <-chan *Token
	outs []*Channel // Conditional Fan-out
}

func (task *Task) log(msg string, data ...interface{}) {
	if data == nil {
		log.Printf("[Task<%s>::%s]\t%s\n", task.Type, task.ID, msg)
	} else {
		log.Printf("[Task<%s>::%s]\t%s: %+v\n", task.Type, task.ID, msg, data)
	}
}

func (task *Task) fanIn() <-chan *Token {
	var wg sync.WaitGroup

	out := make(chan *Token)

	output := func(c <-chan *Token) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(task.ins))
	for _, c := range task.ins {
		go output(c.ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (task *Task) fanOut(token *Token) {
	if task.Type == Exporter || token == nil {
		return
	}
	for _, channel := range task.outs {
		channel.ch <- token
	}
}

func (task *Task) execCollector() *Token {
	task.log("Executing collector")
	collector := task.ExecFunc.(CollectorFunc)
	return collector()
}

func (task *Task) execProcessor(token *Token) *Token {
	task.log("Executing processor")
	processor := task.ExecFunc.(ProcessorFunc)
	return processor(token)
}

func (task *Task) execExporter(token *Token) {
	task.log("Executing exporter")
	exporter := task.ExecFunc.(ExporterFunc)
	exporter(token)
}

func (task *Task) serveToken(w *Workflow) {
	if task.Type == Collector {
		token := task.execCollector()
		atomic.AddInt64(&w.tokensFlowing, 1)
		task.fanOut(token)
	}

	for {
		token := <-task.fin
		switch task.Type {
		case Processor:
			task.fanOut(task.execProcessor(token))
			continue

		case Exporter:
			task.execExporter(token)
			atomic.AddInt64(&w.tokensFlowing, -1)
			atomic.AddInt64(&w.tokensFinished, 1)
			continue
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
	// schema := make(map[string]interface{})
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

// CollectorFunc is function signature for entry point of data flow. Think E in ETL.
type CollectorFunc func() *Token

// ProcessorFunc is function signature for processing data. Think T in ETL.
type ProcessorFunc func(in *Token) *Token

// ExporterFunc is function signature for exporting data to third party service. Think L in ETL.
type ExporterFunc func(in *Token)

// FuncMap is storage for user defined functions which maps to defined tasks from schema.
type FuncMap struct {
	collectors map[string]CollectorFunc
	processors map[string]ProcessorFunc
	exporters  map[string]ExporterFunc
}

// AddCollector is used for adding a collectors mapping between data collecting task and execution function.
func (fm *FuncMap) AddCollector(taskID string, fn CollectorFunc) {
	fm.collectors[taskID] = fn
}

// AddProcessor is used for adding a processor mapping between data processing task and execution function.
func (fm *FuncMap) AddProcessor(taskID string, fn ProcessorFunc) {
	fm.processors[taskID] = fn
}

// AddExporter is used for adding a exporter mapping between exporting task and execution function.
func (fm *FuncMap) AddExporter(taskID string, fn ExporterFunc) {
	fm.exporters[taskID] = fn
}

// NewFuncMap is constructor for FuncMap.
func NewFuncMap() *FuncMap {
	return &FuncMap{
		make(map[string]CollectorFunc),
		make(map[string]ProcessorFunc),
		make(map[string]ExporterFunc),
	}
}

// Workflow is structure which will be used to execute a a workflow
type Workflow struct {
	Tasks  map[string]Task
	Flows  map[string]Arc
	config *Config

	tokensFlowing  int64
	tokensFinished int64
}

// Register will register function mapping between workflow schema and actual code you wish to execute.
func (w *Workflow) Register(fnMap *FuncMap) error {
	for taskName, taskFn := range fnMap.collectors {
		if task, ok := w.Tasks[taskName]; ok {
			task.ExecFunc = taskFn
			w.Tasks[taskName] = task
		} else {
			return errTaskNotInSchema
		}

		for taskName, taskFn := range fnMap.processors {
			if task, ok := w.Tasks[taskName]; ok {
				task.ExecFunc = taskFn
				w.Tasks[taskName] = task
			} else {
				return errTaskNotInSchema
			}
		}

		for taskName, taskFn := range fnMap.exporters {
			if task, ok := w.Tasks[taskName]; ok {
				task.ExecFunc = taskFn
				w.Tasks[taskName] = task
			} else {
				return errTaskNotInSchema
			}
		}
	}
	return nil
}

// Run will start processing token through workflow.
func (w *Workflow) Run(done chan bool) error {
	// TODO: check that all tasks have attached functions

	if w.tokensFlowing > 0 || w.tokensFinished > 0 {
		return errAlreadyRunning
	}

	for _, task := range w.Tasks {
		if len(task.ID) > 0 {
			tt := task
			go tt.serveToken(w)
		}
	}

	select {
	case <-done:
		if w.tokensFlowing != 0 {
			// wait for drain out
		}
		return nil
	}
}

func (w *Workflow) bootstrapLocal(wfSchema *WorkflowSchema) error {
	for _, arc := range wfSchema.Flow {
		arcCh := NewChannel()
		if fromTask, ok := w.Tasks[arc.FromID]; ok {
			fromTask.outs = append(fromTask.outs, arcCh) // attach out channel to task
			w.Tasks[arc.FromID] = fromTask
		} else {
			return errWrongSchemaDefinition
		}
		if toTask, ok := w.Tasks[arc.ToID]; ok {
			toTask.ins = append(toTask.ins, arcCh) // attach in channel to task
			toTask.fin = toTask.fanIn()
			w.Tasks[arc.ToID] = toTask
		} else {
			return errWrongSchemaDefinition
		}

		w.Flows[arc.ID] = arc
	}
	return nil
}

func (w *Workflow) bootstrapNATS(wfSchema *WorkflowSchema, connector *nats.EncodedConn) error {
	for _, arc := range wfSchema.Flow {
		topic := arc.ID

		sendCh := NewChannel()
		connector.BindSendChan(topic, sendCh.ch)

		recvCh := NewChannel()
		connector.BindRecvChan(topic, recvCh.ch)

		if fromTask, ok := w.Tasks[arc.FromID]; ok {
			fromTask.outs = append(fromTask.outs, sendCh)
			w.Tasks[arc.FromID] = fromTask
		} else {
			return errWrongSchemaDefinition
		}

		if toTask, ok := w.Tasks[arc.ToID]; ok {
			toTask.ins = append(toTask.ins, recvCh)
			toTask.fin = toTask.fanIn()
			w.Tasks[arc.ToID] = toTask
		} else {
			return errWrongSchemaDefinition
		}
	}
	return nil
}

// NewWorkflow is constructor for new workflow by certain schema.
func NewWorkflow(wfSchema *WorkflowSchema, connector *nats.EncodedConn) (*Workflow, error) {
	wf := &Workflow{
		Tasks: make(map[string]Task),
		Flows: make(map[string]Arc),

		tokensFlowing:  0,
		tokensFinished: 0,
	}

	for _, task := range wfSchema.Tasks {
		wf.Tasks[task.ID] = task
	}

	if connector == nil {
		log.Println("Connector is nil. Using local flows.")
		err := wf.bootstrapLocal(wfSchema)
		if err != nil {
			return nil, err
		}
	} else {
		err := wf.bootstrapNATS(wfSchema, connector)
		if err != nil {
			return nil, err
		}
	}
	return wf, nil
}
