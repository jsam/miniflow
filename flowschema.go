package miniflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"

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
	recvToken := NewToken()
	err := json.Unmarshal(msg.Data, &recvToken)
	if err != nil {
		log.Println(err)
	}
	recvToken.TaskID = t.ID
	recvToken.TaskType = t.Type

	token := t.Fn(recvToken)
	if token == nil {
		token = NewToken()
	}

	for _, toArc := range t.ToArcs {
		subj := fmt.Sprintf("%s:%s", toArc.ID, t.workflow.ID)
		t.workflow.Publish(subj, token)
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
