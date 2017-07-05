package miniflow

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var config *NATSStreamingConfig
var workflowSchema *WorkflowSchema
var workflow *Workflow
var workflowConditional *Workflow

func init() {
	config = NewNATSStreamingConfig("miniflow", "")
	wfs, errSchema := NewWorkflowSchema("./schemas/example_workflow.yaml")
	wf, errWf := NewWorkflow(wfs, config)
	if errSchema != nil || errWf != nil {
		panic("Problem bootstraping test. Make sure NATS is avaialable at nats://localhost:4222")
	}
	workflowSchema = wfs
	workflow = wf

	workflowSchemaCond, errSchemaCond := NewWorkflowSchema("./schemas/example_conditional_workflow.yaml")
	wfc, errWfc := NewWorkflow(workflowSchemaCond, config)
	if errSchemaCond != nil || errWfc != nil {
		panic("Problem bootstraping test. Make sure NATS is avaialable at nats://localhost:4222")
	}
	workflowConditional = wfc
}

func TestMain(m *testing.M) {
	statusCode := m.Run()
	workflow.Teardown(time.After(1 * time.Second))
	workflowConditional.Teardown(time.After(1 * time.Second))
	os.Exit(statusCode)
}

func Test_NewWorkflow_FromSchema(t *testing.T) {
	assert(t, nil, config, false)
	assert(t, nil, workflowSchema, false)
	assert(t, nil, workflow, false)
	assert(t, nil, workflowConditional, false)

}

func Test_Workflow_Relations(t *testing.T) {
	fetchWordlistTask := workflow.Tasks["fetch_wordlist"]

	assert(t, "fetch_wordlist", fetchWordlistTask.ID, true)
	assert(t, "collector", fetchWordlistTask.Type, true)
	assert(t, 0, len(fetchWordlistTask.FromArcs), true) // Starting task should have zero From arcs
	assert(t, 0, len(fetchWordlistTask.ToArcs), false)
	fetchMutateArc := fetchWordlistTask.ToArcs[0]
	assert(t, "fetch_mutate", fetchMutateArc.ID, true)

	from, mutateWordlistTask := workflow.ArcTasks(fetchMutateArc)
	assert(t, "fetch_wordlist", from.ID, true)
	assert(t, fetchWordlistTask, from, true)

	assert(t, "mutate_wordlist", mutateWordlistTask.ID, true)
	assert(t, "processor", mutateWordlistTask.Type, true)

	mutatePersistArc := mutateWordlistTask.ToArcs[0]
	assert(t, "mutate_persist", mutatePersistArc.ID, true)

	from, fsPersistTask := workflow.ArcTasks(mutatePersistArc)
	assert(t, "mutate_wordlist", from.ID, true)
	assert(t, mutateWordlistTask, from, true)
	assert(t, "fs_persist", fsPersistTask.ID, true)

	assert(t, nil, fetchWordlistTask.workflow, false)
	assert(t, nil, mutateWordlistTask.workflow, false)
	assert(t, nil, fsPersistTask.workflow, false)
}

func Test_Workflow_NoImplementation(t *testing.T) {
	assert(t, errTaskNotImplemented, workflow.Run(), true)
}

func Test_Workflow_PartialImplementation(t *testing.T) {
	work := func(token *Token) *Token {
		log.Println("basic work")
		return nil
	}
	workflow.Register("fetch_wordlist", work)

	assert(t, errTaskNotImplemented, workflow.Run(), true)
}

func Test_Workflow_NoConfigImplementation(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	noConfWorkflow, err := NewWorkflow(workflowSchema, nil)
	assert(t, nil, err, true)

	work := func(token *Token) *Token {
		log.Println("basic work")
		return nil
	}
	noConfWorkflow.Register("fetch_wordlist", work)
	noConfWorkflow.Register("mutate_wordlist", work)
	noConfWorkflow.Register("fs_persist", work)

	assert(t, errNatsNoConnection, noConfWorkflow.Run(), true)
}

func Test_Workflow_FullImplementation(t *testing.T) {
	assert(t, nil, config, false)
	assert(t, nil, config.NATSConn, false)

	work := func(token *Token) *Token {
		log.Printf("my name is %s", token.TaskID)
		log.Printf("my type is %s", token.TaskType)
		log.Printf("received token: %+#v", token.ContextID)
		return token
	}
	workflow.Register("fetch_wordlist", work)
	workflow.Register("mutate_wordlist", work)
	workflow.Register("fs_persist", work)

	assert(t, nil, workflow.Run(), true)

	token := NewToken()
	token.ContextID = "my token"
	workflow.Publish("start", token)

	token.ContextID = "my second token"
	workflow.Publish("start", token)
}

func Test_Workflow_FirstTerminationImplementation(t *testing.T) {
	work := func(token *Token) *Token {
		log.Printf("my name is %s", token.TaskID)
		log.Printf("my type is %s", token.TaskType)
		log.Printf("received token: %+#v", token.ContextID)
		return nil
	}
	workflow.Register("fetch_wordlist", work)
	workflow.Register("mutate_wordlist", work)
	workflow.Register("fs_persist", work)

	assert(t, nil, workflow.Run(), true)

	token := NewToken()
	token.ContextID = "my token"
	workflow.Publish("start", token)

	token.ContextID = "my second token"
	workflow.Publish("start", token)

}

func Test_Workflow_SecondTerminationImplementation(t *testing.T) {
	workCollector := func(token *Token) *Token {
		return token
	}

	work := func(token *Token) *Token {
		log.Printf("my name is %s", token.TaskID)
		log.Printf("my type is %s", token.TaskType)
		log.Printf("received token: %+#v", token.ContextID)
		return nil
	}
	workflow.Register("fetch_wordlist", workCollector)
	workflow.Register("mutate_wordlist", work)
	workflow.Register("fs_persist", work)

	assert(t, nil, workflow.Run(), true)

	token := NewToken()
	token.ContextID = "my token"
	workflow.Publish("start", token)

	token.ContextID = "my second token"
	workflow.Publish("start", token)
}

func Test_Workflow_ConditionalImplementation(t *testing.T) {
	workCollector := func(token *Token) *Token {
		return token
	}

	gateway := func(token *Token) *Token {
		return token
	}

	valueHandler := func(token *Token) *Token {
		someID := int(token.Data["some_id"].(float64))
		log.Printf("I received some value: %d\n", someID)
		assert(t, int(0), someID, false)
		return nil
	}

	zeroHandler := func(token *Token) *Token {
		someID := int(token.Data["some_id"].(float64))

		log.Printf("I received zero. val == %d\n", someID)
		assert(t, int(0), someID, true)
		return nil
	}

	workflowConditional.Register("A", workCollector)
	workflowConditional.Register("B", gateway)
	workflowConditional.Register("C", zeroHandler)
	workflowConditional.Register("D", valueHandler)

	assert(t, nil, workflowConditional.Run(), true)

	token := NewToken()
	token.ContextID = "my token"
	token.Data["someId"] = 0
	workflowConditional.Publish("start", token)

	token.ContextID = "my second token"
	token.Data["someId"] = 1
	workflowConditional.Publish("start", token)

	token.ContextID = "my third token"
	token.Data["someId"] = 1000
	workflowConditional.Publish("start", token)
}

func BenchmarkFlow(b *testing.B) {
	workCollector := func(token *Token) *Token {
		return token
	}

	gateway := func(token *Token) *Token {
		return token
	}

	valueHandler := func(token *Token) *Token {
		someID := int(token.Data["some_id"].(float64))
		log.Printf("I received some value: %d\n", someID)
		return nil
	}

	zeroHandler := func(token *Token) *Token {
		someID := int(token.Data["some_id"].(float64))

		log.Printf("I received zero. val == %d\n", someID)
		return nil
	}

	workflowConditional.Register("A", workCollector)
	workflowConditional.Register("B", gateway)
	workflowConditional.Register("C", zeroHandler)
	workflowConditional.Register("D", valueHandler)

	b.StartTimer()
	rand.Seed(1234)
	publish1000 := func() {
		for i := 0; i < 1000; i++ {
			token := NewToken()
			token.ContextID = "some token"
			token.Data["someId"] = rand.Intn(1)
			go func() {
				workflowConditional.Publish("start", token)
			}()
		}
	}
	for i := 0; i < 1000; i++ {
		go publish1000()
	}
	workflowConditional.Teardown(time.After(1 * time.Millisecond))
	b.StopTimer()
}
