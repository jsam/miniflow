package miniflow

import (
	"log"
	"testing"
	"time"
)

func Test_NewWorkflow_FromSchema(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")

	if workflowSchema == nil {
		t.Fatal("Cannot read schema.")
	}
	workflow, err := NewWorkflow(workflowSchema, nil)

	if workflow == nil && err != nil {
		t.Fatal("Constructor failed creating Workflow.")
	}
}

func Test_Workflow_Relations(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, nil)

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
	config := NewNATSStreamingConfig("test-cluster", "")
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, config)
	assert(t, errTaskNotImplemented, workflow.Run(), true)
}

func Test_Workflow_PartialImplementation(t *testing.T) {
	config := NewNATSStreamingConfig("test-cluster", "")
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, config)

	work := func(token *Token) *Token {
		log.Println("basic work")
		return nil
	}
	workflow.Register("fetch_wordlist", work)

	assert(t, errTaskNotImplemented, workflow.Run(), true)
}

func Test_Workflow_NoConfigImplementation(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, nil)

	work := func(token *Token) *Token {
		log.Println("basic work")
		return nil
	}
	workflow.Register("fetch_wordlist", work)
	workflow.Register("mutate_wordlist", work)
	workflow.Register("fs_persist", work)

	assert(t, errNatsNoConnection, workflow.Run(), true)
}

func Test_Workflow_FullImplementation(t *testing.T) {
	config := NewNATSStreamingConfig("test-cluster", "")
	assert(t, nil, config, false)
	assert(t, nil, config.NATSConn, false)

	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, config)

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

	workflow.Teardown(time.After(3 * time.Second))
}
