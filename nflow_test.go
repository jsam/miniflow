package miniflow

import (
	"log"
	"os"
	"testing"
	"time"
)

var config *NATSStreamingConfig

var workflowSchema *WorkflowSchema
var workflowSchemaCond *WorkflowSchema

func init() {
	config = NewNATSStreamingConfig("miniflow", "", true)
	wfs, errSchema := NewWorkflowSchema("./schemas/example_workflow.yaml")
	_, errWf := NewWorkflow(wfs, config)
	if errSchema != nil || errWf != nil {
		panic("Problem bootstraping test. Make sure NATS is avaialable at nats://localhost:4222")
	}
	workflowSchema = wfs

	wfsCond, errSchemaCond := NewWorkflowSchema("./schemas/example_conditional_workflow.yaml")
	_, errWfc := NewWorkflow(wfsCond, config)
	if errSchemaCond != nil || errWfc != nil {
		panic("Problem bootstraping test. Make sure NATS is avaialable at nats://localhost:4222")
	}
	workflowSchemaCond = wfsCond

}

func TestMain(m *testing.M) {
	statusCode := m.Run()
	os.Exit(statusCode)
}

func Test_NewWorkflow_FromSchema(t *testing.T) {
	assert(t, nil, config, false)
	assert(t, nil, workflowSchema, false)
}

func Test_Workflow_Relations(t *testing.T) {
	workflow, _ := NewWorkflow(workflowSchema, config)

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
	workflow, _ := NewWorkflow(workflowSchema, config)
	err := workflow.Run()
	assert(t, errTaskNotImplemented, err.(FlowError).Err, true)
}

func Test_Workflow_PartialImplementation(t *testing.T) {
	workflow, _ := NewWorkflow(workflowSchema, config)
	notInvoked := true
	work := func(token *Token) *Token {
		notInvoked = false
		return nil
	}
	workflow.Register("fetch_wordlist", work)
	assertEqual(t, true, notInvoked)

	err := workflow.Run()
	assert(t, errTaskNotImplemented, err.(FlowError).Err, true)
	workflow.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_NoConfigImplementation(t *testing.T) {
	wfs, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	noConfWorkflow, err := NewWorkflow(wfs, nil)
	assert(t, nil, err, true)

	notInvoked := true
	work := func(token *Token) *Token {
		notInvoked = false
		return nil
	}
	noConfWorkflow.Register("fetch_wordlist", work)
	noConfWorkflow.Register("mutate_wordlist", work)
	noConfWorkflow.Register("fs_persist", work)

	assert(t, true, notInvoked, true)
	err = noConfWorkflow.Run()
	assert(t, errNatsNoConnection, err, true)

	noConfWorkflow.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_FullImplementation(t *testing.T) {
	workflow, _ := NewWorkflow(workflowSchema, config)
	assert(t, nil, config, false)
	assert(t, nil, config.NATSConn, false)
	tokenPath := []*Token{}
	work := func(token *Token) *Token {
		log.Printf("fullimpl: %s :: %s", token.TaskID, token.ContextID)
		if token.ContextID == "my token" {
			tokenPath = append(tokenPath, token)
		}
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

	recvCount := 0
	for {
		<-workflow.EndTokens

		recvCount++
		if recvCount == 2 {
			break
		}
	}
	assertEqual(t, 2, recvCount)

	assert(t, 3, len(tokenPath), true)
	assert(t, "fetch_wordlist", tokenPath[0].TaskID, true)
	assert(t, "mutate_wordlist", tokenPath[1].TaskID, true)
	assert(t, "fs_persist", tokenPath[2].TaskID, true)

	for i := 0; i < 3; i++ {
		assert(t, "my token", tokenPath[0].ContextID, true)
	}

	workflow.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_FirstTerminationImplementation(t *testing.T) {
	wf, _ := NewWorkflow(workflowSchema, config)

	invoked := false
	var recordToken *Token
	work := func(token *Token) *Token {
		invoked = true
		recordToken = token
		return nil
	}

	wf.Register("fetch_wordlist", work)
	wf.Register("mutate_wordlist", work)
	wf.Register("fs_persist", work)

	assert(t, nil, wf.Run(), true)

	token := NewToken()
	token.ContextID = "my token"
	wf.Publish("start", token)

	token.ContextID = "my second token"
	wf.Publish("start", token)

	recvCount := 0
	for {
		<-wf.EndTokens
		recvCount++
		log.Print(recvCount)

		if recvCount == 2 {
			break
		}
	}
	assertEqual(t, 2, recvCount)
	assert(t, nil, recordToken, false)
	assert(t, true, invoked, true)
	assert(t, "fetch_wordlist", recordToken.TaskID, true)

	wf.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_SecondTerminationImplementation(t *testing.T) {
	workflow, _ := NewWorkflow(workflowSchema, config)

	firstInvoked := false
	var firstRecorded *Token
	workCollector := func(token *Token) *Token {
		firstInvoked = true
		firstRecorded = token
		return token
	}

	invoked := false
	var recordedToken *Token
	work := func(token *Token) *Token {
		invoked = true
		recordedToken = token
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

	recvCount := 0
	for {
		<-workflow.EndTokens
		recvCount++
		if recvCount == 2 {
			break
		}
	}
	assertEqual(t, 2, recvCount)

	assertEqual(t, true, firstInvoked)
	assertEqual(t, "collector", firstRecorded.TaskType)

	assert(t, true, invoked, true)
	assert(t, nil, recordedToken, false)

	workflow.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_ConditionalImplementation(t *testing.T) {
	workflowConditional, _ := NewWorkflow(workflowSchemaCond, config)
	zeroCount := 0
	nonZeroCount := 0

	workCollectorInvoked := false
	var wcToken *Token
	workCollector := func(token *Token) *Token {
		workCollectorInvoked = true
		wcToken = token
		return token
	}

	gatewayInvoked := false
	var gatewayToken *Token
	gateway := func(token *Token) *Token {
		gatewayInvoked = true
		gatewayToken = token
		return token
	}

	valueHandler := func(token *Token) *Token {
		someID := int(token.Data["some_id"].(float64))
		log.Printf("I received some value: %d\n", someID)
		assert(t, int(0), someID, false)
		nonZeroCount++
		return nil
	}

	zeroHandler := func(token *Token) *Token {
		someID := int(token.Data["some_id"].(float64))

		log.Printf("I received zero. val == %d\n", someID)
		assert(t, int(0), someID, true)
		zeroCount++
		return nil
	}

	workflowConditional.Register("A", workCollector)
	workflowConditional.Register("B", gateway)
	workflowConditional.Register("C", zeroHandler)
	workflowConditional.Register("D", valueHandler)
	assert(t, nil, workflowConditional.Run(), true)

	token := NewToken()
	token.ContextID = "my token"
	token.Data["some_id"] = 0
	workflowConditional.Publish("start", token)

	token.ContextID = "my second token"
	token.Data["some_id"] = 1
	workflowConditional.Publish("start", token)

	token.ContextID = "my third token"
	token.Data["some_id"] = 1000
	workflowConditional.Publish("start", token)

	recvCount := 0
	for {
		<-workflowConditional.EndTokens
		recvCount++
		if recvCount == 3 {
			break
		}
	}
	assertEqual(t, 3, recvCount)

	assert(t, 1, zeroCount, true)
	assert(t, 2, nonZeroCount, true)

	assertTrue(t, workCollectorInvoked)
	assertNotNil(t, wcToken)

	assertTrue(t, gatewayInvoked)
	assertNotNil(t, gatewayToken)

	workflowConditional.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_ConditionalImplementation_Publisher(t *testing.T) {
	workflowConditional, _ := NewWorkflow(workflowSchemaCond, config)
	workCollector := func(token *Token) *Token {
		return token
	}

	gateway := func(token *Token) *Token {
		return token
	}

	valueCounter := 0
	valueHandler := func(token *Token) *Token {
		valueCounter++
		return nil
	}

	zeroCounter := 0
	zeroHandler := func(token *Token) *Token {
		zeroCounter++
		return nil
	}

	workflowConditional.Register("A", workCollector)
	workflowConditional.Register("B", gateway)
	workflowConditional.Register("C", zeroHandler)
	workflowConditional.Register("D", valueHandler)

	counter := 0
	publish := func() *Token {
		token := NewToken()
		token.ContextID = "some token"
		token.Data["some_id"] = 0
		token.Data["counter"] = counter
		counter++
		return token
	}

	assert(t, nil, workflowConditional.AttachProducer(1, publish), true)
	assert(t, nil, workflowConditional.Run(), true)

	recvCount := 0
	for {
		<-workflowConditional.EndTokens
		recvCount++
		if recvCount == 3 {
			break
		}
	}
	assertEqual(t, 3, recvCount)

	assertNotEqual(t, 0, zeroCounter)
	assertEqual(t, 0, valueCounter)
	log.Printf("counter %d", counter)
	workflowConditional.Teardown(time.After(time.Microsecond))
}

func Test_Workflow_Ctx(t *testing.T) {
	workflowConditional, _ := NewWorkflow(workflowSchemaCond, config)
	workflowConditional.Ctx = map[string]interface{}{
		"redisURL": "localhost",
	}

	workCollector := func(token *Token) *Token {
		return token
	}

	gateway := func(token *Token) *Token {
		return token
	}

	valueHandler := func(token *Token) *Token {
		return nil
	}

	zeroHandler := func(token *Token) *Token {
		assertEqual(t, token.Ctx.(map[string]interface{})["redisURL"], "localhost")
		return nil
	}

	workflowConditional.Register("A", workCollector)
	workflowConditional.Register("B", gateway)
	workflowConditional.Register("C", zeroHandler)
	workflowConditional.Register("D", valueHandler)

	counter := 0
	publish := func() *Token {
		token := NewToken()
		token.ContextID = "some token"
		token.Data["some_id"] = 0
		token.Data["counter"] = counter
		counter++
		return token
	}

	assert(t, nil, workflowConditional.AttachProducer(1, publish), true)
	assert(t, nil, workflowConditional.Run(), true)

	recvCount := 0
	for {
		<-workflowConditional.EndTokens
		recvCount++
		if recvCount == 3 {
			break
		}
	}
	assertEqual(t, 3, recvCount)
	log.Printf("counter %d", counter)
	workflowConditional.Teardown(time.After(time.Microsecond))
}
