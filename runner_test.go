package miniflow

import (
	"log"
	"testing"
)

func Test_NewWorkflowRunner(t *testing.T) {
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

	wr := NewWorkflowRunner(config)
	wr.Register(workflowConditional)
	wr.RunAll()
	log.Print("Workflow runner started all workflows.")
	recvCount := 0
	for {
		<-workflowConditional.EndTokens
		recvCount++
		if recvCount == 3 {
			break
		}
	}
	wr.StopAll()
	assertEqual(t, 3, recvCount)

	assertNotEqual(t, 0, zeroCounter)
	assertEqual(t, 0, valueCounter)
	log.Printf("counter %d", counter)
}
