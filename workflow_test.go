package miniflow

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"testing"
)

func CheckChannel(t *testing.T, in *Channel, out *Channel, token *Token) {
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		tkn := <-out.ch
		if !reflect.DeepEqual(tkn, token) {
			t.Fatal("Token which is send and token which go received does not match.")
		}
		wg.Done()
	}()

	go func() {
		in.ch <- token
		wg.Wait()
	}()
}

func Test_NewWorkflowSchema(t *testing.T) {
	workflowSchema, err := NewWorkflowSchema("./schemas/example_workflow.yaml")
	if workflowSchema == nil && err != nil {
		t.Fatal("Construct failed creating WorkflowSchema.")
	}
}

func Test_NewWorkflow_FromSchema(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, err := NewWorkflow(workflowSchema, nil)

	if workflow == nil && err != nil {
		t.Fatal("Constructor failed creating Workflow.")
	}
}

func TestWorkflow_RelationChannels(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, nil)

	fetchWordlistTask := workflow.Tasks["fetch_wordlist"]
	mutateWordlist := workflow.Tasks["mutate_wordlist"]
	fsPersist := workflow.Tasks["fs_persist"]

	token := NewToken()
	token.Data["check"] = "fetch_mutate"
	if len(fetchWordlistTask.outs) != 1 || len(mutateWordlist.ins) != 1 {
		t.Fatal("Task has to many outs or in channels.")
	}
	CheckChannel(t, fetchWordlistTask.outs[0], mutateWordlist.ins[0], token)

	token.Data["check"] = "mutate_persist"
	if len(mutateWordlist.outs) != 1 || len(fsPersist.ins) != 1 {
		t.Fatal("Task has to many outs or in channels.")
	}
	CheckChannel(t, mutateWordlist.outs[0], fsPersist.ins[0], token)
}

func Test_Register_FuncMap(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, nil)

	collectorFn := func(token *Token) *Token {
		log.Println("collector: assembling wordlist")
		token.Data["wordlist"] = []string{"a", "b", "c"}
		return token
	}

	processorFn := func(token *Token) *Token {
		log.Println("processor: processing token")
		if !reflect.DeepEqual(token.Data["wordlist"], []string{"a", "b", "c"}) {
			t.Fatal("Received token is not what collector sent.")
		}

		token.Data["processed"] = make([]string, len(token.Data["wordlist"].([]string)))
		for index, item := range token.Data["wordlist"].([]string) {
			token.Data["processed"].([]string)[index] = fmt.Sprintf("%s_processed", item)
		}
		return token
	}

	exporterFn := func(token *Token) {
		log.Println("exporter: exporting processed data")
		if token.Data["processed"].([]string)[0] != "a_processed" {
			t.Fatal("wrong value received.")
		}
		if token.Data["processed"].([]string)[1] != "b_processed" {
			t.Fatal("wrong value received.")
		}
		if token.Data["processed"].([]string)[2] != "c_processed" {
			t.Fatal("wrong value received.")
		}
		return
	}

	funcMap := NewFuncMap()
	funcMap.AddCollector("fetch_wordlist", collectorFn)
	funcMap.AddProcessor("mutate_wordlist", processorFn)
	funcMap.AddExporter("fs_persist", exporterFn)

	workflow.Register(funcMap)

	start := make(chan *Token)
	teardown := make(chan bool)
	go workflow.Run(start, teardown)

	start <- NewToken()
	teardown <- true
	<-teardown

	if workflow.tokensFinished != 1 {
		t.Fatal("Token did not finish.")
	}
}

func Test_MultipleTokens(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	workflow, _ := NewWorkflow(workflowSchema, nil)

	collectorFn := func(ctx *Token) *Token {
		log.Println("Hello, I'm Collector!")
		token := NewToken()
		token.Data["wordlist"] = []string{"a", "b", "c"}
		return token
	}

	processorFn := func(token *Token) *Token {
		log.Println("Hello, I'm Processor!")
		token.Data["processed"] = make([]string, len(token.Data["wordlist"].([]string)))
		for _, item := range token.Data["wordlist"].([]string) {
			token.Data["processed"] = append(token.Data["processed"].([]string), fmt.Sprintf("%s_processed", item))
		}
		return token
	}

	exporterFn := func(token *Token) {
		log.Println("Hello, I'm Exporter!")
	}

	funcMap := NewFuncMap()

	funcMap.AddCollector("fetch_wordlist", collectorFn)
	funcMap.AddProcessor("mutate_wordlist", processorFn)
	funcMap.AddExporter("fs_persist", exporterFn)

	workflow.Register(funcMap)

	start := make(chan *Token)
	teardown := make(chan bool)
	go workflow.Run(start, teardown)

	for i := 0; i < 10; i++ {
		start <- NewToken()
	}

	teardown <- true
	<-teardown
	if workflow.tokensFinished != 10 {
		t.Fatal("Not all tokens have finished.")
	}
}

func Test_NATS_Flow(t *testing.T) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	config := NewNATSConfig()
	workflow, _ := NewWorkflow(workflowSchema, config.EncodedConn)

	collectorFn := func(ctx *Token) *Token {
		log.Println("Hello, I'm Collector!")
		token := NewToken()
		token.Data["wordlist"] = []string{"a", "b", "c"}
		return token
	}

	processorFn := func(token *Token) *Token {
		log.Println("Hello, I'm Processor!")
		token.Data["processed"] = true
		return token
	}

	exporterFn := func(token *Token) {
		log.Println("Hello, I'm Exporter!")
	}

	funcMap := NewFuncMap()

	funcMap.AddCollector("fetch_wordlist", collectorFn)
	funcMap.AddProcessor("mutate_wordlist", processorFn)
	funcMap.AddExporter("fs_persist", exporterFn)

	workflow.Register(funcMap)

	start := make(chan *Token)
	teardown := make(chan bool)
	go workflow.Run(start, teardown)

	for i := 0; i < 100; i++ {
		start <- NewToken()
	}

	teardown <- true
	<-teardown
}

func BenchmarkFlow(b *testing.B) {
	workflowSchema, _ := NewWorkflowSchema("./schemas/example_workflow.yaml")
	config := NewNATSConfig()

	collectorFn := func(token *Token) *Token {
		//log.Printf("[%v] Hello, I'm Collector!\n", token.Data["ID"])
		token.Data["wordlist"] = []string{"a", "b", "c"}
		return token
	}

	processorFn := func(token *Token) *Token {
		//log.Printf("[%v] Hello, I'm Processor!\n", token.Data["ID"])
		token.Data["processed"] = true
		return token
	}

	exporterFn := func(token *Token) {
		//log.Printf("[%v] Hello, I'm Exporter!\n", token.Data["ID"])
	}

	funcMap := NewFuncMap()
	funcMap.AddCollector("fetch_wordlist", collectorFn)
	funcMap.AddProcessor("mutate_wordlist", processorFn)
	funcMap.AddExporter("fs_persist", exporterFn)

	workflow, _ := NewWorkflow(workflowSchema, config.EncodedConn)
	workflow.Register(funcMap)

	start := make(chan *Token)
	teardown := make(chan bool)
	go workflow.Run(start, teardown)

	b.StartTimer()
	for i := 0; i < 1000000; i++ {
		token := NewToken()
		token.Data["ID"] = i
		start <- token
	}

	teardown <- true
	<-teardown
	b.StopTimer()
}
