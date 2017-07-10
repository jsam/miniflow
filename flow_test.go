package miniflow

import "testing"

func Test_NewWorkflowSchema(t *testing.T) {
	workflowSchema, err := NewWorkflowSchema("./schemas/example_workflow.yaml")
	if workflowSchema == nil && err != nil {
		t.Fatal("Construct failed creating WorkflowSchema.")
	}
}

func Test_NewProducer(t *testing.T) {
	i := 0
	produceFn := func() *Token {
		token := NewToken()
		token.Data["id"] = i
		i++
		return token
	}

	producer := NewProducer(10, produceFn)
	assert(t, nil, producer, false)
}
