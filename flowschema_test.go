package miniflow

import "testing"

func Test_NewWorkflowSchema(t *testing.T) {
	workflowSchema, err := NewWorkflowSchema("./schemas/example_workflow.yaml")
	if workflowSchema == nil && err != nil {
		t.Fatal("Construct failed creating WorkflowSchema.")
	}
}
