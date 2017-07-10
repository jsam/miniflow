package miniflow

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"time"
)

// ErrWorkflowNotFound is error when workflow is not registered
var ErrWorkflowNotFound = errors.New("Workflow is not found")

// WorkflowRunner is manager for all workflow which desired to be run
type WorkflowRunner struct {
	Workflows map[string]*Workflow
	Config    *NATSStreamingConfig
}

// Register will register defined workflow for running state.
func (wr *WorkflowRunner) Register(wf *Workflow) {
	wr.Workflows[wf.Name] = wf
}

// Run will start workflow and attach kill signal channel listener.
func (wr *WorkflowRunner) Run(name string) (*Workflow, error) {
	wf, ok := wr.Workflows[name]
	if !ok {
		return nil, ErrWorkflowNotFound
	}

	wf.Run()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	select {
	case <-signalChan:
		log.Println("[runner] Received interrupt signal. Shutting down the workflow gracefully.")
		wf.Teardown(time.After(1 * time.Second))
	}
	return wf, nil
}

// RunAll will execute all registered workflows.
func (wr *WorkflowRunner) RunAll() {
	for name := range wr.Workflows {
		go wr.Run(name)
	}
}

// StopAll will stop all registered workflows.
func (wr *WorkflowRunner) StopAll() {
	for _, wf := range wr.Workflows {
		wf.Teardown(time.After(time.Microsecond))
	}
}

// NewWorkflowRunner creates new WorkflowRunner object.
func NewWorkflowRunner(config *NATSStreamingConfig) *WorkflowRunner {
	return &WorkflowRunner{
		make(map[string]*Workflow),
		config,
	}
}
