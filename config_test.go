package miniflow

import "testing"

func Test_New_NatsConfig(t *testing.T) {
	config := NewNATSStreamingConfig("test-cluster", "")
	assert(t, nil, config, false)
}
