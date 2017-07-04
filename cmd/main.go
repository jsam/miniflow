package main

import (
	"fmt"

	"github.com/jsam/miniflow"
)

func main() {
	config := miniflow.NewNATSStreamingConfig("test-cluster", "")
	fmt.Printf("%+v", config)
}
