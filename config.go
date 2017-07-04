package miniflow

import (
	"log"

	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nuid"
)

// NATSStreamingConfig is object which holds important information about connection and nats streaming server.
type NATSStreamingConfig struct {
	ClusterID   string
	ClientID    string
	ShowTime    bool
	StartSeq    uint64
	StartDelta  string
	DeliverAll  bool
	DeliverLast bool
	Durable     string
	URL         string

	NATSConn stan.Conn
}

// NewNATSStreamingConfig is constructor object for NATSStreamingConfig object.
func NewNATSStreamingConfig(clusterID, url string) *NATSStreamingConfig {
	nuid := nuid.New()
	clientID := nuid.Next()

	if url == "" {
		url = stan.DefaultNatsURL
	}

	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(url))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s with params %s, %s", err, url, clusterID, clientID)
	}
	return &NATSStreamingConfig{
		ClusterID:   clusterID,
		ClientID:    clientID,
		ShowTime:    false,
		StartSeq:    0,
		StartDelta:  "",
		DeliverAll:  false,
		DeliverLast: true,
		Durable:     "",
		NATSConn:    sc,
	}
}
