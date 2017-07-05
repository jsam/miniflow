package miniflow

import (
	"errors"
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"

	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/nuid"
)

var (
	errCannotMarshalConfig = errors.New("Cannot marshal config struct")
	errCannotWriteConfig   = errors.New("Cannot write to file")
	errCannotReadConfig    = errors.New("Cannot read config")
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

	NATSConn stan.Conn `yaml:"-"`
}

func (c *NATSStreamingConfig) loadConfig() error {
	// TODO: neccessary for durable connections
	d, err := ioutil.ReadFile(".miniflow")
	if err != nil {
		return errCannotReadConfig
	}

	err = yaml.Unmarshal(d, c)
	if err != nil {
		return errCannotMarshalConfig
	}
	return nil
}

func (c *NATSStreamingConfig) writeConfig() error {
	// TODO: neccessary for durable connections
	// TODO: serialize c to yaml struct
	d, err := yaml.Marshal(c)
	if err != nil {
		return errCannotMarshalConfig
	}
	err = ioutil.WriteFile(".miniflow", d, 0644)
	if err != nil {
		return errCannotWriteConfig
	}
	return nil
}

// NewNATSStreamingConfig is constructor object for NATSStreamingConfig object.
func NewNATSStreamingConfig(clusterID, url string) *NATSStreamingConfig {
	natsConfig := &NATSStreamingConfig{}
	err := natsConfig.loadConfig()
	if err != nil && err != errCannotReadConfig {
		panic("Corrupted config file.")
	}
	if len(natsConfig.ClientID) == 0 {
		nuid := nuid.New()
		clientID := nuid.Next()

		natsConfig.ClusterID = clusterID
		natsConfig.ClientID = clientID
		natsConfig.ShowTime = false
		natsConfig.StartSeq = 0
		natsConfig.StartDelta = ""
		natsConfig.DeliverAll = false
		natsConfig.DeliverLast = true
		natsConfig.Durable = ""
	}

	if url == "" {
		url = stan.DefaultNatsURL
	}

	sc, err := stan.Connect(clusterID, natsConfig.ClientID, stan.NatsURL(url))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s with params %s, %s", err, url, natsConfig.ClusterID, natsConfig.ClientID)
	}
	natsConfig.NATSConn = sc
	return natsConfig
}
