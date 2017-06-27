package miniflow

import nats "github.com/nats-io/go-nats"

// Config for flowbee
type Config struct {
	conn        *nats.Conn
	encodedConn *nats.EncodedConn
}

// NewConfig constructor
func NewConfig() *Config {
	// TODO: read from os.Getenv
	nc, _ := nats.Connect(nats.DefaultURL)
	c, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	return &Config{nc, c}
}
