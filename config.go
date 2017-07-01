package miniflow

import nats "github.com/nats-io/go-nats"

// NATSConfig for flowbee
type NATSConfig struct {
	conn        *nats.Conn
	EncodedConn *nats.EncodedConn
}

// NewNATSConfig constructor
func NewNATSConfig() *NATSConfig {
	// TODO: read from os.Getenv
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	c, errEnc := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if errEnc != nil {
		panic(errEnc)
	}
	if nc.Status() == nats.CONNECTED {
		return &NATSConfig{nc, c}
	}
	return &NATSConfig{nil, nil}
}
