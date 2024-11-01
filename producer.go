package kafka-tester

import "encoding/json"

// configuration file kafka-config.yaml
type Config struct {
	Producer         bool   `yaml:"producer"`
	BootstrapServers string `yaml:"bootstrap.servers"`
	SaslMechanisms   string `yaml:"sasl.mechanisms"`
	SecurityProtocol string `yaml:"security.protocol"`
	SaslUsername     string `yaml:"sasl.username"`
	SaslPassword     string `yaml:"sasl.password"`
	SslCaLocation    string `yaml:"ssl.ca.location"`
	ClientID         string `yaml:"client.id"`
	Topics           string `yaml:"topics"`
}

// Event Message partial struct
type Message struct {
	Name      string `json:"name"`
	Timestamp int64  `json:"timestamp"`
	Tags      Tags   `json:"tags"`
	// Tags      struct {
	// 	Path             string `json:"path"`
	// 	Prefix           string `json:"prefix"`
	// 	Source           string `json:"source"`
	// 	SubscriptionName string `json:"subscription-name"`
	// } `json:"tags"`
	Values json.RawMessage `json:"values"`
}

type Tags struct {
	Path             string `json:"path"`
	Prefix           string `json:"prefix"`
	Source           string `json:"source"`
	SubscriptionName string `json:"subscription-name"`
}
