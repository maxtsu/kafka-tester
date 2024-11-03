package kafkatraffic

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

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
	Topic            string `yaml:"topic"`
}

// Event Message partial struct
type Message struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Tags      map[string]string `json:"tags"`
	Values    json.RawMessage   `json:"values"`
}

// type Tags struct {
// 	Path             string `json:"path"`
// 	Prefix           string `json:"prefix"`
// 	Source           string `json:"source"`
// 	SubscriptionName string `json:"subscription-name"`
// }

type Subscription struct {
	Name   string   `json:"name"`
	Index  []string `json:"index"`
	Path   string   `json:"path"`
	Prefix string   `json:"prefix"`
}

// Function to read text file return byteResult
func ReadFile(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	byteResult, _ := io.ReadAll(file)
	file.Close()
	return byteResult
}

func ListDevice() []string {
	li := []string{"192.168.1.1", "192.168.1.2"}
	return li
}

func CreateJsonData(source string, subscription Subscription) (Message, string) {
	//{"name":"global","timestamp":1730383584027000000,"tags":{"interface_name":"TwentyFiveGigE0/0/0/50","path":"/interfaces/interface/state/counters/","prefix":"openconfig-interfaces:","source":"10.49.2.73:57344","subscription-name":"global"},"values":{"openconfig-interfaces:/interfaces/interface/state/counters/carrier-transitions":"0"}}
	// test message
	timestamp := (time.Now().UnixMicro())
	var tags = map[string]string{"path": subscription.Path, "prefix": subscription.Prefix, "source": source, "subscription-name": "global"}
	//have to add list of indexes as indivdual tags
	jsondata := Message{Name: "global", Timestamp: timestamp, Tags: tags}
	key := source + ":57344_global"
	return jsondata, key
}

func CreateProducer(configYaml Config) (*kafka.Producer, error) {
	fmt.Printf("Create Kafka producer \n")
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": configYaml.BootstrapServers,
		"sasl.mechanisms":   configYaml.SaslMechanisms,
		"security.protocol": configYaml.SecurityProtocol,
		"sasl.username":     configYaml.SaslUsername,
		"sasl.password":     configYaml.SaslPassword,
		"ssl.ca.location":   configYaml.SslCaLocation,
		"client.id":         configYaml.ClientID,
		"acks":              "all"})
	if err != nil {
		return producer, fmt.Errorf("failed to create producer: %w", err)
	}
	return producer, nil
}
