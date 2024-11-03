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

type Index struct {
	Name   string `json:"name"`
	Number int    `json:"number"`
}

type Subscription struct {
	Name   string          `json:"name"`
	Index  []Index         `json:"index"`
	Path   string          `json:"path"`
	Prefix string          `json:"prefix"`
	Values json.RawMessage `json:"values"`
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
	timestamp := (time.Now().UnixMicro())
	var tags = map[string]string{"path": subscription.Path, "prefix": subscription.Prefix, "source": source, "subscription-name": "global"}
	jsondata := Message{Name: "global", Timestamp: timestamp, Tags: tags, Values: subscription.Values}
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
