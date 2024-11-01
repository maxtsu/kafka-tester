package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

const config_file = "kafka-tester.yaml"

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Rad the config file
	byteResult := ReadFile(config_file)
	var configYaml Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-tester.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-tester.yaml: %+v\n", configYaml)
	//Create producer
	fmt.Printf("Kafka producer \n")
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
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", producer)
	defer producer.Close()
	// delivery channel
	var deliveryChan = make(chan kafka.Event, 10000)

	//{"name":"global","timestamp":1730383584027000000,"tags":{"interface_name":"TwentyFiveGigE0/0/0/50","path":"/interfaces/interface/state/counters/","prefix":"openconfig-interfaces:","source":"10.49.2.73:57344","subscription-name":"global"},"values":{"openconfig-interfaces:/interfaces/interface/state/counters/carrier-transitions":"0"}}
	// test message
	topic := "gnf.network.telemetry"
	timestamp := (time.Now().UnixMicro())
	test_tags := Tags{Path: "/test/path", Prefix: "openconfig-test:", Source: "192.168.10.10"}
	test_msg := Message{Name: "global", Timestamp: timestamp, Tags: test_tags}

	// Serialize the message Marshall from JSON
	jsonData, err := json.Marshal(test_msg)
	if err != nil {
		fmt.Printf("failed to serialize message: %w", err)
	}

	// construct kafka message
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: jsonData,
		//Headers: []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}

	// Produce the message to the Kafka topic
	err = producer.Produce(message, deliveryChan)
	if err != nil {
		fmt.Printf("Failed to produce message: %s\n", err)
	}

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
