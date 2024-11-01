package main

import (
	"encoding/json"
	"fmt"
	"generator/kafkatraffic"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

const config_file = "kafka-tester.yaml"

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Read the config file
	byteResult := kafkatraffic.ReadFile(config_file)
	var configYaml kafkatraffic.Config
	err := yaml.Unmarshal(byteResult, &configYaml)
	if err != nil {
		fmt.Println("kafka-tester.yaml Unmarshall error", err)
	}
	fmt.Printf("kafka-tester.yaml: %+v\n", configYaml)
	//Create producer
	producer, err := kafkatraffic.CreateProducer(configYaml)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()
	// delivery channel
	var deliveryChan = make(chan kafka.Event, 10000)
	fmt.Printf("Created Producer %v\n", producer)

	list_of_devices := kafkatraffic.ListDevice()
	for _, dev := range list_of_devices {
		msg, key := kafkatraffic.CreateJsonData(dev)

		// Serialize the message Marshall from JSON
		jsonData, err := json.Marshal(msg)
		if err != nil {
			fmt.Printf("failed to serialize message: %w", err)
		}
		// construct kafka message
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &configYaml.Topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(key),
			Value: jsonData,
		}
		// Produce the message to the Kafka topic
		err = producer.Produce(message, deliveryChan)
		if err != nil {
			fmt.Printf("Failed to produce message: %s\n", err)
		}

	}

	// //{"name":"global","timestamp":1730383584027000000,"tags":{"interface_name":"TwentyFiveGigE0/0/0/50","path":"/interfaces/interface/state/counters/","prefix":"openconfig-interfaces:","source":"10.49.2.73:57344","subscription-name":"global"},"values":{"openconfig-interfaces:/interfaces/interface/state/counters/carrier-transitions":"0"}}
	// // test message
	// topic := "gnf.network.telemetry"
	// timestamp := (time.Now().UnixMicro())
	// test_tags := kafkatraffic.Tags{Path: "/test/path", Prefix: "openconfig-test:", Source: "192.168.10.10"}
	// test_msg := kafkatraffic.Message{Name: "global", Timestamp: timestamp, Tags: test_tags}
	// key := "192.168.10.10:57344_global"

	// // Serialize the message Marshall from JSON
	// jsonData, err := json.Marshal(test_msg)
	// if err != nil {
	// 	fmt.Printf("failed to serialize message: %w", err)
	// }
	// // construct kafka message
	// message := &kafka.Message{
	// 	TopicPartition: kafka.TopicPartition{
	// 		Topic:     &topic,
	// 		Partition: kafka.PartitionAny,
	// 	},
	// 	Key:   []byte(key),
	// 	Value: jsonData,
	// }

	// // Produce the message to the Kafka topic
	// err = producer.Produce(message, deliveryChan)
	// if err != nil {
	// 	fmt.Printf("Failed to produce message: %s\n", err)
	// }

}
