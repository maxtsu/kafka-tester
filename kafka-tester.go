package main

import (
	"encoding/json"
	"fmt"
	"generator/kafkatraffic"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gologme/log"
	"gopkg.in/yaml.v2"
)

const config_file = "kafka-tester.yaml"

func main() {
	log.EnableLevelsByNumber(5) //enable debug logging
	log.EnableFormattedPrefix()
	log.Infof("debug logging enabled\n")

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
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}

		close(deliveryChan)

	}
	time.Sleep(5 * time.Second)
	fmt.Printf("Finshed program\n")
}
