package main

import (
	"encoding/json"
	"fmt"
	"generator/kafkatraffic"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gologme/log"
	"gopkg.in/yaml.v2"
)

const config_file = "kafka-tester.yaml"
const subscription_file = "subscriptions.json"

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Read the config file
	byteResult := kafkatraffic.ReadFile(config_file)
	var configYaml kafkatraffic.Config
	configYaml_err := yaml.Unmarshal(byteResult, &configYaml)
	if configYaml_err != nil {
		fmt.Println("kafka-tester.yaml Unmarshall error", configYaml_err)
	}
	fmt.Printf("kafka-tester.yaml: %+v\n", configYaml)

	// Read subscription file
	subscription_file_byteResult := kafkatraffic.ReadFile(subscription_file)
	var subscriptions []kafkatraffic.Subscription
	subscriptionJson_err := json.Unmarshal(subscription_file_byteResult, &subscriptions)
	if subscriptionJson_err != nil {
		log.Errorln("subscriptions.json Unmarshall error", subscriptionJson_err)
	}
	fmt.Printf("Subscriptions: %+v\n\n", subscriptions)
	//Create producer
	producer, err := kafkatraffic.CreateProducer(configYaml)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()
	fmt.Printf("Created Producer %v\n", producer)

	list_of_devices := kafkatraffic.ListDevice()
	for _, dev := range list_of_devices {
		for _, subscription := range subscriptions { //range over subscriptions
			// Create message for kafka using device and subscription (without indexes)
			msg, key := kafkatraffic.CreateJsonData(dev, subscription)

			for _, index := range subscription.Index { // range over subscription indexes
				for i := range index.Number { // range over index number amount
					index_string := index.Name + "_" + strconv.Itoa(i)
					fmt.Printf("index: %s\n", index_string)
					msg.Tags[index.Name] = index_string // Add index tag

					fmt.Printf("Message: %+v\n", msg)

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
					err = producer.Produce(message, nil)
					if err != nil {
						fmt.Printf("Failed to produce message: %s\n", err)
					}
				}
			}

		}
	}
	fmt.Printf("Finshed program\n")
}
