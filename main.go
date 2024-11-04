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
	var full_message_list []kafkatraffic.Message

	//Create producer
	producer, err := kafkatraffic.CreateProducer(configYaml)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()
	fmt.Printf("Created Producer %v\n", producer)

	for _, subscription := range subscriptions { //range over subscriptions
		// Create message for kafka using device and subscription (without indexes)
		msg := kafkatraffic.CreateJsonData(subscription)

		// Create the list of indexes for the subscription
		list_index_tags := kafkatraffic.Index_looping(subscription.Index)
		fmt.Printf("list index tags: %+v\n", list_index_tags)
		for _, indexes := range list_index_tags { // range over subscription indexes
			// Perform the manual deep copy of message Tag maps
			map_of_tags := kafkatraffic.DeepCopy(msg.Tags)
			for _, index := range indexes {
				map_of_tags[index.Key] = index.Value // Add index tag
			}
			msg.Tags = map_of_tags
			full_message_list = append(full_message_list, msg)
		}
	}
	// for _, ms := range full_message_list {
	// 	fmt.Printf("Full List: %+v\n", ms.Tags)
	// }
	list_of_devices := kafkatraffic.ListDevice()
	for _, source := range list_of_devices {
		//fmt.Printf("Dev: %+v\n", source)
		timestamp := (time.Now().UnixMicro())
		for _, message := range full_message_list {
			//fmt.Printf("Pre-Message: %+v\n", message.Tags)
			message = kafkatraffic.AddSource(source, message, timestamp)
			key := source + ":57344_global"

			// Serialize the message Marshall from JSON
			jsonData, err := json.Marshal(message)
			if err != nil {
				fmt.Printf("failed to serialize message: %w", err)
			}
			// construct kafka message
			msg := &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &configYaml.Topic,
					Partition: kafka.PartitionAny,
				},
				Key:   []byte(key),
				Value: jsonData,
			}
			// Produce the message to the Kafka topic
			err = producer.Produce(msg, nil)
			if err != nil {
				fmt.Printf("Failed to produce message: %s\n", err)
			}

		}
	}
	// //Create producer
	// producer, err := kafkatraffic.CreateProducer(configYaml)
	// if err != nil {
	// 	fmt.Printf("Failed to create producer: %s\n", err)
	// 	os.Exit(1)
	// }
	// defer producer.Close()
	// fmt.Printf("Created Producer %v\n", producer)

	// // Serialize the message Marshall from JSON
	// jsonData, err := json.Marshal(msg)
	// if err != nil {
	// 	fmt.Printf("failed to serialize message: %w", err)
	// }
	// key := "dummy"
	// construct kafka message
	// message := &kafka.Message{
	// 	TopicPartition: kafka.TopicPartition{
	// 		Topic:     &configYaml.Topic,
	// 		Partition: kafka.PartitionAny,
	// 	},
	// 	Key:   []byte(key),
	// 	Value: jsonData,
	// }
	// Produce the message to the Kafka topic
	// err = producer.Produce(message, nil)
	// if err != nil {
	// 	fmt.Printf("Failed to produce message: %s\n", err)
	// }
	fmt.Printf("Finshed program\n")
}
