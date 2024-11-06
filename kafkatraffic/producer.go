package kafkatraffic

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

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
	Frequency        int    `yaml:"frequency"`
}

// Event Message partial struct
type Message struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Tags      map[string]string `json:"tags"`
	Values    json.RawMessage   `json:"values"`
}

type Tag struct {
	Key   string
	Value string
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

type devices struct {
	Devices []string `json:"devices"`
}

func ListDevice(devices_file string) []string {
	// Read the config file
	byteResult := ReadFile(devices_file)
	//convert the devices.json to a struct
	var devicesjson devices
	err := json.Unmarshal(byteResult, &devicesjson)
	if err != nil {
		fmt.Println("config.json Unmarshall error", err)
	}
	return devicesjson.Devices
}

// Create JSON message without source
func CreateJsonData(subscription Subscription) Message {
	var tags = map[string]string{"path": subscription.Path, "prefix": subscription.Prefix, "subscription-name": "global"}
	jsondata := Message{Name: "global", Tags: tags, Values: subscription.Values}
	return jsondata
}

// Add device source to teh message
func AddSource(source string, message Message, timestamp int64) Message {
	tags := message.Tags
	tags["source"] = source + ":57344"
	message.Timestamp = timestamp
	message.Tags = tags
	//message_with_source := Message{Timestamp: timestamp, Tags: tags}
	return message
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
		"linger.ms":         200})
	//"acks":              "all"})
	if err != nil {
		return producer, fmt.Errorf("failed to create producer: %w", err)
	}
	return producer, nil
}

func matrix_tags(master_list [][]Tag, myindexes []Index, level int) [][]Tag {
	length_of_indexes := len(myindexes)
	index := myindexes[length_of_indexes-level] //saftey check level
	var update_index_list [][]Tag
	for _, sub_index := range master_list {
		var intermediate_list []Tag
		for i := range index.Number {
			var t Tag
			t.Value = index.Name + "_" + strconv.Itoa(i)
			t.Key = index.Name
			intermediate_list = append(sub_index, t)
			update_index_list = append(update_index_list, intermediate_list)
		}
	}
	return update_index_list
}

// Creating matix of indexes
func Index_looping(indexes []Index) [][]Tag {
	var master_list_index_tags [][]Tag
	index := indexes[len(indexes)-1]
	for i := range index.Number {
		var t Tag
		t.Value = index.Name + "_" + strconv.Itoa(i)
		t.Key = index.Name
		t_list := []Tag{t}
		master_list_index_tags = append(master_list_index_tags, t_list)
	}
	for n := 2; n <= len(indexes); n++ {
		master_list_index_tags = matrix_tags(master_list_index_tags, indexes, n)
	}
	return master_list_index_tags
}

// Deepcopy function to copy a map
func DeepCopy(original map[string]string) map[string]string {
	copy := make(map[string]string)
	for key, value := range original {
		copy[key] = value
	}
	return copy
}
