package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/prashanth8983/gofka/pkg/client"
)

func main() {
	var brokerAddr string
	var topic string
	var message string
	var partition int

	flag.StringVar(&brokerAddr, "broker", "localhost:9092", "Broker address")
	flag.StringVar(&topic, "topic", "test-topic", "Topic to produce to")
	flag.StringVar(&message, "message", "Hello, Gofka!", "Message to send")
	flag.IntVar(&partition, "partition", 0, "Partition to produce to")
	flag.Parse()

	// Create cluster client
	clusterClient := client.NewClusterClient(brokerAddr, 30*time.Second)
	defer clusterClient.Close()

	// Create producer
	config := &client.ProducerConfig{
		Acks:        1,
		Timeout:     30 * time.Second,
		Retries:     3,
		Compression: "none",
	}

	producer := client.NewProducer(clusterClient, config)
	defer producer.Close()

	// Connect to broker
	if err := producer.Connect(topic, int32(partition)); err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}

	fmt.Printf("Connected to broker at %s\n", brokerAddr)

	// Produce message
	offset, err := producer.Produce(topic, []byte(message))
	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}

	fmt.Printf("Successfully sent message to topic '%s' partition %d at offset %d: %s\n", topic, partition, offset, message)
}
