package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/user/gofka/pkg/client"
)

func main() {
	var brokerAddr string
	var topic string
	var groupID string
	var partition int

	flag.StringVar(&brokerAddr, "broker", "localhost:9092", "Broker address")
	flag.StringVar(&topic, "topic", "test-topic", "Topic to consume from")
	flag.StringVar(&groupID, "group", "test-group", "Consumer group ID")
	flag.IntVar(&partition, "partition", 0, "Partition to consume from")
	flag.Parse()

	// Create cluster client
	clusterClient := client.NewClusterClient(brokerAddr, 30*time.Second)
	defer clusterClient.Close()

	// Create consumer
	config := &client.ConsumerConfig{
		GroupID:     groupID,
		Topic:       topic,
		Partition:   int32(partition),
		AutoCommit:  true,
		Timeout:     30 * time.Second,
		BatchSize:   100,
		PollTimeout: 100 * time.Millisecond,
	}

	consumer := client.NewConsumer(clusterClient, config)
	defer consumer.Close()

	// Connect to broker
	if err := consumer.Connect(); err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}

	fmt.Printf("Connected to broker at %s\n", brokerAddr)

	fmt.Printf("Subscribed to topic '%s' partition %d in group '%s'\n", topic, partition, groupID)

	// Start consuming
	for {
		messages, err := consumer.Consume()
		if err != nil {
			log.Printf("Failed to consume messages: %v", err)
			time.Sleep(1 * time.Second) // Wait before retrying
			continue
		}

		if len(messages) > 0 {
			for _, msg := range messages {
				fmt.Printf("Consumed message: %s\n", string(msg))
			}
		} else {
			fmt.Println("No new messages, polling again...")
		}
		time.Sleep(consumer.PollTimeout)
	}
}
