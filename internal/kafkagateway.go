package internal

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
)

type Streamer struct {
	producer *kafka.Producer
	consumer *kafka.Consumer
}

func (streamer *Streamer) StreamSetup(host string, id string) Streamer {

	vortex_producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": host,
		"client.id":         id,
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Initiated kafka producer %v\n", vortex_producer)
	vortex_consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": host,
		"group.id":          id,
		"auto.offset.reset": "smallest"})
	if err != nil {
		log.Println("failed to create consumer for media")
		os.Exit(1)
	}
	fmt.Printf("Initiated kafka connector %v\n", vortex_consumer.String())
	streamer.consumer = vortex_consumer
	streamer.producer = vortex_producer
	return *streamer

}

func (stream *Streamer) Consume(topic string) {

	topics := []string{topic}
	log.Printf("\nDownloader started listening on:\n\t kafka : %v \n\t topics : %v \n", stream.consumer, topics)
	err := stream.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Printf("failed to initiate kafka consumer %s", err)
		os.Exit(1)
	}
	Job := map[string]string{}
	//defer stream.consumer.Close()
	for {
		ev := stream.consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			err := json.Unmarshal(e.Value, &Job)
			if err != nil {
				fmt.Printf("%% failed to decode message into nested structure: %v\n", err)
			}
			fmt.Printf("%% Message Decoded: %v\n", Job)
			UpdateJobRegister <- Job
		case kafka.Error:
			fmt.Printf("\n | Consumer Error: %v\n", e)
		default:
			continue
		}
	}

}
