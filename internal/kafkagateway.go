package internal

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
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
		log.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}
	log.Printf("Initiated kafka producer %v", vortex_producer)
	vortex_consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": host,
		"group.id":          id,
		"auto.offset.reset": "smallest"})
	if err != nil {
		log.Println("failed to create consumer for media")
		os.Exit(1)
	}
	log.Printf("Initiated kafka connector %v", vortex_consumer.String())
	streamer.consumer = vortex_consumer
	streamer.producer = vortex_producer
	return *streamer

}

func (stream *Streamer) Consume(topic string) {

	topics := []string{topic}
	log.Printf("Downloader started listening on: %v ", topics)
	err := stream.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Printf("failed to initiate kafka consumer %s", err)
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
				log.Printf("failed to decode message into nested structure: %v", err)
			}
			log.Printf("Message Decoded: %v", Job)
			UpdateJobRegister <- Job
		case kafka.Error:
			log.Printf("\n | Consumer Error: %v\n", e)
		default:
			continue
		}
	}

}
