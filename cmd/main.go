package main

import (
	"github.com/vortex/mongoupdate/internal"
	"os"
)

func main() {
	//err := godotenv.Load()
	//if err != nil {
	//	log.Fatal("Error loading .env file")
	//}
	kafkaHostName := os.Getenv("BROKER_HOST")
	kafkaTopicName := os.Getenv("KAFKA_TOPIC_LISTEN_UPDATOR")
	databaseAddress := os.Getenv("MONGODB_URI")
	go internal.MongoUpdater(databaseAddress)
	kafkaEngine := internal.Streamer{}
	kafkaEngine.StreamSetup(kafkaHostName, "media_updator")
	kafkaEngine.Consume(kafkaTopicName)

}
