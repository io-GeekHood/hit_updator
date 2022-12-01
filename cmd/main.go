package main

import (
	"github.com/joho/godotenv"
	"github.com/vortex/mongoupdate/internal"
	"log"
	"os"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	kafkaHostName := os.Getenv("KAFKA_HOST")
	kafkaTopicName := os.Getenv("KAFKA_TOPIC")
	databaseAddress := os.Getenv("MONGODB_URI")
	go internal.MongoUpdater(databaseAddress)
	kafkaEngine := internal.Streamer{}
	kafkaEngine.StreamSetup(kafkaHostName, "media_downloader")
	kafkaEngine.Consume(kafkaTopicName)

}
