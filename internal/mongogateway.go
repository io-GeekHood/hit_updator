package internal

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func MongoUpdater(mongoCNN string) {
	mongoClient := Dbclient(mongoCNN)

	for {
		task := <-UpdateJobRegister
		log.Printf("update task recieved by mongo updater %+v", task)
		pid := task["product_id"]
		index := task["image_id"]
		coll := task["vendor"]
		file := task["path"]
		exist := checkExist(mongoClient, pid, coll)
		if exist {
			Update(mongoClient, pid, coll, index, file)
		} else {
			log.Println("ID does not exist !")
		}
	}
}

func checkExist(connection *mongo.Client, keyname string, collection string) bool {
	coll := connection.Database("kafka-based").Collection(collection)
	var result bson.M
	err := coll.FindOne(context.TODO(), bson.M{"_id": keyname}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return false
		}
	}
	return true

}
func Update(connection *mongo.Client, keyname string, collection string, idx string, newaddress string) bool {
	coll := connection.Database("kafka-based").Collection(collection)
	result := coll.FindOneAndUpdate(
		context.Background(),
		bson.D{
			{"_id", keyname},
		},
		bson.M{"$set": bson.M{"product.media.$[elem].url": newaddress}},
		options.FindOneAndUpdate().SetArrayFilters(options.ArrayFilters{
			Filters: []interface{}{bson.M{"elem.id": idx}},
		}),
	)
	if result != nil {
		log.Printf("Succesfully updated %s in image index %s", keyname, idx)

	} else {
		log.Printf("something went wrong with doneJob task register%s", keyname, idx)
	}

	//if err != nil {
	//	fmt.Printf("\nMONGO UPDATE RESULT IS %s\n", err)
	//}
	return true

}

func Dbclient(databaseAddress string) *mongo.Client {

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(databaseAddress))
	if err != nil {
		Detail := fmt.Sprintf("mongo-db connection failure ! %s %v", databaseAddress, err)
		log.Errorln(Detail)
	}
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		panic(err)
	}

	return client
}
