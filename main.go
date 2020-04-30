package main

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

var bg = context.Background()

type MyDoc struct {
	ID   primitive.ObjectID `bson:"_id"`
	Name string
}

// https://docs.mongodb.com/v3.6/reference/change-events/
type ChangedMyDoc struct {
	Op           string `bson:"operationType"` // delete, insert, replace
	FullDocument MyDoc  `bson:"fullDocument"`
	DocumentKey  struct {
		ID primitive.ObjectID `bson:"_id"`
	} `bson:"documentKey"`
}

func main() {
	// How to setup a local replica set https://gist.github.com/davisford/bb37079900888c44d2bbcb2c52a5d6e8
	client, err := mongo.Connect(bg, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}

	db := client.Database("pubsub")
	col := db.Collection("pubsub")

	go func() {
		for {
			time.Sleep(time.Second)
			doc := &MyDoc{
				ID:   primitive.NewObjectID(),
				Name: "One",
			}
			_, err := col.InsertOne(bg, doc)
			if err != nil {
				panic(err)
			}

			time.Sleep(time.Second)
			doc.Name = "Two"
			err = col.FindOneAndReplace(bg, bson.M{"_id": doc.ID}, doc, options.FindOneAndReplace().SetReturnDocument(options.After)).Decode(&doc)
			if err != nil {
				panic(err)
			}
			if doc.Name != "Two" {
				panic(doc.Name)
			}

			time.Sleep(time.Second)
			delResult, err := col.DeleteOne(bg, bson.M{"_id": doc.ID})
			if err != nil {
				panic(err)
			}
			if delResult.DeletedCount != 1 {
				panic("Maybe you put in the wrong ID somehow")
			}
		}
	}()

	fullUpdateDocument := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	stream, err := col.Watch(bg, mongo.Pipeline{}, fullUpdateDocument)
	if err != nil {
		panic(err)
	}
	defer stream.Close(bg)

	for stream.Next(bg) {
		var data ChangedMyDoc
		if err = stream.Decode(&data); err != nil {
			panic(err)
		}
		if data.Op == "delete" {
			log.Println(data.Op, data.DocumentKey.ID)
		} else {
			log.Println(data.Op, data.FullDocument)
		}
	}
}
