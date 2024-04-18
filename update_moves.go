package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"regexp"
	"strings"
)

func main() {
	fmt.Println("Start Parsing")

	if err := godotenv.Load(); err != nil {
		fmt.Println("No .env file found")
	}

	mongoUri := os.Getenv("MONGODB_URI")
	mongoDatabase := os.Getenv("MONGODB_DATABASE")
	mongoCollection := os.Getenv("MONGODB_COLLECTION")

	// MongoDB Client
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoUri))
	if err != nil {
		fmt.Println("Failed to connect to MongoDB:", err)
		return
	}
	defer client.Disconnect(context.Background())

	// Collection
	collection := client.Database(mongoDatabase).Collection(mongoCollection)

	// Get current move from mongodb
	existingMoves, err := getExistingMoves(collection)
	if err != nil {
		fmt.Println("Failed to get existing moves:", err)
		return
	}

	// Use our filter
	cleanedExistingMoves := cleanMoves(existingMoves)

	// Create updating for Mongo
	update := bson.D{{"$set", bson.D{{"moves", cleanedExistingMoves}}}}

	// Confirm to collection
	_, err = collection.UpdateMany(context.Background(), bson.D{}, update)
	if err != nil {
		fmt.Println("Failed to update documents in MongoDB:", err)
		return
	}

	fmt.Println("Documents updated successfully.")
}

func getExistingMoves(collection *mongo.Collection) (string, error) {
	// Get one document from collection
	var result struct {
		Moves string `bson:"moves"`
	}
	err := collection.FindOne(context.Background(), bson.D{}).Decode(&result)
	if err != nil {
		return "", err
	}
	return result.Moves, nil
}

// Clear Moves from Numbers & Notations
func cleanMoves(moves string) string {
	moves = regexp.MustCompile(`\{[^}]*\}|\b\d+\.|\d+-\d+|\.`).ReplaceAllString(moves, "")
	return strings.Join(strings.Fields(moves), " ")
}
