package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/moacirtorress/go-csv-parser/models"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	records, err := readData("person.csv")
	if err != nil {
		log.Fatal("Erro ao ler dados de arquivo csv!")
	}

	for _, record := range records {
		user := models.Person{
			Id:      record[0],
			Name:    record[1],
			Age:     record[2],
			NotUsed: record[3],
		}
		fmt.Printf("Id %s Name %s Age %s NotUsed %s\n\n\n\n", user.Id, user.Name, user.Age, user.NotUsed)

		connectToDB(user)
	}

}

func connectToDB(user models.Person) {
	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	clientOptions := options.Client().
		ApplyURI("mongodb+srv://moatorres:abacate123@cluster1.l5qph.mongodb.net/myFirstDatabase?retryWrites=true&w=majority").
		SetServerAPIOptions(serverAPIOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database("cluster1").Collection("people")

	// Preparing to connect to mongo...
	result, err := collection.InsertOne(ctx, user)
	if err != nil {
		log.Fatal("Error:", err)
	}

	fmt.Println(result)
}

func readData(fileName string) ([][]string, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return [][]string{}, err
	}

	defer f.Close()

	r := csv.NewReader(f)

	// we have to skip first line
	if _, err := r.Read(); err != nil {
		return [][]string{}, err
	}

	records, err := r.ReadAll()
	if err != nil {
		return [][]string{}, err
	}

	return records, nil
}
