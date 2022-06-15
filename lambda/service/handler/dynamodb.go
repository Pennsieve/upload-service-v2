package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/pennsieve/pennsieve-go-api/models/manifest"
	"log"
)

type manifestTable struct {
	ManifestId string `dynamodbav:"ManifestId"`
	DatasetId  string `dynamodbav:"DatasetId"`
	UserId     int64  `dynamodbav:"UserId"`
	Status     string `dynamodbav:"Status"`
}

type manifestFileTable struct {
	ManifestId string `dynamodbav:"ManifestId"`
	UploadId   string `dynamodbav:"UploadId"`
	FilePath   string `dynamodbav:"FilePath,omitempty"`
	FileName   string `dynamodbav:"FileName"`
	Status     string `dynamodbav:"Status"`
}

func getFromManifest(manifestId string) (*manifestTable, error) {

	item := manifestTable{}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("LoadDefaultConfig: %v\n", err)
	}

	// Create an Amazon DynamoDB client.
	client := dynamodb.NewFromConfig(cfg)

	data, err := client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("dev-manifest-table-use1"),
		Key: map[string]types.AttributeValue{
			"ManifestId": &types.AttributeValueMemberS{Value: manifestId},
		},
	})

	if err != nil {
		return &item, fmt.Errorf("GetItem: %v\n", err)
	}

	if data.Item == nil {
		return &item, fmt.Errorf("GetItem: Manifest not found.\n")
	}

	err = attributevalue.UnmarshalMap(data.Item, &item)
	if err != nil {
		return &item, fmt.Errorf("UnmarshalMap: %v\n", err)
	}

	return &item, nil

}

func createManifest(item manifestTable) error {

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Printf("LoadDefaultConfig: %v\n\n", err)
		return fmt.Errorf("LoadDefaultConfig: %v\n", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	data, err := attributevalue.MarshalMap(item)
	if err != nil {
		log.Printf("MarshalMap: %v\n", err)
		return fmt.Errorf("MarshalMap: %v\n", err)
	}

	fmt.Println(data)

	_, err = client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("dev-manifest-table-use1"),
		Item:      data,
	})

	if err != nil {
		log.Printf("PutItem: %v\n", err)
		return fmt.Errorf("PutItem: %v\n", err)
	}

	return nil
}

func createFiles(items []manifest.FileDTO) error {

}
