package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"log"
	"os"
	"strings"
)

type AppConfig struct {
	Cluster           string `json:"cluster"`
	TaskDefinitionArn string `json:"taskDefinitionArn"`
	SubnetID          string `json:"subnetId"`
	Region            string `json:"region"`
}

var configJson []byte
var TaskDefinitionArn string
var SubNetIds []string
var SecurityGroup string
var cluster string

func init() {

	// Get relevant info from SSM
	log.Println("In init")
	TaskDefinitionArn = os.Getenv("TASK_DEF_ARN")
	subIdStr := os.Getenv("SUBNET_IDS")
	SubNetIds = strings.Split(subIdStr, ",")
	cluster = os.Getenv("CLUSTER_ARN")

}

// MoveTriggerHandler starts the upload-move fargate task.
func MoveTriggerHandler(ctx context.Context, sqsEvent events.SQSEvent) error {

	log.Println("In Handler")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	log.Println(TaskDefinitionArn)
	log.Println(cluster)
	log.Println(SubNetIds)

	client := ecs.NewFromConfig(cfg)
	runTaskIn := &ecs.RunTaskInput{
		TaskDefinition: aws.String(TaskDefinitionArn),
		Cluster:        aws.String(cluster),
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        SubNetIds,
				SecurityGroups: []string{"dev-up-6916"},
				AssignPublicIp: types.AssignPublicIpEnabled,
			},
		},
		LaunchType: types.LaunchTypeFargate,
	}

	_, rerr := client.RunTask(context.TODO(), runTaskIn)
	if rerr != nil {
		return rerr
	}

	return nil
}
