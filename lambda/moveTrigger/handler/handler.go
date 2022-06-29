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
	TaskDefinitionArn = os.Getenv("TASK_DEF_ARN")
	subIdStr := os.Getenv("SUBNET_IDS")
	SubNetIds = strings.Split(subIdStr, ",")
	cluster = os.Getenv("CLUSTER_ARN")
	SecurityGroup = os.Getenv("SECURITY_GROUP")
}

// MoveTriggerHandler starts the upload-move fargate task if task is not running.
func MoveTriggerHandler(ctx context.Context, sqsEvent events.SQSEvent) error {

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	client := ecs.NewFromConfig(cfg)
	// Check if fargate is already running

	params := ecs.ListTasksInput{
		Cluster: &cluster,
	}

	result, err := client.ListTasks(context.Background(), &params)
	if err != nil {
		log.Fatalf("Problem with describing tasks: %v", err)
	}

	if len(result.TaskArns) > 0 {
		input := ecs.DescribeTasksInput{
			Tasks:   result.TaskArns,
			Cluster: &cluster,
		}

		tasks, err := client.DescribeTasks(context.Background(), &input)
		if err != nil {
			log.Fatalf("Problem with describing tasks: %v", err)
		}

		for _, t := range tasks.Tasks {
			log.Printf("Task status: %s\n", *t.LastStatus)
			if contains([]string{"RUNNING", "PROVISIONING", "PENDING", "ACTIVATING"}, *t.LastStatus) {
				log.Println("Upload Fargate Task already running --> returning.")
				return nil

			}
		}
	}

	// If task is not running yet, start task.
	log.Println("Initiating new Upload Move Files Fargate Task.")
	runTaskIn := &ecs.RunTaskInput{
		TaskDefinition: aws.String(TaskDefinitionArn),
		Cluster:        aws.String(cluster),
		NetworkConfiguration: &types.NetworkConfiguration{
			AwsvpcConfiguration: &types.AwsVpcConfiguration{
				Subnets:        SubNetIds,
				SecurityGroups: []string{SecurityGroup},
				AssignPublicIp: types.AssignPublicIpEnabled,
			},
		},
		LaunchType: types.LaunchTypeFargate,
	}

	_, err = client.RunTask(context.TODO(), runTaskIn)
	if err != nil {
		return err
	}

	return nil
}

// https://play.golang.org/p/Qg_uv_inCek
// contains checks if a string is present in a slice
func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
