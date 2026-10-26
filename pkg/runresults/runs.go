// Package runresults imports a workflow run's results into a dataset.
//
// A run's "Pennsieve Dataset" data target copies its input to
// s3://workflow-logs-<account>-<env>-<node>/imports/<runId>/<targetId>/ in
// the compute account (compute-node-aws-provisioner-v2's stage-results),
// then asks upload-service to import those files with the run's callback
// token. As in download-service's run archives, the token is checked by
// workflow-service's callback-validator Lambda, which names the run's
// compute node and its dataset targets; the bucket and the role that reads
// it follow from the node and its account (account-service's tables), and
// the dataset and folder from the target. Nothing about where to read or
// write comes from the request, only which files below the target's prefix.
//
// The service lambda registers the files on a manifest and queues a Job per
// file; the run-import lambda copies each one into the dataset's storage
// (reading with the compute account's role, writing with the storage
// credentials role) and hands it to the upload lambda, which creates the
// package as for any direct-to-storage upload.
package runresults

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// ErrUnauthorized means the callback token isn't valid for the run: wrong,
// or the run is over.
var ErrUnauthorized = errors.New("callback token not valid for this run")

// Run is a run whose callback token checked out.
type Run struct {
	Id            string
	UserNodeId    string // the run's creator
	OrgNodeId     string // empty for a personal run
	ComputeNodeId string
	// DatasetTargets are the run's data targets that write to a dataset.
	DatasetTargets []DatasetTarget
}

// DatasetTarget is one of a run's data targets that writes to a dataset, as
// the run says (workflow-service's RunDatasetTarget).
type DatasetTarget struct {
	NodeId         string `json:"nodeId"`
	TargetType     string `json:"targetType"`
	DatasetNodeId  string `json:"datasetNodeId"`
	TargetFolder   string `json:"targetFolder,omitempty"`
	OverwriteFiles bool   `json:"overwriteFiles,omitempty"`
}

// TargetTypePennsieveDataset is the target type that imports into a dataset.
const TargetTypePennsieveDataset = "Pennsieve Dataset"

// ErrNotATarget means the request names no dataset import of the run's.
var ErrNotATarget = errors.New("not one of the run's dataset targets")

// ImportTarget is the run's dataset target targetId when it imports into
// datasetNodeId.
func (r Run) ImportTarget(targetId, datasetNodeId string) (DatasetTarget, error) {
	for _, t := range r.DatasetTargets {
		if t.NodeId != targetId {
			continue
		}
		if t.TargetType != TargetTypePennsieveDataset || t.DatasetNodeId != datasetNodeId {
			return DatasetTarget{}, ErrNotATarget
		}
		return t, nil
	}
	return DatasetTarget{}, ErrNotATarget
}

// Token reads a callback token, "Callback <service>:<runId>:<token>".
func Token(authorization string) (service, runId, token string, ok bool) {
	payload, found := strings.CutPrefix(authorization, "Callback ")
	if !found {
		return "", "", "", false
	}
	parts := strings.SplitN(payload, ":", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// LambdaAPI is the subset of the Lambda client used.
type LambdaAPI interface {
	Invoke(ctx context.Context, in *lambda.InvokeInput, opts ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// Validator checks callback tokens with workflow-service's callback-validator
// Lambda (direct invocation, as the platform authorizers do).
type Validator struct {
	Lambda       LambdaAPI
	FunctionName string
}

type validateRequest struct {
	CallbackToken  string `json:"callbackToken"`
	ExecutionRunId string `json:"executionRunId"`
}

type validateResponse struct {
	IsAuthorized       bool            `json:"isAuthorized"`
	UserNodeId         string          `json:"userNodeId,omitempty"`
	OrganizationNodeId string          `json:"organizationNodeId,omitempty"`
	ComputeNodeId      string          `json:"computeNodeId,omitempty"`
	DatasetTargets     []DatasetTarget `json:"datasetTargets,omitempty"`
	Error              string          `json:"error,omitempty"`
}

// Validate returns the run a callback token is valid for, or ErrUnauthorized.
func (v *Validator) Validate(ctx context.Context, runId, token string) (Run, error) {
	payload, err := json.Marshal(validateRequest{CallbackToken: token, ExecutionRunId: runId})
	if err != nil {
		return Run{}, err
	}
	out, err := v.Lambda.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(v.FunctionName),
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
		Payload:        payload,
	})
	if err != nil {
		return Run{}, fmt.Errorf("invoking the callback validator: %w", err)
	}
	if out.FunctionError != nil {
		return Run{}, fmt.Errorf("callback validator failed: %s", aws.ToString(out.FunctionError))
	}
	var resp validateResponse
	if err := json.Unmarshal(out.Payload, &resp); err != nil {
		return Run{}, fmt.Errorf("decoding the callback validator's response: %w", err)
	}
	if !resp.IsAuthorized {
		return Run{}, fmt.Errorf("%w: %s", ErrUnauthorized, resp.Error)
	}
	if resp.UserNodeId == "" || resp.ComputeNodeId == "" {
		return Run{}, fmt.Errorf("callback validator returned no user or compute node for run %s", runId)
	}
	return Run{Id: runId, UserNodeId: resp.UserNodeId, OrgNodeId: resp.OrganizationNodeId, ComputeNodeId: resp.ComputeNodeId, DatasetTargets: resp.DatasetTargets}, nil
}

// computeRoleRE matches the compute-account roles a run's files are read
// with (account-service's: Pennsieve-Compute-* from the CloudFormation
// template, ROLE-* for older accounts).
var computeRoleRE = regexp.MustCompile(`^arn:aws:iam::\d{12}:role/(Pennsieve-Compute-|ROLE-)[A-Za-z0-9+=,.@_-]+$`)

// Results is where a run's results are and how to read them.
type Results struct {
	Bucket  string
	RoleArn string
	// ExternalId is the account's external ID, when its role's trust policy
	// requires one (accounts connected through CloudFormation).
	ExternalId string
	Region     string
}

// Prefix is the key prefix of one data target's staged files in the run's
// node bucket: imports/<runId>/<targetId>/, apart from a download target's
// results/<runId>/.
func Prefix(runId, targetId string) string {
	return "imports/" + runId + "/" + targetId + "/"
}

// DynamoAPI is the subset of the DynamoDB client used.
type DynamoAPI interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

// Locator finds a run's results from account-service's compute node and
// account records.
type Locator struct {
	DB            DynamoAPI
	NodesTable    string
	AccountsTable string
	// Region is where compute nodes run; the provisioner deploys them in the
	// platform's region.
	Region string
}

type node struct {
	AccountUuid string `dynamodbav:"accountUuid"`
	AccountId   string `dynamodbav:"accountId"`
	Env         string `dynamodbav:"environment"`
	Identifier  string `dynamodbav:"identifier"`
}

type account struct {
	AccountId string `dynamodbav:"accountId"`
	RoleName  string `dynamodbav:"roleName"`
	// RuntimeRoleName is the account's narrower runtime role (template v2),
	// used for reads when set; older accounts have only RoleName.
	RuntimeRoleName string `dynamodbav:"runtimeRoleName,omitempty"`
	ExternalId      string `dynamodbav:"externalId,omitempty"`
}

// Locate returns where run's results are.
func (l *Locator) Locate(ctx context.Context, run Run) (Results, error) {
	var n node
	if err := l.get(ctx, l.NodesTable, run.ComputeNodeId, &n); err != nil {
		return Results{}, fmt.Errorf("compute node %s: %w", run.ComputeNodeId, err)
	}
	if n.AccountUuid == "" || n.Env == "" || n.Identifier == "" {
		return Results{}, fmt.Errorf("compute node %s has no account, environment or identifier", run.ComputeNodeId)
	}
	var a account
	if err := l.get(ctx, l.AccountsTable, n.AccountUuid, &a); err != nil {
		return Results{}, fmt.Errorf("account %s: %w", n.AccountUuid, err)
	}
	accountId := a.AccountId
	if accountId == "" {
		accountId = n.AccountId
	}
	roleName := a.RuntimeRoleName
	if roleName == "" {
		roleName = a.RoleName
	}
	if accountId == "" || roleName == "" {
		return Results{}, fmt.Errorf("account %s has no AWS account id or role", n.AccountUuid)
	}
	return Results{
		// The provisioner's workflow bucket (terraform/infrastructure,
		// aws_s3_bucket.workflow_logs).
		Bucket:     fmt.Sprintf("workflow-logs-%s-%s-%s", accountId, n.Env, n.Identifier),
		RoleArn:    fmt.Sprintf("arn:aws:iam::%s:role/%s", accountId, roleName),
		ExternalId: a.ExternalId,
		Region:     l.Region,
	}, nil
}

func (l *Locator) get(ctx context.Context, table, uuid string, into any) error {
	out, err := l.DB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key:       map[string]types.AttributeValue{"uuid": &types.AttributeValueMemberS{Value: uuid}},
	})
	if err != nil {
		return err
	}
	if len(out.Item) == 0 {
		return errors.New("not found")
	}
	return attributevalue.UnmarshalMap(out.Item, into)
}
