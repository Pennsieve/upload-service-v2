package handler

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pennsieve/pennsieve-go-core/pkg/authorizer"
	"github.com/pennsieve/pennsieve-go-core/pkg/models/permissions"
	log "github.com/sirupsen/logrus"
	"os"
	"regexp"
)

//var manifestFileTableName string
//var manifestTableName string
//var client *dynamodb.Client

var store *UploadServiceStore

// init runs on cold start of lambda and gets jwt key-sets from Cognito user pools.
func init() {

	log.SetFormatter(&log.JSONFormatter{})
	ll, err := log.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(ll)
	}

	manifestFileTableName := os.Getenv("MANIFEST_FILE_TABLE")
	manifestTableName := os.Getenv("MANIFEST_TABLE")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	s3Client := s3.NewFromConfig(cfg)
	lambdaClient := lambda.NewFromConfig(cfg)

	store = NewUploadServiceStore(client, s3Client, lambdaClient, manifestFileTableName, manifestTableName)

}

// ManifestHandler handles requests to the API V2 /manifest endpoints.
func ManifestHandler(request events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {

	var apiResponse *events.APIGatewayV2HTTPResponse
	var err error

	r := regexp.MustCompile(`(?P<method>) (?P<pathKey>.*)`)
	routeKeyParts := r.FindStringSubmatch(request.RouteKey)
	routeKey := routeKeyParts[r.SubexpIndex("pathKey")]

	claims := authorizer.ParseClaims(request.RequestContext.Authorizer.Lambda)
	authorized := false

	switch routeKey {
	case "/manifest":
		switch request.RequestContext.HTTP.Method {
		case "GET":
			if authorized = authorizer.HasRole(*claims, permissions.ViewFiles); authorized {
				apiResponse, err = getManifestRoute(request, claims)
			}
		case "POST":
			if authorized = authorizer.HasRole(*claims, permissions.CreateDeleteFiles); authorized {
				apiResponse, err = postManifestRoute(request, claims)
			}
		}
	case "/manifest/files":
		switch request.RequestContext.HTTP.Method {
		case "GET":
			if authorized = authorizer.HasRole(*claims, permissions.ViewFiles); authorized {
				apiResponse, err = getManifestFilesRoute(request, claims)
			}
		}
	case "/manifest/status":
		switch request.RequestContext.HTTP.Method {
		case "GET":
			if authorized = authorizer.HasRole(*claims, permissions.ViewFiles); authorized {
				apiResponse, err = getManifestFilesStatusRoute(request, claims)
			}
		}
	case "/manifest/{id}/archive":
		switch request.RequestContext.HTTP.Method {
		case "GET":
			// Return zipped file containing the manifest

			//if authorized = authorizer.HasRole(*claims, permissions.ViewFiles); authorized {
			//	apiResponse, err = getManifestRoute(request, claims)
			//}
		case "DELETE":
			// Completely removes a previously archived manifest (archive must be archived before deleting)

			//if authorized = authorizer.HasRole(*claims, permissions.ViewFiles); authorized {
			//	apiResponse, err = getManifestRoute(request, claims)
			//}
		case "POST":
			// Archive manifest
			if authorized = authorizer.HasRole(*claims, permissions.CreateDeleteFiles); authorized {
				apiResponse, err = postManifestArchiveRoute(request, claims)
			}
		}
	case "/manifest/{id}/updates":
		//if authorized = checkOwner(*claims, manifestId); authorized {
		//	apiResponse, err = handleManifestIdUpdatesRoute(request, claims)
		//}
	default:
		log.Fatalln("Incorrect Route")
	}

	// Return unauthorized if
	if !authorized {
		apiResponse := events.APIGatewayV2HTTPResponse{
			StatusCode: 403,
			Body:       `{"message": "User is not authorized to perform this action on the dataset."}`,
		}
		return &apiResponse, nil
	}

	// Response
	if err != nil {
		log.Fatalln("Something is wrong with creating the response", err)
	}

	return apiResponse, nil
}
