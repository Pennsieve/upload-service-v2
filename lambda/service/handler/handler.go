package handler

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/pennsieve/pennsieve-go-api/models/dataset"
	"github.com/pennsieve/pennsieve-go-api/models/dbTable"
	"github.com/pennsieve/pennsieve-go-api/models/organization"
	"github.com/pennsieve/pennsieve-go-api/models/permissions"
	"log"
	"regexp"
)

// ManifestHandler handles requests to the API V2 /manifest endpoints.
func ManifestHandler(request events.APIGatewayV2HTTPRequest) (*events.APIGatewayV2HTTPResponse, error) {

	var apiResponse *events.APIGatewayV2HTTPResponse
	var err error

	r := regexp.MustCompile(`(?P<method>) (?P<pathKey>.*)`)
	routeKeyParts := r.FindStringSubmatch(request.RouteKey)
	routeKey := routeKeyParts[r.SubexpIndex("pathKey")]

	claims := parseClaims(request)
	authorized := false
	switch routeKey {
	case "/manifest":
		fmt.Println("Handling /manifest request")
		if authorized = hasRole(*claims, permissions.CreateDeleteFiles); authorized {
			apiResponse, err = handleManifestRoute(request, claims)
		}
	case "/manifest/{id}":
		//if authorized = checkOwner(*claims, manifestId); authorized {
		//	apiResponse, err = handleManifestIdRoute(request, claims)
		//}
	case "/manifest/{id}/remove":
		//if authorized = checkOwner(*claims, manifestId); authorized {
		//	apiResponse, err = handleManifestIdRemoveRoute(request, claims)
		//}
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

// Claims is an object containing claims and user info
type Claims struct {
	orgClaim       organization.Claim
	datasetClaim   dataset.Claim
	userId         int64
	isSuperAdmin   bool
	organizationId int64
}

// parseClaims parses the claims in the provided request.
func parseClaims(request events.APIGatewayV2HTTPRequest) *Claims {

	claims := request.RequestContext.Authorizer.Lambda

	var orgClaim organization.Claim
	if val, ok := claims["org_claim"]; ok {
		orgClaims := val.(map[string]interface{})
		orgRole := int64(orgClaims["Role"].(float64))
		orgClaim = organization.Claim{
			Role:            dbTable.DbPermission(orgRole),
			IntId:           int64(orgClaims["IntId"].(float64)),
			EnabledFeatures: nil,
		}
	}

	var datasetClaim dataset.Claim
	if val, ok := claims["dataset_claim"]; ok {
		if val != nil {
			datasetClaims := val.(map[string]interface{})
			datasetRole := int64(datasetClaims["Role"].(float64))
			datasetClaim = dataset.Claim{
				Role:   dataset.Role(datasetRole),
				NodeId: datasetClaims["NodeId"].(string),
				IntId:  int64(datasetClaims["IntId"].(float64)),
			}
		}
	}

	userId := int64(claims["user_id"].(float64))
	isSuperAdmin := claims["is_super_admin"].(bool)
	organizationId := int64(claims["organization_id"].(float64))

	returnedClaims := Claims{
		orgClaim:       orgClaim,
		datasetClaim:   datasetClaim,
		userId:         userId,
		isSuperAdmin:   isSuperAdmin,
		organizationId: organizationId,
	}

	fmt.Println(returnedClaims)

	return &returnedClaims

}

// hasRole returns a boolean indicating whether the user has the correct permissions.
func hasRole(claims Claims, permission permissions.DatasetPermission) bool {

	//hasOrgRole := claims.orgClaim.Role >= dbTable.Delete

	hasValidPermissions := permissions.HasDatasetPermission(claims.datasetClaim.Role, permission)

	return hasValidPermissions

}
