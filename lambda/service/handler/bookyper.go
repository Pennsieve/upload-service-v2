package handler


// postManifestRoute synchronizes manifests with a provided ID
func postBookyperRoute(request events.APIGatewayV2HTTPRequest, claims *Claims) (*events.APIGatewayV2HTTPResponse, error) {
	fmt.Println("Handling POST /manifest request")

	apiResponse := events.APIGatewayV2HTTPResponse{}

	// PARSING INPUTS
	//validates json and returns error if not working
	err := fastjson.Validate(request.Body)
	if err != nil {
		message := "Error: Invalid JSON payload ||| " + fmt.Sprint(err) + " Body Obtained" + "||||" + request.Body
		apiResponse = events.APIGatewayV2HTTPResponse{
			Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
		return &apiResponse, nil
	}

	// Unmarshal JSON into Manifest DTOs
	bytes := []byte(request.Body)
	var res manifest.DTO
	json.Unmarshal(bytes, &res)

	fmt.Println("SessionID: ", res.ID, " NrFiles: ", len(res.Files))

	s := manifestPkg.ManifestSession{
		FileTableName: manifestFileTableName,
		TableName:     manifestTableName,
		Client:        client,
	}

	// ADDING MANIFEST IF NEEDED
	var activeManifest *dbTable.ManifestTable
	if res.ID == "" {
		log.Printf("Creating new manifest")
		// Create new manifest
		activeManifest = &dbTable.ManifestTable{
			ManifestId:     uuid.New().String(),
			DatasetId:      claims.datasetClaim.IntId,
			DatasetNodeId:  claims.datasetClaim.NodeId,
			OrganizationId: claims.organizationId,
			UserId:         claims.userId,
			Status:         manifest.Initiated.String(),
			DateCreated:    time.Now().Unix(),
		}

		s.CreateManifest(*activeManifest)

	} else {
		// Check that manifest exists.
		log.Printf("Has existing manifest")

		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, fmt.Errorf("LoadDefaultConfig: %v\n", err)
		}

		// Create an Amazon DynamoDB client.
		client := dynamodb.NewFromConfig(cfg)

		activeManifest, err = dbTable.GetFromManifest(client, manifestTableName, res.ID)
		if err != nil {
			message := "Error: Invalid ManifestID |||| Manifest ID: " + res.ID
			apiResponse = events.APIGatewayV2HTTPResponse{
				Body: gateway.CreateErrorMessage(message, 500), StatusCode: 500}
			return &apiResponse, nil
		}

	}

	// 	MERGE PACKAGES FOR SPECIFIC FILETYPES
	s.PackageTypeResolver(res.Files)

	// ADDING FILES TO MANIFEST
	addFilesResponse, err := s.AddFiles(activeManifest.ManifestId, res.Files, nil)

	// CREATING API RESPONSE
	responseBody := manifest.PostResponse{
		ManifestNodeId: activeManifest.ManifestId,
		UpdatedFiles:   addFilesResponse.FileStatus,
		NrFilesUpdated: addFilesResponse.NrFilesUpdated,
		NrFilesRemoved: addFilesResponse.NrFilesRemoved,
		FailedFiles:    addFilesResponse.FailedFiles,
	}

	jsonBody, _ := json.Marshal(responseBody)
	apiResponse = events.APIGatewayV2HTTPResponse{Body: string(jsonBody), StatusCode: 200}

	return &apiResponse, nil
}
