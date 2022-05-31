package handler

import (
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/valyala/fastjson"
)

func Handler(request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {

	fmt.Println("Hello event.")
	ApiResponse := events.APIGatewayV2HTTPResponse{}
	// Switch for identifying the HTTP request
	switch request.RequestContext.HTTP.Method {
	case "GET":
		// Obtain the QueryStringParameter
		name := request.QueryStringParameters["name"]
		if name != "" {
			ApiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        200,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Hey " + name + " welcome! ",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		} else {
			ApiResponse = events.APIGatewayV2HTTPResponse{
				StatusCode:        500,
				Headers:           nil,
				MultiValueHeaders: nil,
				Body:              "Error: Query Parameter name missing",
				IsBase64Encoded:   false,
				Cookies:           nil,
			}
		}

	case "POST":
		//validates json and returns error if not working
		err := fastjson.Validate(request.Body)

		if err != nil {
			body := "Error: Invalid JSON payload ||| " + fmt.Sprint(err) + " Body Obtained" + "||||" + request.Body
			ApiResponse = events.APIGatewayV2HTTPResponse{Body: body, StatusCode: 500}
		} else {
			ApiResponse = events.APIGatewayV2HTTPResponse{Body: request.Body, StatusCode: 200}
		}
	}
	// Response
	return ApiResponse, nil
}
