package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emrserverless"
	_ "github.com/lib/pq"
)

type FailureResponse struct {
	Message string `json:message`
}

type SuccessResponse struct {
	JobId     string `json:"jobId"`
	JobStatus string `json:"jobStatus"`
	Message   string `json:"message,omitempty"`
}

func SkyflowValidation(token string, vaultId string) bool {
	client := &http.Client{Timeout: 1 * time.Minute}
	var managementUrl = os.Getenv("MANAGEMENT_URL")
	var url = managementUrl + "/v1/vaults/" + vaultId

	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Add("Accept", "apaplication/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", "Bearer "+token)

	response, err := client.Do(request)
	if err != nil {
		log.Printf("Got error on Skyflow Validation request: %v\n", err.Error())
		return false
	}

	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		defer response.Body.Close()

		log.Printf("Got status on Skyflow Validation: %v\n", response.StatusCode)
		log.Printf("Got response on Skyflow Validation: %v\n", string(responseBody))
		return false
	}
	return true
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	apiResponse := events.APIGatewayProxyResponse{}

	token := request.Headers["Authorization"]

	vaultId := request.PathParameters["vaultID"]
	jobId := request.PathParameters["jobID"]

	validation := SkyflowValidation(token, vaultId)
	if !validation {
		responseBody, _ := json.Marshal(FailureResponse{
			Message: "Failed on Skyflow Validation",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest

		return apiResponse, nil
	}

	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	databaseName := os.Getenv("DB_NAME")

	connection := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host,
		port,
		user,
		password,
		databaseName,
	)
	db, _ := sql.Open("postgres", connection)

	if request.HTTPMethod == "GET" {
		var recordJobId string
		var recordJobStatus string
		statement := `select job_id, job_status from "job_details" where job_id=$1`

		log.Printf("Checking record for jobId: %v\n", jobId)
		record := db.QueryRow(statement, jobId)

		switch err := record.Scan(&recordJobId, &recordJobStatus); err {
		case sql.ErrNoRows:
			responseBody, _ := json.Marshal(FailureResponse{
				Message: fmt.Sprintf("No record found for jobId:%v\n", jobId),
			})

			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusBadRequest
		case nil:
			responseBody, _ := json.Marshal(SuccessResponse{
				JobId:     recordJobId,
				JobStatus: recordJobStatus,
			})

			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusOK
		default:
			responseBody, _ := json.Marshal(FailureResponse{
				Message: fmt.Sprintf("Failed to check record for jobId: %v with error: %v\n", jobId, err.Error()),
			})

			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusBadRequest
		}
	}

	if request.HTTPMethod == "DELETE" {
		sess, _ := session.NewSession(&aws.Config{
			Region: aws.String(os.Getenv("REGION")),
		})

		service := emrserverless.New(sess)

		params := &emrserverless.CancelJobRunInput{
			ApplicationId: aws.String(os.Getenv("APPLICATION_ID")),
			JobRunId:      aws.String(jobId),
		}

		_, err := service.CancelJobRun(params)

		if err != nil {
			responseBody, _ := json.Marshal(FailureResponse{
				Message: fmt.Sprintf("Failed to cancel job for jobId: %v with error: %v\n", jobId, err.Error()),
			})

			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusBadRequest
		}

		responseBody, _ := json.Marshal(SuccessResponse{
			JobId:   jobId,
			Message: "Successfully deleted",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusOK
	}

	return apiResponse, nil
}

func main() {
	lambda.Start(HandleRequest)
}
