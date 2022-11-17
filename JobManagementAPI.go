package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emrserverless"
	_ "github.com/lib/pq"
)

type FailureResponse struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

type SuccessResponse struct {
	Id        string `json:"id"`
	JobId     string `json:"jobId"`
	RequestId string `json:"requestId"`
	JobStatus string `json:"jobStatus,omitempty"`
	Message   string `json:"message,omitempty"`
}

type JobDetail struct {
	Id          string `json:"id"`
	JobId       string `json:"jobId"`
	JobStatus   string `json:"jobStatus"`
	RequestId   string `json:"requestId"`
	Query       string `json:"query"`
	Destination string `json:"destination"`
}

type SkyflowAuthorizationResponse struct {
	RequestId    string `json:"requestId"`
	StatusCode   int    `json:"statusCode"`
	ResponseBody string `json:"responseBody"`
	Error        string `json:"error"`
}

var db *sql.DB
var managementUrl string
var service *emrserverless.EMRServerless
var applicationId string
var validVaultIds []string

func init() {
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
	db, _ = sql.Open("postgres", connection)

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(os.Getenv("REGION")),
	})
	service = emrserverless.New(sess)

	managementUrl = os.Getenv("MANAGEMENT_URL")

	applicationId = os.Getenv("APPLICATION_ID")

	validVaultIds = strings.Split(os.Getenv("VALID_VAULT_IDS"), ",")
}

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	apiResponse := events.APIGatewayProxyResponse{}

	id := request.PathParameters["jobID"]

	log.Printf("%v-> Initiated with id: %v", id, id)

	clientIpAddress := strings.Split(request.Headers["X-Forwarded-For"], ",")[0]
	log.Printf("%v-> Client IP address: %v", id, clientIpAddress)

	vaultId := request.PathParameters["vaultID"]
	token := request.Headers["Authorization"]

	authSchemeValidation := ValidateAuthScheme(token)
	if !authSchemeValidation {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: "Auth Scheme not supported",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusUnauthorized

		return apiResponse, nil
	}

	validVaultIdValidation := ValidateVaultId(vaultId)
	if !validVaultIdValidation {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: "Invalid Vault ID",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusForbidden

		return apiResponse, nil
	}

	authResponse := SkyflowAuthorization(token, vaultId, id)
	if authResponse.Error != "" {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: authResponse.Error,
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = authResponse.StatusCode
		return apiResponse, nil
	}

	if authResponse.StatusCode != http.StatusOK {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: authResponse.ResponseBody,
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = authResponse.StatusCode
		return apiResponse, nil
	}

	log.Printf("%v-> Sucessfully Authorized", id)

	log.Printf("%v-> Checking record for id: %v\n", id, id)

	jobDetail, err := GetJobDetail(id)
	if err != nil {
		log.Printf("%v-> Failed to get job details for id: %v with error: %v\n", id, id, err.Error())

		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to check record for id: %v with error: %v\n", id, err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest
		return apiResponse, nil
	}

	log.Printf("%v-> Successfully Executed query", id)

	if request.HTTPMethod == "GET" {
		responseBody, _ := json.Marshal(SuccessResponse{
			Id:        jobDetail.Id,
			JobId:     jobDetail.JobId,
			JobStatus: jobDetail.JobStatus,
			RequestId: jobDetail.RequestId,
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusOK
		return apiResponse, nil
	}

	if request.HTTPMethod == "DELETE" {
		if jobDetail.JobStatus == "SUCCESS" || jobDetail.JobStatus == "FAILURE" {
			responseBody, _ := json.Marshal(FailureResponse{
				Id:      id,
				Message: fmt.Sprintf("Job for jobId:%v is already completed", jobDetail.JobId),
			})
			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusBadRequest
			return apiResponse, nil
		}

		if jobDetail.JobStatus == "CANCELLING" || jobDetail.JobStatus == "CANCELLED" {
			responseBody, _ := json.Marshal(FailureResponse{
				Id:      id,
				Message: fmt.Sprintf("Job for jobId:%v is already cancelled", jobDetail.JobId),
			})
			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusBadRequest
			return apiResponse, nil
		}

		params := &emrserverless.CancelJobRunInput{
			ApplicationId: aws.String(applicationId),
			JobRunId:      aws.String(jobDetail.JobId),
		}

		log.Printf("%v-> Cancelling job", id)

		_, err := service.CancelJobRun(params)
		if err != nil {
			responseBody, _ := json.Marshal(FailureResponse{
				Id:      id,
				Message: fmt.Sprintf("Failed to cancel job for jobId: %v with error: %v\n", jobDetail.JobId, err.Error()),
			})

			apiResponse.Body = string(responseBody)
			apiResponse.StatusCode = http.StatusBadRequest
			return apiResponse, nil
		}

		log.Printf("%v-> Successfully cancelled job", id)

		responseBody, _ := json.Marshal(SuccessResponse{
			Id:        jobDetail.Id,
			JobId:     jobDetail.JobId,
			RequestId: jobDetail.RequestId,
			Message:   "Successfully deleted",
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusOK
		return apiResponse, nil
	}

	return apiResponse, nil
}

func GetJobDetail(id string) (JobDetail, error) {
	log.Printf("%v-> Initiating GetJobDetail", id)

	statement := `SELECT id, jobid, jobstatus, requestid, query, destination FROM emr_job_details WHERE id=$1`
	var jobDetail JobDetail

	record := db.QueryRow(statement, id)

	switch err := record.Scan(
		&jobDetail.Id,
		&jobDetail.JobId,
		&jobDetail.JobStatus,
		&jobDetail.RequestId,
		&jobDetail.Query,
		&jobDetail.Destination,
	); err {
	case sql.ErrNoRows:
		return jobDetail, sql.ErrNoRows
	case nil:
		return jobDetail, nil
	default:
		return jobDetail, err
	}
}

func SkyflowAuthorization(token string, vaultId string, id string) SkyflowAuthorizationResponse {
	var authResponse SkyflowAuthorizationResponse

	log.Printf("%v-> Initiating SkyflowAuthorization", id)

	client := &http.Client{Timeout: 1 * time.Minute}
	var url = managementUrl + "/v1/vaults/" + vaultId

	log.Printf("%v-> Initiating Skyflow Request for Authorization", id)

	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Add("Accept", "apaplication/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", token)

	response, err := client.Do(request)
	if err != nil {
		log.Printf("%v-> Got error on Skyflow Validation request: %v\n", id, err.Error())

		authResponse.StatusCode = http.StatusInternalServerError
		authResponse.Error = err.Error()
		return authResponse
	}

	responseBody, _ := io.ReadAll(response.Body)
	defer response.Body.Close()

	authResponse.RequestId = response.Header.Get("x-request-id")
	authResponse.StatusCode = response.StatusCode
	authResponse.ResponseBody = string(responseBody)

	if response.StatusCode != http.StatusOK {
		log.Printf("%v-> Unable/Fail to call Skyflow API status code:%v and message:%v", id, response.StatusCode, string(responseBody))
	}

	return authResponse
}

func ValidateAuthScheme(token string) bool {
	authScheme := strings.Split(token, " ")[0]

	if authScheme != "Bearer" {
		return false
	}
	return true
}

func ValidateVaultId(vaultId string) bool {
	for _, validVaultId := range validVaultIds {
		if vaultId == validVaultId {
			return true
		}
	}
	return false
}
