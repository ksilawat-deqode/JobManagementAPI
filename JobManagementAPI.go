package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/emrserverless"
	"github.com/golang-jwt/jwt"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
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
	Jti         string `json:"jti"`
	Region      string `json:"cross_bucket_region"`
}

type SkyflowAuthorizationResponse struct {
	RequestId    string `json:"requestId"`
	StatusCode   int    `json:"statusCode"`
	ResponseBody string `json:"responseBody"`
	Error        string `json:"error"`
}

var logger *log.Entry
var db *sql.DB
var managementUrl string
var service *emrserverless.EMRServerless
var applicationId string
var validVaultIds []string
var source string

func init() {
	log.SetFormatter(&log.JSONFormatter{})

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

	source = "JobManagementAPI"
}

func main() {
	lambda.Start(HandleRequest)
}

func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	apiResponse := events.APIGatewayProxyResponse{}

	id := request.PathParameters["jobID"]
	logger = log.WithFields(log.Fields{
		"queryId": id,
		"source":  source,
	})

	logger.Info(fmt.Sprintf("Initiated %v", source))

	clientIpAddress := strings.Split(request.Headers["X-Forwarded-For"], ",")[0]
	logger.Info(fmt.Sprintf("Client IP address: %v", clientIpAddress))

	logger = logger.WithFields(log.Fields{
		"clientIp": clientIpAddress,
	})

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

	jti, err := ExtractJTI(token)
	if err != nil {
		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to extract jti with error: %v", err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusForbidden

		return apiResponse, nil
	}

	logger = logger.WithFields(log.Fields{
		"jti": jti,
	})

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

	logger = logger.WithFields(log.Fields{
		"skyflowRequestId": authResponse.RequestId,
	})

	logger.Info("Sucessfully Authorized")

	logger.Info(fmt.Sprintf("Checking record for id: %v", id))

	jobDetail, err := GetJobDetail(id)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to get job details for id: %v with error: %v", id, err.Error()))

		responseBody, _ := json.Marshal(FailureResponse{
			Id:      id,
			Message: fmt.Sprintf("Failed to check record for id: %v with error: %v\n", id, err.Error()),
		})

		apiResponse.Body = string(responseBody)
		apiResponse.StatusCode = http.StatusBadRequest
		return apiResponse, nil
	}

	logger = logger.WithFields(log.Fields{
		"query":             jobDetail.Query,
		"destinationBucket": jobDetail.Destination,
		"region":            jobDetail.Region,
	})

	logger.Info("Successfully Executed query")

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

		logger.Info("Cancelling job")

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

		logger.Info("Successfully cancelled job")

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
	logger.Info("Initiating GetJobDetail")

	statement := `SELECT id, jobid, jobstatus, requestid, query, destination, jti, cross_bucket_region FROM emr_job_details WHERE id=$1`
	var jobDetail JobDetail

	record := db.QueryRow(statement, id)

	switch err := record.Scan(
		&jobDetail.Id,
		&jobDetail.JobId,
		&jobDetail.JobStatus,
		&jobDetail.RequestId,
		&jobDetail.Query,
		&jobDetail.Destination,
		&jobDetail.Jti,
		&jobDetail.Region,
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

	logger.Info("Initiating SkyflowAuthorization")

	client := &http.Client{Timeout: 1 * time.Minute}
	var url = managementUrl + "/v1/vaults/" + vaultId

	logger.Info("Initiating Skyflow Request for Authorization")

	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Add("Accept", "apaplication/json")
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Authorization", token)

	response, err := client.Do(request)
	if err != nil {
		logger.Error(fmt.Sprintf("Got error on Skyflow Validation request: %v", err.Error()))

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
		logger.Error(fmt.Sprintf("Unable/Fail to call Skyflow API status code:%v and message:%v", response.StatusCode, string(responseBody)))
	}

	return authResponse
}

func ValidateAuthScheme(token string) bool {
	logger.Info("Initiating ValidateAuthScheme")

	authScheme := strings.Split(token, " ")[0]

	if authScheme != "Bearer" {
		return false
	}
	return true
}

func ValidateVaultId(vaultId string) bool {
	logger.Info("Initiating ValidateVaultId")

	for _, validVaultId := range validVaultIds {
		if vaultId == validVaultId {
			return true
		}
	}
	return false
}

func ExtractJTI(authToken string) (string, error) {
	logger.Info("Initiating ExtractJTI")

	tokenString := strings.Split(authToken, " ")[1]

	logger.Info("Initiating token parsing")
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		logger.Error(fmt.Sprintf("Got error: %v in token parsing", err.Error()))
		return "", err
	}

	logger.Info("Successfully parsed token")
	claims := token.Claims.(jwt.MapClaims)
	jti := claims["jti"].(string)

	return jti, nil
}
