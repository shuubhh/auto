package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

// ProcessedFile represents the latest processed file info
type ProcessedFile struct {
	FileName    string    `json:"fileName"`
	URL         string    `json:"url"`
	ProcessedAt time.Time `json:"processedAt"`
}

// EventGridSubscriptionValidation represents the validation handshake request
type EventGridSubscriptionValidation struct {
	ID        string `json:"id"`
	EventType string `json:"eventType"`
	Data      struct {
		ValidationCode string `json:"validationCode"`
	} `json:"data"`
}

// EventGridValidationResponse represents the response for validation handshake
type EventGridValidationResponse struct {
	ValidationResponse string `json:"validationResponse"`
}

var latestProcessedFile *ProcessedFile

func main() {
	// Serve frontend static files
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	// Upload API
	http.HandleFunc("/upload", handleUpload)

	// API to get latest processed file
	http.HandleFunc("/api/latest-processed", handleLatestProcessed)

	// Download proxy endpoint
	http.HandleFunc("/api/download/", handleDownload)

	// Webhook endpoint for Event Grid to notify about new processed files
	http.HandleFunc("/api/processed-notification", handleProcessedNotification)

	// Health check
	http.HandleFunc("/health", handleHealth)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Uploader API running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "✅ Frontend ACA is healthy")
}

func handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// Increase max upload size to 50MB
	err := r.ParseMultipartForm(50 << 20)
	if err != nil {
		log.Printf("Failed to parse multipart form: %+v", err)
		http.Error(w, "failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		log.Printf("Failed to get file from form: %+v", err)
		http.Error(w, "failed to get file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Check file extension - only allow .xlsx files
	fileName := header.Filename
	fileExt := strings.ToLower(filepath.Ext(fileName))

	if fileExt != ".xlsx" {
		log.Printf("Invalid file type attempted: %s", fileName)
		http.Error(w, "❌ Only .xlsx files are allowed. Please upload an Excel file with .xlsx extension.", http.StatusBadRequest)
		return
	}

	storageAccount := os.Getenv("STORAGE_ACCOUNT")
	containerName := os.Getenv("STORAGE_CONTAINER")
	if storageAccount == "" || containerName == "" {
		log.Printf("STORAGE_ACCOUNT or STORAGE_CONTAINER not set")
		http.Error(w, "STORAGE_ACCOUNT and STORAGE_CONTAINER must be set", http.StatusInternalServerError)
		return
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("Failed to get Azure credential: %+v", err)
		http.Error(w, "failed to get credential: "+err.Error(), http.StatusInternalServerError)
		return
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccount)
	serviceClient, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		log.Printf("Failed to create service client: %+v", err)
		http.Error(w, "failed to create service client: "+err.Error(), http.StatusInternalServerError)
		return
	}

	containerClient := serviceClient.NewContainerClient(containerName)
	blobName := filepath.Base(fileName)
	blobClient := containerClient.NewBlockBlobClient(blobName)

	ctx := context.Background()
	_, err = blobClient.Upload(ctx, file, nil)
	if err != nil {
		log.Printf("Upload failed: %+v", err)
		http.Error(w, "failed to upload blob: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Upload successful: %s", blobName)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":       "success",
		"message":      fmt.Sprintf("✅ Upload successful: %s. File is being processed...", blobName),
		"originalFile": blobName,
	})
}

func handleLatestProcessed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if latestProcessedFile == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"available": false,
			"message":   "No processed files available yet",
		})
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"available": true,
		"file":      latestProcessedFile,
	})
}

func handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract filename from URL path
	fileName := strings.TrimPrefix(r.URL.Path, "/api/download/")
	if fileName == "" {
		http.Error(w, "Filename required", http.StatusBadRequest)
		return
	}

	log.Printf("Download request for: %s", fileName)

	// Get storage configuration
	storageAccount := os.Getenv("OUTPUT_STORAGE_ACCOUNT")
	if storageAccount == "" {
		storageAccount = os.Getenv("STORAGE_ACCOUNT")
	}

	outputContainer := os.Getenv("OUTPUT_STORAGE_CONTAINER")
	if outputContainer == "" {
		outputContainer = "processed-files" // Default container name
	}

	if storageAccount == "" {
		log.Printf("STORAGE_ACCOUNT not set")
		http.Error(w, "Storage account not configured", http.StatusInternalServerError)
		return
	}

	// Create Azure credential using managed identity
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Printf("Failed to get Azure credential: %v", err)
		http.Error(w, "Authentication failed", http.StatusInternalServerError)
		return
	}

	// Create service client
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccount)
	serviceClient, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		log.Printf("Failed to create service client: %v", err)
		http.Error(w, "Failed to connect to storage", http.StatusInternalServerError)
		return
	}

	// Get blob client
	containerClient := serviceClient.NewContainerClient(outputContainer)
	blobClient := containerClient.NewBlockBlobClient(fileName)

	// Download the blob
	ctx := context.Background()
	downloadResponse, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		log.Printf("Failed to download blob: %v", err)
		http.Error(w, "Failed to download file", http.StatusInternalServerError)
		return
	}
	defer downloadResponse.Body.Close()

	// Set response headers for file download
	w.Header().Set("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileName))

	// Stream the blob content to the response
	_, err = io.Copy(w, downloadResponse.Body)
	if err != nil {
		log.Printf("Failed to stream file: %v", err)
		return
	}

	log.Printf("✅ Successfully streamed file: %s", fileName)
}

func handleProcessedNotification(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Failed to read request body: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	log.Printf("Received Event Grid notification: %s", string(body))

	// First, try to parse as Event Grid subscription validation
	var validationReq []EventGridSubscriptionValidation
	if err := json.Unmarshal(body, &validationReq); err == nil && len(validationReq) > 0 {
		if validationReq[0].EventType == "Microsoft.EventGrid.SubscriptionValidationEvent" {
			log.Printf("Handling validation request: %s", validationReq[0].Data.ValidationCode)
			response := EventGridValidationResponse{
				ValidationResponse: validationReq[0].Data.ValidationCode,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
			log.Printf("✅ Validation handshake completed")
			return
		}
	}

	// If not validation, try to parse as regular events
	var events []struct {
		EventType string `json:"eventType"`
		Data      struct {
			URL string `json:"url"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &events); err != nil {
		log.Printf("Failed to decode event: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Handle events
	for _, event := range events {
		if event.EventType == "Microsoft.EventGrid.SubscriptionValidationEvent" {
			continue
		}

		if event.EventType != "Microsoft.Storage.BlobCreated" {
			log.Printf("Skipping event type: %s", event.EventType)
			continue
		}

		blobURL := event.Data.URL

		// Only process files that end with _processed.xlsx
		if strings.Contains(blobURL, "_processed.xlsx") {
			log.Printf("📢 Event Grid: New processed file detected: %s", blobURL)

			// Extract filename from URL
			urlParts := strings.Split(blobURL, "/")
			fileName := urlParts[len(urlParts)-1]

			// Update latest processed file
			latestProcessedFile = &ProcessedFile{
				FileName:    fileName,
				URL:         blobURL,
				ProcessedAt: time.Now(),
			}

			log.Printf("✅ Updated latest processed file to: %s", fileName)
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "✅ Notification processed")
}
