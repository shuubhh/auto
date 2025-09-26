package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/xuri/excelize/v2"
)

// EventGridEvent represents a single Event Grid event
type EventGridEvent struct {
	ID        string `json:"id"`
	EventType string `json:"eventType"`
	Data      struct {
		URL string `json:"url"`
	} `json:"data"`
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

// ReadSeekCloser wraps a bytes.Reader to implement io.ReadSeekCloser
type ReadSeekCloser struct {
	*bytes.Reader
}

// Close implements the Closer interface (no-op for bytes.Reader)
func (r ReadSeekCloser) Close() error {
	return nil
}

func main() {
	http.HandleFunc("/process", handleProcess)
	// Add health check endpoint
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "✅ Service is healthy")
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Processor API running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// handleProcess handles Event Grid calls including validation handshake
func handleProcess(w http.ResponseWriter, r *http.Request) {
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

	log.Printf("Received request: %s", string(body))

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
	var events []EventGridEvent
	if err := json.Unmarshal(body, &events); err != nil {
		log.Printf("Failed to decode event: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Handle events
	for _, event := range events {
		if event.EventType == "Microsoft.EventGrid.SubscriptionValidationEvent" {
			// This should already be handled above, but just in case
			log.Printf("Unexpected validation event in main flow")
			continue
		}

		if event.EventType != "Microsoft.Storage.BlobCreated" {
			log.Printf("Skipping event type: %s", event.EventType)
			continue
		}

		blobURL := event.Data.URL
		log.Printf("New blob uploaded: %s", blobURL)

		if err := processExcelBlob(blobURL); err != nil {
			log.Printf("Failed to process blob: %v", err)
			http.Error(w, "failed to process blob: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "✅ Event processed")
}

// processExcelBlob downloads the blob, processes it, and uploads to output container in different storage account
func processExcelBlob(blobURL string) error {
	ctx := context.Background()
	cred, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to get MI credential: %w", err)
	}

	// Create block blob client for input blob
	blockBlobClient, err := blockblob.NewClient(blobURL, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create block blob client: %w", err)
	}

	// Download blob
	resp, err := blockBlobClient.DownloadStream(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to download blob: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read blob: %w", err)
	}

	// Open Excel directly from memory
	f, err := excelize.OpenReader(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to open excel file: %w", err)
	}
	defer f.Close()

	// Get output storage account and container from environment variables
	outputStorageAccount := os.Getenv("OUTPUT_STORAGE_ACCOUNT")
	if outputStorageAccount == "" {
		return fmt.Errorf("OUTPUT_STORAGE_ACCOUNT environment variable not set")
	}

	outputContainer := os.Getenv("OUTPUT_STORAGE_CONTAINER")
	if outputContainer == "" {
		return fmt.Errorf("OUTPUT_STORAGE_CONTAINER environment variable not set")
	}

	// Process the Excel file
	statusUpdates, err := processExcelFile(f)
	if err != nil {
		return fmt.Errorf("failed to process excel file: %w", err)
	}

	// Save the modified Excel file to memory
	var excelBuffer bytes.Buffer
	if err := f.Write(&excelBuffer); err != nil {
		return fmt.Errorf("failed to write excel to buffer: %w", err)
	}

	// Upload processed file to output storage account
	if err := uploadToOutputContainer(ctx, cred, outputStorageAccount, outputContainer, blobURL, &excelBuffer); err != nil {
		return fmt.Errorf("failed to upload to output container: %w", err)
	}

	log.Printf("✅ Processing completed. Status updates: %+v", statusUpdates)
	return nil
}

// extractStorageAccountFromURL extracts storage account name from blob URL
func extractStorageAccountFromURL(blobURL string) (string, error) {
	// Regex to match blob URLs
	regex := regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/`)
	matches := regex.FindStringSubmatch(blobURL)
	if len(matches) < 2 {
		return "", fmt.Errorf("invalid blob URL format: %s", blobURL)
	}
	return matches[1], nil
}

// processExcelFile processes the Excel file and adds status column
func processExcelFile(f *excelize.File) (map[string]int, error) {
	stats := map[string]int{
		"processed": 0,
		"changed":   0,
		"skipped":   0,
		"errors":    0,
	}

	// Regex to match blob URLs in Excel cells
	regex := regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)`)

	// Get all rows from Sheet1
	rows, err := f.GetRows("Sheet1")
	if err != nil {
		return stats, fmt.Errorf("failed to get rows: %w", err)
	}

	if len(rows) == 0 {
		return stats, nil
	}

	// Find the URL column index and add Status column
	urlColIndex := -1
	headers := rows[0]
	for i, header := range headers {
		if strings.EqualFold(strings.TrimSpace(header), "url") {
			urlColIndex = i
			break
		}
	}

	// If URL column not found, use first column as reference
	if urlColIndex == -1 {
		urlColIndex = 0
	}

	// Status column will be added after URL column
	statusColIndex := urlColIndex + 1

	// Add "Status" header to the first row
	statusCell, _ := excelize.CoordinatesToCellName(statusColIndex+1, 1) // +1 because excelize is 1-based
	if err := f.SetCellValue("Sheet1", statusCell, "Status"); err != nil {
		return stats, fmt.Errorf("failed to set status header: %w", err)
	}

	// Process each row starting from row 2 (skip header)
	for rowIndex := 1; rowIndex < len(rows); rowIndex++ {
		if urlColIndex >= len(rows[rowIndex]) {
			continue // Skip rows that don't have URL column
		}

		cellValue := rows[rowIndex][urlColIndex]
		m := regex.FindStringSubmatch(cellValue)
		if m == nil {
			continue // Skip if not a blob URL
		}

		stats["processed"]++
		account := m[1]
		containerName := m[2]
		blobPath := m[3]

		log.Printf("Processing blob: account=%s, container=%s, path=%s", account, containerName, blobPath)

		// Process the blob and get status
		status, err := processBlobTier(account, containerName, blobPath)
		if err != nil {
			stats["errors"]++
			status = fmt.Sprintf("Error: %v", err)
		} else {
			if status == "Changed: Archive → Cool" {
				stats["changed"]++
			} else {
				stats["skipped"]++
			}
		}

		// Write status to the Status column
		statusCell, err := excelize.CoordinatesToCellName(statusColIndex+1, rowIndex+1) // +1 because excelize is 1-based
		if err != nil {
			stats["errors"]++
			log.Printf("Failed to get status cell coordinates: %v", err)
			continue
		}

		if err := f.SetCellValue("Sheet1", statusCell, status); err != nil {
			stats["errors"]++
			log.Printf("Failed to set status cell value: %v", err)
			continue
		}
	}

	return stats, nil
}

// processBlobTier checks and updates blob tier if necessary
func processBlobTier(account, containerName, blobPath string) (string, error) {
	ctx := context.Background()
	cred, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		return "Error: Failed to get credential", err
	}

	// Properly URL encode the blob path
	pathSegments := strings.Split(blobPath, "/")
	for i, segment := range pathSegments {
		pathSegments[i] = url.PathEscape(segment)
	}
	encodedBlobPath := strings.Join(pathSegments, "/")

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
	serviceClient, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		return "Error: Failed to create service client", err
	}

	containerClient := serviceClient.NewContainerClient(containerName)
	blobClient := containerClient.NewBlobClient(encodedBlobPath)

	props, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return "Error: Blob not accessible", err
	}

	if props.AccessTier == nil {
		return "Skipped: No access tier set", nil
	}

	currentTier := blob.AccessTier(*props.AccessTier)
	if currentTier == blob.AccessTierArchive {
		blockClient, err := blockblob.NewClient(blobClient.URL(), cred, nil)
		if err != nil {
			return "Error: Failed to create block client", err
		}

		_, err = blockClient.SetTier(ctx, blob.AccessTierCool, nil)
		if err != nil {
			return "Error: Failed to set tier", err
		}

		return "Changed: Archive → Cool", nil
	}

	return fmt.Sprintf("Skipped: Already %s", string(currentTier)), nil
}

// uploadToOutputContainer uploads the processed file to the output container in the specified storage account
func uploadToOutputContainer(ctx context.Context, cred *azidentity.ManagedIdentityCredential, storageAccount, outputContainer, originalBlobURL string, excelBuffer *bytes.Buffer) error {
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccount)
	serviceClient, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create service client for output storage account: %w", err)
	}

	// Ensure output container exists
	containerClient := serviceClient.NewContainerClient(outputContainer)
	_, err = containerClient.GetProperties(ctx, nil)
	if err != nil {
		// Container doesn't exist, try to create it
		_, err = containerClient.Create(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to create output container: %w", err)
		}
		log.Printf("Created output container: %s in storage account: %s", outputContainer, storageAccount)
	}

	// Extract original filename from blob URL
	originalFilename, err := extractFilenameFromURL(originalBlobURL)
	if err != nil {
		return fmt.Errorf("failed to extract filename from URL: %w", err)
	}

	// Create new filename with timestamp or processed marker
	// For simplicity, we'll just add "_processed" suffix
	newFilename := strings.TrimSuffix(originalFilename, ".xlsx") + "_processed.xlsx"

	// Convert bytes.Buffer to ReadSeekCloser which implements io.ReadSeekCloser
	excelData := excelBuffer.Bytes()
	reader := ReadSeekCloser{bytes.NewReader(excelData)}

	// Upload to output container in the specified storage account
	blobClient := containerClient.NewBlockBlobClient(newFilename)
	contentType := "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	
	_, err = blobClient.Upload(ctx, reader, &blockblob.UploadOptions{
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: &contentType,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload processed file to output storage account: %w", err)
	}

	log.Printf("✅ Processed file uploaded to: %s/%s in storage account: %s", outputContainer, newFilename, storageAccount)
	return nil
}

// extractFilenameFromURL extracts filename from blob URL
func extractFilenameFromURL(blobURL string) (string, error) {
	parsedURL, err := url.Parse(blobURL)
	if err != nil {
		return "", err
	}

	// Extract the last part of the path as filename
	pathParts := strings.Split(parsedURL.Path, "/")
	if len(pathParts) == 0 {
		return "", fmt.Errorf("invalid URL path: %s", parsedURL.Path)
	}

	return pathParts[len(pathParts)-1], nil
}