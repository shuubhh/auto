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

// processExcelBlob downloads the blob and applies the tier-change logic
func processExcelBlob(blobURL string) error {
	ctx := context.Background()
	cred, err := azidentity.NewManagedIdentityCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to get MI credential: %w", err)
	}

	// Create block blob client
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

	// Regex to match blob URLs in Excel cells
	regex := regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)`)
	rows, err := f.GetRows("Sheet1")
	if err != nil {
		return fmt.Errorf("failed to get rows: %w", err)
	}

	var processed, changed, skipped, errors int
	for _, row := range rows {
		for _, cell := range row {
			m := regex.FindStringSubmatch(cell)
			if m == nil {
				continue
			}
			processed++
			account := m[1]
			containerName := m[2]
			blobPath := m[3]

			log.Printf("Processing blob: account=%s, container=%s, path=%s", account, containerName, blobPath)

			// Properly URL encode the blob path
			pathSegments := strings.Split(blobPath, "/")
			for i, segment := range pathSegments {
				pathSegments[i] = url.PathEscape(segment)
			}
			encodedBlobPath := strings.Join(pathSegments, "/")

			serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
			serviceClient, err := service.NewClient(serviceURL, cred, nil)
			if err != nil {
				log.Printf("error creating service client: %v", err)
				errors++
				continue
			}

			containerClient := serviceClient.NewContainerClient(containerName)
			blobClient := containerClient.NewBlobClient(encodedBlobPath)

			props, err := blobClient.GetProperties(ctx, nil)
			if err != nil {
				log.Printf("blob does not exist or cannot access: %v", err)
				errors++
				continue
			}

			if props.AccessTier == nil {
				log.Printf("AccessTier not set for blob: %s", blobClient.URL())
				skipped++
				continue
			}

			currentTier := blob.AccessTier(*props.AccessTier)
			if currentTier == blob.AccessTierArchive {
				blockClient, err := blockblob.NewClient(blobClient.URL(), cred, nil)
				if err != nil {
					log.Printf("failed to create block blob client: %v", err)
					errors++
					continue
				}
				_, err = blockClient.SetTier(ctx, blob.AccessTierCool, nil)
				if err != nil {
					log.Printf("failed to set tier: %v", err)
					errors++
				} else {
					log.Printf("Tier changed from Archive to Cool ✅")
					changed++
				}
			} else {
				log.Printf("No change needed (current: %s)", string(currentTier))
				skipped++
			}
		}
	}

	log.Printf("Done. Processed=%d, Changed=%d, Skipped=%d, Errors=%d", processed, changed, skipped, errors)
	return nil
}
