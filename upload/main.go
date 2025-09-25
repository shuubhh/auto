package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

func main() {
	// Serve frontend static files
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	// Upload API
	http.HandleFunc("/upload", handleUpload)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Uploader API running on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
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
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "✅ Upload successful: %s", blobName)
}
