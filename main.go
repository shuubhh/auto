package main

import (
	"context"
	"fmt"
	"log"

	//"net/url"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/xuri/excelize/v2"
)

func main() {
	ctx := context.Background()

	// 1. Load Excel file (local)
	f, err := excelize.OpenFile("input.xlsx")
	if err != nil {
		log.Fatalf("failed to open xlsx: %v", err)
	}

	// Regex to match blob URLs like:
	// https://staccauto.blob.core.windows.net/cont1/folder1/folder2/testschedule - Copy.docx
	regex := regexp.MustCompile(`https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)`)

	// Read rows
	rows, err := f.GetRows("Sheet1")
	if err != nil {
		log.Fatalf("failed to get rows: %v", err)
	}

	// Managed Identity credential
	cred, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		log.Fatalf("failed to get MI cred: %v", err)
	}

	// Counters
	var processed, changed, skipped, errors int

	// Process cells
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

			fmt.Printf("\nProcessing blob: account=%s, container=%s, path=%s\n",
				account, containerName, blobPath)

			// Build service client
			serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", account)
			serviceClient, err := service.NewClient(serviceURL, cred, nil)
			if err != nil {
				log.Printf("error creating service client: %v", err)
				errors++
				continue
			}

			// NOTE: if the path contains unescaped characters (spaces), it's safer to percent-encode only the name segments.
			// Quick approach: encode spaces only (you can improve to encode each path segment properly).
			if strings.Contains(blobPath, " ") {
				blobPath = strings.ReplaceAll(blobPath, " ", "%20")
			}
			// Alternative (more correct): split on '/', path-escape each segment via url.PathEscape and join back.

			// Get blob client (containerClient.NewBlobClient takes a blob name/path)
			containerClient := serviceClient.NewContainerClient(containerName)
			blobClient := containerClient.NewBlobClient(blobPath)

			// Get properties
			props, err := blobClient.GetProperties(ctx, nil)
			if err != nil {
				log.Printf("blob does not exist or cannot access: %v", err)
				errors++
				continue
			}

			// Guard for nil AccessTier
			if props.AccessTier == nil {
				log.Printf("AccessTier not set for blob: %s", blobClient.URL())
				skipped++
				continue
			}

			// Strong-typed enum comparison
			if blob.AccessTier(*props.AccessTier) == blob.AccessTierArchive {
				// create block blob client to set tier
				blockBlobClient, err := blockblob.NewClient(blobClient.URL(), cred, nil)
				if err != nil {
					log.Printf("failed to create block blob client: %v", err)
					errors++
					continue
				}

				// Use the blob package constant here (blob.AccessTierCool)
				_, err = blockBlobClient.SetTier(ctx, blob.AccessTierCool, nil)
				if err != nil {
					log.Printf("failed to set tier: %v", err)
					errors++
				} else {
					fmt.Println("Tier changed to Cool ✅")
					changed++
				}
			} else {
				fmt.Printf("No change needed (current: %s)\n", string(*props.AccessTier))
				skipped++
			}
		}
	}

	// Final summary
	fmt.Println("\n===== Summary =====")
	fmt.Printf("Total blobs found in Excel: %d\n", processed)
	fmt.Printf("Blobs changed to Cool: %d\n", changed)
	fmt.Printf("Blobs skipped (not Archive or missing tier): %d\n", skipped)
	fmt.Printf("Errors: %d\n", errors)
}
