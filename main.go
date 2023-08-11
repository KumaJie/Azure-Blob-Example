package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"log"
	"os"
)

const MaxSingleFileSize = 1 << 30

func UploadBlobFromLocalFile(client *azblob.Client, ctx context.Context, fileName string, containerName string, blobName string, cred azcore.TokenCredential) error {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}
	// 获取文件大小
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	fmt.Println(size)
	if size > MaxSingleFileSize {
		// NOTE: The blockID must be <= 64 bytes and ALL blockIDs for the block must be the same length
		blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }
		blockIDBase64ToBinary := func(blockID string) []byte { _binary, _ := base64.StdEncoding.DecodeString(blockID); return _binary }

		// These helper functions convert an int block ID to a base-64 string and vice versa
		blockIDIntToBase64 := func(blockID int) string {
			binaryBlockID := &[4]byte{} // All block IDs are 4 bytes long
			binary.LittleEndian.PutUint32(binaryBlockID[:], uint32(blockID))
			return blockIDBinaryToBase64(binaryBlockID[:])
		}
		blockIDBase64ToInt := func(blockID string) int {
			blockIDBase64ToBinary(blockID)
			return int(binary.LittleEndian.Uint32(blockIDBase64ToBinary(blockID)))
		}
		bbClient, err := blockblob.NewClient(fmt.Sprintf("%s%s/%s", client.URL(), containerName, blobName), cred, nil)
		if err != nil {
			return err
		}
		base64BlockIDs := make([]string, 0, 1024)
		blockID := 0
		block := make([]byte, 1024*1024*10)
		for n, _ := file.Read(block); n > 0; n, _ = file.Read(block) {
			fmt.Printf("Put %v bytes\n", n)
			base64BlockIDs = append(base64BlockIDs, blockIDIntToBase64(blockID))
			_, err = bbClient.StageBlock(ctx, base64BlockIDs[blockID], streaming.NopCloser(bytes.NewReader(block[:n])), nil)
			if err != nil {
				return err
			}
			blockID++
		}
		_, err = bbClient.CommitBlockList(ctx, base64BlockIDs, nil)
		if err != nil {
			return err
		}
		// For the blob, show each block (ID and size) that is a committed part of it.
		getBlock, err := bbClient.GetBlockList(ctx, blockblob.BlockListTypeAll, nil)
		for _, block := range getBlock.BlockList.CommittedBlocks {
			fmt.Printf("Block ID=%d, Size=%d\n", blockIDBase64ToInt(*block.Name), block.Size)
		}
		return err
	}
	_, err = client.UploadFile(ctx, containerName, blobName, file, &azblob.UploadFileOptions{})
	return err
}

func DownloadBlobFromAzure(client *azblob.Client, ctx context.Context, fileName string, containerName string, blobName string) error {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}

	_, err = client.DownloadFile(ctx, containerName, blobName, file, &azblob.DownloadFileOptions{})
	return err
}

func DeleteBlobFromAzure(client *azblob.Client, ctx context.Context, containerName string, blobName string) error {
	_, err := client.DeleteBlob(ctx, containerName, blobName, nil)
	return err
}

func GetBlobProperty(client *azblob.Client, ctx context.Context, cred azcore.TokenCredential, containerName string, blobName string) error {
	blobClient, err := blob.NewClient(fmt.Sprintf("%s%s/%s", client.URL(), containerName, blobName), cred, nil)
	if err != nil {
		return err
	}
	_, err = blobClient.GetProperties(ctx, &blob.GetPropertiesOptions{})
	return err
}

func ListBlob(client *azblob.Client, containerName string) error {
	pager := client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return err
		}

		for _, blob := range resp.Segment.BlobItems {
			fmt.Println(*blob.Name)
		}
	}
	return nil
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	// TODO: replace <storage-account-name> with your actual storage account name
	url := "https://kuma2blob.blob.core.windows.net/"
	ctx := context.Background()

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	containerName := "test-container"

	op := flag.String("o", "", "operation")
	filePath := flag.String("f", "", "file name")
	blobName := flag.String("b", "", "blob name")
	flag.Parse()
	switch *op {
	case "put":
		if err := UploadBlobFromLocalFile(client, ctx, *filePath, containerName, *blobName, credential); err != nil {
			handleError(err)
		}
	case "get":
		if err := DownloadBlobFromAzure(client, ctx, *filePath, containerName, *blobName); err != nil {
			handleError(err)
		}
	case "delete":
		if err := DeleteBlobFromAzure(client, ctx, containerName, *blobName); err != nil {
			handleError(err)
		}
	case "head":
		if err := GetBlobProperty(client, ctx, credential, containerName, *blobName); err != nil {
			handleError(err)
		}
	case "list":
		if err := ListBlob(client, containerName); err != nil {
			handleError(err)
		}
	}
}
