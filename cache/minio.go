package cache

import (
	"log"

	"github.com/minio/minio-go"
)

type MinioClient struct {
	Endpoint string
	AccessKeyID string
	SecretAccessKey string
	BucketName string
	Location string
	MinioCli *minio.Client
}

type IMinioClient interface {
	GetObject(objectName, filePath string) error
	PutObject(objectName, filePath string) error
	RMObject(objectName string) error
}

func InitMinioClient() IMinioClient {
	endpoint := "0.0.0.0:9000"
	accessKeyID := "AKIAIOSFODNN7EXAMPLE"
	secretAccessKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	useSSL := false
	bucketName := "mymusic"
	location := "us-east-1"

	newMinioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		log.Fatalln(err)
	}
	return &MinioClient{
		Endpoint: endpoint,
		AccessKeyID : accessKeyID,
		SecretAccessKey : secretAccessKey,
		BucketName : bucketName,
		Location : location,
		MinioCli : newMinioClient,
	}
}

func (service *MinioClient) GetObject(objectName, filePath string) error {
	err := service.MinioCli.FGetObject(service.BucketName, objectName, filePath, minio.GetObjectOptions{})
	if err != nil {
		log.Fatalln(err)
		return err
	}

	log.Println("MINIO CLIENT: Success on GutObject", objectName, filePath)
	return nil
}

func (service *MinioClient) PutObject(objectName, filePath string) error {
	n, err := service.MinioCli.FPutObject(service.BucketName, objectName, filePath, minio.PutObjectOptions{ContentType: "application/zip"})
	if err != nil {
		log.Fatalln(err)
		return err
	}
	log.Println("MINIO CLIENT: Success on PutObject", objectName, filePath, n)
	return nil
}

func (service *MinioClient) RMObject(objectName string) error {
	err := service.MinioCli.RemoveObject(service.BucketName, objectName)
	if err != nil {
		log.Fatalln(err)
		return err
	}
	return nil
}
