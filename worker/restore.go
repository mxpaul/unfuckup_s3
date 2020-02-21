package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type BackupClient struct {
	BackupUrlPrefix string
	Client          *http.Client
	Timeout         time.Duration
}

func (instance *BackupClient) BackupUrl(file_id string) string {
	Url := instance.BackupUrlPrefix + file_id
	return Url
}

func (instance *BackupClient) RequestBackupBody(file_id string) (io.ReadCloser, error) {
	Url := instance.BackupUrl(file_id)

	req, err := http.NewRequest("GET", Url, nil)
	if err != nil {
		return nil, err
	}

	if instance.Timeout > 0 {
		ctx, _ := context.WithTimeout(context.Background(), instance.Timeout)
		req = req.WithContext(ctx)
	}

	// FIXME: check for redirects
	resp, err := instance.Client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("[%s] status code %d", Url, resp.StatusCode)
	}

	return resp.Body, nil
}

type AmazonRestorer struct {
	UrlPrefix string
	Client    *http.Client
	Bucket    string
	Timeout   time.Duration
}

func (instance *AmazonRestorer) UploadUrl(file_id string) string {
	Url := fmt.Sprintf("%s/%s", instance.UrlPrefix, file_id)
	return Url
}

func (instance *AmazonRestorer) PutObjectFromReader(file_id string, body io.ReadCloser) error {
	Url := instance.UploadUrl(file_id)

	req, err := http.NewRequest("PUT", Url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Host1", instance.Bucket)

	if instance.Timeout > 0 {
		ctx, _ := context.WithTimeout(context.Background(), instance.Timeout)
		req = req.WithContext(ctx)
	}

	// FIXME: check for redirects
	resp, err := instance.Client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("[%s] status code %d", Url, resp.StatusCode)
	}

	return nil
}
