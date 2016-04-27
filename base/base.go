// Package base provides a very low-level interface on top of the B2 v1 API.
package base

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

const (
	apiBase = "https://api.backblaze.com"
	apiV1   = "/b2api/v1/"
)

type B2 struct {
	accountID   string
	authToken   string
	apiURI      string
	downloadURI string
	minPartSize int
}

var Err = errors.New("b2 api error")

type b2AuthorizeAccountResponse struct {
	AccountID   string `json:"accountId"`
	AuthToken   string `json:"authorizationToken"`
	ApiURI      string `json:"apiUrl"`
	DownloadURI string `json:"downloadUrl"`
	MinPartSize int    `json:"minimumPartSize"`
}

// B2AuthorizeAccount wraps b2_authorize_account.
func B2AuthorizeAccount(account, key string) (*B2, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", account, key)))
	req, err := http.NewRequest("GET", apiBase+apiV1+"b2_authorize_account", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Basic %s", auth))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, Err // TODO: actually handle errors
	}
	decoder := json.NewDecoder(resp.Body)
	b2resp := &b2AuthorizeAccountResponse{}
	if err := decoder.Decode(b2resp); err != nil {
		return nil, err
	}
	return &B2{
		accountID:   b2resp.AccountID,
		authToken:   b2resp.AuthToken,
		apiURI:      b2resp.ApiURI,
		downloadURI: b2resp.DownloadURI,
		minPartSize: b2resp.MinPartSize,
	}, nil
}

type Bucket struct {
	Name string
	id   string
}

type b2ListBucketsRequest struct {
	AccountID string `json:"accountId"`
}

type b2ListBucketsResponse struct {
	Buckets []struct {
		BucketID   string `json:"bucketId`
		BucketName string `json:"bucketName"`
		BucketType string `json:"bucketType"`
	} `json:"buckets"`
}

func (b *B2) ListBuckets() ([]Bucket, error) {
	b2req := &b2ListBucketsRequest{
		AccountID: b.accountID,
	}
	enc, err := json.Marshal(b2req)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", b.apiURI+apiV1+"b2_list_buckets", bytes.NewBuffer(enc))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", b.authToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("err %d", resp.StatusCode)
	}
	b2resp := &b2ListBucketsResponse{}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(b2resp); err != nil {
		return nil, err
	}
	var buckets []Bucket
	for _, b := range b2resp.Buckets {
		buckets = append(buckets, Bucket{
			Name: b.BucketName,
			id:   b.BucketID,
		})
	}
	return buckets, nil
}
