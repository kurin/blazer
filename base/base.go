// Package base provides a very low-level interface on top of the B2 v1 API.
// It is not intended to be used directly.
//
// It currently lacks support for the following APIs:
//
// b2_download_file_by_id
// b2_download_file_by_name
// b2_get_file_info
// b2_hide_file
// b2_list_file_versions
// b2_list_parts
// b2_list_unfinished_large_files
// b2_update_bucket
package base

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
)

const (
	apiBase = "https://api.backblaze.com"
	apiV1   = "/b2api/v1/"
)

// B2 holds account information for Backblaze.
type B2 struct {
	accountID   string
	authToken   string
	apiURI      string
	downloadURI string
	minPartSize int
}

type b2AuthorizeAccountResponse struct {
	AccountID   string `json:"accountId"`
	AuthToken   string `json:"authorizationToken"`
	URI         string `json:"apiUrl"`
	DownloadURI string `json:"downloadUrl"`
	MinPartSize int    `json:"minimumPartSize"`
}

type errMsg struct {
	Msg string `json:"message"`
}

func makeRequest(verb, url string, b2req, b2resp interface{}, headers map[string]string, body io.Reader) error {
	if b2req != nil {
		enc, err := json.Marshal(b2req)
		if err != nil {
			return err
		}
		body = bytes.NewBuffer(enc)
	}
	req, err := http.NewRequest(verb, url, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		msg := &errMsg{}
		if err := json.Unmarshal(data, msg); err != nil {
			return err
		}
		return errors.New(msg.Msg)
	}
	if b2resp != nil {
		decoder := json.NewDecoder(resp.Body)
		if err := decoder.Decode(b2resp); err != nil {
			return err
		}
	}
	return nil
}

// B2AuthorizeAccount wraps b2_authorize_account.
func B2AuthorizeAccount(account, key string) (*B2, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", account, key)))
	b2resp := &b2AuthorizeAccountResponse{}
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Basic %s", auth),
	}
	if err := makeRequest("GET", apiBase+apiV1+"b2_authorize_account", nil, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &B2{
		accountID:   b2resp.AccountID,
		authToken:   b2resp.AuthToken,
		apiURI:      b2resp.URI,
		downloadURI: b2resp.DownloadURI,
		minPartSize: b2resp.MinPartSize,
	}, nil
}

type b2CreateBucketRequest struct {
	AccountID string `json:"accountId"`
	Name      string `json:"bucketName"`
	Type      string `json:"bucketType"`
}

type b2CreateBucketResponse struct {
	BucketID string `json:"bucketId"`
}

// CreateBucket wraps b2_create_bucket.
func (b *B2) CreateBucket(name, btype string) (*Bucket, error) {
	if btype != "allPublic" {
		btype = "allPrivate"
	}
	b2req := &b2CreateBucketRequest{
		AccountID: b.accountID,
		Name:      name,
		Type:      btype,
	}
	b2resp := &b2CreateBucketResponse{}
	headers := map[string]string{
		"Authorization": b.authToken,
	}
	if err := makeRequest("POST", b.apiURI+apiV1+"b2_create_bucket", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &Bucket{
		Name: name,
		id:   b2resp.BucketID,
		b2:   b,
	}, nil
}

type b2DeleteBucketRequest struct {
	AccountID string `json:"accountId"`
	BucketID  string `json:"bucketId"`
}

// DeleteBucket wraps b2_delete_bucket.
func (b *Bucket) DeleteBucket() error {
	b2req := &b2DeleteBucketRequest{
		AccountID: b.b2.accountID,
		BucketID:  b.id,
	}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	return makeRequest("POST", b.b2.apiURI+apiV1+"b2_delete_bucket", b2req, nil, headers, nil)
}

// Bucket holds B2 bucket details.
type Bucket struct {
	Name string
	id   string
	b2   *B2
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

// ListBuckets wraps b2_list_buckets.
func (b *B2) ListBuckets() ([]*Bucket, error) {
	b2req := &b2ListBucketsRequest{
		AccountID: b.accountID,
	}
	b2resp := &b2ListBucketsResponse{}
	headers := map[string]string{
		"Authorization": b.authToken,
	}
	if err := makeRequest("POST", b.apiURI+apiV1+"b2_list_buckets", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	var buckets []*Bucket
	for _, bucket := range b2resp.Buckets {
		buckets = append(buckets, &Bucket{
			Name: bucket.BucketName,
			id:   bucket.BucketID,
			b2:   b,
		})
	}
	return buckets, nil
}

type b2GetUploadURLRequest struct {
	BucketID string `json:"bucketId"`
}

type b2GetUploadURLResponse struct {
	URI   string `json:"uploadUrl"`
	Token string `json:"authorizationToken"`
}

// UploadEndpoint holds information from the b2_get_upload_url API.
type UploadEndpoint struct {
	uri   string
	token string
	b2    *B2
}

// GetUploadURL wraps b2_get_upload_url.
func (b *Bucket) GetUploadURL() (*UploadEndpoint, error) {
	b2req := &b2GetUploadURLRequest{
		BucketID: b.id,
	}
	b2resp := &b2GetUploadURLResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := makeRequest("POST", b.b2.apiURI+apiV1+"b2_get_upload_url", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &UploadEndpoint{
		uri:   b2resp.URI,
		token: b2resp.Token,
		b2:    b.b2,
	}, nil
}

type File struct {
	Name string
	id   string
	b2   *B2
}

type b2UploadFileResponse struct {
	FileID string `json:"fileId"`
}

// UploadFile wraps b2_upload_file.
func (ue *UploadEndpoint) UploadFile(r io.Reader, size int, name, contentType, sha1 string, info map[string]string) (*File, error) {
	headers := map[string]string{
		"Authorization":     ue.token,
		"X-Bz-File-Name":    name,
		"Content-Type":      contentType,
		"Content-Length":    fmt.Sprintf("%d", size),
		"X-Bz-Content-Sha1": sha1,
	}
	for k, v := range info {
		headers[fmt.Sprintf("X-Bz-Info-%s", k)] = v
	}
	b2resp := &b2UploadFileResponse{}
	if err := makeRequest("POST", ue.uri, nil, b2resp, headers, r); err != nil {
		return nil, err
	}
	return &File{
		Name: name,
		id:   b2resp.FileID,
		b2:   ue.b2,
	}, nil
}

type b2DeleteFileVersionRequest struct {
	Name   string `json:"fileName"`
	FileID string `json:"fileId"`
}

// DeleteFileVersion wraps b2_delete_file_version.
func (f *File) DeleteFileVersion() error {
	b2req := &b2DeleteFileVersionRequest{
		Name:   f.Name,
		FileID: f.id,
	}
	headers := map[string]string{
		"Authorization": f.b2.authToken,
	}
	return makeRequest("POST", f.b2.apiURI+apiV1+"b2_delete_file_version", b2req, nil, headers, nil)
}

type startLargeFileRequest struct {
	BucketID    string            `json:"bucketId"`
	Name        string            `json:"fileName"`
	ContentType string            `json:"contentType"`
	Info        map[string]string `json:"fileInfo"`
}

type startLargeFileResponse struct {
	ID string `json:"fileId"`
}

// LargeFile holds information necessary to implement B2 large file support.
type LargeFile struct {
	id string
	b2 *B2

	mu     sync.Mutex
	hashes map[int]string
}

// StartLargeFile wraps b2_start_large_file.
func (b *Bucket) StartLargeFile(name, contentType string, info map[string]string) (*LargeFile, error) {
	b2req := &startLargeFileRequest{
		BucketID:    b.id,
		Name:        name,
		ContentType: contentType,
		Info:        info,
	}
	b2resp := &startLargeFileResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := makeRequest("POST", b.b2.apiURI+apiV1+"b2_start_large_file", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &LargeFile{
		id:     b2resp.ID,
		b2:     b.b2,
		hashes: make(map[int]string),
	}, nil
}

type cancelLargeFileRequest struct {
	ID string `json:"fileId"`
}

// CancelLargeFile wraps b2_cancel_large_file.
func (l LargeFile) CancelLargeFile() error {
	b2req := &cancelLargeFileRequest{
		ID: l.id,
	}
	headers := map[string]string{
		"Authorization": l.b2.authToken,
	}
	if err := makeRequest("POST", l.b2.apiURI+apiV1+"b2_cancel_large_file", b2req, nil, headers, nil); err != nil {
		return err
	}
	return nil
}

// FileChunk holds information necessary for uploading file chunks.
type FileChunk struct {
	url   string
	token string
	file  *LargeFile
}

type getUploadPartURLRequest struct {
	ID string `json:"fileId"`
}

type getUploadPartURLResponse struct {
	URL   string `json:"uploadUrl"`
	Token string `json:"authorizationToken"`
}

// GetUploadPartURL wraps b2_get_upload_part_url.
func (l *LargeFile) GetUploadPartURL() (*FileChunk, error) {
	b2req := &getUploadPartURLRequest{
		ID: l.id,
	}
	b2resp := &getUploadPartURLResponse{}
	headers := map[string]string{
		"Authorization": l.b2.authToken,
	}
	if err := makeRequest("POST", l.b2.apiURI+apiV1+"b2_get_upload_part_url", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &FileChunk{
		url:   b2resp.URL,
		token: b2resp.Token,
		file:  l,
	}, nil
}

// UploadPart wraps b2_upload_part.
func (fc *FileChunk) UploadPart(r io.Reader, sha1 string, size, index int) (int, error) {
	headers := map[string]string{
		"Authorization":     fc.token,
		"X-Bz-Part-Number":  fmt.Sprintf("%d", index),
		"Content-Length":    fmt.Sprintf("%d", size),
		"X-Bz-Content-Sha1": sha1,
	}
	if err := makeRequest("POST", fc.url, nil, nil, headers, r); err != nil {
		return 0, err
	}
	fc.file.mu.Lock()
	fc.file.hashes[index] = sha1
	fc.file.mu.Unlock()
	return int(size), nil
}

type b2FinishLargeFileRequest struct {
	ID     string   `json:"fileId"`
	Hashes []string `json:"partSha1Array"`
}

type b2FinishLargeFileResponse struct {
	Name   string `json:"fileName"`
	FileID string `json:"fileId"`
}

// FinishLargeFile wraps b2_finish_large_file.
func (l *LargeFile) FinishLargeFile() (*File, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b2req := &b2FinishLargeFileRequest{
		ID:     l.id,
		Hashes: make([]string, len(l.hashes)),
	}
	b2resp := &b2FinishLargeFileResponse{}
	for k, v := range l.hashes {
		b2req.Hashes[k-1] = v
	}
	headers := map[string]string{
		"Authorization": l.b2.authToken,
	}
	if err := makeRequest("POST", l.b2.apiURI+apiV1+"b2_finish_large_file", b2req, b2resp, headers, nil); err != nil {
		return nil, err
	}
	return &File{
		Name: b2resp.Name,
		id:   b2resp.FileID,
		b2:   l.b2,
	}, nil
}

type b2ListFileNamesRequest struct {
	BucketID     string `json:"bucketId"`
	Count        int    `json:"maxFileCount"`
	Continuation string `json:"startFileName,omitempty"`
}

type b2ListFileNamesResponse struct {
	Continuation string `json:"nextFileName"`
	Files        []struct {
		FileID string `json:"fileId"`
		Name   string `json:"fileName"`
	} `json:"files"`
}

// ListFileNames wraps b2_list_file_names.
func (b *Bucket) ListFileNames(count int, continuation string) ([]*File, string, error) {
	b2req := &b2ListFileNamesRequest{
		Count:        count,
		Continuation: continuation,
		BucketID:     b.id,
	}
	b2resp := &b2ListFileNamesResponse{}
	headers := map[string]string{
		"Authorization": b.b2.authToken,
	}
	if err := makeRequest("POST", b.b2.apiURI+apiV1+"b2_list_file_names", b2req, b2resp, headers, nil); err != nil {
		return nil, "", err
	}
	cont := b2resp.Continuation
	var files []*File
	for _, f := range b2resp.Files {
		files = append(files, &File{
			Name: f.Name,
			id:   f.FileID,
			b2:   b.b2,
		})
	}
	return files, cont, nil
}
