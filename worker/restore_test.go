package worker

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	FileIDSuccess = "success"
	ValidBucketID = "ABCDEF12345"

	AmazonPrefix = "upload"
)

var (
	ExpectedFileContent = strings.Repeat("X", 64)
)

type Middleware func(http.HandlerFunc) http.HandlerFunc

func ChainMiddleware(h http.HandlerFunc, middleware ...Middleware) http.HandlerFunc {
	handler := h
	for i := len(middleware) - 1; i >= 0; i-- {
		handler = middleware[i](handler)
	}
	return handler
}

func MiddlewareEnsureMethod(t *testing.T, method string, desc string) Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !assert.Equal(t, method, r.Method, "request method %s when %s", method, desc) {
				log.Printf("request: %+v", r)
			}

			next.ServeHTTP(w, r)
		})
	}
}

func MiddlewareEnsureHeader(t *testing.T, name, value, desc string) Middleware {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, value, r.Header.Get(name), "header %s match value %s when %s", name, value, desc)
			next.ServeHTTP(w, r)
		})
	}
}

func NewMockHTTPServerBackup(t *testing.T, desc string, userMiddleware []Middleware) *httptest.Server {
	middleware := append([]Middleware{}, userMiddleware...)
	middleware = append(middleware, MiddlewareEnsureMethod(t, http.MethodGet, desc))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, ExpectedFileContent)
	})
	mux := http.NewServeMux()
	mux.Handle("/backup/"+FileIDSuccess, ChainMiddleware(handler, middleware...))
	server := httptest.NewTLSServer(mux)
	return server
}

func NewMockHTTPServerAmazon(t *testing.T, desc string, userMiddleware []Middleware) *httptest.Server {
	middleware := append([]Middleware{}, userMiddleware...)
	middleware = append(middleware, MiddlewareEnsureMethod(t, http.MethodPut, desc))
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Host1"), ValidBucketID) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusForbidden)
		}
	})
	mux := http.NewServeMux()
	prefix := fmt.Sprintf("/%s/%s", AmazonPrefix, FileIDSuccess)
	mux.Handle(prefix, ChainMiddleware(handler, middleware...))
	server := httptest.NewTLSServer(mux)
	return server
}

func TestRequestBackupBodySimple(t *testing.T) {
	desc := "simple request success"
	//mw := []Middleware{MiddlewareEnsureHeader(t, "User", "admin", desc)}
	BackupServ := NewMockHTTPServerBackup(t, desc, nil)
	defer BackupServ.Close()

	restorer := BackupClient{
		BackupUrlPrefix: fmt.Sprintf("%s/%s/", BackupServ.URL, "backup"),
		Client:          BackupServ.Client(),
	}

	body, err := restorer.RequestBackupBody(FileIDSuccess)
	if assert.NoErrorf(t, err, "no errors getting backup data") &&
		assert.NotNil(t, body, "body not nil") {
		defer body.Close()
		body, err := ioutil.ReadAll(body)
		if assert.NoErrorf(t, err, "no body read error") {
			assert.Equal(t, string(body), ExpectedFileContent, "body data as expected")
		}
	}
}

func TestRequestBackupBody_Error_NewRequest(t *testing.T) {
	desc := "error before dispatching request"
	BackupServ := NewMockHTTPServerBackup(t, desc, nil)
	defer BackupServ.Close()

	restorer := BackupClient{
		BackupUrlPrefix: "\x00unparsable:url:makes:NewRequest:return:error",
		Client:          BackupServ.Client(),
	}

	body, err := restorer.RequestBackupBody(FileIDSuccess)
	assert.Error(t, err, "NewRequest error returned")
	assert.Nil(t, body, "body is nil when error")
}

func TestRequestBackupBody_Error_ServerDown(t *testing.T) {
	desc := "error while making request"
	BackupServ := NewMockHTTPServerBackup(t, desc, nil)

	restorer := BackupClient{
		BackupUrlPrefix: fmt.Sprintf("%s/%s/", BackupServ.URL, "backup"),
		Client:          BackupServ.Client(),
	}
	BackupServ.Close()

	body, err := restorer.RequestBackupBody(FileIDSuccess)
	assert.Error(t, err, "Do error returned")
	assert.Nil(t, body, "body is nil when error")
}

func TestRequestBackupBody_Error_Invalid_Code(t *testing.T) {
	desc := "response Status not 200"
	mw := []Middleware{
		func(next http.HandlerFunc) http.HandlerFunc {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				next.ServeHTTP(w, r)
			})
		},
	}
	BackupServ := NewMockHTTPServerBackup(t, desc, mw)
	defer BackupServ.Close()

	restorer := BackupClient{
		BackupUrlPrefix: fmt.Sprintf("%s/%s/", BackupServ.URL, "backup"),
		Client:          BackupServ.Client(),
	}

	body, err := restorer.RequestBackupBody(FileIDSuccess)
	assert.Error(t, err, "error returned")
	assert.Nil(t, body, "body is nil when error")
}

func TestRequestBackupBody_Timeout(t *testing.T) {
	desc := "request timed out"
	mw := []Middleware{
		func(next http.HandlerFunc) http.HandlerFunc {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(50 * time.Millisecond)
				next.ServeHTTP(w, r)
			})
		},
	}
	BackupServ := NewMockHTTPServerBackup(t, desc, mw)
	defer BackupServ.Close()

	restorer := BackupClient{
		BackupUrlPrefix: fmt.Sprintf("%s/%s/", BackupServ.URL, "backup"),
		Client:          BackupServ.Client(),
		Timeout:         10 * time.Millisecond,
	}

	body, err := restorer.RequestBackupBody(FileIDSuccess)
	assert.Error(t, err, "error returned")
	assert.Nil(t, body, "body is nil when error")
}

//func TestMockHttpServer(t *testing.T) {
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/backup/", func(w http.ResponseWriter, r *http.Request) {
//		assert.Equal(t, "admin", r.Header.Get("User"), "Client provided User header with value 'admin'")
//		assert.Equal(t, http.MethodGet, r.Method, "request method GET")
//		fmt.Fprint(w, strings.Repeat("X", 10))
//	})
//	ts := httptest.NewTLSServer(mux)
//	defer ts.Close()
//
//	client := ts.Client()
//	req, err := http.NewRequest("GET", ts.URL+"/backup/12345", nil)
//	req.Header.Set("User", "admin")
//	resp, err := client.Do(req)
//
//	if assert.NoErrorf(t, err, "response with no error") {
//		assert.Equal(t, resp.StatusCode, 200, "200 OK")
//		defer resp.Body.Close()
//		body, err := ioutil.ReadAll(resp.Body)
//		if assert.NoErrorf(t, err, "no body read error") {
//			assert.Equal(t, body, []byte("XXXXXXXXXX"), "response of Xx10")
//		}
//	}
//}

func TestRequestAmazonSimple(t *testing.T) {
	desc := "simple request success"
	requestCount := uint64(0)
	mw := []Middleware{
		func(next http.HandlerFunc) http.HandlerFunc {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				next.ServeHTTP(w, r)
			})
		},
	}
	AmazonServ := NewMockHTTPServerAmazon(t, desc, mw)
	defer AmazonServ.Close()

	amazon := AmazonRestorer{
		UrlPrefix: fmt.Sprintf("%s/%s", AmazonServ.URL, AmazonPrefix),
		Client:    AmazonServ.Client(),
		Bucket:    ValidBucketID,
	}

	body := ioutil.NopCloser(strings.NewReader(ExpectedFileContent))
	err := amazon.PutObjectFromReader(FileIDSuccess, body)
	assert.NoErrorf(t, err, "upload with no errors")
	assert.Equal(t, uint64(1), requestCount, "there were a request")
}

func TestRequestAmazonRequestBuildError(t *testing.T) {
	desc := "unparsable url prefix"
	mw := []Middleware{}
	AmazonServ := NewMockHTTPServerAmazon(t, desc, mw)
	defer AmazonServ.Close()

	amazon := AmazonRestorer{
		UrlPrefix: "\x00%s/unparsable url",
		Client:    AmazonServ.Client(),
	}

	body := ioutil.NopCloser(strings.NewReader(ExpectedFileContent))
	err := amazon.PutObjectFromReader(FileIDSuccess, body)
	assert.Error(t, err, "request build error")
}

func TestRequestAmazonRequestSendError(t *testing.T) {
	desc := "request transport error"
	mw := []Middleware{}
	AmazonServ := NewMockHTTPServerAmazon(t, desc, mw)

	amazon := AmazonRestorer{
		UrlPrefix: fmt.Sprintf("%s/%s", AmazonServ.URL, AmazonPrefix),
		Client:    AmazonServ.Client(),
		Bucket:    ValidBucketID,
	}

	AmazonServ.Close()
	body := ioutil.NopCloser(strings.NewReader(ExpectedFileContent))
	err := amazon.PutObjectFromReader(FileIDSuccess, body)
	assert.Error(t, err, "request client.Do error")
}

func TestRequestAmazon500(t *testing.T) {
	desc := "server reply 500"
	mw := []Middleware{
		func(next http.HandlerFunc) http.HandlerFunc {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				next.ServeHTTP(w, r)
			})
		},
	}
	AmazonServ := NewMockHTTPServerAmazon(t, desc, mw)
	defer AmazonServ.Close()

	amazon := AmazonRestorer{
		UrlPrefix: fmt.Sprintf("%s/%s", AmazonServ.URL, AmazonPrefix),
		Client:    AmazonServ.Client(),
		Bucket:    ValidBucketID,
	}

	body := ioutil.NopCloser(strings.NewReader(ExpectedFileContent))
	err := amazon.PutObjectFromReader(FileIDSuccess, body)
	assert.Error(t, err, "not 200 OK")
}

func TestRequestAmazonTimeout(t *testing.T) {
	desc := "request timed out"
	mw := []Middleware{
		func(next http.HandlerFunc) http.HandlerFunc {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(50 * time.Millisecond)
				next.ServeHTTP(w, r)
			})
		},
	}
	AmazonServ := NewMockHTTPServerAmazon(t, desc, mw)
	defer AmazonServ.Close()

	amazon := AmazonRestorer{
		UrlPrefix: fmt.Sprintf("%s/%s", AmazonServ.URL, AmazonPrefix),
		Client:    AmazonServ.Client(),
		Bucket:    ValidBucketID,
		Timeout:   10 * time.Millisecond,
	}

	body := ioutil.NopCloser(strings.NewReader(ExpectedFileContent))
	err := amazon.PutObjectFromReader(FileIDSuccess, body)
	assert.Error(t, err, "not 200 OK")
}
