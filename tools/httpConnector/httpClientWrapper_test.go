package main

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type closeTracker struct {
	io.Reader
	closed bool
}

func (ct *closeTracker) Close() error {
	ct.closed = true
	return nil
}

func TestNewHTTPWrapperClient_InvalidArgsShouldErr(t *testing.T) {
	t.Run("empty base url", func(t *testing.T) {
		client, err := NewHTTPWrapperClient(HTTPClientWrapperArgs{RequestTimeoutSec: minRequestTimeoutSec})
		require.Nil(t, client)
		require.Equal(t, errEmptyBaseUrl, err)
	})

	t.Run("invalid timeout", func(t *testing.T) {
		client, err := NewHTTPWrapperClient(HTTPClientWrapperArgs{BaseUrl: "http://localhost"})
		require.Nil(t, client)
		require.ErrorIs(t, err, errInvalidValue)
	})

	t.Run("authorization requires username", func(t *testing.T) {
		client, err := NewHTTPWrapperClient(HTTPClientWrapperArgs{
			BaseUrl:           "http://localhost",
			RequestTimeoutSec: minRequestTimeoutSec,
			UseAuthorization:  true,
			Password:          "password",
		})
		require.Nil(t, client)
		require.Equal(t, errEmptyUsername, err)
	})
}

func TestHTTPClientWrapper_PostShouldSetHeadersAndBasicAuth(t *testing.T) {
	body := &closeTracker{Reader: bytes.NewReader([]byte("ok"))}
	wrapper := &httpClientWrapper{
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, http.MethodPost, req.Method)
				require.Equal(t, "http://publisher/events", req.URL.String())
				require.Equal(t, contentTypeValue, req.Header.Get(contentTypeKey))
				require.Equal(t, payloadVersionValue, req.Header.Get(payloadVersionKey))
				username, password, ok := req.BasicAuth()
				require.True(t, ok)
				require.Equal(t, "alice", username)
				require.Equal(t, "secret", password)

				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
				}, nil
			}),
		},
		useAuthorization: true,
		username:         "alice",
		password:         "secret",
		baseUrl:          "http://publisher",
	}

	err := wrapper.Post("/events", map[string]string{"hello": "world"})

	require.Nil(t, err)
	require.True(t, body.closed)
}

func TestHTTPClientWrapper_PostShouldRejectOversizedResponseAndCloseBody(t *testing.T) {
	body := &closeTracker{Reader: bytes.NewReader(bytes.Repeat([]byte("a"), 256*1024+1))}
	wrapper := &httpClientWrapper{
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       body,
				}, nil
			}),
		},
		baseUrl: "http://publisher",
	}

	err := wrapper.Post("/events", struct{}{})

	require.ErrorContains(t, err, "response body exceeds")
	require.True(t, body.closed)
}

func TestHTTPClientWrapper_PostShouldReturnStatusError(t *testing.T) {
	body := &closeTracker{Reader: bytes.NewReader([]byte("denied"))}
	wrapper := &httpClientWrapper{
		httpClient: &http.Client{
			Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusUnauthorized,
					Body:       body,
				}, nil
			}),
		},
		baseUrl: "http://publisher",
	}

	err := wrapper.Post("/events", struct{}{})

	require.ErrorContains(t, err, "HTTP status code: 401")
	require.True(t, body.closed)
}
