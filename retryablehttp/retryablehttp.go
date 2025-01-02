package retryablehttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/condrove10/dukascopy-go/backoffpolicy"
	"github.com/go-playground/validator/v10"
)

type Client struct {
	Context       context.Context                            `validate:"required"`
	HttpClient    *http.Client                               `validate:"required"`
	RetryAttempts int                                        `validate:"required,gt=0"`
	RetryDelay    time.Duration                              `validate:"required"`
	RetryStrategy backoffpolicy.Strategy                     `validate:"required"`
	RetryPolicy   func(resp *http.Response, err error) error `validate:"required"`
}

func (c *Client) Post(url string, body []byte, headers map[string]string) (*http.Response, error) {
	return c.do(url, http.MethodPost, body, headers)
}

func (c *Client) Get(url string, headers map[string]string) (*http.Response, error) {
	return c.do(url, http.MethodGet, make([]byte, 0), headers)
}

func (c *Client) do(url, method string, body []byte, headers map[string]string) (*http.Response, error) {
	if err := validator.New().Struct(c); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	if err := validator.New().Var(url, "required,http_url"); err != nil {
		return nil, fmt.Errorf("url validation failed: %w", err)
	}

	header := http.Header{}
	for k, v := range headers {
		header.Add(k, v)
	}

	req, err := http.NewRequestWithContext(c.Context, method, url, bytes.NewReader(body))
	req.Header = header
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	var resp = &http.Response{}

	backoffpolicy.BackoffPolicy(c.RetryStrategy, c.RetryAttempts, c.RetryDelay, func(attempt int) error {
		if c.Context.Err() != nil {
			err := fmt.Errorf("retryable http call context closed: %w", c.Context.Err())
			return err
		}

		if attempt > 0 {
			req.Body = io.NopCloser(bytes.NewReader(body))
		}

		resp, err = c.HttpClient.Do(req)

		return c.RetryPolicy(resp, err)
	})

	return resp, err
}
