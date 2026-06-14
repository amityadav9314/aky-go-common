package tavily

import (
	"context"
	"fmt"
	"time"

	"aky-go-common/httpclient"
)

type Client struct {
	apiKey string
}

func NewClient(apiKey string) *Client {
	return &Client{apiKey: apiKey}
}

func (c *Client) Search(ctx context.Context, req *SearchRequest) (*SearchResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	req.Normalize()

	var res SearchResponse
	_, err := httpclient.GetClient().Do(ctx, &httpclient.RequestOptions{
		Method:        "POST",
		URL:           "https://api.tavily.com/search",
		Headers:       map[string]string{"Authorization": "Bearer " + c.apiKey},
		Body:          req,
		ReadTimeout:   30 * time.Second,
		LogReqRes:     true,
		ResponseModel: &res,
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}
