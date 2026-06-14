package tavily

// Topic enum
type Topic string

const (
	TopicFinance Topic = "finance"
	TopicNews    Topic = "news"
	TopicGeneral Topic = "general"
)

// SearchDepth enum
type SearchDepth string

const (
	SearchDepthAdvanced  SearchDepth = "advanced"
	SearchDepthFast      SearchDepth = "fast"
	SearchDepthBasic     SearchDepth = "basic"
	SearchDepthUltraFast SearchDepth = "ultra-fast"
)

// IncludeRawContent enum
type IncludeRawContent string

const (
	IncludeRawContentNone     IncludeRawContent = "text"
	IncludeRawContentText     IncludeRawContent = "text"
	IncludeRawContentMarkdown IncludeRawContent = "markdown"
)

type SearchRequest struct {
	Query                string             `json:"query"`
	TopicVal             *Topic             `json:"topic,omitempty"`
	SearchDepthVal       *SearchDepth       `json:"search_depth,omitempty"`
	MaxResults           *int               `json:"max_results,omitempty"`
	TimeRange            *string            `json:"time_range,omitempty"`
	IncludeUsage         *bool              `json:"include_usage,omitempty"`
	IncludeRawContentVal *IncludeRawContent `json:"include_raw_content,omitempty"`
	Country              *string            `json:"country,omitempty"`
}

type Results struct {
	Url        string  `json:"url"`
	Title      string  `json:"title"`
	Content    string  `json:"content"`
	Score      float64 `json:"score"`
	RawContent string  `json:"raw_content,omitempty"`
}

type SearchResponse struct {
	Query        string    `json:"query"`
	Results      []Results `json:"results"`
	ResponseTime float64   `json:"response_time"`
	RequestId    string    `json:"request_id"`
}

// ===== DEFAULT NORMALIZATION =====

func (r *SearchRequest) Normalize() {
	if r.TopicVal == nil {
		v := TopicGeneral
		r.TopicVal = &v
	}
	if r.SearchDepthVal == nil {
		v := SearchDepthBasic
		r.SearchDepthVal = &v
	}
	if r.MaxResults == nil {
		v := 5
		r.MaxResults = &v
	}
	// Don't default time_range — let Tavily search across all time
	if r.IncludeUsage == nil {
		v := true
		r.IncludeUsage = &v
	}
	if r.IncludeRawContentVal == nil {
		v := IncludeRawContentText
		r.IncludeRawContentVal = &v
	}
	if r.Country == nil {
		v := "india"
		r.Country = &v
	}
}
