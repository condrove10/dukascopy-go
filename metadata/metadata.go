package metadata

type Metadata struct {
	Title              string   `json:"title"`
	Special            bool     `json:"special"`
	Name               string   `json:"name"`
	Description        string   `json:"description"`
	HistoricalFilename string   `json:"historical_filename"`
	PipValue           float32  `json:"pipValue"`
	BaseCurrency       string   `json:"base_currency"`
	QuoteCurrency      string   `json:"quote_currency"`
	TagList            []string `json:"tag_list"`
	HistoryStartTick   string   `json:"history_start_tick"`
	HistoryStart10sec  string   `json:"history_start_10sec"`
	HistoryStart60sec  string   `json:"history_start_60sec"`
	HistoryStart60min  string   `json:"history_start_60min"`
	HistoryStartDay    string   `json:"history_start_day"`
}
