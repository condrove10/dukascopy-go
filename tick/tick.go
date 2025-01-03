package tick

type Tick struct {
	Symbol    string  `validate:"required" json:"symbol" csv:"symbol"`
	Timestamp int64   `validate:"required" json:"timestamp" csv:"timestamp"`
	Ask       float64 `validate:"required" json:"ask" csv:"ask"`
	Bid       float64 `validate:"required" json:"bid" csv:"bid"`
	VolumeAsk float64 `validate:"required" json:"volume_ask" csv:"volume_ask"`
	VolumeBid float64 `validate:"required" json:"volume_bid" csv:"volume_bid"`
}
