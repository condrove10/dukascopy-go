package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/kjk/lzma"
)

type Tick struct {
	Symbol    string  `validate:"required" json:"symbol" csv:"symbol"`
	Timestamp int64   `validate:"required" json:"timestamp" csv:"timestamp"`
	Ask       float64 `validate:"required" json:"ask" csv:"ask"`
	Bid       float64 `validate:"required" json:"bid" csv:"bid"`
	VolumeAsk float64 `validate:"required" json:"volume_ask" csv:"volume_ask"`
	VolumeBid float64 `validate:"required" json:"volume_bid" csv:"volume_bid"`
}

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

type MetadataResponse struct {
	Instruments map[string]*Metadata `json:"instruments"`
}

const TickBytes = 20

func Decode(data []byte, symbol string, decimalFactor float32, date time.Time) ([]*Tick, error) {
	dec := lzma.NewReader(bytes.NewBuffer(data[:]))
	defer dec.Close()

	ticksArr := make([]*Tick, 0)
	bytesArr := make([]byte, TickBytes)

	for {
		n, err := dec.Read(bytesArr[:])
		if err == io.EOF {
			err = nil
			break
		}
		if n != TickBytes || err != nil {
			return nil, fmt.Errorf("read mismatch, read %d: %w", n, err)
		}

		t, err := decodeTickData(bytesArr[:], symbol, decimalFactor, date)
		if err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		ticksArr = append(ticksArr, t)
	}

	return ticksArr, nil
}

func decodeTickData(data []byte, symbol string, decimalFactor float32, timeH time.Time) (*Tick, error) {
	raw := struct {
		TimeMs    int32
		Ask       int32
		Bid       int32
		VolumeAsk float32
		VolumeBid float32
	}{}

	if len(data) != TickBytes {
		return nil, fmt.Errorf("invalid length for tick data")
	}

	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.BigEndian, &raw); err != nil {
		return nil, fmt.Errorf("fauiled to read buffer: %w", err)
	}

	t := Tick{
		Symbol:    symbol,
		Timestamp: timeH.UnixNano() + int64(raw.TimeMs)*int64(time.Millisecond),
		Ask:       float64(raw.Ask) / float64(decimalFactor),
		Bid:       float64(raw.Bid) / float64(decimalFactor),
		VolumeAsk: float64(raw.VolumeAsk),
		VolumeBid: float64(raw.VolumeBid),
	}

	return &t, nil
}
