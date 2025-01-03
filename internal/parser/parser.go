package parser

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/condrove10/dukascopy-go/metadata"
	"github.com/condrove10/dukascopy-go/tick"
	"github.com/kjk/lzma"
)

type MetadataResponse struct {
	Instruments map[string]*metadata.Metadata `json:"instruments"`
}

const TickBytes = 20

func Decode(data []byte, symbol string, decimalFactor float32, date time.Time) ([]*tick.Tick, error) {
	dec := lzma.NewReader(bytes.NewBuffer(data[:]))
	defer dec.Close()

	ticksArr := make([]*tick.Tick, 0)
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

func decodeTickData(data []byte, symbol string, decimalFactor float32, timeH time.Time) (*tick.Tick, error) {
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

	t := tick.Tick{
		Symbol:    symbol,
		Timestamp: timeH.UnixNano() + int64(raw.TimeMs)*int64(time.Millisecond),
		Ask:       float64(raw.Ask) / float64(decimalFactor),
		Bid:       float64(raw.Bid) / float64(decimalFactor),
		VolumeAsk: float64(raw.VolumeAsk),
		VolumeBid: float64(raw.VolumeBid),
	}

	return &t, nil
}
