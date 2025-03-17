package tick

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/condrove10/dukascopy-go/metadata"
	"github.com/condrove10/retryablehttp"
	"github.com/kjk/lzma"
)

const urlTemplate = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5"

var headers = map[string]string{
	"User-Agent":      "github.com/condrove10/dukascopy-go",
	"Accept":          "*/*",
	"Connection":      "keep-alive",
	"Origin":          "https://freeserv.dukascopy.com",
	"Referer":         "https://freeserv.dukascopy.com",
	"Accept-Encoding": "gzip, deflate",
	"Cache-Control":   "no-cache",
}

type Tick struct {
	Symbol    string  `json:"symbol"`
	Timestamp uint64  `json:"timestamp"`
	Ask       float64 `json:"ask"`
	Bid       float64 `json:"bid"`
	VolumeAsk float64 `json:"volume_ask"`
	VolumeBid float64 `json:"volume_bid"`
}

type Client struct {
	ctx             context.Context
	httpClient      *retryablehttp.Client
	concurrencyChan chan struct{}
}

func NewClient(ctx context.Context, httpClient *retryablehttp.Client, concurrency int) *Client {
	return &Client{
		ctx:             ctx,
		httpClient:      httpClient,
		concurrencyChan: make(chan struct{}, concurrency),
	}
}

func (c *Client) Download(symbol string, start, end time.Time) ([]*Tick, error) {
	ticks := []*Tick{}
	ticksMap := map[int64][]*Tick{}

	dataCh, errCh, err := c.Stream(symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to intialize stream: %w", err)
	}

	go func() {
		for {
			data, open := <-dataCh
			if !open {
				return
			}
			timestampTrunc := time.Unix(0, int64(data.Timestamp)).Truncate(time.Hour).Unix()
			if _, ok := ticksMap[timestampTrunc]; !ok {
				ticksMap[timestampTrunc] = []*Tick{}
			}
			ticksMap[timestampTrunc] = append(ticksMap[timestampTrunc], data)

		}
	}()

	err = <-errCh
	if err != nil {
		return nil, fmt.Errorf("failed to consume ticks: %w", err)
	}

	hours := []int64{}

	for k := range ticksMap {
		hours = append(hours, k)
	}

	sort.Slice(hours, func(i, j int) bool {
		return hours[i] < hours[j]
	})

	for _, h := range hours {
		ticks = append(ticks, ticksMap[h]...)
	}

	return ticks, nil
}

func (c *Client) Stream(symbol string, start, end time.Time) (<-chan *Tick, <-chan error, error) {
	symbol = strings.ToUpper(symbol)

	if end.Before(start) {
		return nil, nil, fmt.Errorf("end time must be after start time")
	}

	metadata, err := c.getInstrumentsMetadata()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	decimalFactor, err := getInstrumentDecimalFactor(metadata, symbol)
	if err != nil {
		return nil, nil, fmt.Errorf("metadata not found: %w", err)
	}

	var wg sync.WaitGroup
	dataCh := make(chan *Tick, 1)
	errCh := make(chan error, 1)

	go func() {
		for t := start.UTC().Truncate(time.Hour); t.Before(end.UTC().Truncate(time.Hour)) || t.Equal(end.UTC().Truncate(time.Hour)); t = t.Add(time.Hour) {
			if t.After(time.Now().UTC().Truncate(time.Hour)) {
				continue
			}

			if t.Equal(end.UTC().Truncate(time.Hour)) && t.Equal(time.Now().UTC().Truncate(time.Hour)) {
				continue
			}

			wg.Add(1)
			c.concurrencyChan <- struct{}{}

			go func() {
				defer func() {
					wg.Done()
					<-c.concurrencyChan
				}()

				batch, err := c.fetch(symbol, decimalFactor, t)
				if err != nil {
					errCh <- fmt.Errorf("failed to fetch batch: %w", err)
				}

				switch t {
				case start.UTC().Truncate(time.Hour):
					for _, v := range batch {
						if v.Timestamp >= uint64(start.UTC().UnixNano()) {
							dataCh <- v
						}
					}
				case end.UTC().Truncate(time.Hour):
					for _, v := range batch {
						if v.Timestamp < uint64(end.UTC().UnixNano()) {
							dataCh <- v
						}
					}
				default:
					for _, v := range batch {
						dataCh <- v
					}
				}
			}()
		}

		wg.Wait()
		close(dataCh)
		close(errCh)
	}()

	return dataCh, errCh, nil
}

func (c *Client) fetch(symbol string, decimalFactor float32, date time.Time) ([]*Tick, error) {
	url := fmt.Sprintf(urlTemplate, symbol, date.Year(), date.Month()-1, date.Day(), date.Hour())

	resp, err := c.httpClient.Get(url, headers)
	if err != nil {
		return nil, fmt.Errorf("error fetching data for url '%s': %w", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		return []*Tick{}, nil
	}

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading data for url '%s': %w", url, err)
	}

	if len(content) == 0 {
		return []*Tick{}, nil
	}

	parsedTicks, err := decode(content, symbol, decimalFactor, date)
	if err != nil {
		return nil, fmt.Errorf("failed to parse data: %w", err)
	}

	return parsedTicks, nil
}

type metadataResponse struct {
	Instruments map[string]*metadata.Metadata `json:"instruments"`
}

func (c *Client) getInstrumentsMetadata() (map[string]*metadata.Metadata, error) {
	url := "https://freeserv.dukascopy.com/2.0/index.php?path=common/instruments"

	resp, err := c.httpClient.Get(url, headers)
	if err != nil {
		return nil, fmt.Errorf("error fetching data for url '%s': %w", url, err)
	}
	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error creating gzip reader: %w", err)
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading data for url '%s': %w", url, err)
	}

	metadataResp := metadataResponse{}

	if err := json.Unmarshal(content[6:len(content)-1], &metadataResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return metadataResp.Instruments, nil
}

func getInstrumentDecimalFactor(instruments map[string]*metadata.Metadata, name string) (float32, error) {
	// credits to Leo4815162342, commit 67c6903
	switch name {
	case "batusd":
		return 100000, nil
	case "uniusd":
		return 1000, nil
	case "lnkusd":
		return 1000, nil
	default:
		for _, instrument := range instruments {
			if strings.ToUpper(instrument.HistoricalFilename) == name {
				return 10 / instrument.PipValue, nil
			}
		}
	}

	return 0, fmt.Errorf("no match found for %s", name)
}

const tickBytes = 20

func decode(data []byte, symbol string, decimalFactor float32, date time.Time) ([]*Tick, error) {
	dec := lzma.NewReader(bytes.NewBuffer(data[:]))
	defer dec.Close()

	ticksArr := make([]*Tick, 0)
	bytesArr := make([]byte, tickBytes)

	for {
		n, err := dec.Read(bytesArr[:])
		if err == io.EOF {
			err = nil
			break
		}
		if n != tickBytes || err != nil {
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

	if len(data) != tickBytes {
		return nil, fmt.Errorf("invalid length for tick data")
	}

	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.BigEndian, &raw); err != nil {
		return nil, fmt.Errorf("fauiled to read buffer: %w", err)
	}

	t := Tick{
		Symbol:    symbol,
		Timestamp: uint64(timeH.UnixNano() + int64(raw.TimeMs)*int64(time.Millisecond)),
		Ask:       float64(raw.Ask) / float64(decimalFactor),
		Bid:       float64(raw.Bid) / float64(decimalFactor),
		VolumeAsk: float64(raw.VolumeAsk),
		VolumeBid: float64(raw.VolumeBid),
	}

	return &t, nil
}
