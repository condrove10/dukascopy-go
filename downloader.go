package downloader

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/condrove10/dukascopy-go/channelmanager"
	"github.com/condrove10/dukascopy-go/internal/conversions"
	"github.com/condrove10/dukascopy-go/internal/csvencoder"
	"github.com/condrove10/dukascopy-go/internal/parser"
	"github.com/condrove10/dukascopy-go/metadata"
	"github.com/condrove10/dukascopy-go/tick"
	"github.com/condrove10/retryablehttp"
	"github.com/condrove10/retryablehttp/backoffpolicy"
	"github.com/go-playground/validator/v10"
)

const urlTemplate = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5"

var headers = map[string]string{
	"User-Agent":      "Dukascopy Go Project",
	"Accept":          "*/*",
	"Connection":      "keep-alive",
	"Origin":          "https://freeserv.dukascopy.com",
	"Referer":         "https://freeserv.dukascopy.com",
	"Accept-Encoding": "gzip, deflate",
	"Cache-Control":   "no-cache",
}

type Downloader struct {
	concurrency     int           `validate:"required,gt=0"`
	httpClient      *http.Client  `validate:"required"`
	bufferSize      int           `validate:"required,gt=0"`
	concurrencyChan chan struct{} `validate:"required"`
}

func (d *Downloader) WithConcurrency(concurrency int) *Downloader {
	if err := validator.New().Var(concurrency, "required,gt=0"); err != nil {
		panic(fmt.Errorf("invalid value: %w", err))
	}
	d.concurrency = concurrency
	d.concurrencyChan = make(chan struct{}, concurrency)
	return d
}

func (d *Downloader) WithHttpClient(httpClient *http.Client) *Downloader {
	d.httpClient = httpClient
	return d
}

func (d *Downloader) WithBufferSize(bufferSize int) *Downloader {
	if err := validator.New().Var(bufferSize, "required,gt=0"); err != nil {
		panic(fmt.Errorf("invalid value: %w", err))
	}
	d.bufferSize = bufferSize
	return d
}

var DefaultDownloader = &Downloader{
	concurrency:     1,
	httpClient:      http.DefaultClient,
	bufferSize:      10000,
	concurrencyChan: make(chan struct{}, 1),
}

func (d *Downloader) Download(symbol string, start, end time.Time) ([]*tick.Tick, error) {
	ticks := []*tick.Tick{}
	ticksMap := map[int64][]*tick.Tick{}

	m, err := d.Stream(symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to intialize stream: %w", err)
	}

	if err := m.Process(func(data *tick.Tick) error {
		timestampTrunc := conversions.TruncateTimestampToHour(data.Timestamp)
		if _, ok := ticksMap[timestampTrunc]; !ok {
			ticksMap[timestampTrunc] = []*tick.Tick{}
		}
		ticksMap[timestampTrunc] = append(ticksMap[timestampTrunc], data)

		return nil
	}); err != nil {
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

func (d *Downloader) Stream(symbol string, start, end time.Time) (*channelmanager.ChannelManager[*tick.Tick], error) {
	symbol = strings.ToUpper(symbol)

	if err := validator.New().Struct(d); err != nil {
		return nil, fmt.Errorf("failed to validate downloader instance: %w", err)
	}

	if end.Before(start) {
		return nil, fmt.Errorf("end time must be after start time")
	}

	metadata, err := d.getInstrumentsMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	decimalFactor, err := getInstrumentDecimalFactor(metadata, symbol)
	if err != nil {
		return nil, fmt.Errorf("metadata not found: %w", err)
	}

	var wg sync.WaitGroup
	manager := channelmanager.NewChannelManager[*tick.Tick](d.bufferSize)

	go func() {
		for t := start.UTC().Truncate(time.Hour); t.Before(end.UTC().Truncate(time.Hour)) || t.Equal(end.UTC().Truncate(time.Hour)); t = t.Add(time.Hour) {
			if t.After(time.Now().UTC().Truncate(time.Hour)) {
				continue
			}

			if t.Equal(end.UTC().Truncate(time.Hour)) && t.Equal(time.Now().UTC().Truncate(time.Hour)) {
				continue
			}

			wg.Add(1)
			d.concurrencyChan <- struct{}{}

			manager.Subscribe(func(c chan<- *tick.Tick) error {
				defer func() {
					wg.Done()
					<-d.concurrencyChan
				}()

				batch, err := d.fetch(symbol, decimalFactor, t)
				if err != nil {
					return fmt.Errorf("failed to fetch batch: %w", err)
				}

				switch t {
				case start.UTC().Truncate(time.Hour):
					for _, v := range batch {
						if v.Timestamp >= start.UTC().UnixNano() {
							c <- v
						}
					}
				case end.UTC().Truncate(time.Hour):
					for _, v := range batch {
						if v.Timestamp < end.UTC().UnixNano() {
							c <- v
						}
					}
				default:
					for _, v := range batch {
						c <- v
					}
				}

				return nil
			})

		}

		wg.Wait()
		manager.Close()
	}()

	return manager, nil
}

func (d *Downloader) ToCsv(symbol string, start, end time.Time, filePath string) error {
	ce := csvencoder.NewCSVEncoder()
	ce.SetSeparator(';')

	ticksSlice, err := d.Download(symbol, start, end)
	if err != nil {
		return fmt.Errorf("failed to download ticks: %w", err)
	}

	ticksMapSpice := []map[string]interface{}{}
	for _, t := range ticksSlice {
		ticksMap, err := conversions.StructToMap(t, "csv")
		if err != nil {
			return fmt.Errorf("failed to convert ticks: %w", err)
		}

		ticksMapSpice = append(ticksMapSpice, ticksMap)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	defer f.Close()

	if err := ce.Encode(f, ticksMapSpice); err != nil {
		return fmt.Errorf("failed to encode csv: %w", err)
	}

	return nil
}

func (d *Downloader) fetch(symbol string, decimalFactor float32, date time.Time) ([]*tick.Tick, error) {
	url := fmt.Sprintf(urlTemplate, symbol, date.Year(), date.Month()-1, date.Day(), date.Hour())

	client := retryablehttp.Client{
		Context:       context.TODO(),
		HttpClient:    d.httpClient,
		RetryAttempts: 10,
		RetryDelay:    time.Millisecond * 30,
		RetryStrategy: backoffpolicy.StrategyExponential,
		RetryPolicy: func(resp *http.Response, err error) error {
			if err != nil {
				return err
			}

			switch resp.StatusCode {
			case http.StatusOK, http.StatusNotFound:
				return nil
			default:
				return fmt.Errorf("unexpected status code: %s", resp.Status)
			}
		},
	}

	resp, err := client.Get(url, headers)
	if err != nil {
		return nil, fmt.Errorf("error fetching data for url '%s': %w", url, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return []*tick.Tick{}, nil
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

	parsedTicks, err := parser.Decode(content, symbol, decimalFactor, date)
	if err != nil {
		return nil, fmt.Errorf("failed to parse data: %w", err)
	}

	return parsedTicks, nil
}

func (d *Downloader) getInstrumentsMetadata() (map[string]*metadata.Metadata, error) {
	url := "https://freeserv.dukascopy.com/2.0/index.php?path=common/instruments"

	client := retryablehttp.Client{
		Context:       context.TODO(),
		HttpClient:    d.httpClient,
		RetryAttempts: 10,
		RetryDelay:    time.Millisecond * 30,
		RetryStrategy: backoffpolicy.StrategyExponential,
		RetryPolicy: func(resp *http.Response, err error) error {
			if err != nil {
				return err
			}

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("unexpected status code: %s", resp.Status)
			}

			return nil
		},
	}

	resp, err := client.Get(url, headers)
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

	metadataResp := parser.MetadataResponse{}

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
