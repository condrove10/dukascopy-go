package csvencoder

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strings"
)

type CSVEncoder struct {
	separator rune
	headers   []string
}

func NewCSVEncoder() *CSVEncoder {
	return &CSVEncoder{
		separator: ',',
	}
}

func (e *CSVEncoder) SetSeparator(sep rune) {
	e.separator = sep
}

func (e *CSVEncoder) flattenMap(m map[string]interface{}, prefix string) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		key := k
		if prefix != "" {
			key = prefix + "_" + k
		}
		if nestedMap, ok := v.(map[string]interface{}); ok {
			for nk, nv := range e.flattenMap(nestedMap, key) {
				result[nk] = nv
			}
		} else {
			result[key] = v
		}
	}
	return result
}

func (e *CSVEncoder) generateHeaders(data []map[string]interface{}) {
	headerSet := make(map[string]bool)
	for _, item := range data {
		flatItem := e.flattenMap(item, "")
		for k := range flatItem {
			headerSet[k] = true
		}
	}

	e.headers = make([]string, 0, len(headerSet))
	for k := range headerSet {
		e.headers = append(e.headers, k)
	}
	sort.Strings(e.headers)
}

func (e *CSVEncoder) convertToMatrix(data []map[string]interface{}) [][]interface{} {
	if len(data) == 0 {
		return nil
	}

	e.generateHeaders(data)
	matrix := make([][]interface{}, len(data)+1)

	// Add headers as the first row
	matrix[0] = make([]interface{}, len(e.headers))
	for i, header := range e.headers {
		matrix[0][i] = header
	}

	// Add data rows
	for i, item := range data {
		flatItem := e.flattenMap(item, "")
		row := make([]interface{}, len(e.headers))
		for j, header := range e.headers {
			row[j] = flatItem[header]
		}
		matrix[i+1] = row
	}

	return matrix
}

func (e *CSVEncoder) Encode(w io.Writer, data []map[string]interface{}) error {
	matrix := e.convertToMatrix(data)
	if matrix == nil {
		return nil
	}

	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = e.separator

	for _, row := range matrix {
		stringRow := make([]string, len(row))
		for i, cell := range row {
			stringRow[i] = fmt.Sprintf("%v", cell)
		}
		if err := csvWriter.Write(stringRow); err != nil {
			return err
		}
	}

	csvWriter.Flush()
	return csvWriter.Error()
}

func (e *CSVEncoder) EncodeToString(data []map[string]interface{}) (string, error) {
	var b strings.Builder
	err := e.Encode(&b, data)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}
