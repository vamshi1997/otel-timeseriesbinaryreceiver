package timeseriesbinaryreceiver

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

type metricSample struct {
	Name      string
	Labels    map[string]string
	Timestamp time.Time
	Value     float64
}

func decodeTimeseriesBinary(r io.Reader) ([]metricSample, error) {
	samples := []metricSample{}
	for {
		lfm, ms, value, err := readRecord(r)
		if err == io.EOF {
			return samples, nil
		}
		if err != nil {
			return nil, err
		}

		name, labels, err := parseLFM(lfm)
		if err != nil {
			return nil, err
		}

		var ts time.Time
		if ms != 0 {
			ts = time.Unix(0, ms*int64(time.Millisecond)).UTC()
		}

		samples = append(samples, metricSample{
			Name:      name,
			Labels:    labels,
			Timestamp: ts,
			Value:     value,
		})
	}
}

func readRecord(r io.Reader) (string, int64, float64, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if err == io.EOF {
			return "", 0, 0, io.EOF
		}
		return "", 0, 0, fmt.Errorf("read lfm length: %w", err)
	}
	lfmLen := binary.LittleEndian.Uint16(lenBuf[:])

	lfmBytes := make([]byte, lfmLen)
	if _, err := io.ReadFull(r, lfmBytes); err != nil {
		return "", 0, 0, fmt.Errorf("read lfm bytes: %w", err)
	}

	var msBuf [8]byte
	if _, err := io.ReadFull(r, msBuf[:]); err != nil {
		return "", 0, 0, fmt.Errorf("read timestamp: %w", err)
	}
	ms := int64(binary.LittleEndian.Uint64(msBuf[:]))

	var valBuf [8]byte
	if _, err := io.ReadFull(r, valBuf[:]); err != nil {
		return "", 0, 0, fmt.Errorf("read value: %w", err)
	}
	value := math.Float64frombits(binary.LittleEndian.Uint64(valBuf[:]))

	return string(lfmBytes), ms, value, nil
}

func parseLFM(lfm string) (string, map[string]string, error) {
	parts := strings.Split(lfm, "\x00")
	if len(parts)%2 != 1 || len(parts) == 0 {
		return "", nil, fmt.Errorf("invalid lfm format")
	}

	labels := make(map[string]string, (len(parts)-1)/2)
	for i := 1; i < len(parts); i += 2 {
		labels[parts[i]] = parts[i+1]
	}

	return parts[0], labels, nil
}
