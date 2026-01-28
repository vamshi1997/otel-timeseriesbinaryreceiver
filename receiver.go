package timeseriesbinaryreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/golang/snappy"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type timeseriesBinaryReceiver struct {
	settings receiver.Settings
	cfg      *Config
	next     consumer.Metrics
	server   *http.Server
}

func newTimeseriesBinaryReceiver(settings receiver.Settings, cfg *Config, next consumer.Metrics) receiver.Metrics {
	return &timeseriesBinaryReceiver{
		settings: settings,
		cfg:      cfg,
		next:     next,
	}
}

func (r *timeseriesBinaryReceiver) Start(ctx context.Context, host component.Host) error {
	listener, err := net.Listen("tcp", r.cfg.Endpoint)
	if err != nil {
		return err
	}

	r.server = &http.Server{
		Handler:      r,
		ReadTimeout:  r.cfg.ReadTimeout,
		WriteTimeout: r.cfg.WriteTimeout,
	}

	go func() {
		if err := r.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.settings.Logger.Error("timeseries receiver server failed", zap.Error(err))
		}
	}()

	r.settings.Logger.Info("timeseries receiver listening", zap.String("endpoint", r.cfg.Endpoint))
	return nil
}

func (r *timeseriesBinaryReceiver) Shutdown(ctx context.Context) error {
	if r.server == nil {
		return nil
	}
	return r.server.Shutdown(ctx)
}

func (r *timeseriesBinaryReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	if !strings.HasPrefix(req.URL.Path, "/v1/metrics/") {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	defer req.Body.Close()
	snappyReader := snappy.NewReader(req.Body)
	samples, err := decodeTimeseriesBinary(snappyReader)
	if err != nil {
		r.settings.Logger.Warn("failed to decode timeseries payload", zap.Error(err))
		http.Error(w, "invalid timeseries payload", http.StatusBadRequest)
		return
	}

	if len(samples) > 0 {
		metrics := metricsFromSamples(samples)
		if err := r.next.ConsumeMetrics(req.Context(), metrics); err != nil {
			r.settings.Logger.Error("failed to consume metrics", zap.Error(err))
			http.Error(w, "failed to ingest metrics", http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(sonarResponse{
		Success:          true,
		FrequencySeconds: 60,
		MaxBatchSize:     1000,
		MaxMetricLength:  512,
	})
}

type sonarResponse struct {
	Success          bool  `json:"success"`
	FrequencySeconds int32 `json:"frequency"`
	MaxBatchSize     int32 `json:"max_metrics"`
	MaxMetricLength  int32 `json:"max_lfm"`
}

func metricsFromSamples(samples []metricSample) pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	if len(samples) == 0 {
		return metrics
	}

	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("do-agent-timeseries")

	metricIndex := map[string]pmetric.Metric{}
	for _, sample := range samples {
		metric, ok := metricIndex[sample.Name]
		if !ok {
			metric = sm.Metrics().AppendEmpty()
			metric.SetName(sample.Name)
			metric.SetEmptyGauge()
			metricIndex[sample.Name] = metric
		}

		dp := metric.Gauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(sample.Value)
		if sample.Timestamp.IsZero() {
			dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now().UTC()))
		} else {
			dp.SetTimestamp(pcommon.NewTimestampFromTime(sample.Timestamp))
		}

		attrs := dp.Attributes()
		for key, value := range sample.Labels {
			attrs.PutStr(key, value)
		}
	}

	return metrics
}
