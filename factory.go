package timeseriesbinaryreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	defaultEndpoint     = "0.0.0.0:4319"
	defaultReadTimeout  = 30 * time.Second
	defaultWriteTimeout = 30 * time.Second
)

var (
	typeStr           = component.MustNewType("timeseriesbinary")
	receiverStability = component.StabilityLevelAlpha
)

// NewFactory creates a factory for the timeseries binary receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, receiverStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:     defaultEndpoint,
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
	}
}

func createMetricsReceiver(_ context.Context, settings receiver.Settings, cfg component.Config, next consumer.Metrics) (receiver.Metrics, error) {
	return newTimeseriesBinaryReceiver(settings, cfg.(*Config), next), nil
}
