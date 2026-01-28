package timeseriesbinaryreceiver

import "time"

// Config defines configuration for the timeseries binary receiver.
type Config struct {
	Endpoint     string        `mapstructure:"endpoint"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}
