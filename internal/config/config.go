package config

type Config struct {
	DataDir            string // folder store logs
	Port               string // TCP Broker gate listening
	MaxSegmentBytes    int64  // max size of 1 log segment file
	IndexIntervalBytes int64  // after a number of byte wrote in .log -> write 1 more entry into .index file (4096)
}

func DefaultConfig() *Config {
	return &Config{
		DataDir:            "./data",
		Port:               "9092",
		MaxSegmentBytes:    10 * 1024 * 1024, // 10kb
		IndexIntervalBytes: 4096,             // 4kb
	}
}
