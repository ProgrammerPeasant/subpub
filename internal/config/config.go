package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env        string     `yaml:"env" env:"ENV" env-default:"development"`
	GRPCServer GRPCConfig `yaml:"grpc_server"`
	Log        LogConfig  `yaml:"log"`
}

type GRPCConfig struct {
	Port              string        `yaml:"port" env:"GRPC_PORT" env-default:"8082"`
	Timeout           time.Duration `yaml:"timeout" env:"GRPC_TIMEOUT" env-default:"5s"`
	MaxConnectionIdle time.Duration `yaml:"max_connection_idle" env:"GRPC_MAX_CONNECTION_IDLE" env-default:"10m"`
	TimeoutGraceful   time.Duration `yaml:"timeout_graceful" env:"GRPC_TIMEOUT_GRACEFUL" env-default:"15s"`
}

type LogConfig struct {
	Level string `yaml:"level" env:"LOG_LEVEL" env-default:"info"` // debug, info, warn, error всё стандартно
}

func Load() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "./config.yaml"
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file does not exist: %s", configPath)
	}

	var cfg Config

	err := cleanenv.ReadConfig(configPath, &cfg)
	if err != nil {
		log.Fatalf("cannot read config: %s", err)
	}

	return &cfg
}
