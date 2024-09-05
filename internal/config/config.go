package config

import (
	"context"
	"fmt"

	"github.com/sethvargo/go-envconfig"
)

type Config struct {
	ListenAddress      string   `env:"LISTEN_ADDRESS, default=:8080"`
	AdminListenAddress string   `env:"ADMIN_LISTEN_ADDRESS, default=:8081"`
	IdmURL             string   `env:"IDM_URL"`
	AllowedOrigins     []string `env:"ALLOWED_ORIGINS, default=*"`
	MongoDBURL         string   `env:"MONGO_URL"`
	MongoDatabaseName  string   `env:"MONGO_DATABASE, default=customer-service"`
}

func LoadConfig(ctx context.Context) (*Config, error) {
	var cfg Config

	if err := envconfig.Process(ctx, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse configuration from environment: %w", err)
	}

	return &cfg, nil
}
