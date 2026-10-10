package config

import (
	"os"
	"strconv"
	"time"
)

// Config represents the application configuration
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	AWS      AWSConfig
	WAL      WALConfig
	AI       AIConfig
}

type ServerConfig struct {
	Port         string
	Env          string
	LogLevel     string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type DatabaseConfig struct {
	URL          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  time.Duration
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type AWSConfig struct {
	Region               string
	Endpoint             string
	AccessKeyID          string
	SecretAccessKey      string
	SQSIncidentsQueueURL string
	SQSCreditsQueueURL   string
	S3ReceiptsBucket     string
}

type WALConfig struct {
	Directory       string
	SegmentMaxBytes int64
	BatchDelay      time.Duration
	BatchMaxSize    int
}

type AIConfig struct {
	Provider     string
	GeminiAPIKey string
	GroqAPIKey   string
}

// Load reads configuration from environment variables with sensible defaults
func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Port:         getEnv("PORT", "8080"),
			Env:          getEnv("ENV", "development"),
			LogLevel:     getEnv("LOG_LEVEL", "debug"),
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		},
		Database: DatabaseConfig{
			URL:          getEnv("DATABASE_URL", "postgres://ecobrio:ecobriosecret@localhost:5432/ecobrio_db?sslmode=disable"),
			MaxOpenConns: getEnvAsInt("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns: getEnvAsInt("DB_MAX_IDLE_CONNS", 10),
			MaxIdleTime:  5 * time.Minute,
		},
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: getEnv("REDIS_PASSWORD", "ecobrioredis"),
			DB:       getEnvAsInt("REDIS_DB", 0),
		},
		AWS: AWSConfig{
			Region:               getEnv("AWS_REGION", "us-east-1"),
			Endpoint:             getEnv("AWS_ENDPOINT", "http://localhost:4566"),
			AccessKeyID:          getEnv("AWS_ACCESS_KEY_ID", "test"),
			SecretAccessKey:      getEnv("AWS_SECRET_ACCESS_KEY", "test"),
			SQSIncidentsQueueURL: getEnv("SQS_INCIDENTS_QUEUE_URL", "http://localhost:4566/000000000000/ecobrio-incidents"),
			SQSCreditsQueueURL:   getEnv("SQS_CREDITS_QUEUE_URL", "http://localhost:4566/000000000000/ecobrio-credits"),
			S3ReceiptsBucket:     getEnv("S3_RECEIPTS_BUCKET", "ecobrio-receipts"),
		},
		WAL: WALConfig{
			Directory:       getEnv("WAL_DIR", "./data/wal"),
			SegmentMaxBytes: getEnvAsInt64("WAL_SEGMENT_MAX_BYTES", 10*1024*1024), // 10 MB
			BatchDelay:      time.Duration(getEnvAsInt("WAL_BATCH_DELAY_MS", 10)) * time.Millisecond,
			BatchMaxSize:    getEnvAsInt("WAL_BATCH_MAX_SIZE", 50),
		},
		AI: AIConfig{
			Provider:     getEnv("AI_PROVIDER", "mock"),
			GeminiAPIKey: getEnv("GEMINI_API_KEY", ""),
			GroqAPIKey:   getEnv("GROQ_API_KEY", ""),
		},
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}

func getEnvAsInt64(key string, defaultVal int64) int64 {
	valStr := getEnv(key, "")
	if val, err := strconv.ParseInt(valStr, 10, 64); err == nil {
		return val
	}
	return defaultVal
}
