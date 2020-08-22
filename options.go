package mysqlclient

import (
	"database/sql"
	"time"
)

type Config struct {
	timeout time.Duration
	pool    *sql.DB
	ddlPath string
	flyway  bool
}

type Option func(*Config)

func Pool(pool *sql.DB) Option {
	return func(c *Config) {
		c.pool = pool
	}
}

func DDLPath(ddlPath string) Option {
	return func(c *Config) {
		c.ddlPath = ddlPath
	}
}

func Flyway(flyway bool) Option {
	return func(c *Config) {
		c.flyway = flyway
	}
}

func Timeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.timeout = timeout
	}
}
