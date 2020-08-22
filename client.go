package mysqlclient

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"time"
)

var (
	CheckConfigNilError = errors.New("check config nil")
	CheckDBPoolError    = errors.New("check db pool nil")
)

type MysqlClient struct {
	config *Config
	mu     sync.Mutex
}

func NewMysqlClient(opts ...Option) (*MysqlClient, error) {
	//default
	config := &Config{
		ddlPath: "",
		flyway:  false,
		timeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(config)
	}
	mc := &MysqlClient{
		config: config,
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	err := mc.validate()
	if err != nil {
		return nil, err
	}
	err = mc.initialFlayway()
	if err != nil {
		return nil, err
	}
	return mc, nil
}

func (mc *MysqlClient) validate() error {
	if mc.config == nil {
		return CheckConfigNilError
	}
	if mc.config.pool == nil {
		return CheckDBPoolError
	}
	return mc.Ping()
}

func (mc *MysqlClient) Ping() error {
	return mc.GetDB().Ping()
}

func (mc *MysqlClient) GetDB() *sql.DB {
	return mc.config.pool
}

func (mc *MysqlClient) GetTransaction() (*sql.Tx, error) {
	return mc.GetDB().Begin()
}
