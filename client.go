package mysqlclient

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sillyhatxu/db-client/customerrors"
	"sync"
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
		return customerrors.CheckConfigNilError
	}
	if mc.config.pool == nil {
		return customerrors.CheckDBPoolError
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
