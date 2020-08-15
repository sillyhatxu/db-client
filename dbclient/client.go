package dbclient

import (
	"database/sql"
	"fmt"
	"net/url"
	"time"
)

const (
	dsnFormat               = "%s:%s@tcp(%s:%d)/%s?%s"
	driverName              = "mysql"
	allowAllFiles           = true
	allowCleartextPasswords = true
	allowNativePasswords    = true
	allowOldPasswords       = true
	charset                 = "utf8mb4"
	collation               = "utf8mb4_general_ci"
	clientFoundRows         = false
	columnsWithAlias        = false
	interpolateParams       = false
	loc                     = "Asia/Singapore" //Asia%2FSingapore
	maxAllowedPacket        = 4194304          //default : 4MB
	multiStatements         = false
	parseTime               = true
	readTimeout             = time.Duration(30) * time.Second
	rejectReadOnly          = false
	timeout                 = time.Duration(30) * time.Second
	tls                     = false
	writeTimeout            = time.Duration(30) * time.Second
	maxIdleConns            = 2
	maxOpenConns            = 5
	connMaxLifetime         = time.Duration(6) * time.Hour
)

func NewDBClient(opts ...Option) (*sql.DB, error) {
	//default
	config := &Config{
		driverName:              driverName,
		allowAllFiles:           allowAllFiles,
		allowCleartextPasswords: allowCleartextPasswords,
		allowNativePasswords:    allowNativePasswords,
		allowOldPasswords:       allowOldPasswords,
		charset:                 charset,
		collation:               collation,
		clientFoundRows:         clientFoundRows,
		columnsWithAlias:        columnsWithAlias,
		interpolateParams:       interpolateParams,
		loc:                     loc,
		maxAllowedPacket:        maxAllowedPacket,
		multiStatements:         multiStatements,
		parseTime:               parseTime,
		readTimeout:             readTimeout,
		rejectReadOnly:          rejectReadOnly,
		timeout:                 timeout,
		tls:                     tls,
		writeTimeout:            writeTimeout,
		maxIdleConns:            maxIdleConns,
		maxOpenConns:            maxOpenConns,
		connMaxLifetime:         connMaxLifetime,
		ddlPath:                 "",
		flyway:                  false,
	}
	for _, opt := range opts {
		opt(config)
	}
	pool, err := getDatabasePool(*config)
	if err != nil {
		return nil, err
	}
	return pool, pool.Ping()
}

func getMysqlDataSourceName(config Config) string {
	params := url.Values{}
	params.Add("allowAllFiles", setupBool(config.allowAllFiles))
	params.Add("allowCleartextPasswords", setupBool(config.allowCleartextPasswords))
	params.Add("allowNativePasswords", setupBool(config.allowNativePasswords))
	params.Add("allowOldPasswords", setupBool(config.allowOldPasswords))
	params.Add("charset", config.charset)
	params.Add("collation", config.collation)
	params.Add("clientFoundRows", setupBool(config.clientFoundRows))
	params.Add("columnsWithAlias", setupBool(config.columnsWithAlias))
	params.Add("interpolateParams", setupBool(config.interpolateParams))
	params.Add("loc", config.loc)
	params.Add("maxAllowedPacket", setupInt64(config.maxAllowedPacket))
	params.Add("multiStatements", setupBool(config.multiStatements))
	params.Add("parseTime", setupBool(config.parseTime))
	params.Add("readTimeout", setupTime(config.readTimeout))
	params.Add("rejectReadOnly", setupBool(config.rejectReadOnly))
	if config.serverPubKey != nil {
		params.Add("serverPubKey", *config.serverPubKey)
	}
	params.Add("timeout", setupTime(config.timeout))
	params.Add("tls", setupBool(config.tls))
	params.Add("writeTimeout", setupTime(config.writeTimeout))
	return fmt.Sprintf(dsnFormat, config.userName, config.password, config.host, config.port, config.schema, params.Encode())
}

func getDatabasePool(config Config) (*sql.DB, error) {
	dataSourceName := getMysqlDataSourceName(config)
	pool, err := sql.Open(config.driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	pool.SetMaxIdleConns(config.maxIdleConns)
	pool.SetMaxOpenConns(config.maxOpenConns)
	pool.SetConnMaxLifetime(config.connMaxLifetime)
	return pool, nil
}
