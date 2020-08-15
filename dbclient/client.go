package dbclient

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sillyhatxu/mysql-client/utils"
	"net/url"
	"time"
)

//flayway open multiStatements = true
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
	return pool, nil
}

func getMysqlDataSourceName(config Config) string {
	params := url.Values{}
	params.Add("allowAllFiles", utils.SetupBool(config.allowAllFiles))
	params.Add("allowCleartextPasswords", utils.SetupBool(config.allowCleartextPasswords))
	params.Add("allowNativePasswords", utils.SetupBool(config.allowNativePasswords))
	params.Add("allowOldPasswords", utils.SetupBool(config.allowOldPasswords))
	params.Add("charset", config.charset)
	params.Add("collation", config.collation)
	params.Add("clientFoundRows", utils.SetupBool(config.clientFoundRows))
	params.Add("columnsWithAlias", utils.SetupBool(config.columnsWithAlias))
	params.Add("interpolateParams", utils.SetupBool(config.interpolateParams))
	params.Add("loc", config.loc)
	params.Add("maxAllowedPacket", utils.SetupInt64(config.maxAllowedPacket))
	params.Add("multiStatements", utils.SetupBool(config.multiStatements))
	params.Add("parseTime", utils.SetupBool(config.parseTime))
	params.Add("readTimeout", utils.SetupTime(config.readTimeout))
	params.Add("rejectReadOnly", utils.SetupBool(config.rejectReadOnly))
	if config.serverPubKey != nil {
		params.Add("serverPubKey", *config.serverPubKey)
	}
	params.Add("timeout", utils.SetupTime(config.timeout))
	params.Add("tls", utils.SetupBool(config.tls))
	params.Add("writeTimeout", utils.SetupTime(config.writeTimeout))
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
