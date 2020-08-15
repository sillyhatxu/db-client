package dbclient

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	userName = "sillyhat_xu"
	password = "sillyhat_xu_password"
	host     = "127.0.0.1"
	port     = 3306
	schema   = "sillyhat_xu_db"
)

func TestNewMysqlClient(t *testing.T) {
	mysqlClient, err := NewDBClient(UserName(userName), Password(password), Host(host), Port(port), Schema(schema))
	assert.Nil(t, err)
	err = mysqlClient.Ping()
	assert.Nil(t, err)
}
