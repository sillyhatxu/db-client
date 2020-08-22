package mysqlclient

import (
	"github.com/sillyhatxu/db-client/dbclient"
	"sync"
	"time"
)

const (
	userName = "sillyhat_xu"
	password = "sillyhat_xu_password"
	host     = "127.0.0.1"
	port     = 3306
	schema   = "sillyhat_xu_db"
)

var mysqlClient *MysqlClient
var once sync.Once

func setup() {
	mc, err := dbclient.NewDBClient(
		dbclient.UserName(userName),
		dbclient.Password(password),
		dbclient.Host(host),
		dbclient.Port(port),
		dbclient.Schema(schema),
	)
	if err != nil {
		panic(err)
	}
	mysqlClient, err = NewMysqlClient(Pool(mc), Timeout(20*time.Second))
	if err != nil {
		panic(err)
	}
}
