package mysqlclient

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMysqlClient(t *testing.T) {
	once.Do(setup)
	err := mysqlClient.Ping()
	assert.Nil(t, err)
}

//func TestGetMysqlDataSourceName(t *testing.T) {
//	dbclient, err := NewMysqlClient(userName, password, host, port, schema)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(dbclient.getMysqlDataSourceName())
//}
//
//func TestClientGetConnection(t *testing.T) {
//	dbclient, err := NewMysqlClient(userName, password, host, port, schema, Attempts(20), ConnMaxLifetime(500*time.Millisecond))
//	if err != nil {
//		panic(err)
//	}
//	for {
//		count, err := dbclient.Count(count_sql)
//		if err != nil {
//			log.Println(err)
//			continue
//		}
//		log.Println(count)
//		time.Sleep(5 * time.Second)
//	}
//
//}
//
//func TestHasTable(t *testing.T) {
//	dbclient, err := NewMysqlClient(userName, password, host, port, schema)
//	if err != nil {
//		panic(err)
//	}
//	b, err := dbclient.HasTable("test")
//	assert.Nil(t, err)
//	assert.EqualValues(t, b, false)
//	b, err = dbclient.HasTable("userinfo")
//	assert.Nil(t, err)
//	assert.EqualValues(t, b, true)
//}
//
//func TestMysqlClient_Initial(t *testing.T) {
//	//sillyhat:sillyhat@tcp(127.0.0.1:3308)/sillyhat_user?loc=Asia%2FSingapore&parseTime=true
//	var Client, err = NewMysqlClient(userName, password, host, port, schema, DDLPath("/Users/shikuanxu/go/src/github.com/sillyhatxu/user-backend/db/migration"))
//	//var Client, err = NewMysqlClient(dataSourceName, DDLPath("/Users/cookie/go/gopath/src/github.com/sillyhatxu/mini-mq/db/migration"))
//	if err != nil {
//		panic(err)
//	}
//	err = Client.Ping()
//	assert.Nil(t, err)
//}
//
//func TestMysqlClient_SchemaVersionArray(t *testing.T) {
//	var Client, err = NewMysqlClient(userName, password, host, port, schema, DDLPath("/Users/shikuanxu/go/src/github.com/sillyhatxu/user-backend/db/migration"))
//	//var Client, err = NewMysqlClient(dataSourceName, DDLPath("/Users/cookie/go/gopath/src/github.com/sillyhatxu/mini-mq/db/migration"))
//	if err != nil {
//		panic(err)
//	}
//	array, err := Client.SchemaVersionArray()
//	assert.Nil(t, err)
//	for _, sv := range array {
//		logrus.Infof("%#v; time : %v", sv, sv.CreatedTime.UnixNano()/int64(time.Millisecond))
//	}
//}
