package mysqlclient

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMysqlClient_Insert(t *testing.T) {

}

func TestMysqlClient_Find(t *testing.T) {
	once.Do(setup)
	type User struct {
	}
	var userArray []User
	sql := "select * from user"
	err := mysqlClient.Find(sql, &userArray)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, len(userArray))
}
