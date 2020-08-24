package mysqlclient

import (
	"github.com/sillyhatxu/db-client/builder"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type User struct {
	Id               int64      `column:"id"`
	LoginName        string     `column:"login_name"`
	Password         string     `column:"password"`
	UserName         string     `column:"user_name"`
	Status           bool       `column:"status"`
	Platform         string     `column:"platform"`
	Age              *int       `column:"age"`
	Amount           *float64   `column:"amount"`
	Desc             *string    `column:"Description"`
	Birthday         *time.Time `column:"birthday"`
	CreatedTime      time.Time  `column:"created_time"`
	LastModifiedTime time.Time  `column:"last_modified_time"`
}

func TestMysqlClient_Insert(t *testing.T) {
	once.Do(setup)
	data := []map[string]interface{}{
		{
			"foo": "bar",
			"age": 23,
		},
	}
	//TODO struct to map
	sql, args, err := builder.BuildInsert("user_info", data)
	assert.Nil(t, err)

}

func TestMysqlClient_Find(t *testing.T) {
	once.Do(setup)
	var userArray []User
	sql := "select * from user"
	err := mysqlClient.Find(sql, &userArray)
	assert.Nil(t, err)
	assert.EqualValues(t, 5, len(userArray))
}
