package mysqlclient

import (
	"github.com/sillyhatxu/db-client/builder"
	"github.com/sillyhatxu/db-client/structs"
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
	Desc             *string    `column:"description"`
	Birthday         *time.Time `column:"birthday"`
	CreatedTime      time.Time  `column:"created_time"`
	LastModifiedTime time.Time  `column:"last_modified_time"`
}

func TestMysqlClient_Insert(t *testing.T) {
	once.Do(setup)
	for i := 1; i <= 100; i++ {
		Age := 31
		Amount := 354.25
		Description := "Description Description Description Description Description"
		user := User{
			LoginName:        "LoginName",
			Password:         "Password",
			UserName:         "UserName",
			Status:           true,
			Platform:         "Platform",
			Age:              &Age,
			Amount:           &Amount,
			Desc:             &Description,
			Birthday:         nil,
			CreatedTime:      time.Now(),
			LastModifiedTime: time.Now(),
		}
		data := []map[string]interface{}{structs.New(user).Map()}
		sql, args, err := builder.BuildInsert("user", data)
		assert.Nil(t, err)
		id, err := mysqlClient.Insert(sql, args...)
		assert.Nil(t, err)
		assert.EqualValues(t, i, id)
	}
}

func TestMysqlClient_Find(t *testing.T) {
	once.Do(setup)
	var userArray []User
	sql := "select * from user"
	//mysqlClient.Find(sql, &userArray)
	//TODO fix nil bug
	//TODO I can't fix this bug
	err := mysqlClient.Find(sql, &userArray)
	assert.Nil(t, err)

	assert.EqualValues(t, 100, len(userArray))
}
