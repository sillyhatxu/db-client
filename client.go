package sqliteclient

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mitchellh/mapstructure"
	"github.com/sillyhatxu/convenient-utils/encryption/hash"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DBClient struct {
	DataSourceName  string
	DriverName      string
	DDLPath         string
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	Flyway          bool
	db              *sql.DB
	mu              sync.Mutex
}

const SchemaVersionStatusSuccess = `SUCCESS`

const SchemaVersionStatusError = `ERROR`

const SqliteMasterSQL = `
SELECT count(1) FROM sqlite_master WHERE type='table' AND name = ?
`

const InsertSchemaVersionSQL = `
INSERT INTO schema_version (script, checksum, execution_time, status) values (?, ?, ?, ?)
`
const DDLSchemaVersion = `
CREATE TABLE IF NOT EXISTS schema_version
(
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  script         TEXT    NOT NULL,
  checksum       TEXT    NOT NULL,
  execution_time TEXT    NOT NULL,
  status         TEXT    NOT NULL,
  created_time   datetime default current_timestamp
);
`

type SchemaVersion struct {
	Id            int64
	Script        string
	Checksum      string
	ExecutionTime string
	Status        string
	CreatedTime   time.Time
}

func NewClient(DataSourceName string, DriverName string) *DBClient {
	return &DBClient{
		DataSourceName:  DataSourceName,
		DriverName:      DriverName,
		DDLPath:         "",
		Flyway:          false,
		MaxIdleConns:    10,
		MaxOpenConns:    20,
		ConnMaxLifetime: 12 * time.Hour,
	}
}

func (dbClient *DBClient) SetDDLPath(DDLPath string) {
	dbClient.mu.Lock()
	defer dbClient.mu.Unlock()
	dbClient.Flyway = true
	dbClient.DDLPath = DDLPath
}

func (dbClient *DBClient) SetMaxIdleConns(MaxIdleConns int) {
	dbClient.mu.Lock()
	defer dbClient.mu.Unlock()
	dbClient.MaxIdleConns = MaxIdleConns
}

func (dbClient *DBClient) SetMaxOpenConns(MaxOpenConns int) {
	dbClient.mu.Lock()
	defer dbClient.mu.Unlock()
	dbClient.MaxOpenConns = MaxOpenConns
}

func (dbClient *DBClient) SetConnMaxLifetime(SetConnMaxLifetime time.Duration) {
	dbClient.mu.Lock()
	defer dbClient.mu.Unlock()
	dbClient.ConnMaxLifetime = SetConnMaxLifetime
}

func (dbClient *DBClient) Initial() error {
	dbClient.mu.Lock()
	defer dbClient.mu.Unlock()
	db, err := sql.Open(dbClient.DriverName, dbClient.DataSourceName)
	if err != nil {
		return err
	}
	err = db.Ping()
	if err != nil {
		return err
	}
	dbClient.db = db
	if !dbClient.Flyway {
		return nil
	}
	if dbClient.DDLPath == "" {
		return fmt.Errorf("ddl path is nil")
	}
	err = dbClient.initialSchemaVersion()
	if err != nil {
		return err
	}
	err = dbClient.initialFlayway()
	if err != nil {
		return err
	}
	return nil
}

func (dbClient *DBClient) findByScript(script string, svArray []SchemaVersion) (bool, *SchemaVersion) {
	for _, sv := range svArray {
		if sv.Script == script {
			return true, &sv
		}
	}
	return false, nil
}

func (dbClient *DBClient) hasError(svArray []SchemaVersion) error {
	for _, sv := range svArray {
		if sv.Status == SchemaVersionStatusError {
			return fmt.Errorf("schema version has abnormal state. You need to prioritize exceptional states. %#v", sv)
		}
	}
	return nil
}

func shortDur(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}

func (dbClient *DBClient) initialFlayway() error {
	files, err := ioutil.ReadDir(dbClient.DDLPath)
	if err != nil {
		return nil
	}
	svArray, err := dbClient.SchemaVersionArray()
	if err != nil {
		return err
	}
	err = dbClient.hasError(svArray)
	if err != nil {
		return err
	}
	for _, f := range files {
		err := dbClient.readFile(f, svArray)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dbClient *DBClient) readFile(fileInfo os.FileInfo, svArray []SchemaVersion) error {
	b, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", dbClient.DDLPath, fileInfo.Name()))
	if err != nil {
		return err
	}
	checksum, err := hash.Hash64(string(b))
	if err != nil {
		return err
	}
	exist, sv := dbClient.findByScript(fileInfo.Name(), svArray)
	if exist {
		if sv.Checksum != strconv.FormatUint(checksum, 10) {
			return fmt.Errorf("sql file has been changed. %#v", sv)
		}
		return nil
	}
	execTime := time.Now()
	schemaVersion := SchemaVersion{
		Script:   fileInfo.Name(),
		Checksum: strconv.FormatUint(checksum, 10),
		Status:   SchemaVersionStatusError,
	}
	err = dbClient.ExecDDL(string(b))
	if err == nil {
		schemaVersion.Status = SchemaVersionStatusSuccess
	}
	elapsed := time.Since(execTime)
	schemaVersion.ExecutionTime = shortDur(elapsed)
	dbClient.insertSchemaVersion(schemaVersion)
	if err != nil {
		return err
	}
	return nil
}

func (dbClient *DBClient) insertSchemaVersion(schemaVersion SchemaVersion) {
	_, err := dbClient.Insert(InsertSchemaVersionSQL, schemaVersion.Script, schemaVersion.Checksum, schemaVersion.ExecutionTime, schemaVersion.Status)
	if err != nil {
		logrus.Errorf("insert schema version error. %v", err)
	}
}

func (dbClient *DBClient) initialSchemaVersion() error {
	exist, err := dbClient.HasTable("schema_version")
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	return dbClient.ExecDDL(DDLSchemaVersion)
}

func (dbClient *DBClient) HasTable(table string) (bool, error) {
	var count int
	err := dbClient.Query(SqliteMasterSQL, func(rows *sql.Rows) error {
		return rows.Scan(&count)
	}, table)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (dbClient *DBClient) SchemaVersionArray() ([]SchemaVersion, error) {
	var svArray []SchemaVersion
	err := dbClient.Query(`select * from schema_version`, func(rows *sql.Rows) error {
		var sv SchemaVersion
		err := rows.Scan(&sv.Id, &sv.Script, &sv.Checksum, &sv.ExecutionTime, &sv.Status, &sv.CreatedTime)
		svArray = append(svArray, sv)
		return err
	})
	if err != nil {
		return nil, err
	}
	if svArray == nil {
		svArray = make([]SchemaVersion, 0)
	}
	return svArray, nil
}

func (dbClient *DBClient) Find(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	db, err := dbClient.GetDB()
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		logrus.Errorf("sqlite client get transaction error. %v", err)
		return nil, err
	}
	defer tx.Commit()
	rows, err := tx.Query(sql, args...)
	if err != nil {
		logrus.Errorf("query error. %v", err)
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		logrus.Errorf("rows.Columns() error. %v", err)
		return nil, err
	}
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}
	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{})
		for k, v := range values {
			key := columns[k]
			row[key] = string(v)
		}
		results = append(results, row)
	}
	return results, nil
}

func (dbClient *DBClient) GetDB() (*sql.DB, error) {
	if err := dbClient.db.Ping(); err != nil {
		logrus.Errorf("get connect error. %v", err)
		return nil, err
	}
	return dbClient.db, nil
}

func (dbClient *DBClient) ExecDDL(ddl string) error {
	db, err := dbClient.GetDB()
	if err != nil {
		return err
	}
	logrus.Infof("exec ddl : ")
	logrus.Infof(ddl)
	logrus.Infof("--------------------")
	_, err = db.Exec(ddl)
	return err
}

func (dbClient *DBClient) Insert(sql string, args ...interface{}) (int64, error) {
	db, err := dbClient.GetDB()
	if err != nil {
		return 0, nil
	}
	stm, err := db.Prepare(sql)
	if err != nil {
		logrus.Errorf("prepare sqlite error. %v", err)
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		logrus.Errorf("insert data error. %v", err)
		return 0, err
	}
	return result.LastInsertId()
}

func (dbClient *DBClient) Update(sql string, args ...interface{}) (int64, error) {
	db, err := dbClient.GetDB()
	if err != nil {
		return 0, nil
	}
	stm, err := db.Prepare(sql)
	if err != nil {
		logrus.Errorf("prepare sqlite error. %v", err)
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		logrus.Errorf("update data error. %v", err)
		return 0, err
	}
	return result.RowsAffected()
}

func (dbClient *DBClient) Delete(sql string, args ...interface{}) (int64, error) {
	db, err := dbClient.GetDB()
	if err != nil {
		return 0, nil
	}
	stm, err := db.Prepare(sql)
	if err != nil {
		logrus.Errorf("prepare sqlite error. %v", err)
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		logrus.Errorf("delete data error. %v", err)
		return 0, err
	}
	return result.RowsAffected()
}

type TransactionCallback func(*sql.Tx) error

func (dbClient *DBClient) Transaction(callback TransactionCallback) error {
	db, err := dbClient.GetDB()
	if err != nil {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		logrus.Errorf("sqlite client get transaction error. %v", err)
		return err
	}
	err = callback(tx)
	if err != nil {
		logrus.Errorf("transaction data error. %v", err)
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

type FieldFunc func(rows *sql.Rows) error

func (dbClient *DBClient) Query(query string, fieldFunc FieldFunc, args ...interface{}) error {
	db, err := dbClient.GetDB()
	if err != nil {
		return err
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err := fieldFunc(rows)
		if err != nil {
			return err
		}
	}
	return rows.Err()
}

func (dbClient *DBClient) FindMapFirst(sql string, args ...interface{}) (map[string]interface{}, error) {
	array, err := dbClient.FindMapArray(sql, args...)
	if err != nil {
		return nil, err
	}
	if array == nil || len(array) == 0 {
		return nil, nil
	}
	return array[0], nil
}

func (dbClient *DBClient) FindMapArray(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	db, err := dbClient.GetDB()
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Println("sqlite client get transaction error.", err)
		return nil, err
	}
	defer tx.Commit()
	rows, err := tx.Query(sql, args...)
	if err != nil {
		log.Println("Query error.", err)
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		log.Println("rows.Columns() error.", err)
		return nil, err
	}
	//values是每个列的值，这里获取到byte里
	values := make([][]byte, len(columns))
	//query.Scan的参数，因为每次查询出来的列是不定长的，用len(cols)定住当次查询的长度
	scans := make([]interface{}, len(columns))
	//让每一行数据都填充到[][]byte里面
	for i := range values {
		scans[i] = &values[i]
	}
	//最后得到的map
	var results []map[string]interface{}
	for rows.Next() { //循环，让游标往下推
		if err := rows.Scan(scans...); err != nil { //query.Scan查询出来的不定长值放到scans[i] = &values[i],也就是每行都放在values里
			return nil, err
		}
		row := make(map[string]interface{}) //每行数据
		for k, v := range values {          //每行数据是放在values里面，现在把它挪到row里
			key := columns[k]
			//valueType := reflect.TypeOf(v)
			//log.Info(valueType)
			row[key] = string(v)
		}
		results = append(results, row)
	}
	return results, nil
}

func (dbClient *DBClient) FindList(sql string, input interface{}, args ...interface{}) error {
	results, err := dbClient.FindMapArray(sql, args...)
	if err != nil {
		return err
	}
	config := &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeHookFunc("2006-01-02 15:04:05"),
		WeaklyTypedInput: true,
		Result:           input,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(results)
	if err != nil {
		return err
	}
	return nil
}

func (dbClient *DBClient) FindListByConfig(sql string, input interface{}, config *mapstructure.DecoderConfig, args ...interface{}) error {
	results, err := dbClient.FindMapArray(sql, args...)
	if err != nil {
		return err
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(results)
	if err != nil {
		return err
	}
	return nil
}

func (dbClient *DBClient) FindFirst(sql string, input interface{}, args ...interface{}) error {
	result, err := dbClient.FindMapFirst(sql, args...)
	if err != nil {
		return err
	}
	config := &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeHookFunc("2006-01-02 15:04:05"),
		WeaklyTypedInput: true,
		Result:           input,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (dbClient *DBClient) FindFirstByConfig(sql string, input interface{}, config *mapstructure.DecoderConfig, args ...interface{}) error {
	result, err := dbClient.FindMapFirst(sql, args...)
	if err != nil {
		return err
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}
	err = decoder.Decode(result)
	if err != nil {
		return err
	}
	return nil
}

func (dbClient *DBClient) Count(sql string, args ...interface{}) (int64, error) {
	db, err := dbClient.GetDB()
	if err != nil {
		return 0, err
	}
	tx, err := db.Begin()
	if err != nil {
		log.Println("sqlite client get connection error.", err)
		return 0, err
	}
	defer tx.Commit()
	var count int64
	countErr := tx.QueryRow(sql, args...).Scan(&count)
	if countErr != nil {
		log.Println("Query count error.", err)
		return 0, err
	}
	return count, nil
}
