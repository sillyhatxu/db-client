package mysqlclient

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	schemaVersionStatusSuccess = `SUCCESS`

	schemaVersionStatusError = `ERROR`

	insertSchemaVersionSQL = `
INSERT INTO schema_version (script, checksum, execution_time, status) values (?, ?, ?, ?)
`

	ddlSchemaVersion = `
CREATE TABLE IF NOT EXISTS schema_version
(
  id             bigint(48)   NOT NULL AUTO_INCREMENT PRIMARY KEY,
  script         varchar(100) NOT NULL,
  checksum       TEXT         NOT NULL,
  execution_time varchar(50)  NOT NULL,
  status         varchar(10)  NOT NULL,
  created_time   timestamp(3) NOT NULL DEFAULT current_timestamp(3)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4
`
)

type SchemaVersion struct {
	Id            int64
	Script        string
	Checksum      string
	ExecutionTime string
	Status        string
	CreatedTime   *time.Time
}

func (mc *MysqlClient) initialFlayway() (err error) {
	if !mc.config.flyway {
		return nil
	}
	err = mc.initialSchemaVersion()
	if err != nil {
		return err
	}
	err = mc.executeFlayway()
	if err != nil {
		return err
	}
	return nil
}

func (mc *MysqlClient) ExecDDL(ddl string) error {
	startT := time.Now()
	result, err := mc.GetDB().Exec(ddl)
	if err != nil {
		return err
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		return err
	}
	rowsAffected, err := result.LastInsertId()
	if err != nil {
		return err
	}
	tc := time.Since(startT)
	log.Println("lastInsertId:", lastInsertId, "; rowsAffected : ", rowsAffected, " (execution: ", tc, ")")
	return nil
}

func (mc *MysqlClient) executeFlayway() error {
	files, err := ioutil.ReadDir(mc.config.ddlPath)
	if err != nil {
		return nil
	}
	svArray, err := mc.SchemaVersionArray()
	if err != nil {
		return err
	}
	err = mc.hasError(svArray)
	if err != nil {
		return err
	}
	for _, f := range files {
		err := mc.readFile(f, svArray)
		if err != nil {
			return err
		}
	}
	return nil
}

func hash64(s string) (uint64, error) {
	h := fnv.New64()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

func (mc *MysqlClient) readFile(fileInfo os.FileInfo, svArray []SchemaVersion) error {
	b, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", mc.config.ddlPath, fileInfo.Name()))
	if err != nil {
		return err
	}
	checksum, err := hash64(string(b))
	if err != nil {
		return err
	}
	exist, sv := mc.findByScript(fileInfo.Name(), svArray)
	if exist {
		if sv.Checksum != strconv.FormatUint(checksum, 10) {
			return fmt.Errorf("sql file has been changed. check : %d; db : %#v", checksum, sv)
		}
		return nil
	}
	execTime := time.Now()
	schemaVersion := SchemaVersion{
		Script:   fileInfo.Name(),
		Checksum: strconv.FormatUint(checksum, 10),
		Status:   schemaVersionStatusError,
	}
	err = mc.ExecDDL(string(b))
	if err == nil {
		schemaVersion.Status = schemaVersionStatusSuccess
	}
	elapsed := time.Since(execTime)
	schemaVersion.ExecutionTime = shortDur(elapsed)
	//mc.insertSchemaVersion(schemaVersion)
	if err != nil {
		return err
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

func (mc *MysqlClient) insertSchemaVersion(schemaVersion SchemaVersion) {
	_, err := mc.Insert(insertSchemaVersionSQL, schemaVersion.Script, schemaVersion.Checksum, schemaVersion.ExecutionTime, schemaVersion.Status)
	if err != nil {
		//logrus.Errorf("insert schema version error. %v", err)
	}
}

func (mc *MysqlClient) findByScript(script string, svArray []SchemaVersion) (bool, *SchemaVersion) {
	for _, sv := range svArray {
		if sv.Script == script {
			return true, &sv
		}
	}
	return false, nil
}

func (mc *MysqlClient) hasError(svArray []SchemaVersion) error {
	for _, sv := range svArray {
		if sv.Status == schemaVersionStatusError {
			return fmt.Errorf("schema version has abnormal state. You need to prioritize exceptional states. %#v", sv)
		}
	}
	return nil
}

func (mc *MysqlClient) SchemaVersionArray() ([]SchemaVersion, error) {
	var svArray []SchemaVersion
	err := mc.FindCustom(`select * from schema_version`, func(rows *sql.Rows) error {
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

func (mc *MysqlClient) initialSchemaVersion() error {
	exist, err := mc.HasTable("schema_version")
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	return mc.ExecDDL(ddlSchemaVersion)
}

func (mc *MysqlClient) HasTable(tableName string) (bool, error) {
	_, err := mc.GetDB().Query(fmt.Sprintf("SELECT 1 FROM %s LIMIT 1", tableName))
	if err != nil {
		if strings.HasSuffix(err.Error(), "doesn't exist") {
			return false, nil
		}
		return true, err
	}
	return true, nil
}
