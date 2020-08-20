package mysqlclient

import (
	"database/sql"
	"github.com/sillyhatxu/db-client/decoder"
)

func (mc *MysqlClient) Insert(sql string, args ...interface{}) (int64, error) {
	stm, err := mc.GetDB().Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (mc *MysqlClient) Update(sql string, args ...interface{}) (int64, error) {
	stm, err := mc.GetDB().Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (mc *MysqlClient) Delete(sql string, args ...interface{}) (int64, error) {
	stm, err := mc.GetDB().Prepare(sql)
	if err != nil {
		return 0, err
	}
	defer stm.Close()
	result, err := stm.Exec(args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

type TransactionCallback func(*sql.Tx) error

func (mc *MysqlClient) Transaction(callback TransactionCallback) error {
	tx, err := mc.GetTransaction()
	if err != nil {
		return err
	}
	err = callback(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (mc *MysqlClient) Count(sql string, args ...interface{}) (int64, error) {
	tx, err := mc.GetTransaction()
	if err != nil {
		return 0, err
	}
	defer tx.Commit()
	var count int64
	err = tx.QueryRow(sql, args...).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

type FieldFunc func(rows *sql.Rows) error

func (mc *MysqlClient) FindCustom(query string, fieldFunc FieldFunc, args ...interface{}) error {
	rows, err := mc.GetDB().Query(query, args...)
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

func (mc *MysqlClient) Find(sql string, output interface{}, args ...interface{}) error {
	result, err := mc.FindByConfig(sql, args...)
	if err != nil {
		return err
	}
	return decoder.DefaultConfig().Decode(result, output)
}

func (mc *MysqlClient) FindByConfig(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	//rows, err := mc.GetDB().Query(sql, args...)
	//if err != nil {
	//	return err
	//}
	//defer rows.Close()
	//colNames, err := rows.Columns()
	//if err != nil {
	//	return err
	//}
	//cols := make([]interface{}, len(colNames))
	//colPtrs := make([]interface{}, len(colNames))
	//for i := 0; i < len(colNames); i++ {
	//	colPtrs[i] = &cols[i]
	//}
	//var array []map[string]interface{}
	//var myMap = make(map[string]interface{})
	//for rows.Next() {
	//	err = rows.Scan(colPtrs...)
	//	if err != nil {
	//		return err
	//	}
	//	for i, col := range cols {
	//		myMap[colNames[i]] = col
	//	}
	//	array = append(array, myMap)
	//	//for key, val := range myMap {
	//	//	fmt.Println("Key:", key, "Value Type:", reflect.TypeOf(val))
	//	//}
	//}
	//return nil

	rows, err := mc.GetDB().Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
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
			row[key] = string(v)
		}
		results = append(results, row)
	}
	return results, nil
}

//func (mc *MyqlClient) FindFirst(sql string, input interface{}, args ...interface{}) error {
//	if isStruct(input) {
//		return fmt.Errorf("%v must be a struct or a struct pointer", input)
//	}
//	result, err := mc.FindMapFirst(sql, args...)
//	if err != nil {
//		return err
//	}
//	config := &mapstructure.DecoderConfig{
//		DecodeHook:       mapstructure.StringToTimeHookFunc("2006-01-02T15:04:05Z07:00"),
//		WeaklyTypedInput: true,
//		Result:           input,
//	}
//	decoder, err := mapstructure.NewDecoder(config)
//	if err != nil {
//		return err
//	}
//	err = decoder.Decode(result)
//	if err != nil {
//		return err
//	}
//	return nil
//}
