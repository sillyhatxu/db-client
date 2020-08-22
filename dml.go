package mysqlclient

import (
	"context"
	"database/sql"
	"errors"
	"github.com/sillyhatxu/db-client/decoder"
)

var TimeOutError = errors.New("database connect timeout")

func (mc *MysqlClient) getContext() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), mc.config.timeout)
	return ctx
}

func (mc *MysqlClient) Exec(sql string, args ...interface{}) (sql.Result, error) {
	stm, err := mc.GetDB().Prepare(sql)
	if err != nil {
		return nil, err
	}
	defer stm.Close()
	result, err := stm.ExecContext(mc.getContext(), args...)
	if err != nil && err == context.DeadlineExceeded {
		return nil, TimeOutError
	} else if err != nil {
		return nil, err
	}
	return result, nil
}

func (mc *MysqlClient) Insert(sql string, args ...interface{}) (int64, error) {
	result, err := mc.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (mc *MysqlClient) Update(sql string, args ...interface{}) (int64, error) {
	result, err := mc.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (mc *MysqlClient) Delete(sql string, args ...interface{}) (int64, error) {
	result, err := mc.Exec(sql, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (mc *MysqlClient) Count(sql string, args ...interface{}) (int64, error) {
	var count int64
	err := mc.GetDB().QueryRowContext(mc.getContext(), sql, args...).Scan(&count)
	if err != nil && err == context.DeadlineExceeded {
		return 0, TimeOutError
	} else if err != nil {
		return 0, err
	}
	return count, nil
}

type TransactionCallback func(context.Context, *sql.Tx) error

func (mc *MysqlClient) Transaction(callback TransactionCallback) error {
	tx, err := mc.GetTransaction()
	if err != nil {
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), mc.config.timeout)
	err = callback(ctx, tx)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
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
	result, err := mc.FindMapArray(sql, args...)
	if err != nil {
		return err
	}
	return decoder.DefaultConfig().Decode(result, output)
}

func (mc *MysqlClient) FindFirst(sql string, output interface{}, args ...interface{}) error {
	array, err := mc.FindMapArray(sql, args...)
	if err != nil {
		return err
	}
	if array == nil || len(array) == 0 {
		return nil
	}
	return decoder.DefaultConfig().Decode(array[0], output)
}

func (mc *MysqlClient) FindMapArray(sql string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := mc.GetDB().QueryContext(mc.getContext(), sql, args...)
	if err != nil && err == context.DeadlineExceeded {
		return nil, TimeOutError
	} else if err != nil {
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
