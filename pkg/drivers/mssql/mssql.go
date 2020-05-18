package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/rancher/kine/pkg/logstructured"
	"github.com/rancher/kine/pkg/logstructured/sqllog"
	"github.com/rancher/kine/pkg/server"
)

const (
	defaultDSN = "sqlserver://username:password@host/instance?param1=value&param2=value"
)

var (
	schema = []string{
		`if not exists (select * from sysobjects where name='kine' and xtype='U')
		create table kine
 			(
 				id INT IDENTITY(1,1) PRIMARY KEY,
				name VARCHAR(630),
				created BIGINT,
				deleted BIGINT,
 				create_revision INT,
 				prev_revision INT,
 				lease INT,
 				value VARBINARY(max),
				old_value VARBINARY(max)
			 );`,
		`IF NOT EXISTS (SELECT name from sys.indexes  
			WHERE name = N'kine_name_index')
		CREATE INDEX kine_name_index ON kine (name);`,
		`IF NOT EXISTS (SELECT name from sys.indexes  
			WHERE name = N'kine_name_id_index')
		CREATE INDEX kine_name_id_index ON kine (name,id);`,
		`IF NOT EXISTS (SELECT name from sys.indexes  
			WHERE name = N'kine_name_prev_revision_uindex')
		CREATE UNIQUE INDEX kine_name_prev_revision_uindex ON kine (name, prev_revision);`,
	}
	createDB = "create database "
)

func New(ctx context.Context, dataSourceName string) (server.Backend, error) {
	parsedDSN, err := prepareDSN(dataSourceName)
	if err != nil {
		return nil, err
	}
	fmt.Printf("parsedDSN: %v\n", parsedDSN)
	dialect, err := Open(ctx, "sqlserver", parsedDSN, "@p", true)
	if err != nil {
		return nil, err
	}
	fmt.Println("got past dialext")
	dialect.TranslateErr = func(err error) error {
		if test, ok := err.(*mssql.Error); ok {
			fmt.Printf("MSSQL Error: num: %v --- sqlNum: %v\n", test.Number, test.SQLErrorNumber())
		}
		if err, ok := err.(*mssql.Error); ok && err.Number == 2601 {
			return server.ErrKeyExists
		}
		if strings.Contains(err.Error(), "Cannot insert duplicate") {
			return server.ErrKeyExists
		}
		return err
	}

	if err := setup(dialect.DB); err != nil {
		return nil, err
	}
	fmt.Println("got passed setup")
	dialect.Migrate(context.Background())
	return logstructured.New(sqllog.New(dialect)), nil
}

func setup(db *sql.DB) error {
	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func prepareDSN(dataSourceName string) (string, error) {
	fmt.Println(dataSourceName)
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	}

	params := strings.Split(dataSourceName, ";")

	paramMap := make(map[string]string)
	first := true
	for _, str := range params {
		fmt.Println(str)
		if first {
			splits := strings.Split(str, ":")
			paramMap["server"] = splits[0]
			paramMap["port"] = splits[1]
			first = false
		} else {
			splits := strings.Split(str, "=")
			paramMap[splits[0]] = splits[1]
		}
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%s;database=%s;",
		paramMap["server"],
		paramMap["user"],
		paramMap["password"],
		paramMap["port"],
		paramMap["database"])
	return connString, nil
}
