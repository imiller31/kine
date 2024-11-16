package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/k3s-io/kine/pkg/tls"
	"github.com/k3s-io/kine/pkg/util"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/sirupsen/logrus"
)

const (
	defaultDSN = "sqlserver://sa:yourStrong(!)Password@localhost:1433?database=master"
)

var (
	schema = []string{
		`if not exists (SELECT * FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_NAME = N'kine')
		begin
 			create table kine (
				id bigint primary key identity (1, 1),
				name varchar(630) COLLATE SQL_Latin1_General_CP1_CI_AS,  
				created int,
				deleted int,
				create_revision bigint,
				prev_revision bigint,
				lease int,
				value varbinary(max),
				old_value varbinary(max) )
		end 
		`,
		`if not exists ( select * from sys.indexes
			where name = 'kine_name_index' and
			object_id = OBJECT_ID('kine')) 
		begin
    		create nonclustered index kine_name_index on kine (name)
		end
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_name_prev_revision_uindex' and
			object_id = OBJECT_ID('kine')
		) begin
		create unique index kine_name_prev_revision_uindex on kine (name, prev_revision)
		end
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_name_prev_revision_index' and
			object_id = OBJECT_ID('kine')
		) begin
		create nonclustered index kine_name_prev_revision_index on kine (prev_revision)
		end
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_id_deleted_index' and
			object_id = OBJECT_ID('kine')
		) begin
		create nonclustered index kine_id_deleted_index on kine (id, deleted)
		end
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_list_query_index' and
			object_id = OBJECT_ID('kine')
		) begin
		create nonclustered index kine_list_query_index on kine (name, id DESC, deleted)
		end
		`,
	}

	createDB = `CREATE DATABASE "%s";`

	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"

	listSQL = `
SELECT TOP 100 PERCENT
	(SELECT MAX(rkv.id) AS id FROM kine AS rkv),
	(SELECT MAX(crkv.prev_revision) AS prev_revision FROM kine AS crkv WHERE crkv.name = 'compact_rev_key'),
	maxkv.theid, maxkv.name, maxkv.created, maxkv.deleted, maxkv.create_revision, maxkv.prev_revision, maxkv.lease, maxkv.value, maxkv.old_value
FROM (
	SELECT 
		kv.id AS theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value,
		ROW_NUMBER() OVER (PARTITION BY kv.name ORDER BY kv.name, kv.id DESC) AS rn
	FROM
		kine AS kv
	WHERE
		kv.name LIKE ? 
		%s
) AS maxkv
WHERE
	maxkv.rn = 1 AND (maxkv.deleted = 0 OR 'true' = ?)
ORDER BY maxkv.name, maxkv.theid DESC`

	countSQL = `
SELECT
	(SELECT MAX(rkv.id) AS id FROM kine AS rkv),
	COUNT(c.theid)
FROM (
	SELECT 
		kv.id AS theid, kv.deleted,
		ROW_NUMBER() OVER (PARTITION BY kv.name ORDER BY kv.name, kv.id DESC) AS rn
	FROM kine AS kv
	WHERE
		kv.name LIKE ?
		%s
) AS c
WHERE c.rn = 1 AND (c.deleted = 0 OR 'true' = ?)`

	getSizeSQL = `EXEC sp_spaceused 'kine'`
)

func New(ctx context.Context, cfg *drivers.Config) (bool, server.Backend, error) {
	parsedDSN, isAzureSql, err := prepareDSN(cfg.DataSourceName, cfg.BackendTLSConfig)
	if err != nil {
		return false, nil, err
	}

	if err := createDBIfNotExist(parsedDSN, isAzureSql); err != nil {
		return false, nil, err
	}
	driverName := "sqlserver"
	if isAzureSql {
		driverName = azuread.DriverName
	}
	dialect, err := generic.Open(ctx, driverName, parsedDSN, cfg.ConnectionPoolConfig, "@p", true, cfg.MetricsRegisterer)
	if err != nil {
		return false, nil, err
	}

	dialect.GetSizeSQL = getSizeSQL
	dialect.CompactSQL = `
		DELETE kv
FROM kine AS kv
INNER JOIN (
    SELECT kp.prev_revision AS id
    FROM kine AS kp
    WHERE
        kp.name != 'compact_rev_key' AND
        kp.prev_revision != 0 AND
        kp.id <= @p1
    UNION
    SELECT kd.id AS id
    FROM kine AS kd
    WHERE
        kd.deleted != 0 AND
        kd.id <= @p2
) AS ks
ON kv.id = ks.id`
	dialect.GetCurrentSQL = q(fmt.Sprintf(listSQL, "AND kv.name > ?"))
	dialect.ListRevisionStartSQL = q(fmt.Sprintf(listSQL, "AND kv.id <= ?"))
	dialect.GetRevisionAfterSQL = q(fmt.Sprintf(listSQL, "AND kv.name > ? AND kv.id <= ?"))
	dialect.CountCurrentSQL = q(fmt.Sprintf(countSQL, "AND kv.name > ?"))
	dialect.CountRevisionSQL = q(fmt.Sprintf(countSQL, "AND kv.name > ? AND kv.id <= ?"))
	dialect.InsertSQL = q(`INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, value, old_value) OUTPUT INSERTED.id VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)

	dialect.FillRetryDuration = time.Millisecond + 5

	dialect.InsertRetry = func(err error) bool {
		if err, ok := err.(*mssql.Error); ok && err.Number == 2627 && strings.Contains(err.Message, "cannot insert duplicate key") {
			return true
		}
		return false
	}
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(mssql.Error); ok && (err.Number == 2627 || err.Number == 2601) {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.ErrCode = func(err error) string {
		if err == nil {
			return ""
		}
		if err, ok := err.(*mssql.Error); ok {
			return strconv.Itoa(int(err.Number))
		}
		return err.Error()
	}

	dialect.ApplyLimit = func(sql string, limit int64) string {
		limitRewrite := fmt.Sprintf("SELECT TOP %d ", limit)
		sql = strings.Replace(sql, "SELECT TOP 100 PERCENT", limitRewrite, 1)
		return sql
	}

	if err := setup(dialect.DB); err != nil {
		return false, nil, err
	}

	return true, logstructured.New(sqllog.New(dialect)), nil
}

func createDBIfNotExist(dataSourceName string, isAzureSql bool) error {
	if isAzureSql {
		db, err := sql.Open(azuread.DriverName, dataSourceName)
		if err != nil {
			return err
		}
		defer db.Close()
		if err = db.Ping(); err != nil {
			return err
		}
		return nil
	}
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := u.Query().Get("database")
	logrus.Infof("dbname: %v", dbName)
	db, err := sql.Open("sqlserver", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	if err = db.Ping(); err != nil && strings.Contains(err.Error(), "mssql: Cannot open database") {
		params := u.Query()
		params.Del("database")
		u.RawQuery = params.Encode()
		db, err := sql.Open("sqlserver", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(createDB + dbName + ";")
		if err != nil {
			return err
		}
		return nil
	}

	if _, ok := err.(mssql.Error); !ok {
		return err
	}

	if err := err.(mssql.Error); err.Number != 1801 { // 1801 = database already exists
		db, err := sql.Open("sqlserver", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(createDB + dbName + ":")
		if err != nil {
			return err
		}
	}
	return nil
}

func setup(db *sql.DB) error {
	logrus.Infof("Configuring database table schema and indexes, this may take a moment...")

	for _, stmt := range schema {
		logrus.Tracef("SETUP EXEC : %v", util.Stripped(stmt))
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	logrus.Infof("Database tables and indexes are up to date")
	return nil
}

func q(sql string) string {
	regex := regexp.MustCompile(`\?`)
	pref := "@p"
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		n++
		return pref + strconv.Itoa(n)
	})
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, bool, error) {
	var isAzureSql bool
	if len(dataSourceName) == 0 {
		dataSourceName = defaultDSN
	} else {
		dataSourceName = "sqlserver://" + dataSourceName
	}
	//if strings.Contains(dataSourceName, "user id") {
	//	return dataSourceName, nil
	//}
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", false, err
	}
	/*
		if len(u.Path) == 0 || u.Path == "/" {
			u.Path = "/kubernetes"
		}*/

	queryMap, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", false, err
	}
	// set up tls dsn
	params := url.Values{}

	/* TODO@ismille: basically just trying to get the basics working here, can come back to this later
	sslmode := ""
	if _, ok := queryMap["sslcert"]; tlsInfo.CertFile != "" && !ok {
		params.Add("sslcert", tlsInfo.CertFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslkey"]; tlsInfo.KeyFile != "" && !ok {
		params.Add("sslkey", tlsInfo.KeyFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslrootcert"]; tlsInfo.CAFile != "" && !ok {
		params.Add("sslrootcert", tlsInfo.CAFile)
		sslmode = "verify-full"
	}
	if _, ok := queryMap["sslmode"]; !ok && sslmode != "" {
		params.Add("sslmode", sslmode)
	}
	*/

	for k, v := range queryMap {
		params.Add(k, v[0])
		if k == "fedauth" {
			isAzureSql = true
		}
	}

	u.RawQuery = params.Encode()
	return u.String(), isAzureSql, nil
}

func init() {
	drivers.Register("sqlserver", New)
	drivers.Register("mssql", New)
	drivers.Register("azuresql", New)
	drivers.SetDefault("sqlserver")
}
