package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Rican7/retry/backoff"
	"github.com/Rican7/retry/strategy"
	"github.com/sirupsen/logrus"
)

var (
	revSQL = `
		SELECT TOP(1) rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC
	`

	compactRevSQL = `
		SELECT TOP(1) crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC
	`

	getRevisionSQL = `
	SELECT
			0, 0, kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
			FROM kine kv
			WHERE kv.id = ?
	`
	getCurrentSQL = `
	SELECT (
		SELECT TOP(1) rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC ), (
		SELECT TOP(1) crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC), kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE '%?%'
				
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR kv.deleted = ?)
		ORDER BY kv.id ASC
	`

	listRevisionStartSQL = `
	SELECT (
		SELECT TOP(1) rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC ), (
		SELECT TOP(1) crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC), kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE '%?%'
				and mkv.id  <= ?
				
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR kv.deleted = ?)
		ORDER BY kv.id ASC
	`

	getRevisionAfterSQL = `
	SELECT (
		SELECT TOP(1) rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC ), (
		SELECT TOP(1) crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC), kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE '%?%'
				AND mkv.id <= ? and mkv.id > (
					SELECT TOP(1) ikv.id
					FROM kine ikv
					WHERE
						ikv.name = '?' AND
						ikv.id <= ?
					ORDER BY ikv.id DESC)	
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR kv.deleted = ?)
		ORDER BY kv.id ASC
	`

	countSQL = `
	SELECT (
		SELECT TOP(1) rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC), COUNT(c.theid)
			FROM (
				SELECT TOP 100 PERCENT kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE '%?%'
				
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR kv.deleted = ?)
		ORDER BY kv.id ASC
		
			) c
	`

	afterSQL = `
	SELECT (
		SELECT TOP 1 rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC), (
		SELECT TOP 1 crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC), kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value
			FROM kine kv
			WHERE
				kv.name LIKE '%?%' AND
				kv.id > ?
			ORDER BY kv.id ASC
	`

	insertSQL = `
	DECLARE @OutputTable table(
		id int,
		name VARCHAR(630),
		created BIGINT,
		deleted BIGINT,
		create_revision INT,
		prev_revision INT,
		lease INT,
		value VARBINARY(max),
		old_value VARBINARY(max)
	);
	INSERT dbo.kine 
		OUTPUT INSERTED.id, INSERTED.name, INSERTED.created, INSERTED.deleted, INSERTED.create_revision, INSERTED.prev_revision, INSERTED.lease, INSERTED.value, INSERTED.old_value
		INTO @OutputTable 
		VALUES(?, ?, ?, ?, ?, ?, ?, ?);
	SELECT id FROM @OutputTable;
	`
)

type Stripped string

func (s Stripped) String() string {
	str := strings.ReplaceAll(string(s), "\n", "")
	return regexp.MustCompile("[\t ]+").ReplaceAllString(str, " ")
}

type ErrRetry func(error) bool
type TranslateErr func(error) error

type MssqlGeneric struct {
	sync.Mutex

	LockWrites            bool
	LastInsertID          bool
	DB                    *sql.DB
	GetCurrentSQL         string
	GetRevisionSQL        string
	RevisionSQL           string
	ListRevisionStartSQL  string
	GetRevisionAfterSQL   string
	CountSQL              string
	AfterSQL              string
	DeleteSQL             string
	UpdateCompactSQL      string
	InsertSQL             string
	FillSQL               string
	InsertLastInsertIDSQL string
	Retry                 ErrRetry
	TranslateErr          TranslateErr
}

func q(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func (d *MssqlGeneric) Migrate(ctx context.Context) {
	var (
		count     = 0
		countKV   = d.queryRow(ctx, "SELECT COUNT(*) FROM key_value")
		countKine = d.queryRow(ctx, "SELECT COUNT(*) FROM kine")
	)

	if err := countKV.Scan(&count); err != nil || count == 0 {
		return
	}

	if err := countKine.Scan(&count); err != nil || count != 0 {
		return
	}

	logrus.Infof("Migrating content from old table")
	_, err := d.execute(ctx,
		`INSERT INTO kine(deleted, create_revision, prev_revision, name, value, created, lease)
					SELECT 0, 0, 0, kv.name, kv.value, 1, CASE WHEN kv.ttl > 0 THEN 15 ELSE 0 END
					FROM key_value kv
						WHERE kv.id IN (SELECT MAX(kvd.id) FROM key_value kvd GROUP BY kvd.name)`)
	if err != nil {
		logrus.Errorf("Migration failed: %v", err)
	}
}

func openAndTest(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func Open(ctx context.Context, driverName, dataSourceName string, paramCharacter string, numbered bool) (*MssqlGeneric, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = openAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return &MssqlGeneric{
		DB: db,

		GetRevisionSQL: q(getRevisionSQL, paramCharacter, numbered),

		GetCurrentSQL:        q(getCurrentSQL, paramCharacter, numbered),
		ListRevisionStartSQL: q(listRevisionStartSQL, paramCharacter, numbered),
		GetRevisionAfterSQL:  q(getRevisionAfterSQL, paramCharacter, numbered),

		CountSQL: q(countSQL, paramCharacter, numbered),

		AfterSQL: q(afterSQL, paramCharacter, numbered),

		DeleteSQL: q(`
			DELETE FROM kine
			WHERE id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(insertSQL, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),
	}, err
}

func (d *MssqlGeneric) query(ctx context.Context, sql string, args ...interface{}) (*sql.Rows, error) {
	fmt.Printf("QUERY %v : %s\n", args, Stripped(sql))
	logrus.Tracef("QUERY %v : %s", args, Stripped(sql))
	return d.DB.QueryContext(ctx, sql, args...)
}

func (d *MssqlGeneric) queryRow(ctx context.Context, sql string, args ...interface{}) *sql.Row {
	logrus.Tracef("QUERY ROW %v : %s", args, Stripped(sql))
	return d.DB.QueryRowContext(ctx, sql, args...)
}

func (d *MssqlGeneric) execute(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	wait := strategy.Backoff(backoff.Linear(100 + time.Millisecond))
	for i := uint(0); i < 20; i++ {
		if i > 2 {
			logrus.Debugf("EXEC (try: %d) %v : %s", i, args, Stripped(sql))
		} else {
			logrus.Tracef("EXEC (try: %d) %v : %s", i, args, Stripped(sql))
		}
		result, err = d.DB.ExecContext(ctx, sql, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			wait(i)
			continue
		}
		logrus.Error(err)
		return result, err
	}
	return
}

func (d *MssqlGeneric) GetCompactRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, compactRevSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *MssqlGeneric) SetCompactRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.UpdateCompactSQL, revision)
	return err
}

func (d *MssqlGeneric) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, d.GetRevisionSQL, revision)
}

func (d *MssqlGeneric) DeleteRevision(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.DeleteSQL, revision)
	return err
}

func (d *MssqlGeneric) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	if limit > 0 {
		sql = strings.Replace(sql, "SELECT", fmt.Sprintf("SELECT TOP(%d)", limit), 1)
	}
	return d.query(ctx, sql, prefix, includeDeleted)
}

func (d *MssqlGeneric) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = strings.Replace(sql, "SELECT", fmt.Sprintf("SELECT TOP(%d)", limit), 1)
		}
		return d.query(ctx, sql, prefix, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = strings.Replace(sql, "SELECT", fmt.Sprintf("SELECT TOP(%d)", limit), 1)
	}
	return d.query(ctx, sql, prefix, revision, startKey, revision, includeDeleted)
}

func (d *MssqlGeneric) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	row := d.queryRow(ctx, d.CountSQL, prefix, false)
	err := row.Scan(&rev, &id)
	return rev.Int64, id, err
}

func (d *MssqlGeneric) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	row := d.queryRow(ctx, revSQL)
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *MssqlGeneric) After(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	if limit > 0 {
		sql = strings.Replace(sql, "SELECT", fmt.Sprintf("SELECT TOP(%d)", limit), 1)
	}
	return d.query(ctx, sql, prefix, rev)
}

func (d *MssqlGeneric) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}

func (d *MssqlGeneric) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

func (d *MssqlGeneric) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	if d.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = d.TranslateErr(err)
			}
		}()
	}

	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		row, err := d.execute(ctx, d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	row := d.queryRow(ctx, d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
	err = row.Scan(&id)
	return id, err
}
