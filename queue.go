package queue

import (
	"database/sql"
	"time"

	"github.com/BurntSushi/migration"
	_ "github.com/mattn/go-sqlite3"
)

const (
	STATE_UNKNOWN = iota
	STATE_READY
	STATE_RESERVED
)

type (
	Queue struct {
		db     *sql.DB
		ticker *time.Ticker
		wait   chan struct{}
		exit   chan struct{}
	}

	Tube struct {
		q    *Queue
		Name string
	}

	Job struct {
		q        *Queue
		ID       int
		Tube     string
		Created  time.Time
		Modified time.Time
		State    int
		Priority uint
		Data     []byte
		TTR      time.Duration
	}
)

func New(filename string, buffer int, maintanence int) (*Queue, error) {
	db, err := migration.OpenWith("sqlite3", filename,
		[]migration.Migrator{
			func(tx migration.LimitedTx) error {
				_, err := tx.Exec(`
               CREATE table simple_queue (
                 id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                 tube text NOT NULL,
                 priority INTEGERT DEFAULT 0,
                 created INTEGER NOT NULL,
                 modified INTEGER NOT NULL,
                 state INTEGER NOT NULL,
                 data text NOT NULL,
                 ttr INTEGER NOT NULL
               )`)
				return err
			},
			func(tx migration.LimitedTx) error {
				_, err := tx.Exec(`CREATE INDEX simple_queue_tube_idx ON simple_queue(tube)`)
				return err
			},
		},
		defaultGetVersion,
		defaultSetVersion)

	if err != nil {
		return nil, err
	}

	q := &Queue{
		db:     db,
		wait:   make(chan struct{}, buffer),
		exit:   make(chan struct{}),
		ticker: time.NewTicker(time.Second * time.Duration(maintanence)),
	}

	go q.maintanence()

	return q, nil
}

func (q *Queue) Maintanence() error {
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("UPDATE simple_queue SET state=? WHERE state=? AND (modified + ttr) < ?", STATE_READY, STATE_RESERVED, time.Now().Unix())
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (q *Queue) maintanence() {
LOOP:
	for {
		select {
		case <-q.exit:
			q.ticker.Stop()
			break LOOP
		case <-q.ticker.C:
			q.Maintanence()
		}
	}
}

// Close closes the underlying database handle and stops maintainence routines
func (q *Queue) Close() error {
	close(q.wait)
	q.exit <- struct{}{}
	q.db.Close()
	return nil
}

func (q *Queue) Put(tube string, priority int, ttr int, data []byte) error {
	if ttr <= 0 {
		ttr = 1
	}
	now := time.Now().Unix()
	tx, err := q.db.Begin()
	defer tx.Rollback()
	_, err = tx.Exec("INSERT into simple_queue (tube, created, modified, state, data, ttr, priority) VALUES(?, ?, ?, ?, ?, ?, ?)",
		tube, now, now, STATE_READY, data, ttr, priority)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	q.wait <- struct{}{}
	return nil
}

func (q *Queue) Reserve(tube string, timeout int) (*Job, error) {

	if timeout > 0 {
		select {
		case <-q.wait:
		case <-time.After(time.Second * time.Duration(timeout)):
		}
	}

	tx, err := q.db.Begin()
	if err != nil {

		return nil, err
	}
	defer tx.Rollback()
	row := tx.QueryRow("SELECT id, created, data, ttr, priority from simple_queue WHERE tube=? AND state=? ORDER BY priority DESC, created ASC LIMIT 1",
		tube, STATE_READY)

	now := time.Now()
	j := Job{
		q:        q,
		Tube:     tube,
		Modified: now,
		State:    STATE_RESERVED,
	}

	var created, ttr int64
	if err := row.Scan(&j.ID, &created, &j.Data, &ttr, &j.Priority); err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return nil, err
	}
	j.Created = time.Unix(created, 0)
	j.TTR = time.Second * time.Duration(ttr)
	_, err = tx.Exec("UPDATE simple_queue SET state=?, modified=? WHERE id=?", STATE_RESERVED, now.Unix(), j.ID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &j, nil
}

// Jobs returns all Jobs in a tube
func (q *Queue) Jobs(tube string) ([]*Job, error) {
	tx, err := q.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	jobs := make([]*Job, 0)
	rows, err := tx.Query("SELECT id, created, modified, data, ttr, state, priority from simple_queue WHERE tube=? ORDER BY priority DESC, created ASC",
		tube)
	if err != nil {
		if err == sql.ErrNoRows {
			return jobs, nil
		}
		return nil, err
	}

	for rows.Next() {
		j := Job{Tube: tube}
		var modified, created, ttr int64
		if err := rows.Scan(&j.ID, &modified, &created, &j.Data, &ttr, &j.State, &j.Priority); err != nil {
			return nil, err
		}
		j.Modified = time.Unix(modified, 0)
		j.Created = time.Unix(created, 0)
		j.TTR = time.Second * time.Duration(ttr)
		jobs = append(jobs, &j)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return jobs, nil
}

func (j *Job) Delete() error {
	tx, err := j.q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE from simple_queue WHERE id=?", j.ID)
	if err != nil {
		return err
	}
	return tx.Commit()

}

func (j *Job) Touch(ttr int) error {

	if ttr <= 0 {
		ttr = int(j.TTR.Seconds())
	}

	tx, err := j.q.db.Begin()
	defer tx.Rollback()

	now := time.Now()
	j.Modified = now
	// should we make sure job is actually reserved?
	_, err = tx.Exec("UPDATE simple_queue SET modified=?, ttr=? WHERE id=?", now.Unix(), ttr, j.ID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (q *Queue) Tube(tube string) (*Tube, error) {
	return &Tube{
		Name: tube,
		q:    q,
	}, nil
}

func (t *Tube) Put(priority int, ttr int, data []byte) error {
	return t.q.Put(t.Name, priority, ttr, data)
}

func (t *Tube) Reserve(timeout int) (*Job, error) {
	return t.q.Reserve(t.Name, timeout)
}

func defaultGetVersion(tx migration.LimitedTx) (int, error) {
	v, err := getVersion(tx)
	if err != nil {
		if err := createVersionTable(tx); err != nil {
			return 0, err
		}
		return getVersion(tx)
	}
	return v, nil
}

func defaultSetVersion(tx migration.LimitedTx, version int) error {
	if err := setVersion(tx, version); err != nil {
		if err := createVersionTable(tx); err != nil {
			return err
		}
		return setVersion(tx, version)
	}
	return nil
}

func getVersion(tx migration.LimitedTx) (int, error) {
	var version int
	r := tx.QueryRow("SELECT version FROM simple_queue_version")
	if err := r.Scan(&version); err != nil {
		return 0, err
	}
	return version, nil
}

func setVersion(tx migration.LimitedTx, version int) error {
	_, err := tx.Exec("UPDATE simple_queue_version SET version = $1", version)
	return err
}

func createVersionTable(tx migration.LimitedTx) error {
	_, err := tx.Exec(`
		CREATE TABLE simple_queue_version (
			version INTEGER
		);
		INSERT INTO simple_queue_version (version) VALUES (0)`)
	return err
}
