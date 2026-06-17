package postgressql

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Client interface {
	Close()
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	AcquireFunc(ctx context.Context, f func(*pgxpool.Conn) error) error
	AcquireAllIdle(ctx context.Context) []*pgxpool.Conn
	Stat() *pgxpool.Stat
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// NewClient creates new postgres client.
func NewClient(
	ctx context.Context,
	maxAttempts int,
	maxDelay time.Duration,
	config Config,
	binary bool,
) (pool *pgxpool.Pool, err error) {
	dsn := config.ConnStringFromCfg()

	pgxCfg, parseConfigErr := pgxpool.ParseConfig(dsn)
	if parseConfigErr != nil {
		log.Printf("Unable to parse config: %v\n", parseConfigErr)
		return nil, parseConfigErr
	}

	if binary {
		pgxCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	}

	pool, parseConfigErr = pgxpool.NewWithConfig(ctx, pgxCfg)
	if parseConfigErr != nil {
		log.Printf("Failed to parse PostgreSQL configuration due to error: %v\n", parseConfigErr)
		return nil, parseConfigErr
	}

	err = DoWithAttempts(func() error {
		pingErr := pool.Ping(ctx)
		if pingErr != nil {
			log.Printf("Failed to connect to postgres due to error %v... Going to do the next attempt\n", pingErr)
			return pingErr
		}

		return nil
	}, maxAttempts, maxDelay)
	if err != nil {
		log.Fatal("List attempts are exceeded. Unable to connect to PostgreSQL")
	}

	return pool, nil
}

func DoWithAttempts(fn func() error, maxAttempts int, delay time.Duration) error {
	var err error

	for maxAttempts > 0 {
		if err = fn(); err != nil {
			time.Sleep(delay)
			maxAttempts--

			continue
		}

		return nil
	}

	return err
}

func trimQuotes(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		first, last := s[0], s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func (c *Config) ConnStringFromCfg() string {
	user := trimQuotes(c.User)
	pass := trimQuotes(c.Password)
	host := trimQuotes(c.Host)
	port := trimQuotes(c.Port)
	db := trimQuotes(c.DB)

	url := strings.Builder{}
	url.WriteString("postgres://")
	url.WriteString(user)
	url.WriteString(":")
	url.WriteString(pass)
	url.WriteString("@")
	url.WriteString(host)
	url.WriteString(":")
	url.WriteString(port)
	url.WriteString("/")
	url.WriteString(db)
	url.WriteString("?sslmode=disable")

	return url.String()
}

func UnmarshalJSON(data []byte, dest any) error {
	return json.Unmarshal(data, dest)
}

func IsSchemaExists(ctx context.Context, client Client, schemaName string) (bool, error) {
	query, args, err := sq.
		Select("1").
		From("information_schema.schemata").
		Where(sq.Eq{"schema_name": schemaName}).
		PlaceholderFormat(sq.Dollar).
		Limit(1).
		ToSql()
	if err != nil {
		return false, err
	}

	var exists int
	err = client.QueryRow(ctx, query, args...).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func IsTableExists(ctx context.Context, client Client, schemaName, tableName string) (bool, error) {
	query, args, err := sq.
		Select("1").
		From("information_schema.tables").
		Where(sq.Eq{"table_schema": schemaName, "table_name": tableName}).
		PlaceholderFormat(sq.Dollar).
		Limit(1).
		ToSql()
	if err != nil {
		return false, err
	}

	var exists int
	err = client.QueryRow(ctx, query, args...).Scan(&exists)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

var (
	_ = IsSchemaExists
	_ = IsTableExists
)
