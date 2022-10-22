package nrpgx5

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/egon12/pgsnap"
	"github.com/jackc/pgx/v5"
	"github.com/newrelic/go-agent/v3/internal"
	"github.com/newrelic/go-agent/v3/internal/integrationsupport"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/stretchr/testify/assert"
)

// to create pgnsap__** snapshot file, we are using real database.
// delete all pgnap_*.txt file and fill PGSNAP_DB_URL to recreate the snapshot file
func getTestCon(t testing.TB) (*pgx.Conn, func()) {
	snap := pgsnap.NewSnap(t, "")
	// snap := pgsnap.NewSnap(t, "")

	cfg, _ := pgx.ParseConfig(snap.Addr())
	cfg.Tracer = NewTracer()

	con, _ := pgx.ConnectConfig(context.Background(), cfg)

	return con, func() {
		con.Close(context.Background())
		snap.Finish()
	}
}

func TestTracer_Trace_CRUD(t *testing.T) {
	con, finish := getTestCon(t)
	defer finish()

	tests := []struct {
		name   string
		fn     func(context.Context, *pgx.Conn)
		metric []internal.WantMetric
	}{
		{
			name: "query should send the metric after the row close",
			fn: func(ctx context.Context, con *pgx.Conn) {
				rows, _ := con.Query(ctx, "SELECT id, name, timestamp FROM mytable LIMIT $1", 2)
				rows.Close()
			},
			metric: []internal.WantMetric{
				{Name: "Datastore/operation/Postgres/select"},
				{Name: "Datastore/statement/Postgres/mytable/select"},
			},
		},
		{
			name: "queryrow should send the metric after scan",
			fn: func(ctx context.Context, con *pgx.Conn) {
				row := con.QueryRow(ctx, "SELECT id, name, timestamp FROM mytable")
				_ = row.Scan()
			},
			metric: []internal.WantMetric{
				{Name: "Datastore/operation/Postgres/select"},
				{Name: "Datastore/statement/Postgres/mytable/select"},
			},
		},
		{
			name: "insert should send the metric",
			fn: func(ctx context.Context, con *pgx.Conn) {
				_, _ = con.Exec(ctx, "INSERT INTO mytable(name) VALUES ($1)", "myname is")
			},
			metric: []internal.WantMetric{
				{Name: "Datastore/operation/Postgres/insert"},
				{Name: "Datastore/statement/Postgres/mytable/insert"},
			},
		},
		{
			name: "update should send the metric",
			fn: func(ctx context.Context, con *pgx.Conn) {
				_, _ = con.Exec(ctx, "UPDATE mytable set name = $2 WHERE id = $1", 1, "myname is")
			},
			metric: []internal.WantMetric{
				{Name: "Datastore/operation/Postgres/update"},
				{Name: "Datastore/statement/Postgres/mytable/update"},
			},
		},
		{
			name: "delete should send the metric",
			fn: func(ctx context.Context, con *pgx.Conn) {
				_, _ = con.Exec(ctx, "DELETE FROM mytable WHERE id = $1", 4)
			},
			metric: []internal.WantMetric{
				{Name: "Datastore/operation/Postgres/delete"},
				{Name: "Datastore/statement/Postgres/mytable/delete"},
			},
		},
		{
			name: "select 1 should send the metric",
			fn: func(ctx context.Context, con *pgx.Conn) {
				_, _ = con.Exec(ctx, "SELECT 1")
			},
			metric: []internal.WantMetric{
				{Name: "Datastore/operation/Postgres/select"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := integrationsupport.NewBasicTestApp()
			txn := app.StartTransaction(t.Name())
			ctx := newrelic.NewContext(context.Background(), txn)

			tt.fn(ctx, con)

			txn.End()
			app.ExpectMetricsPresent(t, tt.metric)
		})
	}
}

func TestTracer_connect(t *testing.T) {
	conn, finish := getTestCon(t)
	defer finish()

	cfg := conn.Config()
	tracer := cfg.Tracer.(*Tracer)

	// hostname will
	t.Run("connect should set tracer host port and database", func(t *testing.T) {
		assert.Equal(t, cfg.Host, tracer.BaseSegment.Host)
		assert.Equal(t, cfg.Database, tracer.BaseSegment.DatabaseName)
		assert.Equal(t, strconv.FormatUint(uint64(cfg.Port), 10), tracer.BaseSegment.PortPathOrID)
	})

	t.Run("exec should send metric with instance host and port ", func(t *testing.T) {
		app := integrationsupport.NewBasicTestApp()

		txn := app.StartTransaction(t.Name())

		ctx := newrelic.NewContext(context.Background(), txn)
		_, _ = conn.Exec(ctx, "INSERT INTO mytable(name) VALUES ($1)", "myname is")

		txn.End()

		app.ExpectMetricsPresent(t, []internal.WantMetric{
			{Name: "Datastore/instance/Postgres/" + hostnameTest() + "/" + tracer.BaseSegment.PortPathOrID},
		})
	})
}

func hostnameTest() string {
	h, err := os.Hostname()
	if err != nil {
		return "127.0.0.1"
	}

	return h
}
