package avroql

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

//Database represents a db
type Database struct {
	tables map[string]sql.Table
}

//NewDatabase creates a new db from directory
func NewDatabase(path string) (*Database, error) {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read %s", path)
	}

	tables := make(map[string]sql.Table)
	for _, fi := range fis {
		name := strings.ToLower(fi.Name())
		if fi.IsDir() || filepath.Ext(name) != ".avro" {
			continue
		}
		t, err := newTable(filepath.Join(path, name))
		if err != nil {
			return nil, errors.Wrapf(err, "could not create table from %s", name)
		}
		tables[strings.TrimSuffix(name, ".avro")] = t
	}

	return &Database{tables}, nil
}

//Name returns name of database "avroql" in this case
func (d *Database) Name() string { return "avroql" }

//Tables returns the underlying tabels
func (d *Database) Tables() map[string]sql.Table { return d.tables }

type table struct {
	name   string
	schema sql.Schema
	path   string
}

func newTable(path string) (sql.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read %s", path)
	}
	defer f.Close()

	ocf, err := goavro.NewOCFReader(f)
	codec := ocf.Codec()
	fmt.Println(codec)

	var sch interface{}
	json.Unmarshal([]byte(codec.Schema()), &sch)

	fields := sch.(map[string]interface{})

	for field, value := range fields {
		fmt.Println(field, value)
	}
	r := csv.NewReader(f)

	headers, err := r.Read()
	if err != nil {
		return nil, errors.Wrapf(err, "could not read headers in %s", path)
	}

	var schema []*sql.Column
	for _, header := range headers {
		schema = append(schema, &sql.Column{
			Name:   header,
			Type:   sql.Text,
			Source: path,
		})
	}

	name := strings.TrimSuffix(filepath.Base(path), ".avro")
	return &table{name: name, schema: schema, path: path}, nil
}

func (t *table) Name() string       { return t.name }
func (t *table) String() string     { return t.name }
func (t *table) Schema() sql.Schema { return t.schema }

type partitionIter struct{ done bool }

func (p *partitionIter) Close() error { return nil }

type partition struct{}

func (partition) Key() []byte { return []byte{'@'} }

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.done {
		return nil, io.EOF
	}
	p.done = true
	return &partition{}, nil
}

func (t *table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{}, nil
}

type rowIter struct {
	*csv.Reader
	io.Closer
}

func (r *rowIter) Next() (sql.Row, error) {
	cols, err := r.Read()
	if err == io.EOF {
		return nil, err
	} else if err != nil {
		return nil, errors.Wrap(err, "could not read row")
	}
	row := make(sql.Row, len(cols))
	for i, col := range cols {
		row[i] = col
	}
	return row, err
}

func (t *table) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	f, err := os.Open(t.path)
	if err != nil {
		return nil, errors.Wrapf(err, "could not open %s", t.path)
	}

	r := csv.NewReader(f)
	r.Read()
	return &rowIter{r, f}, nil
}
