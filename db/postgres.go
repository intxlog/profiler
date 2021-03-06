package db

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type PostgresConn struct {
	dataSourceName string
	conn           *sql.DB
}

//Creates a new postgres connection object
func NewPostgresConn(dataSourceName string) *PostgresConn {
	return &PostgresConn{
		dataSourceName: dataSourceName,
	}
}

//Connect to default database
func (p *PostgresConn) GetConnection() (*sql.DB, error) {
	if p.conn != nil {
		return p.conn, nil
	}

	conn, err := sql.Open(`postgres`, p.dataSourceName)
	p.conn = conn //ide error? cant just do this above
	return p.conn, err
}

func (p *PostgresConn) GetSelectSingle(tableName string, selects []string) (*sql.Rows, error) {
	qry := fmt.Sprintf(`select %s from %s limit 1`, p.getConcatSelects(selects), tableName)
	conn, err := p.GetConnection()
	if err != nil {
		return nil, err
	}

	return conn.Query(qry)
}

func (p *PostgresConn) GetSelectAllColumnsSingle(tableName string) (*sql.Rows, error) {
	qry := fmt.Sprintf(`select * from %s limit 1`, tableName)
	conn, err := p.GetConnection()
	if err != nil {
		return nil, err
	}

	return conn.Query(qry)
}

func (p *PostgresConn) DoesTableExist(tableName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}
	tableName = strings.ToLower(tableName)
	query := fmt.Sprintf(`select to_regclass('%s')`, tableName)
	row := conn.QueryRow(query)

	var name string
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return name == tableName, nil
}

func (p *PostgresConn) CreateTable(tableName string, columns []DBColumnDefinition) error {
	conn, err := p.GetConnection()
	if err != nil {
		return err
	}

	columnItems := []string{}
	for _, col := range columns {
		columnSQLType, err := p.convertTypeToSQLType(col.ColumnType)
		if err != nil {
			return err
		}
		columnItems = append(columnItems, fmt.Sprintf(`%s %s`, col.ColumnName, columnSQLType))
	}

	columnQuery := strings.Join(columnItems, `,`)

	query := `create table %s (
			id serial primary key,
			%s
		);`

	query = fmt.Sprintf(query, tableName, columnQuery)

	_, err = conn.Exec(query)
	return err
}

func (p *PostgresConn) CreateTableIfNotExists(tableName string, columns []DBColumnDefinition) error {
	if ok, err := p.DoesTableExist(tableName); ok && err == nil {
		return p.createColumnsIfNotExists(tableName, columns)
	}
	return p.CreateTable(tableName, columns)
}

func (p *PostgresConn) DoesTableColumnExist(tableName string, columnName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf(`select %s from %s limit 1`, columnName, tableName)

	row := conn.QueryRow(query)

	var name interface{}
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (p *PostgresConn) AddTableColumn(tableName string, column DBColumnDefinition) error {
	return p.AddTableColumns(tableName, []DBColumnDefinition{column})
}

func (p *PostgresConn) AddTableColumns(tableName string, columns []DBColumnDefinition) error {
	conn, err := p.GetConnection()
	if err != nil {
		return err
	}

	addTableQuery := `add column %s %s`
	addTableItems := []string{}

	// Build the addTableItems slice with
	// all of the new columns
	for _, columnDef := range columns {
		dataType, err := p.convertTypeToSQLType(columnDef.ColumnType)
		if err != nil {
			return err
		}
		addTableItems = append(addTableItems, fmt.Sprintf(addTableQuery, columnDef.ColumnName, dataType))
	}

	query := `alter table %s %s;`
	query = fmt.Sprintf(query, tableName, strings.Join(addTableItems, ", "))
	fmt.Println(query)

	_, err = conn.Exec(query)
	return err
}

func (p *PostgresConn) ProfilesByType(columnType string) map[string]string {
	profileColumns := map[string]string{}
	switch columnType {
	case `INT4`, `NUMERIC`, `INT2`, `INT8`:
		profileColumns["maximum"] = "max(%s)"
		profileColumns["minimum"] = "min(%s)"
		profileColumns["average"] = "avg(%s)"
		break
	case `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`:
		profileColumns["maximum"] = "max(%s)"
		profileColumns["minimum"] = "min(%s)"
		break
	case `VARCHAR`, `BPCHAR`, `TEXT`:
		profileColumns["max_length"] = "max(length(%s))"
		profileColumns["avg_length"] = "avg(length(%s))"
		break
	}

	return profileColumns
}

func (p *PostgresConn) InsertRowAndReturnID(tableName string, values map[string]interface{}) int {

	insertColumns := []string{}
	insertValuePlaceholders := []string{}
	insertValues := []interface{}{}
	idx := 1
	for col, val := range values {
		insertColumns = append(insertColumns, col)
		insertValuePlaceholders = append(insertValuePlaceholders, fmt.Sprintf(`$%d`, idx))
		insertValues = append(insertValues, val)
		idx = idx + 1
	}

	insertQuery := fmt.Sprintf(`insert into %s (%s) values (%s) returning id`,
		tableName,
		strings.Join(insertColumns, `,`),
		strings.Join(insertValuePlaceholders, `,`),
	)

	conn, err := p.GetConnection()
	if err != nil {
		panic(err)
	}

	row := conn.QueryRow(insertQuery, insertValues...)
	var newID int
	err = row.Scan(&newID)
	if err != nil {
		panic(err)
	}

	return newID
}

func (p *PostgresConn) GetRowsSelect(tableName string, selects []string) (*sql.Rows, error) {
	query := p.getSelectQueryString(tableName, selects)

	conn, err := p.GetConnection()
	if err != nil {
		panic(err)
	}

	return conn.Query(query)
}

func (p *PostgresConn) GetRowsSelectWhere(tableName string, selects []string, wheres map[string]interface{}) (*sql.Rows, error) {
	whereClauses := []string{}
	whereValues := []interface{}{}
	idx := 1
	for col, val := range wheres {
		whereClauses = append(whereClauses, fmt.Sprintf(`%s=$%d`, col, idx))
		whereValues = append(whereValues, val)
		idx = idx + 1
	}

	query := p.getSelectQueryString(tableName, selects)

	//if we have where claues then add them to our query
	if len(whereClauses) > 0 {
		query = fmt.Sprintf(`%s where %s`,
			query,
			strings.Join(whereClauses, ` AND `),
		)
	}

	conn, err := p.GetConnection()
	if err != nil {
		panic(err)
	}

	return conn.Query(query, whereValues...)
}

// Returns a list of column names for this table
func (p *PostgresConn) getExistingColumnNames(tableName string) ([]string, error) {
	rows, err := p.GetSelectAllColumnsSingle(tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.Columns()
}

func (p *PostgresConn) createColumnsIfNotExists(tableName string, columns []DBColumnDefinition) error {
	//Columns we need to add
	columnsToAdd := []DBColumnDefinition{}

	existingColumns, err := p.getExistingColumnNames(tableName)
	if err != nil {
		return err
	}

	for _, columnDefinition := range columns {
		exists := false
		//Check our existing columns list to see if
		//this definition exists already (we are not doing a type check)
		for _, existingColumn := range existingColumns {
			if existingColumn == columnDefinition.ColumnName {
				exists = true
			}
		}
		// If this column does not exist in profiler db
		// then add it to our list to be added
		if !exists {
			columnsToAdd = append(columnsToAdd, columnDefinition)
		}
	}

	if len(columnsToAdd) > 0 {
		return p.AddTableColumns(tableName, columnsToAdd)
	}
	return nil
}

func (p *PostgresConn) getConcatSelects(selects []string) string {
	return strings.Join(selects, `,`)
}

func (p *PostgresConn) getSelectQueryString(tableName string, selects []string) string {
	return fmt.Sprintf(`select %s from %s`,
		p.getConcatSelects(selects),
		tableName,
	)
}

func (p *PostgresConn) GetRows(tableName string, wheres map[string]interface{}) (*sql.Rows, error) {
	return p.GetRowsSelectWhere(tableName, []string{`*`}, wheres)
}

func (p *PostgresConn) GetTableRowCount(tableName string) (int, error) {
	rows, err := p.GetRowsSelect(tableName, []string{`count(*) as count`})

	if err != nil {
		return 0, err
	}
	defer rows.Close()

	rows.Next()
	var count int
	err = rows.Scan(&count)

	return count, err
}

func (p *PostgresConn) GetTableAndIndexesSize(tableName string) (tableSize int64, indexesSize int64, err error) {
	conn, err := p.GetConnection()
	if err != nil {
		return
	}

	qry := fmt.Sprintf(`select pg_relation_size('%s'), pg_indexes_size('%s')`, tableName, tableName)

	rows, err := conn.Query(qry)
	defer rows.Close()

	if err != nil {
		return
	}
	
	rows.Next()
	err = rows.Scan(&tableSize, &indexesSize)

	return
}

func (p *PostgresConn) dbExists(dbName string) (bool, error) {
	conn, err := p.GetConnection()
	if err != nil {
		return false, err
	}

	row := conn.QueryRow(
		`select datname from pg_catalog.pg_database where datname = $1;`,
		dbName,
	)

	var name string
	err = row.Scan(&name)
	if err != nil {
		return false, err
	}

	return name == dbName, nil
}

func (p *PostgresConn) convertTypeToSQLType(dataType reflect.Type) (string, error) {
	if dataType == nil {
		return ``, fmt.Errorf(`data type is a nil pointer, this is likely due to a null value which cannot be interpreted to a data type`)
	}
	switch dataType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return `int`, nil
	case reflect.Int64:
		return `bigint`, nil
	case reflect.String:
		return `text`, nil
	case reflect.Struct:
		if isSameStructType(dataType, time.Time{}) {
			return `timestamptz`, nil
		}
	case reflect.Slice:
		sliceType := dataType.Elem().Kind()
		if sliceType == reflect.Uint8 {
			return `numeric`, nil
		}
	default:
		fmt.Printf("\nunable to find a sql type for %v", dataType.Kind())
	}
	return ``, fmt.Errorf(`no defined sql type for reflect type of %v`, dataType)
}

func isSameStructType(dataType reflect.Type, compareInterface interface{}) bool {
	return dataType == reflect.TypeOf(compareInterface)
}
