package profiler

import "time"

type TableColumnName struct {
	ID                int    `db:"id"`
	TableNameID       int    `db:"table_name_id"`
	TableColumnName   string `db:"table_column_name"`
	TableColumnTypeID int    `db:"table_column_type_id"`
}

type TableColumnType struct {
	ID              int    `db:"id"`
	TableColumnType string `db:"table_column_type"`
}

type TableName struct {
	ID        int    `db:"id"`
	TableName string `db:"table_name"`
}

type TableProfile struct {
	ID            int `db:"id"`
	TableNameID   int `db:"table_name_id"`
	TableRowCount int `db:"table_row_count"`
}

type ProfileRecord struct {
	ID          int       `db:"id"`
	ProfileDate time.Time `db:"profile_date"`
}
