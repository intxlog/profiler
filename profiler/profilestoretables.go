package profiler

import "time"

type TableColumnName struct {
	ID                int    `db:"id" table:"table_column_names" primaryKey:"true"`
	TableNameID       int    `db:"table_name_id"`
	TableColumnName   string `db:"table_column_name"`
	TableColumnTypeID int    `db:"table_column_type_id"`
}

type TableColumnType struct {
	ID              int    `db:"id" table:"table_column_types" primaryKey:"true"`
	TableColumnType string `db:"table_column_type"`
}

type TableName struct {
	ID        int    `db:"id" table:"table_names" primaryKey:"true"`
	TableName string `db:"table_name"`
}

type TableProfile struct {
	ID              int   `db:"id" table:"table_profiles" primaryKey:"true"`
	TableNameID     int   `db:"table_name_id"`
	TableRowCount   int   `db:"table_row_count"`
	TableSize       int64 `db:"table_size"`
	IndexesSize		int64 `db:"indexes_size"`
	ProfileRecordID int   `db:"profile_record_id"`
}

type ProfileRecord struct {
	ID          int       `db:"id" table:"profile_records" primaryKey:"true"`
	ProfileDate time.Time `db:"profile_date"`
}

type TableCustomColumnName struct {
	ID                     int    `db:"id" table:"table_custom_column_names" primaryKey:"true"`
	TableNameID            int    `db:"table_name_id"`
	TableColumnName        string `db:"table_column_name"`
	TableColumnTypeID      int    `db:"table_column_type_id"`
	CustomColumnDefinition string `db:"table_custom_column_definition"`
}
