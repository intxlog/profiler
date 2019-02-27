package profiler

type ProfileDefinition struct {
	FullProfileTables   []string
	CustomProfileTables []TableDefinition
}

type TableDefinition struct {
	TableName     string
	Columns       []string
	CustomColumns []CustomColumnDefition
}

type CustomColumnDefition struct {
	ColumnName       string
	ColumnDefinition string
}
