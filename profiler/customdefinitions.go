package profiler

type ProfileDefinition struct {
	FullProfileTables   []string          `json:"FullProfileTables"`
	CustomProfileTables []TableDefinition `json:"CustomProfileTables"`
}

type TableDefinition struct {
	TableName     string                 `json:"TableName"`
	Columns       []string               `json:"Columns"`
	CustomColumns []CustomColumnDefition `json:"CustomColumns"`
}

type CustomColumnDefition struct {
	ColumnName       string `json:"ColumnName"`
	ColumnDefinition string `json:"ColumnDefinition"`
}
