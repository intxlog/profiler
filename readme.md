# Profiler
![Profiler Logo](profiler_logo.png)

Profiler is a SQL profiler utility written in _Go_.  It is designed to be used as either a library in an existing _Go_ project or as a standalone CLI tool.

It is quick to setup but highly configurable.

## Examples

### Library Usage
```
//Setup the target database connection
targetCon, err := db.GetDBConnByType(*targetConnDBType, *targetConnString)
if err != nil{
    log.Fatal(fmt.Errorf(`error getting target database connection: %v`, err))
}

//Setup the profile database connection
profileCon, err := db.GetDBConnByType(*profileConnDBType, *profileConnString)
if err != nil{
    log.Fatal(fmt.Errorf(`error getting profile database connection: %v`, err))
}

p := profiler.NewProfiler(targetCon, profileCon)

//Define a profile definition
profile := profiler.ProfileDefinition{
    FullProfileTables: []string{"logs"},
}

err = p.RunProfile(profile)
```

### CLI Usage
```
./profiler -targetDB="postgres://user:pass@localhost:5432/targetdb" -targetDBType="postgres" -profileDB="postgres://user:pass@localhost:5432/profiledb" -profileDBType="postgres" -profileDefinition=path/to/definition.json
```

## Setup
Before running Profiler for the first time, you must create a database for the profile connection to use.  Profiler will not create the database itself, only the tables and columns.

## Profile Configuration 
Profile Definitions are how Profiler knows what to profile in the target database.  It can be used minimally by only using the `FullProfileTables` field, or it can be used to profile custom columns per table using `CustomProfileTables.CustomColumns`.

For CLI usage, the definition should be stored in a file as JSON and passed in via the `profileDefinition` flag.

For usage in a Go program, you can build the definition directly using the `profiler.ProfileDefinition` type.

### `FullProfileTables`
Any tables listed in this array will be fully profiled.  This means that every field will be profiled according to the default profiles for their corresponding types.  **This can be slow if run on a wide table.**

Using this is the quickest way to get Profiler running, but is the most generic.

### `CustomProfileTables`
Each entry in this property is a separate table.  If you want more control over what columns are profiled or want custom aggregates to be profiled, this is where to define it.

- `TableName` - Name of the table for this custom profile definition.
- `Columns` - Any columns listed here will be profiled using the default profiles by their type.
- `CustomColumns` - Define custom column aggregates to be run here.  **These must be aggregates to work correctly**.
    - `ColumnName` - The name of the aggregate column.
    - `ColumnDefinition` - The aggregate function to assign to this custom column.

## Profile Configuration Example
```
{
    "FullProfileTables": [
        "users"
    ],
    "CustomProfileTables": [
        {
            "TableName": "logs",
            "Columns": [
                "description",
                "log_time"
            ],
            "CustomColumns": [
                {
                    "ColumnName": "description_over_128",
                    "ColumnDefinition": "count(length(description) > 128)"
                }
            ]
        }
    ]
}
```

In the above example, here is what would happen:

The `users` table would have every field profiled using the db wrapper default profiles.

The `logs` table would have the `description` and `log_time` columns profiled using the db wrapper defaults for the column types.

Additionally, a custom aggregate column `description_over_128` is defined as `count(length(description) > 128)`.  The result of this aggregate will recorded for this profile.

## Additional Configuration
### Pascal Case
Profiler can be configured to use either `snake_case` or `PascalCase` for profile table and column names.  By default, it will use `snake_case`.

For CLI usage, you can set the flag `usePascalCase` to true.

For usage in a Go program, you can create a `profiler.ProfilerOptions` type with the `UsePascalCase` property set to true and pass this to `profiler.NewProfilerWithOptions`.

**NOTE: If you already ran Profiler without setting this flag, you will end up with new tables in `PascalCase`!  You will have to either manually migrate the existing `snake_case` tables and fields or start fresh.**

## Database Compatibility
Profiler currently works with the following databases:
- Postgres

## Adding support for a database
To add your own database wrapper for Profiler, you must do the following:
- Create a database wrapper under the `db` package that implements the `DBConn` interface.
- Add a new constant for the database type in the `db` package under `dbconn.go`.
- Update `db.GetDBConnByType` to properly instantiate your database wrapper.

## Tips/Tricks
### Profiling Custom Tables or Views
Profiler does not support custom table definitions or views right now.  As a workaround you may want to build a script to generate any custom tables before running Profiler.

### Indexing/Constraints
Profiler does not generate constraints or indexes right now for the profile database.  However, it does not delete/alter any existing columns.  If you find that your queries are particularly slow, you can build your own indexes and constraints in the profile database and they will persist through profiles.

## TODO
- Generating and profiling custom table views
- Automatically building indexes and constraints
- Define custom non-aggregate columns on a table to profile
- Define generalized profiles by type via profile definition