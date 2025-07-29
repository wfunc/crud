# CRUD - Database-First Go ORM Library

CRUD is a database-first Go ORM library that automatically generates Go structs and CRUD methods by reading database structures, making database operations simple and intuitive.

## Features

- 🗄️ **Database-First** - Auto-generate Go code from database table structures
- 🔧 **Code Generation** - Automatically generate structs, enum types, and CRUD methods
- 🏷️ **Flexible Filters** - Control field selection and condition building via filter parameters
- 📦 **Multi-Database Support** - Support for PostgreSQL, SQLite, and other databases
- 🔌 **Extensible** - Support for custom field conversion, naming conversion, etc.
- ⚡ **High Performance** - Efficient SQL construction based on reflection

## Installation

```bash
go get -u github.com/wfunc/crud
```

## Development Workflow

### 1. Design Database

First, create database tables:

```sql
-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INTEGER DEFAULT 0,
    status INTEGER DEFAULT 1,
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add comments
COMMENT ON COLUMN users.status IS 'Active=1:Active User, Inactive=0:Inactive User, Deleted=-1:Deleted User';
```

### 2. Configure Code Generator

Create code generation configuration:

```go
package main

import (
    "github.com/wfunc/crud/gen"
)

var AutoGenConfig = gen.AutoGen{
    // Database connection
    Queryer: queryer, // Your database connection
    
    // Output directory
    Out: "./autogen",
    
    // Package name
    Package: "autogen",
    
    // Custom field type mapping
    TypeField: map[string]map[string]string{
        "users": {
            "create_time": "xsql.Time",
            "update_time": "xsql.Time",
        },
    },
    
    // Field filter configuration
    FieldFilter: map[string]map[string]string{
        "users": {
            "FieldsOrder": "name,email,status,create_time",
        },
    },
    
    // Custom naming conversion
    NameConv: func(isTable bool, name string) string {
        if isTable {
            return gen.ConvCamelCase(true, name)
        }
        return gen.ConvCamelCase(false, name)
    },
}
```

### 3. Run Code Generator

```go
func main() {
    err := AutoGenConfig.Generate()
    if err != nil {
        panic(err)
    }
    fmt.Println("Code generation completed!")
}
```

This will generate code similar to the following:

```go
// Auto-generated struct
type Users struct {
    T          string    `json:"-" table:"users"`
    ID         int64     `json:"id,omitempty"`
    Name       string    `json:"name,omitempty"`
    Email      *string   `json:"email,omitempty"`
    Age        int       `json:"age,omitempty"`
    Status     UsersStatus `json:"status,omitempty"`
    CreateTime xsql.Time `json:"create_time,omitempty"`
    UpdateTime xsql.Time `json:"update_time,omitempty"`
}

// Auto-generated enum type
type UsersStatus int
const (
    UsersStatusActive   UsersStatus = 1  // Active User
    UsersStatusInactive UsersStatus = 0  // Inactive User  
    UsersStatusDeleted  UsersStatus = -1 // Deleted User
)
```

### 4. Use Generated CRUD Methods

```go
import (
    "context"
    "database/sql"
    "your-project/autogen"
    _ "github.com/lib/pq"
)

func main() {
    // Connect to database
    db, err := sql.Open("postgres", "postgres://user:pass@localhost/dbname")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Insert data
    user := &autogen.Users{
        Name:   "John Doe",
        Email:  &[]string{"john@example.com"}[0],
        Age:    25,
        Status: autogen.UsersStatusActive,
    }
    
    // Use InsertFilter to insert data
    insertID, err := crud.InsertFilter(ctx, db, user, "", "", "")
    if err != nil {
        panic(err)
    }
    user.ID = insertID

    // Update data - only update age field
    user.Age = 26
    affected, err := crud.UpdateFilter(ctx, db, user, "age", []string{"id=$"}, "and", []any{user.ID})
    if err != nil {
        panic(err)
    }

    // Query single record
    var foundUser autogen.Users
    err = crud.QueryRowFilter(ctx, db, &foundUser, "", []string{"id=$"}, "and", []any{user.ID})
    if err != nil {
        panic(err)
    }

    // Query multiple records
    var users []autogen.Users
    err = crud.QueryFilter(ctx, db, &users, "", []string{"status=$"}, "and", []any{autogen.UsersStatusActive}, "", 0, 10)
    if err != nil {
        panic(err)
    }

    // Count records
    var count int64
    err = crud.CountFilter(ctx, db, &autogen.Users{}, "", []string{"status=$"}, "and", []any{autogen.UsersStatusActive}, "", &count)
    if err != nil {
        panic(err)
    }
}
```

## Core API Reference

### Insert Operations

```go
// InsertFilter - Insert data
func InsertFilter(ctx context.Context, queryer any, v any, filter, join, scan string) (insertID int64, err error)

// Parameters:
// - queryer: Database connection (*sql.DB, *sql.Tx, etc.)
// - v: Struct pointer to insert
// - filter: Field filter, empty string means all fields
// - join: Join string (usually empty)
// - scan: Scan fields (usually empty)

// Example
user := &Users{Name: "John Doe", Age: 25}
insertID, err := crud.InsertFilter(ctx, db, user, "", "", "")
```

### Update Operations

```go
// UpdateFilter - Update data
func UpdateFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any) (affected int64, err error)

// Parameters:
// - filter: Fields to update, e.g., "name,age"
// - where: WHERE condition array, e.g., []string{"id=$", "status=$"}
// - sep: Condition separator, e.g., "and" or "or"
// - args: Condition parameters

// Example - Update specific fields
user.Age = 26
affected, err := crud.UpdateFilter(ctx, db, user, "age", []string{"id=$"}, "and", []any{user.ID})

// Example - Update all non-zero fields
affected, err := crud.UpdateFilter(ctx, db, user, "", []string{"id=$"}, "and", []any{user.ID})
```

### Query Operations

```go
// QueryFilter - Query multiple records
func QueryFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error)

// QueryRowFilter - Query single record
func QueryRowFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, dest ...any) (err error)

// CountFilter - Count records
func CountFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error)

// Example - Query multiple records
var users []Users
err := crud.QueryFilter(ctx, db, &users, "", 
    []string{"status=$", "age>=$"}, "and", 
    []any{UsersStatusActive, 18}, 
    "order by create_time desc", 0, 10)

// Example - Query single record
var user Users
err := crud.QueryRowFilter(ctx, db, &user, "", 
    []string{"id=$"}, "and", []any{123})

// Example - Count records
var count int64
err := crud.CountFilter(ctx, db, &Users{}, "", 
    []string{"status=$"}, "and", []any{UsersStatusActive}, 
    "", &count)
```

### Dynamic SQL Building

```go
// AppendWhere - Dynamically build WHERE conditions
func AppendWhere(where []string, args []any, ok bool, format string, v any) (whereClauses []string, updatedArgs []any)

// JoinWhere - Join WHERE conditions to SQL
func JoinWhere(sql string, where []string, sep string, suffix ...string) (resultSQL string)

// Example
var whereClauses []string
var args []any

// Dynamically add conditions
if name != "" {
    whereClauses, args = crud.AppendWhere(whereClauses, args, true, "name like $", "%"+name+"%")
}
if status > 0 {
    whereClauses, args = crud.AppendWhere(whereClauses, args, true, "status=$", status)
}

// Build complete SQL
sql := "select * from users"
sql = crud.JoinWhere(sql, whereClauses, "and")

// Execute query
var users []Users
err := crud.Query(ctx, db, &users, "", sql, args)
```

## Struct Tags Reference

### Basic Tags

- `table:"table_name"` - Specify database table name
- `json:"field_name"` - JSON serialization field name, also used as database field name
- `cmp:"condition_expression"` - Query condition expression, `$` represents parameter placeholder
- `filter:"filter_type"` - Field filter, controls which fields participate in operations

### Query Condition Expressions (cmp)

```go
// Basic comparisons
`cmp:"field=$"`           // field = ?
`cmp:"field>$"`           // field > ?
`cmp:"field<$"`           // field < ?
`cmp:"field>=$"`          // field >= ?
`cmp:"field<=$"`          // field <= ?
`cmp:"field!=$"`          // field != ?

// Pattern matching
`cmp:"field like $"`      // field LIKE ?
`cmp:"field ilike $"`     // field ILIKE ? (PostgreSQL)

// IN queries
`cmp:"field=any($)"`      // field = ANY(?) (PostgreSQL)
`cmp:"field in ($)"`      // field IN (?)

// Range queries
`cmp:"field between $ and $"` // field BETWEEN ? AND ?

// NULL checks
`cmp:"field is null"`     // field IS NULL
`cmp:"field is not null"` // field IS NOT NULL

// Complex conditions
`cmp:"(field1=$ or field2=$)"` // (field1 = ? OR field2 = ?)
```

### Filter Syntax

#### Field-Level Filters
```go
// Insert operations
`filter:"insert"`         // Include this field only in insert operations
`filter:"-insert"`        // Exclude this field from insert operations

// Update operations  
`filter:"update"`         // Include this field only in update operations
`filter:"-update"`        // Exclude this field from update operations

// Query operations
`filter:"query"`          // Include this field only in query operations
`filter:"-query"`         // Exclude this field from query operations

// Where conditions
`filter:"where"`          // Use as query condition
`filter:"-"`              // Completely ignore this field

// Combined usage
`filter:"insert,update"`  // Include in both insert and update operations
```

#### Function-Level Filter Syntax
* Format: `[^]<field list>#<options>`
* Examples:
  * `""`: default, for all fields and skip nil/zero values
  * `"#all"`: for all fields and not skip nil/zero values
  * `"#nil"`: for all fields and only skip nil values
  * `"#zero"`: for all fields and only skip zero values
  * `"tid,name"`: only include fields tid,name and auto skip nil/zero values
  * `"tid,name#all"`: only include fields tid,name and not skip nil/zero values
  * `"^tid,name"`: exclude fields tid,name and auto skip nil/zero values
  * `"^tid,name#all"`: exclude fields tid,name and auto skip nil/zero values

## Pagination Support

```go
// Define pagination structure
type ArticlePage struct {
    Where ArticleWhere `filter:"where"`
    Page  crud.Paging
}

// Paging structure
type Paging struct {
    Order  string // Sort fields: "+field" ascending, "-field" descending
    Offset int    // Offset
    Limit  int    // Records per page
}

// Usage example
page := ArticlePage{
    Page: crud.Paging{
        Order:  "-create_time,+status", // Multi-field sorting
        Offset: 20,
        Limit:  10,
    },
}

var articles []Article
total, err := crud.QueryPageFilter(ctx, db, &articles, "", &page)
```

## Transaction Support

```go
// Begin transaction
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    return err
}
defer tx.Rollback()

// Execute operations in transaction
_, err = crud.InsertFilter(ctx, tx, &user, "", "", "")
if err != nil {
    return err
}

_, err = crud.UpdateFilter(ctx, tx, &order, "", []string{"id=$"}, "and", []any{orderID})
if err != nil {
    return err
}

// Commit transaction
return tx.Commit()
```

## Custom Configuration

```go
// Create custom CRUD instance
c := &crud.CRUD{
    // Custom parameter format (default is $1, $2...)
    ArgFormat: "$%d",
    
    // Enable debug logging
    Verbose: true,
    
    // Custom field name conversion
    NameConv: func(on, name string, field reflect.StructField) string {
        // Convert array type fields to PostgreSQL format
        if on == "query" && strings.HasPrefix(field.Type.String(), "xsql.") {
            return name + "::text"
        }
        return name
    },
    
    // Custom parameter conversion
    ParamConv: func(on, fieldName, fieldFunc string, field reflect.StructField, value any) any {
        // Handle array types
        if c, ok := value.(xsql.ArrayConverter); on == "where" && ok {
            return c.DbArray()
        }
        return value
    },
}

// Use custom instance
_, err = c.InsertFilter(ctx, db, &user, "", "", "")
```

## Performance Optimization Tips

1. **Use Indexes** - Create indexes for frequently queried fields
2. **Limit Query Fields** - Use `filter` parameter to limit query fields
3. **Use Pagination** - Avoid querying large amounts of data at once
4. **Prepared Statements** - Reuse the same query patterns
5. **Connection Pool Configuration** - Configure database connection pool properly

## FAQ

### 1. How to handle NULL values?
Use pointer type fields to handle database fields that may be NULL:
```go
type User struct {
    Name  string  `json:"name"`
    Email *string `json:"email"` // Can be NULL
}
```

### 2. How to use complex query conditions?
Use `cmp` tags to define complex conditions, or use `AppendWhere` to build dynamically:
```go
// Using cmp tags
type Where struct {
    Complex string `cmp:"(status=$ and (type=$ or type=$))"`
}

// Dynamic building
where, args = crud.AppendWheref(where, args, 
    "(status=$ and create_time between $ and $)", 
    1, startTime, endTime)
```

### 3. How to customize table names?
Use `TableName()` method or `table` tag:
```go
func (u *User) TableName() string {
    return "custom_users"
}
// Or
type User struct {
    T string `table:"custom_users"`
}
```

### 4. How to handle array types?
Use array types from the `xsql` package:
```go
import "github.com/wfunc/util/xsql"

type User struct {
    Tags    xsql.StringArray `json:"tags"`
    Numbers xsql.IntArray    `json:"numbers"`
}
```

## More Examples

Check the test files in the project for more usage examples:
- `crud_test.go` - Basic CRUD operation examples
- `object_test.go` - Complex data model examples

## Supported Databases

- PostgreSQL
- MySQL
- SQLite
- Other databases compatible with the `database/sql` interface

## Contributing

Issues and Pull Requests are welcome!

## License

MIT License
