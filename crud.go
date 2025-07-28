// Package crud is simple curd tools to process database
//
//nolint:golint
package crud

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/wfunc/util/attrscan"
	"github.com/wfunc/util/xsql"
)

// NilChecker is an interface for checking nil values.
type NilChecker interface {
	IsNil() bool
}

// ZeroChecker is an interface for checking zero values.
type ZeroChecker interface {
	IsZero() bool
}

// ArrayConverter is an interface for converting arrays.
type TableNameGetter interface {
	GetTableName(args ...any) string
}

// TableNameGetterF is a function type that implements the TableNameGetter interface.
type TableNameGetterF func(args ...any) string

// GetTableName returns the table name based on the provided arguments.
func (t TableNameGetterF) GetTableName(args ...any) string { return t(args...) }

// FilterGetter is an interface for getting filter values.
type FilterGetter interface {
	GetFilter(args ...any) string
}

// FilterGetterF is a function type that implements the FilterGetter interface.
type FilterGetterF func(args ...any) string

// GetFilter returns the filter value based on the provided arguments.
func (f FilterGetterF) GetFilter(args ...any) string { return f(args...) }

func jsonString(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

// BuildOrderby constructs an ORDER BY clause based on the provided order string.
func BuildOrderby(supported string, order string) (orderby string) {
	if len(order) > 0 {
		orderAsc := order[0:1]
		orderKey := order[1:]
		if xsql.AsStringArray(supported).HavingOne(orderKey) {
			orderby = "order by " + orderKey
		}
		if len(orderby) > 0 && orderAsc == "+" {
			orderby += " asc"
		} else if len(orderby) > 0 && orderAsc == "-" {
			orderby += " desc"
		}
	}
	return
}

// NewValue creates a new reflect.Value based on the provided value.
func NewValue(v any) (value reflect.Value) {
	if v, ok := v.([]any); ok {
		result := []any{}
		for i, f := range v {
			if _, ok := f.(TableName); ok {
				continue
			}
			item := reflect.New(reflect.ValueOf(v[i]).Type())
			result = append(result, item.Interface())
		}
		value = reflect.ValueOf(result)
		return
	}
	reflectValue := reflect.ValueOf(v)
	if reflect.Indirect(reflectValue).Kind() == reflect.Struct { //only for struct
		reflectValue = reflect.Indirect(reflectValue)
	}
	reflectType := reflectValue.Type()
	value = reflect.New(reflectType)
	return
}

// MetaWith constructs a metadata slice with the provided fields.
func MetaWith(o any, fields ...any) (v []any) {
	tableName := ""
	if name, ok := o.(string); ok {
		tableName = name
	} else if getter, ok := o.(TableNameGetter); ok {
		tableName = getter.GetTableName()
	} else {
		tableName = Table(o)
	}
	v = append(v, TableName(tableName))
	v = append(v, fields...)
	return
}

// Default is the default CRUD instance with basic configurations.
var Default = &CRUD{
	Scanner: attrscan.Scanner{
		Tag: "json",
		NameConv: func(on, name string, field reflect.StructField) string {
			return name
		},
	},
	ArgFormat: "$%v",
	ErrNoRows: nil,
	Log: func(caller int, format string, args ...any) {
		log.Output(caller+3, fmt.Sprintf(format, args...))
	},
	ParamConv: func(on, fieldName, fieldFunc string, field reflect.StructField, value any) any {
		return value
	},
}

// NameConv is a function type for converting field names based on context.
type NameConv func(on, name string, field reflect.StructField) string

// ParamConv is a function type for converting field values based on context.
type ParamConv func(on, fieldName, fieldFunc string, field reflect.StructField, value any) any

// LogF is a function type for logging messages.
type LogF func(caller int, format string, args ...any)

// TableName is a type for representing table names.
type TableName string

// FilterValue is a type for representing filter values.
type FilterValue string

// Args returns a slice of arguments.
func Args(args ...any) []any {
	return args
}

// CRUD is the main structure for CRUD operations.
type CRUD struct {
	attrscan.Scanner
	ArgFormat   string
	ErrNoRows   error
	Verbose     bool
	Log         LogF
	TablePrefix string
	ParamConv   ParamConv
}

func (c *CRUD) getErrNoRows() (err error) {
	if c.ErrNoRows == nil {
		err = ErrNoRows
	} else {
		err = c.ErrNoRows
	}
	return
}

// Table returns the table name based on the provided value.
func Table(v any) (table string) {
	table = Default.Table(v)
	return
}

// Table returns the table name based on the provided value.
func (c *CRUD) Table(v any) (table string) {
	if v, ok := v.([]any); ok {
		for _, f := range v {
			if tableName, ok := f.(TableName); ok {
				table = c.TablePrefix + string(tableName)
				break
			} else if getter, ok := f.(TableNameGetter); ok {
				table = c.TablePrefix + getter.GetTableName(v)
				break
			}
		}
		return
	}
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	numField := reflectType.NumField()
	for i := 0; i < numField; i++ {
		fieldType := reflectType.Field(i)
		fieldValue := reflectValue.Field(i)
		if fieldType.Name == "T" {
			t := fieldType.Tag.Get("table")
			if len(t) < 1 {
				continue
			}
			if getter, ok := fieldValue.Interface().(TableNameGetter); ok {
				table = c.TablePrefix + getter.GetTableName(v, t, fieldType, fieldValue.Interface())
			} else {
				table = c.TablePrefix + t
			}
			break
		}
		if fieldType.Name == "_" {
			if t := fieldType.Tag.Get("table"); len(t) > 0 {
				table = c.TablePrefix + t
				break
			}
		}
	}
	return
}

// Sprintf formats the given integer value according to the specified format string.
func (c *CRUD) Sprintf(format string, v int) string {
	args := []any{}
	arg := fmt.Sprintf("%d", v)
	n := strings.Count(format, c.ArgFormat)
	for i := 0; i < n; i++ {
		args = append(args, arg)
	}
	return fmt.Sprintf(format, args...)
}

// FilterFieldCall processes the fields of a value based on the provided filter and calls the specified function for each field.
func FilterFieldCall(on string, v any, filter string, call func(fieldName, fieldFunc string, field reflect.StructField, value any)) (table string) {
	table = Default.FilterFieldCall(on, v, filter, call)
	return
}

// FilterFieldCall processes the fields of a value based on the provided filter and calls the specified function for each field.
func (c *CRUD) FilterFieldCall(on string, v any, filter string, call func(fieldName, fieldFunc string, field reflect.StructField, value any)) (table string) {
	filters := strings.Split(filter, "|")
	called := map[string]bool{}
	recordCall := func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		if !called[fieldName] {
			called[fieldName] = true
			call(fieldName, fieldFunc, field, value)
		}
	}
	table = c.filterFieldOnceCall(on, v, filters[0], recordCall)
	for _, filter := range filters[1:] {
		c.filterFieldOnceCall(on, v, filter, recordCall)
	}
	return
}

func (c *CRUD) filterFieldOnceCall(on string, v any, filter string, call func(fieldName, fieldFunc string, field reflect.StructField, value any)) (table string) {
	filter = strings.TrimSpace(filter)
	filter = strings.TrimPrefix(filter, "*") //* equal empty
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	if v, ok := v.([]any); ok {
		var tableAlias, fieldAlias string
		parts := strings.SplitN(filter, ".", 2)
		if len(parts) > 1 {
			tableAlias = parts[0]
			fieldAlias = parts[0] + "."
			filter = parts[1]
		}
		filterFields := strings.Split(strings.TrimSpace(strings.SplitN(filter, "#", 2)[0]), ",")
		offset := 0
		for _, f := range v {
			if tableName, ok := f.(TableName); ok {
				table = string(tableName)
				if len(tableAlias) > 0 {
					table = table + " " + tableAlias
				}
				continue
			}
			if offset >= len(filterFields) {
				panic(fmt.Sprintf("meta v[%v] is not found on filter", offset))
			}
			fieldParts := strings.SplitN(strings.Trim(filterFields[offset], ")"), "(", 2)
			fieldName := fieldParts[0]
			fieldFunc := ""
			if len(fieldParts) > 1 {
				fieldName = fieldParts[1]
				fieldFunc = fieldParts[0]
			}
			call(fieldAlias+fieldName, fieldFunc, reflect.StructField{}, f)
			offset++
		}
		return
	}
	if reflectType.Kind() != reflect.Struct {
		var fieldAlias string
		parts := strings.SplitN(filter, ".", 2)
		if len(parts) > 1 {
			fieldAlias = parts[0] + "."
			filter = parts[1]
		}
		fieldParts := strings.SplitN(strings.Trim(strings.TrimSpace(strings.SplitN(filter, "#", 2)[0]), ")"), "(", 2)
		fieldName := fieldParts[0]
		fieldFunc := ""
		if len(fieldParts) > 1 {
			fieldName = fieldParts[1]
			fieldFunc = fieldParts[0]
		}
		call(fieldAlias+fieldName, fieldFunc, reflect.StructField{}, v)
		return
	}
	table = c.Table(v)
	parts := strings.SplitN(filter, ".", 2)
	if len(parts) > 1 {
		table = table + " " + parts[0]
	}
	c.Scanner.FilterFieldCall(on, v, filter, call)
	return
}

// FilterFormatCall processes the format strings and calls the specified function for each format.
func FilterFormatCall(formats string, args []any, call func(format string, arg any)) {
	Default.FilterFormatCall(formats, args, call)
}

// FilterFormatCall processes the format strings and calls the specified function for each format.
func (c *CRUD) FilterFormatCall(formats string, args []any, call func(format string, arg any)) {
	formatParts := strings.SplitN(formats, "#", 2)
	var incNil, incZero bool
	if len(formatParts) > 1 && len(formatParts[1]) > 0 {
		incNil = strings.Contains(","+formatParts[1]+",", ",nil,") || strings.Contains(","+formatParts[1]+",", ",all,")
		incZero = strings.Contains(","+formatParts[1]+",", ",zero,") || strings.Contains(","+formatParts[1]+",", ",all,")
	}
	formatList := strings.Split(formatParts[0], ",")
	if len(formatList) != len(args) {
		panic(fmt.Sprintf("count formats=%v  is not equal to args=%v", len(formatList), len(args)))
	}
	for i, format := range formatList {
		arg := args[i]
		if !c.Scanner.CheckValue(reflect.ValueOf(arg), incNil, incZero) {
			continue
		}
		call(format, arg)
	}
}

// FilterWhere processes the fields of a value based on the provided filter and returns the WHERE clause and arguments.
func (c *CRUD) FilterWhere(args []any, v any, filter string) (where_ []string, args_ []any) {
	args_ = args
	c.FilterFieldCall("where", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, fieldValue any) {
		join := field.Tag.Get("join")
		if field.Type.Kind() == reflect.Struct && len(join) > 0 {
			var cmpInner []string
			cmpInner, args_ = c.FilterWhere(args_, fieldValue, field.Tag.Get("filter"))
			where_ = append(where_, "("+strings.Join(cmpInner, " "+join+" ")+")")
			return
		}
		cmp := field.Tag.Get("cmp")
		if cmp == "-" {
			return
		}
		if len(cmp) < 1 {
			cmp = fieldName + " = " + c.ArgFormat
		}
		if !strings.Contains(cmp, c.ArgFormat) {
			cmp += " " + c.ArgFormat
		}
		if (strings.Contains(cmp, " or ") || strings.Contains(cmp, " and ")) && !strings.HasPrefix(cmp, "(") {
			cmp = "(" + cmp + ")"
		}
		args_ = append(args_, c.ParamConv("where", fieldName, fieldFunc, field, fieldValue))
		where_ = append(where_, c.Sprintf(cmp, len(args_)))
	})
	return
}

// AppendInsert appends a field and its parameter to the insert statement.
func AppendInsert(fields, param []string, args []any, ok bool, format string, v any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = Default.AppendInsert(fields, param, args, ok, format, v)
	return
}

// AppendInsert appends a field and its parameter to the insert statement.
func (c *CRUD) AppendInsert(fields, param []string, args []any, ok bool, format string, v any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = fields, param, args
	if ok {
		args_ = append(args_, c.ParamConv("insert", format, "", reflect.StructField{}, v))
		parts := strings.SplitN(format, "=", 2)
		param_ = append(param_, c.Sprintf(parts[1], len(args_)))
		fields_ = append(fields_, parts[0])
	}
	return
}

// AppendInsertf appends multiple fields and their parameters to the insert statement.
func AppendInsertf(fields, param []string, args []any, formats string, v ...any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = Default.AppendInsertf(fields, param, args, formats, v...)
	return
}

// AppendInsertf appends multiple fields and their parameters to the insert statement.
func (c *CRUD) AppendInsertf(fields, param []string, args []any, formats string, v ...any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = fields, param, args
	c.FilterFormatCall(formats, v, func(format string, arg any) {
		args_ = append(args_, c.ParamConv("insert", format, "", reflect.StructField{}, arg))
		parts := strings.SplitN(format, "=", 2)
		param_ = append(param_, c.Sprintf(parts[1], len(args_)))
		fields_ = append(fields_, parts[0])
	})
	return
}

// AppendSet appends a field and its parameter to the SET clause.
func AppendSet(sets []string, args []any, ok bool, format string, v any) (sets_ []string, args_ []any) {
	sets_, args_ = Default.AppendSet(sets, args, ok, format, v)
	return
}

// AppendSet appends a field and its parameter to the SET clause.
func (c *CRUD) AppendSet(sets []string, args []any, ok bool, format string, v any) (sets_ []string, args_ []any) {
	sets_, args_ = sets, args
	if ok {
		args_ = append(args_, c.ParamConv("update", format, "", reflect.StructField{}, v))
		sets_ = append(sets_, c.Sprintf(format, len(args_)))
	}
	return
}

// AppendSetf appends multiple fields and their parameters to the SET clause.
func AppendSetf(sets []string, args []any, formats string, v ...any) (sets_ []string, args_ []any) {
	sets_, args_ = Default.AppendSetf(sets, args, formats, v...)
	return
}

// AppendSetf appends multiple fields and their parameters to the SET clause.
func (c *CRUD) AppendSetf(sets []string, args []any, formats string, v ...any) (sets_ []string, args_ []any) {
	sets_, args_ = sets, args
	c.FilterFormatCall(formats, v, func(format string, arg any) {
		args_ = append(args_, c.ParamConv("update", format, "", reflect.StructField{}, arg))
		sets_ = append(sets_, c.Sprintf(format, len(args_)))
	})
	return
}

// AppendWhere appends a field and its parameter to the WHERE clause.
func AppendWhere(where []string, args []any, ok bool, format string, v any) (where_ []string, args_ []any) {
	where_, args_ = Default.AppendWhere(where, args, ok, format, v)
	return
}

// AppendWhere appends a field and its parameter to the WHERE clause.
func (c *CRUD) AppendWhere(where []string, args []any, ok bool, format string, v any) (where_ []string, args_ []any) {
	where_, args_ = where, args
	if ok {
		args_ = append(args_, c.ParamConv("where", format, "", reflect.StructField{}, v))
		where_ = append(where_, c.Sprintf(format, len(args_)))
	}
	return
}

// AppendWheref appends multiple fields and their parameters to the WHERE clause.
func AppendWheref(where []string, args []any, format string, v ...any) (where_ []string, args_ []any) {
	where_, args_ = Default.AppendWheref(where, args, format, v...)
	return
}

// AppendWheref appends multiple fields and their parameters to the WHERE clause.
func (c *CRUD) AppendWheref(where []string, args []any, formats string, v ...any) (where_ []string, args_ []any) {
	where_, args_ = where, args
	c.FilterFormatCall(formats, v, func(format string, arg any) {
		args_ = append(args_, c.ParamConv("where", format, "", reflect.StructField{}, arg))
		where_ = append(where_, c.Sprintf(format, len(args_)))
	})
	return
}

// AppendWhereUnify appends a unified WHERE clause based on the provided value and enabled fields.
func AppendWhereUnify(where []string, args []any, v any, enabled ...string) (where_ []string, args_ []any) {
	where_, args_ = Default.AppendWhereUnify(where, args, v, enabled...)
	return
}

// AppendWhereUnify appends a unified WHERE clause based on the provided value and enabled fields.
func (c *CRUD) AppendWhereUnify(where []string, args []any, v any, enabled ...string) (where_ []string, args_ []any) {
	where_, args_ = where, args
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	if len(enabled) < 1 {
		enabled = append(enabled, "Where")
	}
	for _, key := range enabled {
		modelValue := reflectValue.FieldByName(key)
		if !modelValue.IsValid() {
			continue
		}
		modelType, _ := reflectValue.Type().FieldByName(key)
		filterWhere, filterArgs := c.FilterWhere(args, modelValue.Addr().Interface(), modelType.Tag.Get("filter"))
		where_ = append(where_, filterWhere...)
		args_ = append(args_, filterArgs...)
	}
	return
}

// JoinWhere constructs a WHERE clause based on the provided SQL and conditions.
func JoinWhere(sql string, where []string, sep string, suffix ...string) (sql_ string) {
	sql_ = Default.joinWhere(1, sql, where, sep, suffix...)
	return
}

// JoinWhere constructs a WHERE clause based on the provided SQL and conditions.
func (c *CRUD) JoinWhere(sql string, where []string, sep string, suffix ...string) (sql_ string) {
	sql_ = c.joinWhere(1, sql, where, sep, suffix...)
	return
}

func (c *CRUD) joinWhere(caller int, sql string, where []string, sep string, suffix ...string) (sql_ string) {
	sql_ = sql
	if len(where) > 0 {
		sql_ += " where " + strings.Join(where, " "+sep+" ")
	}
	if len(suffix) > 0 {
		sql_ += " " + strings.Join(suffix, " ")
	}
	if c.Verbose {
		c.Log(caller, "CRUD join where done with sql:%v", sql_)
	}
	return
}

// JoinWheref constructs a WHERE clause based on the provided SQL and formatted conditions.
func JoinWheref(sql string, args []any, formats string, formatArgs ...any) (sql_ string, args_ []any) {
	sql_, args_ = Default.joinWheref(1, sql, args, formats, formatArgs...)
	return
}

// JoinWheref constructs a WHERE clause based on the provided SQL and formatted conditions.
func (c *CRUD) JoinWheref(sql string, args []any, formats string, formatArgs ...any) (sql_ string, args_ []any) {
	sql_, args_ = c.joinWheref(1, sql, args, formats, formatArgs...)
	return
}

func (c *CRUD) joinWheref(caller int, sql string, args []any, formats string, formatArgs ...any) (sql_ string, args_ []any) {
	sql_, args_ = sql, args
	if len(formats) < 1 {
		return
	}
	var where []string
	sep := "and"
	formatParts := strings.SplitN(formats, "#", 2)
	if len(formatParts) > 1 {
		optionParts := strings.Split(formatParts[1], ",")
		for _, part := range optionParts {
			if strings.HasPrefix(part, "+") {
				sep = strings.TrimPrefix(part, "+")
				break
			}
		}
	}
	where, args_ = c.AppendWheref(nil, args_, formats, formatArgs...)
	sql_ = c.joinWhere(caller+1, sql, where, sep)
	return
}

// JoinWhereUnify constructs a unified WHERE clause based on the provided SQL and value.
func JoinWhereUnify(sql string, args []any, v any, enabled ...string) (sql_ string, args_ []any) {
	sql_, args_ = Default.joinWhereUnify(1, sql, args, v, enabled...)
	return
}

// JoinWhereUnify constructs a unified WHERE clause based on the provided SQL and value.
func (c *CRUD) JoinWhereUnify(sql string, args []any, v any, enabled ...string) (sql_ string, args_ []any) {
	sql_, args_ = c.joinWhereUnify(1, sql, args, v, enabled...)
	return
}

func (c *CRUD) joinWhereUnify(caller int, sql string, args []any, v any, enabled ...string) (sql_ string, args_ []any) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	if len(enabled) < 1 {
		enabled = append(enabled, "Where")
	}
	whereJoin := ""
	for _, key := range enabled {
		whereType, _ := reflectType.FieldByName(key)
		whereJoin += " " + whereType.Tag.Get("join")
	}
	args_ = args
	where, args_ := c.AppendWhereUnify(nil, args_, v)
	sql_ = c.joinWhere(caller+1, sql, where, whereJoin)
	return
}

// JoinPage constructs a paginated SQL query based on the provided SQL, orderby, offset, and limit.
func JoinPage(sql, orderby string, offset, limit int) (sql_ string) {
	sql_ = Default.joinPage(1, sql, orderby, offset, limit)
	return
}

// JoinPage constructs a paginated SQL query based on the provided SQL, orderby, offset, and limit.
func (c *CRUD) JoinPage(sql, orderby string, offset, limit int) string {
	return c.joinPage(1, sql, orderby, offset, limit)
}

func (c *CRUD) joinPage(caller int, sql, orderby string, offset, limit int) (pagedSQL string) {
	pagedSQL = sql
	if len(orderby) > 0 && (offset >= 0 || limit > 0) {
		pagedSQL += " " + orderby
	}
	if limit > 0 {
		pagedSQL += fmt.Sprintf(" limit %v offset %v", limit, offset)
	}
	if c.Verbose {
		c.Log(caller, "CRUD join page done with sql:%v", pagedSQL)
	}
	return
}

// JoinPageUnify constructs a unified paginated SQL query based on the provided SQL and value.
func JoinPageUnify(sql string, v any) string {
	return Default.joinPageUnify(1, sql, v)
}

// JoinPageUnify constructs a unified paginated SQL query based on the provided SQL and value.
func (c *CRUD) JoinPageUnify(sql string, v any) string {
	return c.joinPageUnify(1, sql, v)
}

func (c *CRUD) joinPageUnify(caller int, sql string, v any) (joinedSQL string) {
	joinedSQL = sql
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	pageValue := reflectValue.FieldByName("Page")
	pageType, _ := reflectType.FieldByName("Page")
	if !pageValue.IsValid() {
		return
	}
	order := ""
	orderType, _ := pageType.Type.FieldByName("Order")
	orderValue := pageValue.FieldByName("Order")
	if orderValue.IsValid() {
		order = orderValue.String()
		if len(order) < 1 {
			order = orderType.Tag.Get("default")
		}
	}
	offset := 0
	if offsetValue := pageValue.FieldByName("Offset"); offsetValue.IsValid() {
		offset = int(offsetValue.Int())
	} else if skipValue := pageValue.FieldByName("Skip"); skipValue.IsValid() {
		offset = int(skipValue.Int())
	}
	limit := 0
	limitValue := pageValue.FieldByName("Limit")
	if limitValue.IsValid() {
		limit = int(limitValue.Int())
	}
	joinedSQL = c.joinPage(caller+1, joinedSQL, order, offset, limit)
	return
}

func (c *CRUD) queryerExec(ctx context.Context, queryer any, sql string, args []any) (insertID, affected int64, err error) {
	reflectValue := reflect.ValueOf(queryer)
	if reflectValue.Kind() == reflect.Func {
		queryer = reflectValue.Call(nil)[0].Interface()
	}
	if q, ok := queryer.(Queryer); ok {
		insertID, affected, err = q.Exec(ctx, sql, args...)
	} else if q, ok := queryer.(CrudQueryer); ok {
		insertID, affected, err = q.CrudExec(ctx, sql, args...)
	} else {
		panic("queryer is not supported")
	}
	return
}

func (c *CRUD) queryerQuery(ctx context.Context, queryer any, sql string, args []any) (rows Rows, err error) {
	reflectValue := reflect.ValueOf(queryer)
	if reflectValue.Kind() == reflect.Func {
		queryer = reflectValue.Call(nil)[0].Interface()
	}
	if q, ok := queryer.(Queryer); ok {
		rows, err = q.Query(ctx, sql, args...)
	} else if q, ok := queryer.(CrudQueryer); ok {
		rows, err = q.CrudQuery(ctx, sql, args...)
	} else {
		panic(fmt.Sprintf("queryer %v is not supported", reflect.TypeOf(queryer)))
	}
	return
}

func (c *CRUD) queryerQueryRow(ctx context.Context, queryer any, sql string, args []any) (row Row) {
	reflectValue := reflect.ValueOf(queryer)
	if reflectValue.Kind() == reflect.Func {
		queryer = reflectValue.Call(nil)[0].Interface()
	}
	if q, ok := queryer.(Queryer); ok {
		row = q.QueryRow(ctx, sql, args...)
	} else if q, ok := queryer.(CrudQueryer); ok {
		row = q.CrudQueryRow(ctx, sql, args...)
	} else {
		panic(fmt.Sprintf("queryer %v is not supported", reflect.TypeOf(queryer)))
	}
	return
}

// InsertArgs generates the insert arguments based on the provided value and filter.
func InsertArgs(v any, filter string, args []any) (table string, fields, param []string, args_ []any) {
	table, fields, param, args_ = Default.insertArgs(1, v, filter, args)
	return
}

// InsertArgs generates the insert arguments based on the provided value and filter.
func (c *CRUD) InsertArgs(v any, filter string, args []any) (table string, fields, param []string, args_ []any) {
	table, fields, param, args_ = c.insertArgs(1, v, filter, args)
	return
}

func (c *CRUD) insertArgs(caller int, v any, filter string, args []any) (table string, fields, param []string, args_ []any) {
	args_ = args
	table = c.FilterFieldCall("insert", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		args_ = append(args_, c.ParamConv("insert", fieldName, fieldFunc, field, value))
		fields = append(fields, fieldName)
		param = append(param, fmt.Sprintf(c.ArgFormat, len(args_)))
	})
	if c.Verbose {
		c.Log(caller, "CRUD generate insert args by struct:%v,filter:%v, result is fields:%v,param:%v,args:%v", reflect.TypeOf(v), filter, fields, param, jsonString(args))
	}
	return
}

// InsertSQL generates the SQL statement for inserting a record based on the provided value and filter.
func InsertSQL(v any, filter string, suffix ...string) (sql string, args []any) {
	sql, args = Default.insertSQL(1, v, filter, suffix...)
	return
}

// InsertSQL generates the SQL statement for inserting a record based on the provided value and filter.
func (c *CRUD) InsertSQL(v any, filter string, suffix ...string) (sql string, args []any) {
	sql, args = c.insertSQL(1, v, filter, suffix...)
	return
}

func (c *CRUD) insertSQL(caller int, v any, filter string, suffix ...string) (sql string, args []any) {
	table, fields, param, args := c.insertArgs(caller+1, v, filter, nil)
	sql = fmt.Sprintf(`insert into %v(%v) values(%v) %v`, table, strings.Join(fields, ","), strings.Join(param, ","), strings.Join(suffix, " "))
	if c.Verbose {
		c.Log(caller, "CRUD generate insert sql by struct:%v,filter:%v, result is sql:%v", reflect.TypeOf(v), filter, sql)
	}
	return
}

// InsertFilter inserts a record into the database with filtering options and returns the inserted ID.
func InsertFilter(ctx context.Context, queryer any, v any, filter, join, scan string) (insertID int64, err error) {
	insertID, err = Default.insertFilter(ctx, 1, queryer, v, filter, join, scan)
	return
}

// InsertFilter inserts a record into the database with filtering options and returns the inserted ID.
func (c *CRUD) InsertFilter(ctx context.Context, queryer any, v any, filter, join, scan string) (insertID int64, err error) {
	insertID, err = c.insertFilter(ctx, 1, queryer, v, filter, join, scan)
	return
}

func (c *CRUD) insertFilter(ctx context.Context, caller int, queryer any, v any, filter, join, scan string) (insertID int64, err error) {
	table, fields, param, args := c.insertArgs(caller+1, v, filter, nil)
	sql := fmt.Sprintf(`insert into %v(%v) values(%v)`, table, strings.Join(fields, ","), strings.Join(param, ","))
	if len(scan) < 1 {
		if len(join) > 0 {
			sql += " " + join
		}
		insertID, _, err = c.queryerExec(ctx, queryer, sql, args)
		if err != nil {
			if c.Verbose {
				c.Log(caller, "CRUD insert filter by struct:%v,sql:%v, result is fail:%v", reflect.TypeOf(v), sql, err)
			}
		} else {
			if c.Verbose {
				c.Log(caller, "CRUD insert filter by struct:%v,sql:%v, result is success", reflect.TypeOf(v), sql)
			}
		}
		return
	}
	_, scanFields := c.queryField(caller+1, v, scan)
	scanArgs := c.ScanArgs(v, scan)
	if len(join) > 0 {
		sql += " " + join
	}
	sql += " " + strings.Join(scanFields, ",")
	err = c.queryerQueryRow(ctx, queryer, sql, args).Scan(scanArgs...)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD insert filter by struct:%v,sql:%v, result is fail:%v", reflect.TypeOf(v), sql, err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD insert filter by struct:%v,sql:%v, result is success", reflect.TypeOf(v), sql)
	}
	return
}

// UpdateArgs generates the arguments for updating a record based on the provided value and filter.
func UpdateArgs(v any, filter string, args []any) (table string, sets []string, args_ []any) {
	table, sets, args_ = Default.updateArgs(1, v, filter, args)
	return
}

// UpdateArgs generates the arguments for updating a record based on the provided value and filter.
func (c *CRUD) UpdateArgs(v any, filter string, args []any) (table string, sets []string, args_ []any) {
	table, sets, args_ = c.updateArgs(1, v, filter, args)
	return
}

func (c *CRUD) updateArgs(caller int, v any, filter string, args []any) (table string, sets []string, args_ []any) {
	args_ = args
	table = c.FilterFieldCall("update", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		args_ = append(args_, c.ParamConv("update", fieldName, fieldFunc, field, value))
		sets = append(sets, fmt.Sprintf("%v="+c.ArgFormat, fieldName, len(args_)))
	})
	if c.Verbose {
		c.Log(caller, "CRUD generate update args by struct:%v,filter:%v, result is sets:%v,args:%v", reflect.TypeOf(v), filter, sets, jsonString(args_))
	}
	return
}

// UpdateSQL generates the SQL statement for updating a record based on the provided value and filter.
func UpdateSQL(v any, filter string, args []any, suffix ...string) (sql string, args_ []any) {
	sql, args_ = Default.updateSQL(1, v, filter, args, suffix...)
	return
}

// UpdateSQL generates the SQL statement for updating a record based on the provided value and filter.
func (c *CRUD) UpdateSQL(v any, filter string, args []any, suffix ...string) (sql string, args_ []any) {
	sql, args_ = c.updateSQL(1, v, filter, args, suffix...)
	return
}

func (c *CRUD) updateSQL(caller int, v any, filter string, args []any, suffix ...string) (sql string, args_ []any) {
	table, sets, args_ := c.updateArgs(caller+1, v, filter, args)
	sql = fmt.Sprintf(`update %v set %v %v`, table, strings.Join(sets, ","), strings.Join(suffix, " "))
	if c.Verbose {
		c.Log(caller, "CRUD generate update sql by struct:%v,filter:%v, result is sql:%v,args:%v", reflect.TypeOf(v), filter, sql, jsonString(args_))
	}
	return
}

// Update updates a record in the database using the provided SQL and conditions, returning the number of affected rows.
func Update(ctx context.Context, queryer any, v any, sql string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = Default.update(ctx, 1, queryer, v, sql, where, sep, args)
	return
}

// Update updates a record in the database using the provided SQL and conditions, returning the number of affected rows.
func (c *CRUD) Update(ctx context.Context, queryer any, v any, sql string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = c.update(ctx, 1, queryer, v, sql, where, sep, args)
	return
}

func (c *CRUD) update(ctx context.Context, caller int, queryer any, v any, sql string, where []string, sep string, args []any) (affected int64, err error) {
	sql = c.joinWhere(caller+1, sql, where, sep)
	_, affected, err = c.queryerExec(ctx, queryer, sql, args)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD update by struct:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD update by struct:%v,sql:%v,args:%v, result is success affected:%v", reflect.TypeOf(v), sql, jsonString(args), affected)
	}
	return
}

// UpdateRow updates a single record in the database using the provided SQL and conditions, expecting exactly one row to be affected.
func UpdateRow(ctx context.Context, queryer any, v any, sql string, where []string, sep string, args []any) (err error) {
	err = Default.updateRow(ctx, 1, queryer, v, sql, where, sep, args)
	return
}

// UpdateRow updates a single record in the database using the provided SQL and conditions, expecting exactly one row to be affected.
func (c *CRUD) UpdateRow(ctx context.Context, queryer any, v any, sql string, where []string, sep string, args []any) (err error) {
	err = c.updateRow(ctx, 1, queryer, v, sql, where, sep, args)
	return
}

func (c *CRUD) updateRow(ctx context.Context, caller int, queryer any, v any, sql string, where []string, sep string, args []any) (err error) {
	affected, err := c.update(ctx, caller+1, queryer, v, sql, where, sep, args)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

// UpdateSet updates a record in the database using a set of fields and conditions, returning the number of affected rows.
func UpdateSet(ctx context.Context, queryer any, v any, sets, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = Default.updateSet(ctx, 1, queryer, v, sets, where, sep, args)
	return
}

// UpdateSet updates a record in the database using a set of fields and conditions.
func (c *CRUD) UpdateSet(ctx context.Context, queryer any, v any, sets, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = c.updateSet(ctx, 1, queryer, v, sets, where, sep, args)
	return
}

func (c *CRUD) updateSet(ctx context.Context, caller int, queryer any, v any, sets, where []string, sep string, args []any) (affected int64, err error) {
	table := c.Table(v)
	sql := fmt.Sprintf(`update %v set %v`, table, strings.Join(sets, ","))
	sql = c.joinWhere(caller+1, sql, where, sep)
	_, affected, err = c.queryerExec(ctx, queryer, sql, args)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD update by struct:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD update by struct:%v,sql:%v,args:%v, result is success affected:%v", reflect.TypeOf(v), sql, jsonString(args), affected)
	}
	return
}

// UpdateRowSet updates a record in the database using a set of fields and conditions, expecting exactly one row to be affected.
func UpdateRowSet(ctx context.Context, queryer any, v any, sets, where []string, sep string, args []any) (err error) {
	err = Default.updateRowSet(ctx, 1, queryer, v, sets, where, sep, args)
	return
}

// UpdateRowSet updates a record in the database using a set of fields and conditions, expecting exactly one row to be affected.
func (c *CRUD) UpdateRowSet(ctx context.Context, queryer any, v any, sets, where []string, sep string, args []any) (err error) {
	err = c.updateRowSet(ctx, 1, queryer, v, sets, where, sep, args)
	return
}

func (c *CRUD) updateRowSet(ctx context.Context, caller int, queryer any, v any, sets, where []string, sep string, args []any) (err error) {
	affected, err := c.updateSet(ctx, caller+1, queryer, v, sets, where, sep, args)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

// UpdateFilter updates records in the database based on a filter and conditions, returning the number of affected rows.
func UpdateFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = Default.updateFilter(ctx, 1, queryer, v, filter, where, sep, args)
	return
}

func (c *CRUD) UpdateFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = c.updateFilter(ctx, 1, queryer, v, filter, where, sep, args)
	return
}

func (c *CRUD) updateFilter(ctx context.Context, caller int, queryer any, v any, filter string, where []string, sep string, args []any) (affected int64, err error) {
	sql, args := c.updateSQL(caller+1, v, filter, args)
	sql = c.joinWhere(caller+1, sql, where, sep)
	_, affected, err = c.queryerExec(ctx, queryer, sql, args)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD update filter by struct:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD update filter by struct:%v,sql:%v,args:%v, result is success affected:%v", reflect.TypeOf(v), sql, jsonString(args), affected)
	}
	return
}

// UpdateRowFilter updates a record in the database based on a filter and conditions, expecting exactly one row to be affected.
func UpdateRowFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any) (err error) {
	err = Default.updateRowFilter(ctx, 1, queryer, v, filter, where, sep, args)
	return
}

// UpdateRowFilter updates a record in the database based on a filter and conditions, expecting exactly one row to be affected.
func (c *CRUD) UpdateRowFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any) (err error) {
	err = c.updateRowFilter(ctx, 1, queryer, v, filter, where, sep, args)
	return
}

func (c *CRUD) updateRowFilter(ctx context.Context, caller int, queryer any, v any, filter string, where []string, sep string, args []any) (err error) {
	affected, err := c.updateFilter(ctx, caller+1, queryer, v, filter, where, sep, args)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

// UpdateWheref updates records in the database based on a filter and formatted conditions, returning the number of affected rows.
func UpdateWheref(ctx context.Context, queryer any, v any, filter, formats string, args ...any) (affected int64, err error) {
	affected, err = Default.updateWheref(ctx, 1, queryer, v, filter, formats, args...)
	return
}

func (c *CRUD) UpdateWheref(ctx context.Context, queryer any, v any, filter, formats string, args ...any) (affected int64, err error) {
	affected, err = c.updateWheref(ctx, 1, queryer, v, filter, formats, args...)
	return
}

func (c *CRUD) updateWheref(ctx context.Context, caller int, queryer any, v any, filter, formats string, args ...any) (affected int64, err error) {
	sql, sqlArgs := c.updateSQL(caller+1, v, filter, nil)
	sql, sqlArgs = c.joinWheref(caller+1, sql, sqlArgs, formats, args...)
	_, affected, err = c.queryerExec(ctx, queryer, sql, sqlArgs)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD update wheref by struct:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), sql, jsonString(sqlArgs), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD update wheref by struct:%v,sql:%v,args:%v, result is success affected:%v", reflect.TypeOf(v), sql, jsonString(sqlArgs), affected)
	}
	return
}

// UpdateRowWheref updates a record in the database based on a filter and formatted conditions, expecting exactly one row to be affected.
func UpdateRowWheref(ctx context.Context, queryer any, v any, filter, formats string, args ...any) (err error) {
	err = Default.updateRowWheref(ctx, 1, queryer, v, filter, formats, args...)
	return
}

// UpdateRowWheref updates a record in the database based on a filter and formatted conditions, expecting exactly one row to be affected.
func (c *CRUD) UpdateRowWheref(ctx context.Context, queryer any, v any, filter, formats string, args ...any) (err error) {
	err = c.updateRowWheref(ctx, 1, queryer, v, filter, formats, args...)
	return
}

func (c *CRUD) updateRowWheref(ctx context.Context, caller int, queryer any, v any, filter, formats string, args ...any) (err error) {
	affected, err := c.updateWheref(ctx, caller+1, queryer, v, filter, formats, args...)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

// QueryField generates the table name and fields for a query based on the provided value and filter.
func QueryField(v any, filter string) (table string, fields []string) {
	table, fields = Default.queryField(1, v, filter)
	return
}

// QueryField generates the table name and fields for a query based on the provided value and filter.
func (c *CRUD) QueryField(v any, filter string) (table string, fields []string) {
	table, fields = c.queryField(1, v, filter)
	return
}

func (c *CRUD) queryField(caller int, v any, filter string) (table string, fields []string) {
	table = c.FilterFieldCall("query", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		conv := field.Tag.Get("conv")
		if len(fieldFunc) > 0 {
			fields = append(fields, fmt.Sprintf("%v(%v%v)", fieldFunc, fieldName, conv))
		} else {
			fields = append(fields, fmt.Sprintf("%v%v", fieldName, conv))
		}
	})
	if c.Verbose {
		c.Log(caller, "CRUD generate query field by struct:%v,filter:%v, result is fields:%v", reflect.TypeOf(v), filter, fields)
	}
	return
}

// QuerySQL generates the SQL statement for querying a record based on the provided value and filter.
func QuerySQL(v any, filter string, suffix ...string) (sql string) {
	sql = Default.querySQL(1, v, "", filter, suffix...)
	return
}

// QuerySQL generates the SQL statement for querying a record based on the provided value and filter.
func (c *CRUD) QuerySQL(v any, filter string, suffix ...string) (sql string) {
	sql = c.querySQL(1, v, "", filter, suffix...)
	return
}

func (c *CRUD) querySQL(caller int, v any, from, filter string, suffix ...string) (sql string) {
	table, fields := c.queryField(caller+1, v, filter)
	if len(from) > 0 {
		table = from
	}
	sql = fmt.Sprintf(`select %v from %v`, strings.Join(fields, ","), table)
	if len(suffix) > 0 {
		sql += " " + strings.Join(suffix, " ")
	}
	if c.Verbose {
		c.Log(caller, "CRUD generate query sql by struct:%v,filter:%v, result is sql:%v", reflect.TypeOf(v), filter, sql)
	}
	return
}

// QueryUnifySQL generates a unified SQL query based on the provided value and field.
func QueryUnifySQL(v any, field string) (sql string, args []any) {
	sql, args = Default.queryUnifySQL(1, v, field)
	return
}

// QueryUnifySQL generates a unified SQL query based on the provided value and field.
func (c *CRUD) QueryUnifySQL(v any, field string) (sql string, args []any) {
	sql, args = c.queryUnifySQL(1, v, field)
	return
}

func (c *CRUD) queryUnifySQL(caller int, v any, field string) (sql string, args []any) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	modelValue := reflectValue.FieldByName("Model")
	modelType, _ := reflectType.FieldByName("Model")
	modelFrom := modelType.Tag.Get("from")
	queryType, _ := reflectType.FieldByName(field)
	queryValue := reflectValue.FieldByName(field)
	if queryFrom := queryType.Tag.Get("from"); len(queryFrom) > 0 {
		modelFrom = queryFrom
	}
	queryFilter := queryType.Tag.Get("filter")
	querySelect := queryType.Tag.Get("select")
	queryGroup := queryType.Tag.Get("group")
	queryNum := queryType.Type.NumField()
	for i := 0; i < queryNum; i++ {
		fieldValue := queryValue.Field(i)
		fieldType := queryType.Type.Field(i)
		if value, ok := fieldValue.Interface().(FilterValue); ok && fieldType.Name == "Filter" && len(value) > 0 {
			queryFilter = string(value)
			continue
		}
		if getter, ok := fieldValue.Interface().(FilterGetter); ok && getter != nil {
			queryFilter = getter.GetFilter(v, fieldValue, fieldValue)
			continue
		}
	}
	if len(querySelect) > 0 {
		sql = querySelect
		if strings.Contains(querySelect, "%v") {
			_, fields := c.queryField(caller+1, modelValue.Addr().Interface(), queryFilter)
			sql = fmt.Sprintf(querySelect, strings.Join(fields, ","))
		}
	} else {
		sql = c.querySQL(caller+1, modelValue.Addr().Interface(), modelFrom, queryFilter)
	}
	sql, args = c.joinWhereUnify(caller+1, sql, nil, v)
	sql += " " + queryGroup
	sql = c.joinPageUnify(caller+1, sql, v)
	return
}

// ScanArgs generates the arguments for scanning a record based on the provided value and filter.
func ScanArgs(v any, filter string) (args []any) {
	args = Default.ScanArgs(v, filter)
	return
}

// ScanArgs generates the arguments for scanning a record based on the provided value and filter.
func (c *CRUD) ScanArgs(v any, filter string) (args []any) {
	c.FilterFieldCall("scan", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		args = append(args, c.ParamConv("scan", fieldName, fieldFunc, field, value))
	})
	return
}

// ScanUnifyDest scans a unified destination based on the provided value and query name.
func ScanUnifyDest(v any, queryName string) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = Default.ScanUnifyDest(v, queryName)
	return
}

// ScanUnifyDest scans a unified destination based on the provided value and query name.
func (c *CRUD) ScanUnifyDest(v any, queryName string) (modelValue any, queryFilter string, dests []any) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	modelValue = reflectValue.FieldByName("Model").Addr().Interface()
	queryType, _ := reflectType.FieldByName(queryName)
	queryValue := reflectValue.FieldByName(queryName)
	if !queryValue.IsValid() {
		panic(fmt.Sprintf("%v is not exits in %v", queryName, reflectType))
	}
	queryFilter = queryType.Tag.Get("filter")
	queryNum := queryType.Type.NumField()
	for i := 0; i < queryNum; i++ {
		fieldValue := queryValue.Field(i)
		fieldType := queryType.Type.Field(i)
		if value, ok := fieldValue.Interface().(FilterValue); ok && fieldType.Name == "Filter" {
			if len(value) > 0 {
				queryFilter = string(value)
			}
			continue
		}
		if getter, ok := fieldValue.Interface().(FilterGetter); ok {
			if getter != nil {
				queryFilter = getter.GetFilter(v, fieldValue, fieldValue)
			}
			continue
		}
		scan := fieldType.Tag.Get("scan")
		if scan == "-" {
			continue
		}
		dests = append(dests, queryValue.Field(i).Addr().Interface())
		if len(scan) > 0 {
			dests = append(dests, scan)
		}
	}
	return
}

func (c *CRUD) destSet(value reflect.Value, filter string, dests ...any) (err error) {
	if len(dests) < 1 {
		err = fmt.Errorf("scan dest is empty")
		return
	}
	valueField := func(key string) (v reflect.Value, e error) {
		if _, ok := value.Interface().([]any); ok {
			parts := strings.SplitN(filter, ".", 2)
			if len(parts) > 1 {
				filter = parts[1]
			}
			filterFields := strings.Split(strings.TrimSpace(strings.SplitN(filter, "#", 2)[0]), ",")
			indexField := -1
			for i, filterField := range filterFields {
				fieldParts := strings.SplitN(strings.Trim(filterField, ")"), "(", 2)
				fieldName := fieldParts[0]
				if len(fieldParts) > 1 {
					fieldName = fieldParts[1]
				}
				if fieldName == key {
					indexField = i
					break
				}
			}
			if indexField < 0 {
				e = fmt.Errorf("field %v is not exists", key)
				return
			}
			v = reflect.Indirect(value.Index(indexField).Elem())
			return
		}
		targetValue := reflect.Indirect(value)
		targetType := targetValue.Type()
		if targetType.Kind() != reflect.Struct {
			e = fmt.Errorf("field %v is not struct", key)
			return
		}
		k := targetValue.NumField()
		for i := 0; i < k; i++ {
			field := targetType.Field(i)
			fieldName := strings.SplitN(field.Tag.Get(c.Tag), ",", 2)[0]
			if fieldName == key {
				v = targetValue.Field(i)
				return
			}
		}
		e = fmt.Errorf("field %v is not exists", key)
		return
	}
	scanMap := func(i int, mapType reflect.Type, scan string, skipNil, skipZero bool) (v reflect.Value, e error) {
		v = reflect.MakeMap(mapType)
		for _, field := range strings.Split(scan, ",") {
			parts := strings.SplitN(field, ":", 2)
			key := reflect.ValueOf(parts[0])
			var val reflect.Value
			if len(parts) < 2 {
				val, e = valueField(parts[0])
			} else {
				val, e = valueField(parts[1])
			}
			if e != nil {
				break
			}
			if !key.CanConvert(mapType.Key()) {
				e = fmt.Errorf("not supported on dests[%v].%v key to set %v=>%v", i-1, field, key.Type(), mapType.Key())
				break
			}
			if !val.CanConvert(mapType.Elem()) {
				e = fmt.Errorf("not supported on dests[%v].%v value to set %v=>%v", i-1, field, val.Type(), mapType.Elem())
				break
			}
			v.SetMapIndex(key.Convert(mapType.Key()), val.Convert(mapType.Elem()))
		}
		return
	}
	n := len(dests)
	for i := 0; i < n; i++ {
		if scanner, ok := dests[i].(Scanner); ok {
			scanner.Scan(value.Interface())
			continue
		}
		destValue := reflect.Indirect(reflect.ValueOf(dests[i]))
		destKind := destValue.Kind()
		destType := destValue.Type()
		if destType == value.Type() {
			destValue.Set(value)
			continue
		}
		// if value.Kind() == reflect.Ptr && !value.IsZero() {
		// 	indirectValue := reflect.Indirect(value)
		// 	if destType == indirectValue.Type() {
		// 		destValue.Set(indirectValue)
		// 		continue
		// 	}
		// }
		switch destKind {
		case reflect.Func:
			destValue.Call([]reflect.Value{value})
		case reflect.Map:
			if i+1 >= len(dests) {
				err = fmt.Errorf("dest[%v] pattern is not setted", i)
				break
			}
			v, ok := dests[i+1].(string)
			if !ok {
				err = fmt.Errorf("dest[%v] pattern is not string", i)
				break
			}
			if len(v) < 1 {
				err = fmt.Errorf("dest[%v] pattern is empty", i)
				break
			}
			i++
			parts := strings.Split(v, "#")
			kvs := strings.SplitN(parts[0], ":", 2)
			targetKey, xerr := valueField(kvs[0])
			if xerr != nil {
				err = xerr
				break
			}
			targetValue := value
			if len(kvs) > 1 {
				targetValue, xerr = valueField(kvs[1])
				if xerr != nil {
					err = xerr
					break
				}
			}
			if destValue.IsNil() {
				destValue.Set(reflect.MakeMap(destType))
			}
			destElemValue := destValue.MapIndex(targetKey)
			destElemType := destType.Elem()
			if destElemType.Kind() == reflect.Slice {
				if !destElemValue.IsValid() {
					destElemValue = reflect.Indirect(reflect.New(destElemType))
				}
				destValue.SetMapIndex(targetKey, reflect.Append(destElemValue, targetValue))
			} else {
				destValue.SetMapIndex(targetKey, targetValue)
			}
		default:
			if destKind == reflect.Slice && destType.Elem() == value.Type() {
				destValue.Set(reflect.Append(destValue, value))
				continue
			}
			if i+1 >= len(dests) {
				err = fmt.Errorf("dest[%v] pattern is not setted", i)
				break
			}
			v, ok := dests[i+1].(string)
			if !ok {
				err = fmt.Errorf("dest[%v] pattern is not string", i)
				break
			}
			if len(v) < 1 {
				err = fmt.Errorf("dest[%v] pattern is empty", i)
				break
			}
			parts := strings.Split(v, "#")
			skipNil, skipZero := true, true
			if len(parts) > 1 && parts[1] == "all" {
				skipNil, skipZero = false, false
			}
			i++
			if destKind == reflect.Slice && destType.Elem().Kind() == reflect.Map {
				targetValue, xerr := scanMap(i, destType.Elem(), parts[0], skipNil, skipZero)
				if xerr != nil {
					err = xerr
					break
				}
				destValue.Set(reflect.Append(destValue, targetValue))
				continue
			}
			targetValue, xerr := valueField(parts[0])
			if xerr != nil {
				err = xerr
				break
			}
			checkValue := targetValue
			targetKind := targetValue.Kind()
			if targetKind == reflect.Ptr && checkValue.IsNil() && skipNil {
				continue
			}
			if targetKind == reflect.Ptr && !checkValue.IsNil() {
				checkValue = reflect.Indirect(checkValue)
			}
			if checkValue.IsZero() && skipZero {
				continue
			}
			if destKind == reflect.Slice && destType.Elem() == targetValue.Type() {
				destValue.Set(reflect.Append(reflect.Indirect(destValue), targetValue))
			} else if destType == targetValue.Type() {
				destValue.Set(targetValue)
			} else {
				err = fmt.Errorf("not supported on dests[%v] to set %v=>%v", i-1, targetValue.Type(), destType)
			}
		}
		if err != nil {
			break
		}
	}
	return
}

// Scan scans the rows into the provided value based on the filter and destination arguments.
func Scan(rows Rows, v any, filter string, dest ...any) (err error) {
	err = Default.Scan(rows, v, filter, dest...)
	return
}

// Scan scans the rows into the provided value based on the filter and destination arguments.
func (c *CRUD) Scan(rows Rows, v any, filter string, dest ...any) (err error) {
	isPtr := reflect.ValueOf(v).Kind() == reflect.Ptr
	isStruct := reflect.Indirect(reflect.ValueOf(v)).Kind() == reflect.Struct
	for rows.Next() {
		value := NewValue(v)
		err = rows.Scan(c.ScanArgs(value.Interface(), filter)...)
		if err != nil {
			break
		}
		if !isPtr || !isStruct {
			value = reflect.Indirect(value)
		}
		err = c.destSet(value, filter, dest...)
		if err != nil {
			break
		}
	}
	return
}

// ScanUnify scans the rows into a unified structure based on the provided value.
func ScanUnify(rows Rows, v any) (err error) {
	err = Default.ScanUnify(rows, v)
	return
}

// ScanUnifyTarget scans the rows into a unified structure based on the provided value and target.
func ScanUnifyTarget(rows Rows, v any, target string) (err error) {
	err = Default.ScanUnifyTarget(rows, v, target)
	return
}

// ScanUnify scans the rows into a unified structure based on the provided value and target.
func (c *CRUD) ScanUnify(rows Rows, v any) (err error) {
	err = c.scanUnify(rows, v, "Query")
	return
}

// ScanUnifyTarget scans the rows into a unified structure based on the provided value and target.
func (c *CRUD) ScanUnifyTarget(rows Rows, v any, target string) (err error) {
	err = c.scanUnify(rows, v, target)
	return
}

func (c *CRUD) scanUnify(rows Rows, v any, target string) (err error) {
	modelValue, modelFilter, dests := c.ScanUnifyDest(v, target)
	err = c.Scan(rows, modelValue, modelFilter, dests...)
	return
}

// Query executes a query on the database and scans the results into the provided value.
func Query(ctx context.Context, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = Default.query(ctx, 1, queryer, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) Query(ctx context.Context, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.query(ctx, 1, queryer, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) query(ctx context.Context, caller int, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	rows, err := c.queryerQuery(ctx, queryer, sql, args)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD query by struct:%v,filter:%v,sql:%v,args:%v result is fail:%v", reflect.TypeOf(v), filter, sql, jsonString(args), err)
		}
		return
	}
	defer rows.Close()
	if c.Verbose {
		c.Log(caller, "CRUD query by struct:%v,filter:%v,sql:%v,args:%v result is success", reflect.TypeOf(v), filter, sql, jsonString(args))
	}
	err = c.Scan(rows, v, filter, dest...)
	return
}

// QueryFilter executes a query with a filter and scans the results into the provided value.
func QueryFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = Default.queryFilter(ctx, 1, queryer, v, filter, where, sep, args, orderby, offset, limit, dest...)
	return
}

// QueryFilter executes a query with a filter and scans the results into the provided value.
func (c *CRUD) QueryFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = c.queryFilter(ctx, 1, queryer, v, filter, where, sep, args, orderby, offset, limit, dest...)
	return
}

func (c *CRUD) queryFilter(ctx context.Context, caller int, queryer any, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql = c.joinWhere(caller+1, sql, where, sep)
	sql = c.joinPage(caller+1, sql, orderby, offset, limit)
	err = c.query(ctx, caller+1, queryer, v, filter, sql, args, dest...)
	return
}

// QueryWheref executes a query with a filter and formatted conditions, scanning the results into the provided value.
func QueryWheref(ctx context.Context, queryer any, v any, filter, formats string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = Default.queryWheref(ctx, 1, queryer, v, filter, formats, args, orderby, offset, limit, dest...)
	return
}

// QueryWheref executes a query with a filter and formatted conditions, scanning the results into the provided value.
func (c *CRUD) QueryWheref(ctx context.Context, queryer any, v any, filter, formats string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = c.queryWheref(ctx, 1, queryer, v, filter, formats, args, orderby, offset, limit, dest...)
	return
}

func (c *CRUD) queryWheref(ctx context.Context, caller int, queryer any, v any, filter, formats string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql, sqlArgs := c.joinWheref(caller+1, sql, nil, formats, args...)
	sql = c.joinPage(caller+1, sql, orderby, offset, limit)
	err = c.query(ctx, caller+1, queryer, v, filter, sql, sqlArgs, dest...)
	return
}

// QueryUnify executes a unified query on the database and scans the results into the provided value.
func QueryUnify(ctx context.Context, queryer any, v any) (err error) {
	err = Default.queryUnify(ctx, 1, queryer, v, "Query")
	return
}

func QueryUnifyTarget(ctx context.Context, queryer any, v any, target string) (err error) {
	err = Default.queryUnify(ctx, 1, queryer, v, target)
	return
}

func (c *CRUD) QueryUnify(ctx context.Context, queryer any, v any) (err error) {
	err = c.queryUnify(ctx, 1, queryer, v, "Query")
	return
}

func (c *CRUD) QueryUnifyTarget(ctx context.Context, queryer any, v any, target string) (err error) {
	err = c.queryUnify(ctx, 1, queryer, v, target)
	return
}

func (c *CRUD) queryUnify(ctx context.Context, caller int, queryer any, v any, target string) (err error) {
	sql, args := c.queryUnifySQL(caller+1, v, target)
	rows, err := c.queryerQuery(ctx, queryer, sql, args)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD query unify by struct:%v,sql:%v,args:%v result is fail:%v", reflect.TypeOf(v), sql, jsonString(args), err)
		}
		return
	}
	defer rows.Close()
	if c.Verbose {
		c.Log(caller, "CRUD query unify by struct:%v,sql:%v,args:%v result is success", reflect.TypeOf(v), sql, jsonString(args))
	}
	err = c.scanUnify(rows, v, target)
	return
}

// ScanRow scans a single row from the database into the provided value based on the filter and destination arguments.
func ScanRow(row Row, v any, filter string, dest ...any) (err error) {
	err = Default.ScanRow(row, v, filter, dest...)
	return
}

// ScanRow scans a single row from the database into the provided value based on the filter and destination arguments.
func (c *CRUD) ScanRow(row Row, v any, filter string, dest ...any) (err error) {
	isPtr := reflect.ValueOf(v).Kind() == reflect.Ptr
	isStruct := reflect.Indirect(reflect.ValueOf(v)).Kind() == reflect.Struct
	value := NewValue(v)
	err = row.Scan(c.ScanArgs(value.Interface(), filter)...)
	if err != nil {
		return
	}
	if !isPtr || !isStruct {
		value = reflect.Indirect(value)
	}
	err = c.destSet(value, filter, dest...)
	if err != nil {
		return
	}
	return
}

// ScanRowUnify scans a single row from the database into a unified structure based on the provided value.
func ScanRowUnify(row Row, v any) (err error) {
	err = Default.ScanRowUnify(row, v)
	return
}

// ScanRowUnifyTarget scans a single row from the database into a unified structure based on the provided value and target.
func ScanRowUnifyTarget(row Row, v any, target string) (err error) {
	err = Default.ScanRowUnifyTarget(row, v, target)
	return
}

// ScanRowUnify scans a single row from the database into a unified structure based on the provided value and target.
func (c *CRUD) ScanRowUnify(row Row, v any) (err error) {
	err = c.scanRowUnify(row, v, "QueryRow")
	return
}

// ScanRowUnifyTarget scans a single row from the database into a unified structure based on the provided value and target.
func (c *CRUD) ScanRowUnifyTarget(row Row, v any, target string) (err error) {
	err = c.scanRowUnify(row, v, target)
	return
}

func (c *CRUD) scanRowUnify(row Row, v any, target string) (err error) {
	modelValue, modelFilter, dests := c.ScanUnifyDest(v, target)
	err = c.ScanRow(row, modelValue, modelFilter, dests...)
	return
}

// QueryRow executes a query that is expected to return a single row and scans the result into the provided value.
func QueryRow(ctx context.Context, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = Default.queryRow(ctx, 1, queryer, v, filter, sql, args, dest...)
	return
}

// QueryRow executes a query that is expected to return a single row and scans the result into the provided value.
func (c *CRUD) QueryRow(ctx context.Context, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.queryRow(ctx, 1, queryer, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) queryRow(ctx context.Context, caller int, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.ScanRow(c.queryerQueryRow(ctx, queryer, sql, args), v, filter, dest...)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD query by struct:%v,filter:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), filter, sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD query by struct:%v,filter:%v,sql:%v,args:%v, result is success", reflect.TypeOf(v), filter, sql, jsonString(args))
	}
	return
}

// QueryRowFilter executes a query with a filter and scans the result into the provided value.
func QueryRowFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, dest ...any) (err error) {
	err = Default.queryRowFilter(ctx, 1, queryer, v, filter, where, sep, args, dest...)
	return
}

// QueryRowFilter executes a query with a filter and scans the result into the provided value.
func (c *CRUD) QueryRowFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, dest ...any) (err error) {
	err = c.queryRowFilter(ctx, 1, queryer, v, filter, where, sep, args, dest...)
	return
}

func (c *CRUD) queryRowFilter(ctx context.Context, caller int, queryer any, v any, filter string, where []string, sep string, args []any, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql = c.joinWhere(caller+1, sql, where, sep)
	err = c.queryRow(ctx, caller+1, queryer, v, filter, sql, args, dest...)
	return
}

// QueryRowWheref executes a query with a filter and formatted conditions, scanning the result into the provided value.
func QueryRowWheref(ctx context.Context, queryer any, v any, filter, formats string, args []any, dest ...any) (err error) {
	err = Default.queryRowWheref(ctx, 1, queryer, v, filter, formats, args, dest...)
	return
}

// QueryRowWheref executes a query with a filter and formatted conditions, scanning the result into the provided value.
func (c *CRUD) QueryRowWheref(ctx context.Context, queryer any, v any, filter, formats string, args []any, dest ...any) (err error) {
	err = c.queryRowWheref(ctx, 1, queryer, v, filter, formats, args, dest...)
	return
}

func (c *CRUD) queryRowWheref(ctx context.Context, caller int, queryer any, v any, filter, formats string, args []any, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql, sqlArgs := c.joinWheref(caller+1, sql, nil, formats, args...)
	err = c.queryRow(ctx, caller+1, queryer, v, filter, sql, sqlArgs, dest...)
	return
}

// QueryRowUnify executes a unified query that is expected to return a single row and scans the result into the provided value.
func QueryRowUnify(ctx context.Context, queryer any, v any) (err error) {
	err = Default.queryRowUnify(ctx, 1, queryer, v, "QueryRow")
	return
}

// QueryRowUnifyTarget executes a unified query that is expected to return a single row and scans the result into the provided value.
func QueryRowUnifyTarget(ctx context.Context, queryer any, v any, target string) (err error) {
	err = Default.queryRowUnify(ctx, 1, queryer, v, target)
	return
}

// QueryRowUnify executes a unified query that is expected to return a single row and scans the result into the provided value.
func (c *CRUD) QueryRowUnify(ctx context.Context, queryer any, v any) (err error) {
	err = c.queryRowUnify(ctx, 1, queryer, v, "QueryRow")
	return
}

// QueryRowUnifyTarget executes a unified query that is expected to return a single row and scans the result into the provided value.
func (c *CRUD) QueryRowUnifyTarget(ctx context.Context, queryer any, v any, target string) (err error) {
	err = c.queryRowUnify(ctx, 1, queryer, v, target)
	return
}

func (c *CRUD) queryRowUnify(ctx context.Context, caller int, queryer any, v any, target string) (err error) {
	sql, args := c.queryUnifySQL(caller+1, v, target)
	err = c.scanRowUnify(c.queryerQueryRow(ctx, queryer, sql, args), v, target)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD query unify row by struct:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD query unify row by struct:%v,sql:%v,args:%v, result is success", reflect.TypeOf(v), sql, jsonString(args))
	}
	return
}

func CountSQL(v any, filter string, suffix ...string) (sql string) {
	sql = Default.countSQL(1, v, "", filter, suffix...)
	return
}

// CountSQL generates a SQL count query based on the provided value and filter.
func (c *CRUD) CountSQL(v any, filter string, suffix ...string) (sql string) {
	sql = c.countSQL(1, v, "", filter, suffix...)
	return
}

func (c *CRUD) countSQL(caller int, v any, from string, filter string, suffix ...string) (sql string) {
	var table string
	var fields []string
	if len(filter) < 1 || filter == "*" || filter == "count(*)" || filter == "count(*)#all" {
		table = c.Table(v)
		fields = []string{"count(*)"}
	} else {
		table, fields = c.queryField(caller+1, v, filter)
	}
	if len(from) > 0 {
		table = from
	}
	sql = fmt.Sprintf(`select %v from %v`, strings.Join(fields, ","), table)
	if len(suffix) > 0 {
		sql += " " + strings.Join(suffix, " ")
	}
	if c.Verbose {
		c.Log(caller, "CRUD generate count sql by struct:%v,filter:%v, result is sql:%v", reflect.TypeOf(v), filter, sql)
	}
	return
}

// CountUnifySQL generates a unified SQL count query based on the provided value.
func CountUnifySQL(v any) (sql string, args []any) {
	sql, args = Default.countUnifySQL(1, v, "Count")
	return
}

// CountUnifySQL generates a unified SQL count query based on the provided value.
func (c *CRUD) CountUnifySQL(v any) (sql string, args []any) {
	sql, args = c.countUnifySQL(1, v, "Count")
	return
}

func (c *CRUD) countUnifySQL(caller int, v any, key string) (sql string, args []any) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	modelValue := reflectValue.FieldByName("Model").Addr().Interface()
	modelType, _ := reflectType.FieldByName("Model")
	modelFrom := modelType.Tag.Get("from")
	queryType, _ := reflectType.FieldByName(key)
	queryFilter := queryType.Tag.Get("filter")
	if queryFrom := queryType.Tag.Get("from"); len(queryFrom) > 0 {
		modelFrom = queryFrom
	}
	querySelect := queryType.Tag.Get("select")
	queryGroup := queryType.Tag.Get("group")
	if len(querySelect) > 0 {
		sql = querySelect
		if strings.Contains(querySelect, "%v") {
			_, fields := c.queryField(caller+1, modelValue, queryFilter)
			sql = fmt.Sprintf(querySelect, strings.Join(fields, ","))
		}
	} else {
		sql = c.countSQL(caller+1, modelValue, modelFrom, queryFilter)
	}
	sql, args = c.joinWhereUnify(caller+1, sql, nil, v)
	sql += " " + queryGroup
	return
}

// CountUnifyDest extracts the model value, query filter, and destination addresses from the provided value.
func CountUnifyDest(v any) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = Default.countUnifyDest(v, "Count")
	return
}

// CountUnifyDestTarget extracts the model value, query filter, and destination addresses from the provided value and target.
func CountUnifyDestTarget(v any, target string) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = Default.countUnifyDest(v, target)
	return
}

// CountUnifyDest extracts the model value, query filter, and destination addresses from the provided value.
func (c *CRUD) CountUnifyDest(v any) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = c.countUnifyDest(v, "Count")
	return
}

// CountUnifyDestTarget extracts the model value, query filter, and destination addresses from the provided value and target.
func (c *CRUD) CountUnifyDestTarget(v any, target string) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = c.countUnifyDest(v, target)
	return
}

func (c *CRUD) countUnifyDest(v any, target string) (modelValue any, queryFilter string, dests []any) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	modelValueList := []any{
		TableName(c.Table(reflectValue.FieldByName("Model").Addr().Interface())),
	}
	queryType, _ := reflectType.FieldByName(target)
	queryValue := reflectValue.FieldByName(target)
	queryFilter = queryType.Tag.Get("filter")
	queryNum := queryType.Type.NumField()
	for i := 0; i < queryNum; i++ {
		scan := queryType.Type.Field(i).Tag.Get("scan")
		if scan == "-" {
			continue
		}
		modelValueList = append(modelValueList, queryValue.Field(i).Interface())
		dests = append(dests, queryValue.Field(i).Addr().Interface())
		if len(scan) > 0 {
			dests = append(dests, scan)
		}
	}
	modelValue = modelValueList
	return
}

// Count executes a count query on the database and scans the result into the provided value.
func Count(ctx context.Context, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = Default.count(ctx, 1, queryer, v, filter, sql, args, dest...)
	return
}

// Count executes a count query on the database and scans the result into the provided value.
func (c *CRUD) Count(ctx context.Context, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.count(ctx, 1, queryer, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) count(ctx context.Context, caller int, queryer any, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.ScanRow(c.queryerQueryRow(ctx, queryer, sql, args), v, filter, dest...)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD count by struct:%v,filter:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), filter, sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD count by struct:%v,filter:%v,sql:%v,args:%v, result is success", reflect.TypeOf(v), filter, sql, jsonString(args))
	}
	return
}

// CountFilter executes a count query with a filter and scans the result into the provided value.
func CountFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error) {
	err = Default.countFilter(ctx, 1, queryer, v, filter, where, sep, args, suffix, dest...)
	return
}

// CountFilter executes a count query with a filter and scans the result into the provided value.
func (c *CRUD) CountFilter(ctx context.Context, queryer any, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error) {
	err = c.countFilter(ctx, 1, queryer, v, filter, where, sep, args, suffix, dest...)
	return
}

func (c *CRUD) countFilter(ctx context.Context, caller int, queryer any, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error) {
	sql := c.countSQL(caller+1, v, "", filter)
	sql = c.joinWhere(caller+1, sql, where, sep, suffix)
	err = c.count(ctx, caller+1, queryer, v, filter, sql, args, dest...)
	return
}

// CountWheref executes a count query with a filter and formatted conditions, scanning the result into the provided value.
func CountWheref(ctx context.Context, queryer any, v any, filter, formats string, args []any, suffix string, dest ...any) (err error) {
	err = Default.countWheref(ctx, 1, queryer, v, filter, formats, args, suffix, dest...)
	return
}

// CountWheref executes a count query with a filter and formatted conditions, scanning the result into the provided value.
func (c *CRUD) CountWheref(ctx context.Context, queryer any, v any, filter, formats string, args []any, suffix string, dest ...any) (err error) {
	err = c.countWheref(ctx, 1, queryer, v, filter, formats, args, suffix, dest...)
	return
}

func (c *CRUD) countWheref(ctx context.Context, caller int, queryer any, v any, filter, formats string, args []any, suffix string, dest ...any) (err error) {
	sql := c.countSQL(caller+1, v, "", filter)
	sql, sqlArgs := c.joinWheref(caller+1, sql, nil, formats, args...)
	if len(suffix) > 0 {
		sql += " " + suffix
	}
	err = c.count(ctx, caller+1, queryer, v, filter, sql, sqlArgs, dest...)
	return
}

// CountUnify executes a unified count query on the database and scans the result into the provided value.
func CountUnify(ctx context.Context, queryer any, v any) (err error) {
	err = Default.countUnify(ctx, 1, queryer, v, "Count")
	return
}

// CountUnifyTarget executes a unified count query on the database and scans the result into the provided value.
func CountUnifyTarget(ctx context.Context, queryer any, v any, target string) (err error) {
	err = Default.countUnify(ctx, 1, queryer, v, target)
	return
}

// CountUnify executes a unified count query on the database and scans the result into the provided value.
func (c *CRUD) CountUnify(ctx context.Context, queryer any, v any) (err error) {
	err = c.countUnify(ctx, 1, queryer, v, "Count")
	return
}

// CountUnifyTarget executes a unified count query on the database and scans the result into the provided value.
func (c *CRUD) CountUnifyTarget(ctx context.Context, queryer any, v any, target string) (err error) {
	err = c.countUnify(ctx, 1, queryer, v, target)
	return
}

func (c *CRUD) countUnify(ctx context.Context, caller int, queryer any, v any, target string) (err error) {
	sql, args := c.countUnifySQL(caller+1, v, target)
	modelValue, queryFilter, dests := c.countUnifyDest(v, target)
	err = c.ScanRow(c.queryerQueryRow(ctx, queryer, sql, args), modelValue, queryFilter, dests...)
	if err != nil {
		if c.Verbose {
			c.Log(caller, "CRUD count unify by struct:%v,sql:%v,args:%v, result is fail:%v", reflect.TypeOf(v), sql, jsonString(args), err)
		}
		return
	}
	if c.Verbose {
		c.Log(caller, "CRUD count unify by struct:%v,sql:%v,args:%v, result is success", reflect.TypeOf(v), sql, jsonString(args))
	}
	return
}

// ApplyUnify applies the unify operation to the provided value using the default CRUD instance.
func ApplyUnify(ctx context.Context, queryer any, v any, enabled ...string) (err error) {
	err = Default.applyUnify(ctx, 1, queryer, v, enabled...)
	return
}

// ApplyUnify applies the unify operation to the provided value using the CRUD instance.
func (c *CRUD) ApplyUnify(ctx context.Context, queryer any, v any, enabled ...string) (err error) {
	err = c.applyUnify(ctx, 1, queryer, v, enabled...)
	return
}

func (c *CRUD) applyUnify(ctx context.Context, caller int, queryer any, v any, enabled ...string) (err error) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	enabledAll := xsql.StringArray(enabled)
	isEnabled := func(key string) bool {
		return len(enabledAll) < 1 || enabledAll.HavingOne(key)
	}
	if value := reflectValue.FieldByName("Query"); value.IsValid() && err == nil && isEnabled("Query") {
		enabled := value.FieldByName("Enabled")
		if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
			err = c.queryUnify(ctx, caller+1, queryer, v, "Query")
		}
	}
	if value := reflectValue.FieldByName("QueryRow"); value.IsValid() && err == nil && isEnabled("QueryRow") {
		enabled := value.FieldByName("Enabled")
		if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
			err = c.queryRowUnify(ctx, caller+1, queryer, v, "QueryRow")
		}
	}
	if value := reflectValue.FieldByName("Count"); value.IsValid() && err == nil && isEnabled("Count") {
		enabled := value.FieldByName("Enabled")
		if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
			err = c.countUnify(ctx, caller+1, queryer, v, "Count")
		}
	}
	for i := 0; i < reflectType.NumField(); i++ {
		fieldType := reflectType.Field(i)
		fieldValue := reflectValue.Field(i)
		apply := fieldType.Tag.Get("apply")
		if len(apply) < 1 || !enabledAll.HavingOne(fieldType.Name) {
			continue
		}
		switch apply {
		case "Query":
			enabled := fieldValue.FieldByName("Enabled")
			if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
				err = c.queryUnify(ctx, caller+1, queryer, v, fieldType.Name)
			}
		case "QueryRow":
			enabled := fieldValue.FieldByName("Enabled")
			if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
				err = c.queryRowUnify(ctx, caller+1, queryer, v, fieldType.Name)
			}
		case "Count":
			enabled := fieldValue.FieldByName("Enabled")
			if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
				err = c.countUnify(ctx, caller+1, queryer, v, fieldType.Name)
			}
		}
	}
	return
}
