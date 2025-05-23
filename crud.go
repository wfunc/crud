// Package crud is simple curd tools to process database
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

type NilChecker interface {
	IsNil() bool
}

type ZeroChecker interface {
	IsZero() bool
}

type TableNameGetter interface {
	GetTableName(args ...any) string
}

type TableNameGetterF func(args ...any) string

func (t TableNameGetterF) GetTableName(args ...any) string { return t(args...) }

type FilterGetter interface {
	GetFilter(args ...any) string
}

type FilterGetterF func(args ...any) string

func (f FilterGetterF) GetFilter(args ...any) string { return f(args...) }

func jsonString(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return err.Error()
	}
	return string(data)
}

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
	ParmConv: func(on, fieldName, fieldFunc string, field reflect.StructField, value any) any {
		return value
	},
}

type NameConv func(on, name string, field reflect.StructField) string
type ParmConv func(on, fieldName, fieldFunc string, field reflect.StructField, value any) any
type LogF func(caller int, format string, args ...any)
type TableName string
type FilterValue string

func Args(args ...any) []any {
	return args
}

type CRUD struct {
	attrscan.Scanner
	ArgFormat   string
	ErrNoRows   error
	Verbose     bool
	Log         LogF
	TablePrefix string
	ParmConv    ParmConv
}

func (c *CRUD) getErrNoRows() (err error) {
	if c.ErrNoRows == nil {
		err = ErrNoRows
	} else {
		err = c.ErrNoRows
	}
	return
}

func Table(v any) (table string) {
	table = Default.Table(v)
	return
}

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

func (c *CRUD) Sprintf(format string, v int) string {
	args := []any{}
	arg := fmt.Sprintf("%d", v)
	n := strings.Count(format, c.ArgFormat)
	for i := 0; i < n; i++ {
		args = append(args, arg)
	}
	return fmt.Sprintf(format, args...)
}

func FilterFieldCall(on string, v any, filter string, call func(fieldName, fieldFunc string, field reflect.StructField, value any)) (table string) {
	table = Default.FilterFieldCall(on, v, filter, call)
	return
}

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

func FilterFormatCall(formats string, args []any, call func(format string, arg any)) {
	Default.FilterFormatCall(formats, args, call)
}

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
		args_ = append(args_, c.ParmConv("where", fieldName, fieldFunc, field, fieldValue))
		where_ = append(where_, c.Sprintf(cmp, len(args_)))
	})
	return
}

func AppendInsert(fields, param []string, args []any, ok bool, format string, v any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = Default.AppendInsert(fields, param, args, ok, format, v)
	return
}

func (c *CRUD) AppendInsert(fields, param []string, args []any, ok bool, format string, v any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = fields, param, args
	if ok {
		args_ = append(args_, c.ParmConv("insert", format, "", reflect.StructField{}, v))
		parts := strings.SplitN(format, "=", 2)
		param_ = append(param_, c.Sprintf(parts[1], len(args_)))
		fields_ = append(fields_, parts[0])
	}
	return
}

func AppendInsertf(fields, param []string, args []any, formats string, v ...any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = Default.AppendInsertf(fields, param, args, formats, v...)
	return
}

func (c *CRUD) AppendInsertf(fields, param []string, args []any, formats string, v ...any) (fields_, param_ []string, args_ []any) {
	fields_, param_, args_ = fields, param, args
	c.FilterFormatCall(formats, v, func(format string, arg any) {
		args_ = append(args_, c.ParmConv("insert", format, "", reflect.StructField{}, arg))
		parts := strings.SplitN(format, "=", 2)
		param_ = append(param_, c.Sprintf(parts[1], len(args_)))
		fields_ = append(fields_, parts[0])
	})
	return
}

func AppendSet(sets []string, args []any, ok bool, format string, v any) (sets_ []string, args_ []any) {
	sets_, args_ = Default.AppendSet(sets, args, ok, format, v)
	return
}

func (c *CRUD) AppendSet(sets []string, args []any, ok bool, format string, v any) (sets_ []string, args_ []any) {
	sets_, args_ = sets, args
	if ok {
		args_ = append(args_, c.ParmConv("update", format, "", reflect.StructField{}, v))
		sets_ = append(sets_, c.Sprintf(format, len(args_)))
	}
	return
}

func AppendSetf(sets []string, args []any, formats string, v ...any) (sets_ []string, args_ []any) {
	sets_, args_ = Default.AppendSetf(sets, args, formats, v...)
	return
}

func (c *CRUD) AppendSetf(sets []string, args []any, formats string, v ...any) (sets_ []string, args_ []any) {
	sets_, args_ = sets, args
	c.FilterFormatCall(formats, v, func(format string, arg any) {
		args_ = append(args_, c.ParmConv("update", format, "", reflect.StructField{}, arg))
		sets_ = append(sets_, c.Sprintf(format, len(args_)))
	})
	return
}

func AppendWhere(where []string, args []any, ok bool, format string, v any) (where_ []string, args_ []any) {
	where_, args_ = Default.AppendWhere(where, args, ok, format, v)
	return
}

func (c *CRUD) AppendWhere(where []string, args []any, ok bool, format string, v any) (where_ []string, args_ []any) {
	where_, args_ = where, args
	if ok {
		args_ = append(args_, c.ParmConv("where", format, "", reflect.StructField{}, v))
		where_ = append(where_, c.Sprintf(format, len(args_)))
	}
	return
}

func AppendWheref(where []string, args []any, format string, v ...any) (where_ []string, args_ []any) {
	where_, args_ = Default.AppendWheref(where, args, format, v...)
	return
}

func (c *CRUD) AppendWheref(where []string, args []any, formats string, v ...any) (where_ []string, args_ []any) {
	where_, args_ = where, args
	c.FilterFormatCall(formats, v, func(format string, arg any) {
		args_ = append(args_, c.ParmConv("where", format, "", reflect.StructField{}, arg))
		where_ = append(where_, c.Sprintf(format, len(args_)))
	})
	return
}

func AppendWhereUnify(where []string, args []any, v any, enabled ...string) (where_ []string, args_ []any) {
	where_, args_ = Default.AppendWhereUnify(where, args, v, enabled...)
	return
}

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

func JoinWhere(sql string, where []string, sep string, suffix ...string) (sql_ string) {
	sql_ = Default.joinWhere(1, sql, where, sep, suffix...)
	return
}

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

func JoinWheref(sql string, args []any, formats string, formatArgs ...any) (sql_ string, args_ []any) {
	sql_, args_ = Default.joinWheref(1, sql, args, formats, formatArgs...)
	return
}

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

func JoinWhereUnify(sql string, args []any, v any, enabled ...string) (sql_ string, args_ []any) {
	sql_, args_ = Default.joinWhereUnify(1, sql, args, v, enabled...)
	return
}

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

func JoinPage(sql, orderby string, offset, limit int) (sql_ string) {
	sql_ = Default.joinPage(1, sql, orderby, offset, limit)
	return
}

func (c *CRUD) JoinPage(sql, orderby string, offset, limit int) (sql_ string) {
	sql_ = c.joinPage(1, sql, orderby, offset, limit)
	return
}

func (c *CRUD) joinPage(caller int, sql, orderby string, offset, limit int) (sql_ string) {
	sql_ = sql
	if len(orderby) > 0 && (offset >= 0 || limit > 0) {
		sql_ += " " + orderby
	}
	if limit > 0 {
		sql_ += fmt.Sprintf(" limit %v offset %v", limit, offset)
	}
	if c.Verbose {
		c.Log(caller, "CRUD join page done with sql:%v", sql_)
	}
	return
}

func JoinPageUnify(sql string, v any) (sql_ string) {
	sql_ = Default.joinPageUnify(1, sql, v)
	return
}

func (c *CRUD) JoinPageUnify(sql string, v any) (sql_ string) {
	sql_ = c.joinPageUnify(1, sql, v)
	return
}

func (c *CRUD) joinPageUnify(caller int, sql string, v any) (sql_ string) {
	sql_ = sql
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
	sql_ = c.joinPage(caller+1, sql_, order, offset, limit)
	return
}

func (c *CRUD) queryerExec(queryer any, ctx context.Context, sql string, args []any) (insertId, affected int64, err error) {
	reflectValue := reflect.ValueOf(queryer)
	if reflectValue.Kind() == reflect.Func {
		queryer = reflectValue.Call(nil)[0].Interface()
	}
	if q, ok := queryer.(Queryer); ok {
		insertId, affected, err = q.Exec(ctx, sql, args...)
	} else if q, ok := queryer.(CrudQueryer); ok {
		insertId, affected, err = q.CrudExec(ctx, sql, args...)
	} else {
		panic("queryer is not supported")
	}
	return
}

func (c *CRUD) queryerQuery(queryer any, ctx context.Context, sql string, args []any) (rows Rows, err error) {
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

func (c *CRUD) queryerQueryRow(queryer any, ctx context.Context, sql string, args []any) (row Row) {
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

func InsertArgs(v any, filter string, args []any) (table string, fields, param []string, args_ []any) {
	table, fields, param, args_ = Default.insertArgs(1, v, filter, args)
	return
}

func (c *CRUD) InsertArgs(v any, filter string, args []any) (table string, fields, param []string, args_ []any) {
	table, fields, param, args_ = c.insertArgs(1, v, filter, args)
	return
}

func (c *CRUD) insertArgs(caller int, v any, filter string, args []any) (table string, fields, param []string, args_ []any) {
	args_ = args
	table = c.FilterFieldCall("insert", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		args_ = append(args_, c.ParmConv("insert", fieldName, fieldFunc, field, value))
		fields = append(fields, fieldName)
		param = append(param, fmt.Sprintf(c.ArgFormat, len(args_)))
	})
	if c.Verbose {
		c.Log(caller, "CRUD generate insert args by struct:%v,filter:%v, result is fields:%v,param:%v,args:%v", reflect.TypeOf(v), filter, fields, param, jsonString(args))
	}
	return
}

func InsertSQL(v any, filter string, suffix ...string) (sql string, args []any) {
	sql, args = Default.insertSQL(1, v, filter, suffix...)
	return
}

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

func InsertFilter(queryer any, ctx context.Context, v any, filter, join, scan string) (insertId int64, err error) {
	insertId, err = Default.insertFilter(1, queryer, ctx, v, filter, join, scan)
	return
}

func (c *CRUD) InsertFilter(queryer any, ctx context.Context, v any, filter, join, scan string) (insertId int64, err error) {
	insertId, err = c.insertFilter(1, queryer, ctx, v, filter, join, scan)
	return
}

func (c *CRUD) insertFilter(caller int, queryer any, ctx context.Context, v any, filter, join, scan string) (insertId int64, err error) {
	table, fields, param, args := c.insertArgs(caller+1, v, filter, nil)
	sql := fmt.Sprintf(`insert into %v(%v) values(%v)`, table, strings.Join(fields, ","), strings.Join(param, ","))
	if len(scan) < 1 {
		if len(join) > 0 {
			sql += " " + join
		}
		insertId, _, err = c.queryerExec(queryer, ctx, sql, args)
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
	err = c.queryerQueryRow(queryer, ctx, sql, args).Scan(scanArgs...)
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

func UpdateArgs(v any, filter string, args []any) (table string, sets []string, args_ []any) {
	table, sets, args_ = Default.updateArgs(1, v, filter, args)
	return
}

func (c *CRUD) UpdateArgs(v any, filter string, args []any) (table string, sets []string, args_ []any) {
	table, sets, args_ = c.updateArgs(1, v, filter, args)
	return
}

func (c *CRUD) updateArgs(caller int, v any, filter string, args []any) (table string, sets []string, args_ []any) {
	args_ = args
	table = c.FilterFieldCall("update", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		args_ = append(args_, c.ParmConv("update", fieldName, fieldFunc, field, value))
		sets = append(sets, fmt.Sprintf("%v="+c.ArgFormat, fieldName, len(args_)))
	})
	if c.Verbose {
		c.Log(caller, "CRUD generate update args by struct:%v,filter:%v, result is sets:%v,args:%v", reflect.TypeOf(v), filter, sets, jsonString(args_))
	}
	return
}

func UpdateSQL(v any, filter string, args []any, suffix ...string) (sql string, args_ []any) {
	sql, args_ = Default.updateSQL(1, v, filter, args, suffix...)
	return
}

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

func Update(queryer any, ctx context.Context, v any, sql string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = Default.update(1, queryer, ctx, v, sql, where, sep, args)
	return
}

func (c *CRUD) Update(queryer any, ctx context.Context, v any, sql string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = c.update(1, queryer, ctx, v, sql, where, sep, args)
	return
}

func (c *CRUD) update(caller int, queryer any, ctx context.Context, v any, sql string, where []string, sep string, args []any) (affected int64, err error) {
	sql = c.joinWhere(caller+1, sql, where, sep)
	_, affected, err = c.queryerExec(queryer, ctx, sql, args)
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

func UpdateRow(queryer any, ctx context.Context, v any, sql string, where []string, sep string, args []any) (err error) {
	err = Default.updateRow(1, queryer, ctx, v, sql, where, sep, args)
	return
}

func (c *CRUD) UpdateRow(queryer any, ctx context.Context, v any, sql string, where []string, sep string, args []any) (err error) {
	err = c.updateRow(1, queryer, ctx, v, sql, where, sep, args)
	return
}

func (c *CRUD) updateRow(caller int, queryer any, ctx context.Context, v any, sql string, where []string, sep string, args []any) (err error) {
	affected, err := c.update(caller+1, queryer, ctx, v, sql, where, sep, args)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

func UpdateSet(queryer any, ctx context.Context, v any, sets, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = Default.updateSet(1, queryer, ctx, v, sets, where, sep, args)
	return
}

func (c *CRUD) UpdateSet(queryer any, ctx context.Context, v any, sets, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = c.updateSet(1, queryer, ctx, v, sets, where, sep, args)
	return
}

func (c *CRUD) updateSet(caller int, queryer any, ctx context.Context, v any, sets, where []string, sep string, args []any) (affected int64, err error) {
	table := c.Table(v)
	sql := fmt.Sprintf(`update %v set %v`, table, strings.Join(sets, ","))
	sql = c.joinWhere(caller+1, sql, where, sep)
	_, affected, err = c.queryerExec(queryer, ctx, sql, args)
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

func UpdateRowSet(queryer any, ctx context.Context, v any, sets, where []string, sep string, args []any) (err error) {
	err = Default.updateRowSet(1, queryer, ctx, v, sets, where, sep, args)
	return
}

func (c *CRUD) UpdateRowSet(queryer any, ctx context.Context, v any, sets, where []string, sep string, args []any) (err error) {
	err = c.updateRowSet(1, queryer, ctx, v, sets, where, sep, args)
	return
}

func (c *CRUD) updateRowSet(caller int, queryer any, ctx context.Context, v any, sets, where []string, sep string, args []any) (err error) {
	affected, err := c.updateSet(caller+1, queryer, ctx, v, sets, where, sep, args)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

func UpdateFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = Default.updateFilter(1, queryer, ctx, v, filter, where, sep, args)
	return
}

func (c *CRUD) UpdateFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any) (affected int64, err error) {
	affected, err = c.updateFilter(1, queryer, ctx, v, filter, where, sep, args)
	return
}

func (c *CRUD) updateFilter(caller int, queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any) (affected int64, err error) {
	sql, args := c.updateSQL(caller+1, v, filter, args)
	sql = c.joinWhere(caller+1, sql, where, sep)
	_, affected, err = c.queryerExec(queryer, ctx, sql, args)
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

func UpdateRowFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any) (err error) {
	err = Default.updateRowFilter(1, queryer, ctx, v, filter, where, sep, args)
	return
}

func (c *CRUD) UpdateRowFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any) (err error) {
	err = c.updateRowFilter(1, queryer, ctx, v, filter, where, sep, args)
	return
}

func (c *CRUD) updateRowFilter(caller int, queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any) (err error) {
	affected, err := c.updateFilter(caller+1, queryer, ctx, v, filter, where, sep, args)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

func UpdateWheref(queryer any, ctx context.Context, v any, filter, formats string, args ...any) (affected int64, err error) {
	affected, err = Default.updateWheref(1, queryer, ctx, v, filter, formats, args...)
	return
}

func (c *CRUD) UpdateWheref(queryer any, ctx context.Context, v any, filter, formats string, args ...any) (affected int64, err error) {
	affected, err = c.updateWheref(1, queryer, ctx, v, filter, formats, args...)
	return
}

func (c *CRUD) updateWheref(caller int, queryer any, ctx context.Context, v any, filter, formats string, args ...any) (affected int64, err error) {
	sql, sqlArgs := c.updateSQL(caller+1, v, filter, nil)
	sql, sqlArgs = c.joinWheref(caller+1, sql, sqlArgs, formats, args...)
	_, affected, err = c.queryerExec(queryer, ctx, sql, sqlArgs)
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

func UpdateRowWheref(queryer any, ctx context.Context, v any, filter, formats string, args ...any) (err error) {
	err = Default.updateRowWheref(1, queryer, ctx, v, filter, formats, args...)
	return
}

func (c *CRUD) UpdateRowWheref(queryer any, ctx context.Context, v any, filter, formats string, args ...any) (err error) {
	err = c.updateRowWheref(1, queryer, ctx, v, filter, formats, args...)
	return
}

func (c *CRUD) updateRowWheref(caller int, queryer any, ctx context.Context, v any, filter, formats string, args ...any) (err error) {
	affected, err := c.updateWheref(caller+1, queryer, ctx, v, filter, formats, args...)
	if err == nil && affected < 1 {
		err = c.getErrNoRows()
	}
	return
}

func QueryField(v any, filter string) (table string, fields []string) {
	table, fields = Default.queryField(1, v, filter)
	return
}

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

func QuerySQL(v any, filter string, suffix ...string) (sql string) {
	sql = Default.querySQL(1, v, "", filter, suffix...)
	return
}

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

func QueryUnifySQL(v any, field string) (sql string, args []any) {
	sql, args = Default.queryUnifySQL(1, v, field)
	return
}

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

func ScanArgs(v any, filter string) (args []any) {
	args = Default.ScanArgs(v, filter)
	return
}

func (c *CRUD) ScanArgs(v any, filter string) (args []any) {
	c.FilterFieldCall("scan", v, filter, func(fieldName, fieldFunc string, field reflect.StructField, value any) {
		args = append(args, c.ParmConv("scan", fieldName, fieldFunc, field, value))
	})
	return
}

func ScanUnifyDest(v any, queryName string) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = Default.ScanUnifyDest(v, queryName)
	return
}

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

func Scan(rows Rows, v any, filter string, dest ...any) (err error) {
	err = Default.Scan(rows, v, filter, dest...)
	return
}

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

func ScanUnify(rows Rows, v any) (err error) {
	err = Default.ScanUnify(rows, v)
	return
}

func ScanUnifyTarget(rows Rows, v any, target string) (err error) {
	err = Default.ScanUnifyTarget(rows, v, target)
	return
}

func (c *CRUD) ScanUnify(rows Rows, v any) (err error) {
	err = c.scanUnify(rows, v, "Query")
	return
}

func (c *CRUD) ScanUnifyTarget(rows Rows, v any, target string) (err error) {
	err = c.scanUnify(rows, v, target)
	return
}

func (c *CRUD) scanUnify(rows Rows, v any, target string) (err error) {
	modelValue, modelFilter, dests := c.ScanUnifyDest(v, target)
	err = c.Scan(rows, modelValue, modelFilter, dests...)
	return
}

func Query(queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = Default.query(1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) Query(queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.query(1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) query(caller int, queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	rows, err := c.queryerQuery(queryer, ctx, sql, args)
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

func QueryFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = Default.queryFilter(1, queryer, ctx, v, filter, where, sep, args, orderby, offset, limit, dest...)
	return
}

func (c *CRUD) QueryFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = c.queryFilter(1, queryer, ctx, v, filter, where, sep, args, orderby, offset, limit, dest...)
	return
}

func (c *CRUD) queryFilter(caller int, queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql = c.joinWhere(caller+1, sql, where, sep)
	sql = c.joinPage(caller+1, sql, orderby, offset, limit)
	err = c.query(caller+1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func QueryWheref(queryer any, ctx context.Context, v any, filter, formats string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = Default.queryWheref(1, queryer, ctx, v, filter, formats, args, orderby, offset, limit, dest...)
	return
}

func (c *CRUD) QueryWheref(queryer any, ctx context.Context, v any, filter, formats string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	err = c.queryWheref(1, queryer, ctx, v, filter, formats, args, orderby, offset, limit, dest...)
	return
}

func (c *CRUD) queryWheref(caller int, queryer any, ctx context.Context, v any, filter, formats string, args []any, orderby string, offset, limit int, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql, sqlArgs := c.joinWheref(caller+1, sql, nil, formats, args...)
	sql = c.joinPage(caller+1, sql, orderby, offset, limit)
	err = c.query(caller+1, queryer, ctx, v, filter, sql, sqlArgs, dest...)
	return
}

func QueryUnify(queryer any, ctx context.Context, v any) (err error) {
	err = Default.queryUnify(1, queryer, ctx, v, "Query")
	return
}

func QueryUnifyTarget(queryer any, ctx context.Context, v any, target string) (err error) {
	err = Default.queryUnify(1, queryer, ctx, v, target)
	return
}

func (c *CRUD) QueryUnify(queryer any, ctx context.Context, v any) (err error) {
	err = c.queryUnify(1, queryer, ctx, v, "Query")
	return
}

func (c *CRUD) QueryUnifyTarget(queryer any, ctx context.Context, v any, target string) (err error) {
	err = c.queryUnify(1, queryer, ctx, v, target)
	return
}

func (c *CRUD) queryUnify(caller int, queryer any, ctx context.Context, v any, target string) (err error) {
	sql, args := c.queryUnifySQL(caller+1, v, target)
	rows, err := c.queryerQuery(queryer, ctx, sql, args)
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

func ScanRow(row Row, v any, filter string, dest ...any) (err error) {
	err = Default.ScanRow(row, v, filter, dest...)
	return
}

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

func ScanRowUnify(row Row, v any) (err error) {
	err = Default.ScanRowUnify(row, v)
	return
}

func ScanRowUnifyTarget(row Row, v any, target string) (err error) {
	err = Default.ScanRowUnifyTarget(row, v, target)
	return
}

func (c *CRUD) ScanRowUnify(row Row, v any) (err error) {
	err = c.scanRowUnify(row, v, "QueryRow")
	return
}

func (c *CRUD) ScanRowUnifyTarget(row Row, v any, target string) (err error) {
	err = c.scanRowUnify(row, v, target)
	return
}

func (c *CRUD) scanRowUnify(row Row, v any, target string) (err error) {
	modelValue, modelFilter, dests := c.ScanUnifyDest(v, target)
	err = c.ScanRow(row, modelValue, modelFilter, dests...)
	return
}

func QueryRow(queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = Default.queryRow(1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) QueryRow(queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.queryRow(1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) queryRow(caller int, queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.ScanRow(c.queryerQueryRow(queryer, ctx, sql, args), v, filter, dest...)
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

func QueryRowFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, dest ...any) (err error) {
	err = Default.queryRowFilter(1, queryer, ctx, v, filter, where, sep, args, dest...)
	return
}

func (c *CRUD) QueryRowFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, dest ...any) (err error) {
	err = c.queryRowFilter(1, queryer, ctx, v, filter, where, sep, args, dest...)
	return
}

func (c *CRUD) queryRowFilter(caller int, queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql = c.joinWhere(caller+1, sql, where, sep)
	err = c.queryRow(caller+1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func QueryRowWheref(queryer any, ctx context.Context, v any, filter, formats string, args []any, dest ...any) (err error) {
	err = Default.queryRowWheref(1, queryer, ctx, v, filter, formats, args, dest...)
	return
}

func (c *CRUD) QueryRowWheref(queryer any, ctx context.Context, v any, filter, formats string, args []any, dest ...any) (err error) {
	err = c.queryRowWheref(1, queryer, ctx, v, filter, formats, args, dest...)
	return
}

func (c *CRUD) queryRowWheref(caller int, queryer any, ctx context.Context, v any, filter, formats string, args []any, dest ...any) (err error) {
	sql := c.querySQL(caller+1, v, "", filter)
	sql, sqlArgs := c.joinWheref(caller+1, sql, nil, formats, args...)
	err = c.queryRow(caller+1, queryer, ctx, v, filter, sql, sqlArgs, dest...)
	return
}

func QueryRowUnify(queryer any, ctx context.Context, v any) (err error) {
	err = Default.queryRowUnify(1, queryer, ctx, v, "QueryRow")
	return
}

func QueryRowUnifyTarget(queryer any, ctx context.Context, v any, target string) (err error) {
	err = Default.queryRowUnify(1, queryer, ctx, v, target)
	return
}

func (c *CRUD) QueryRowUnify(queryer any, ctx context.Context, v any) (err error) {
	err = c.queryRowUnify(1, queryer, ctx, v, "QueryRow")
	return
}

func (c *CRUD) QueryRowUnifyTarget(queryer any, ctx context.Context, v any, target string) (err error) {
	err = c.queryRowUnify(1, queryer, ctx, v, target)
	return
}

func (c *CRUD) queryRowUnify(caller int, queryer any, ctx context.Context, v any, target string) (err error) {
	sql, args := c.queryUnifySQL(caller+1, v, target)
	err = c.scanRowUnify(c.queryerQueryRow(queryer, ctx, sql, args), v, target)
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

func CountUnifySQL(v any) (sql string, args []any) {
	sql, args = Default.countUnifySQL(1, v, "Count")
	return
}

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

func CountUnifyDest(v any) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = Default.countUnifyDest(v, "Count")
	return
}

func CountUnifyDestTarget(v any, target string) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = Default.countUnifyDest(v, target)
	return
}

func (c *CRUD) CountUnifyDest(v any) (modelValue any, queryFilter string, dests []any) {
	modelValue, queryFilter, dests = c.countUnifyDest(v, "Count")
	return
}

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

func Count(queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = Default.count(1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) Count(queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.count(1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func (c *CRUD) count(caller int, queryer any, ctx context.Context, v any, filter, sql string, args []any, dest ...any) (err error) {
	err = c.ScanRow(c.queryerQueryRow(queryer, ctx, sql, args), v, filter, dest...)
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

func CountFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error) {
	err = Default.countFilter(1, queryer, ctx, v, filter, where, sep, args, suffix, dest...)
	return
}

func (c *CRUD) CountFilter(queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error) {
	err = c.countFilter(1, queryer, ctx, v, filter, where, sep, args, suffix, dest...)
	return
}

func (c *CRUD) countFilter(caller int, queryer any, ctx context.Context, v any, filter string, where []string, sep string, args []any, suffix string, dest ...any) (err error) {
	sql := c.countSQL(caller+1, v, "", filter)
	sql = c.joinWhere(caller+1, sql, where, sep, suffix)
	err = c.count(caller+1, queryer, ctx, v, filter, sql, args, dest...)
	return
}

func CountWheref(queryer any, ctx context.Context, v any, filter, formats string, args []any, suffix string, dest ...any) (err error) {
	err = Default.countWheref(1, queryer, ctx, v, filter, formats, args, suffix, dest...)
	return
}

func (c *CRUD) CountWheref(queryer any, ctx context.Context, v any, filter, formats string, args []any, suffix string, dest ...any) (err error) {
	err = c.countWheref(1, queryer, ctx, v, filter, formats, args, suffix, dest...)
	return
}

func (c *CRUD) countWheref(caller int, queryer any, ctx context.Context, v any, filter, formats string, args []any, suffix string, dest ...any) (err error) {
	sql := c.countSQL(caller+1, v, "", filter)
	sql, sqlArgs := c.joinWheref(caller+1, sql, nil, formats, args...)
	if len(suffix) > 0 {
		sql += " " + suffix
	}
	err = c.count(caller+1, queryer, ctx, v, filter, sql, sqlArgs, dest...)
	return
}

func CountUnify(queryer any, ctx context.Context, v any) (err error) {
	err = Default.countUnify(1, queryer, ctx, v, "Count")
	return
}

func CountUnifyTarget(queryer any, ctx context.Context, v any, target string) (err error) {
	err = Default.countUnify(1, queryer, ctx, v, target)
	return
}

func (c *CRUD) CountUnify(queryer any, ctx context.Context, v any) (err error) {
	err = c.countUnify(1, queryer, ctx, v, "Count")
	return
}

func (c *CRUD) CountUnifyTarget(queryer any, ctx context.Context, v any, target string) (err error) {
	err = c.countUnify(1, queryer, ctx, v, target)
	return
}

func (c *CRUD) countUnify(caller int, queryer any, ctx context.Context, v any, target string) (err error) {
	sql, args := c.countUnifySQL(caller+1, v, target)
	modelValue, queryFilter, dests := c.countUnifyDest(v, target)
	err = c.ScanRow(c.queryerQueryRow(queryer, ctx, sql, args), modelValue, queryFilter, dests...)
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

func ApplyUnify(queryer any, ctx context.Context, v any, enabled ...string) (err error) {
	err = Default.applyUnify(1, queryer, ctx, v, enabled...)
	return
}

func (c *CRUD) ApplyUnify(queryer any, ctx context.Context, v any, enabled ...string) (err error) {
	err = c.applyUnify(1, queryer, ctx, v, enabled...)
	return
}

func (c *CRUD) applyUnify(caller int, queryer any, ctx context.Context, v any, enabled ...string) (err error) {
	reflectValue := reflect.Indirect(reflect.ValueOf(v))
	reflectType := reflectValue.Type()
	enabledAll := xsql.StringArray(enabled)
	isEnabled := func(key string) bool {
		return len(enabledAll) < 1 || enabledAll.HavingOne(key)
	}
	if value := reflectValue.FieldByName("Query"); value.IsValid() && err == nil && isEnabled("Query") {
		enabled := value.FieldByName("Enabled")
		if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
			err = c.queryUnify(caller+1, queryer, ctx, v, "Query")
		}
	}
	if value := reflectValue.FieldByName("QueryRow"); value.IsValid() && err == nil && isEnabled("QueryRow") {
		enabled := value.FieldByName("Enabled")
		if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
			err = c.queryRowUnify(caller+1, queryer, ctx, v, "QueryRow")
		}
	}
	if value := reflectValue.FieldByName("Count"); value.IsValid() && err == nil && isEnabled("Count") {
		enabled := value.FieldByName("Enabled")
		if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
			err = c.countUnify(caller+1, queryer, ctx, v, "Count")
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
				err = c.queryUnify(caller+1, queryer, ctx, v, fieldType.Name)
			}
		case "QueryRow":
			enabled := fieldValue.FieldByName("Enabled")
			if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
				err = c.queryRowUnify(caller+1, queryer, ctx, v, fieldType.Name)
			}
		case "Count":
			enabled := fieldValue.FieldByName("Enabled")
			if !enabled.IsValid() || (enabled.IsValid() && enabled.Bool()) {
				err = c.countUnify(caller+1, queryer, ctx, v, fieldType.Name)
			}
		}
	}
	return
}
