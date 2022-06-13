package gen

import (
	"bytes"
	"fmt"
	"go/format"
	"io"
	"strings"
	"text/template"

	"github.com/codingeasygo/crud"
)

func ConvCamelCase(isTable bool, name string) (result string) {
	parts := strings.Split(name, "_")
	for _, part := range parts {
		result += strings.Title(part)
	}
	return
}

func ConvSizeTrim(typeMap map[string][]string, s *Struct, column *Column) (result string) {
	types := typeMap[strings.ToLower(strings.SplitN(column.Type, "(", 2)[0])]
	if len(types) < 1 {
		types = typeMap["*"]
	}
	if len(types) < 1 {
		result = "interface{}"
	} else if column.NotNull {
		result = types[0]
	} else {
		result = types[1]
	}
	return
}

func ConvKeyValueOption(s *Struct, field *Field) (result []*Option) {
	for _, comment := range strings.Split(field.Comment, ",") {
		comment = strings.TrimSpace(comment)
		parts := strings.SplitN(comment, ":", 2)
		kv := strings.SplitN(parts[0], "=", 2)
		if len(kv) < 2 {
			continue
		}
		key := strings.Trim(strings.TrimSpace(kv[0]), `"`)
		val := strings.Trim(strings.TrimSpace(kv[1]), `"`)
		comment := ""
		if len(parts) > 1 {
			comment = strings.TrimSpace(parts[1])
		}
		result = append(result, &Option{
			Name:    fmt.Sprintf("%v%v%v", s.Name, field.Name, key),
			Value:   val,
			Comment: comment,
		})
	}
	return
}

type Column struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	IsPK         bool    `json:"is_pk"`
	NotNull      bool    `json:"not_null"`
	DefaultValue *string `json:"default_value"`
	Ordinal      int     `json:"ordinal"`
	DDLType      string  `json:"ddl_type"`
	Comment      string  `json:"comment"`
}

type Table struct {
	Schema  string    `json:"schema"`
	Name    string    `json:"name"`
	Type    string    `json:"type"`
	Comment string    `json:"comment"`
	Columns []*Column `json:"columns"`
}

func Query(queryer crud.Queryer, tableSQL, columnSQL, schema string) (tables []*Table, err error) {
	err = crud.Query(queryer, &Table{}, "name,type,comment#all", tableSQL, []interface{}{schema}, &tables)
	if err != nil {
		return
	}
	for _, table := range tables {
		err = crud.Query(queryer, &Column{}, "#all", columnSQL, []interface{}{schema, table.Name}, &table.Columns)
		if err != nil {
			break
		}
	}
	return
}

type NameConv func(isTable bool, name string) string
type TypeConv func(typeMap map[string][]string, s *Struct, column *Column) string
type OptionConv func(s *Struct, field *Field) []*Option

type Option struct {
	Name    string
	Value   string
	Comment string
}

type Field struct {
	Name     string
	Type     string
	Tag      string
	Comment  string
	Column   *Column
	Options  []*Option
	External interface{}
}

type Struct struct {
	Name     string
	Comment  string
	Table    *Table
	Fields   []*Field
	External interface{}
}

type Gen struct {
	Tables     []*Table
	TypeMap    map[string][]string
	NameConv   NameConv
	TypeConv   TypeConv
	OptionConv OptionConv
	OnPre      func(*Table) interface{}
}

func NewGen(typeMap map[string][]string, tables []*Table) (gen *Gen) {
	gen = &Gen{
		Tables:     tables,
		TypeMap:    typeMap,
		NameConv:   ConvCamelCase,
		TypeConv:   ConvSizeTrim,
		OptionConv: ConvKeyValueOption,
		OnPre:      nil,
	}
	return
}

func (g *Gen) AsStruct(t *Table) (s *Struct) {
	s = &Struct{
		Name:    g.NameConv(true, t.Name),
		Comment: t.Comment,
		Table:   t,
	}
	for _, col := range t.Columns {
		field := &Field{
			Tag:     col.Name,
			Comment: col.Comment,
			Column:  col,
		}
		field.Name = g.NameConv(false, col.Name)
		field.Type = g.TypeConv(g.TypeMap, s, col)
		field.Options = g.OptionConv(s, field)
		s.Fields = append(s.Fields, field)
	}
	return
}

func (g *Gen) convStruct(t *Table) (data interface{}) {
	if g.OnPre != nil {
		data = g.OnPre(t)
	} else {
		data = map[string]interface{}{
			"Struct": g.AsStruct(t),
		}
	}
	return
}

func (g *Gen) JoinOption(options []*Option, key, seq string) string {
	values := []string{}
	for _, option := range options {
		switch key {
		case "Name":
			values = append(values, option.Name)
		case "Value":
			values = append(values, option.Value)
		case "Comment":
			values = append(values, option.Comment)
		}
	}
	return strings.Join(values, seq)
}

func (g *Gen) Generate(writer io.Writer, call func(buffer io.Writer, data interface{}) error) (err error) {
	var source []byte
	for _, table := range g.Tables {
		buffer := bytes.NewBuffer(nil)
		data := g.convStruct(table)
		err = call(buffer, data)
		if err != nil {
			break
		}
		source, err = format.Source(buffer.Bytes())
		if err != nil {
			break
		}
		source = []byte("\n" + strings.TrimSpace(string(source)) + "\n")
		_, err = writer.Write(source)
		if err != nil {
			break
		}
	}
	return
}

func (g *Gen) GenerateByTemplate(tmpl string, writer io.Writer) (err error) {
	structTmpl := template.New("struct")
	structTmpl.Funcs(template.FuncMap{
		"JoinOption": g.JoinOption,
	})
	_, err = structTmpl.Parse(tmpl)
	if err != nil {
		return
	}
	g.Generate(writer, structTmpl.Execute)
	return
}