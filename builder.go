package sqlbuilder

import (
	"fmt"
	"strings"
)

type PG struct{}
type MySQL struct{}
type MSSQL struct{}
type Oracle struct{}

type SelectStatement struct {
	dbType          string //Database server type
	mainTable       string
	targetTables    []string
	fieldsList      []string            // Target fields in Table
	conditions      *Conditions         // For where clause
	joinType        string              // The keywords for join command such as Left Join,Right Join,Inner Join
	joinConditions  map[string]string   // For join on clause
	joinUsing       map[string][]string // It for join using about single table with fields list such as {table1:["field1","field2","field3"],table2:["field1","field2"]}
	fields4Orderby  []string
	keyword4Orderby []string // It should be DESC or ASC
	fields4Groupby  []string
	inValues        []interface{}
}

type InsertStatement struct {
	dbType        string //Database server type
	table         string
	fields        []string               //fields for insert by sub query
	fvSet         map[string]interface{} // For fields and values in table
	conflictField string                 // Upsert field
	query         *SelectStatement       //New added for query clause in insert statement
}
type UpdateStatement struct {
	dbType     string //Database server type
	table      string
	conditions *Conditions
	fvSet      map[string]interface{}
	joiner     string
	inValues   []interface{}
}

type DeleteStatement struct {
	dbType     string //Database server type
	table      string
	conditions *Conditions
	joiner     string // Linker between each conditional of WHERE,it should be "AND" or "OR"
}

type Conditions struct {
	s         []*SelectStatement // for sub query in WHERE clause
	queryKey  []string           // field for sub query
	connector string
	fvSet     map[string]interface{} //Field-Value set on conditions
}

// Left,Right,Cross,Inner Join clause
type join struct {
	table      []string
	conditions map[string]string
}

// Join Using clause
type joinUsing struct {
	using map[string][]string // It for single table with fields list such as {table1:["field1","field2","field3"],table2:["field1","field2"]}
}

type joinHandler interface {
	create(jointype string) (jointxt string, err error)
}

type sqlHandler interface {
	create() (tSql string, sql string, values []interface{}, err error)
}

type statementHandler interface {
	NewSelect() *SelectStatement
	NewInsert() *InsertStatement
	NewUpdate() *UpdateStatement
	NewDelete() *DeleteStatement
	NewConditions() *Conditions
}

// format is the function to format values to %s in parameter list for input
// parameters:
// fvSet:field-value set for db table
func format(fvSet map[string]interface{}, equalFlag string) (fields string, valuesMask string, values []interface{}, kvMask4Update string) {
	var fieldsList = make([]string, 0) // Fields slice for Insert and Update statement
	var tpStr4Update string = "%s%s%s" // Template string for update field and value pairs,such as field1=value1
	var tpStr4UpdateList = make([]string, 0)
	var tpStr4Insert string = "(%s)" // Template string for insert values such as (value1,value2...valueN)
	var num int                      // value's quantity
	var vMask string                 // Mask for values

	for i, j := range fvSet {
		fieldsList = append(fieldsList, i)
		values = append(values, j)
		tpStr4UpdateList = append(tpStr4UpdateList, fmt.Sprintf(tpStr4Update, i, equalFlag, "%s")) // Slice of Update's field-value pairs,such as [field1=%s,fields2=%s,field3=%s]
	}
	num = len(values)
	vMask = makeStrMask(num) // Make values mask such as %s,%s...%snum
	fields = fmt.Sprintf(tpStr4Insert, strings.Join(fieldsList, ","))
	valuesMask = fmt.Sprintf(tpStr4Insert, vMask)
	kvMask4Update = strings.Join(tpStr4UpdateList, ",") // String for update's field-value,such as field1=%s,field2=%s
	return
}

// formatSQL is replace the %s to db's placeholder in SQL template
func formatSQL(dbType string, tSql string) (sql string) {
	var ls int
	var masks []interface{}

	ls = strings.Count(tSql, "%s")
	masks = makeDBMask(dbType, ls)
	sql = fmt.Sprintf(tSql, masks...)
	return
}

// formatInsertByQuery replace %s to db placeholder in Insert statement template with sub query
func formatInsertByQuery(table string, query *SelectStatement, conflicField string, fields ...string) (iSql string, values []interface{}) {
	var fieldsList string
	var querySQL string
	if strings.TrimSpace(table) == "" || query == nil {
		return
	}
	if fields != nil {
		fieldsList = fmt.Sprintf(" (%s)", strings.Join(fields, ","))
	} else {
		fieldsList = ""
	}
	querySQL, _, values, _ = query.create()
	iSql = fmt.Sprintf(sql4insertq, table, fieldsList, querySQL, conflicField)
	return
}

func (i *InsertStatement) create() (tSql string, sql string, values []interface{}, err error) {
	var iSql string
	var fields string
	var valueMask string
	if strings.TrimSpace(i.table) == "" {
		return "", "", nil, model_sqlbuilder_table_none
	}
	if i.conflictField == "" {
		return "", "", nil, model_sqlbuilder_fields_conflict_none
	}

	if i.query == nil && i.fvSet != nil {
		fields, valueMask, values, _ = format(i.fvSet, "")
		iSql = fmt.Sprintf(sql4insert, i.table, fields, valueMask, i.conflictField)
	} else {
		iSql, values = formatInsertByQuery(i.table, i.query, i.conflictField, i.fields...)
	}
	sql = formatSQL(i.dbType, iSql)
	return
}

func (i *InsertStatement) ByStandard(table string, fvSet map[string]interface{}, conflictField string) (sql string, values []interface{}, err error) {
	i.fvSet = fvSet
	i.table = table
	i.conflictField = conflictField
	_, sql, values, err = i.create()
	return
}

func (i *InsertStatement) ByQuery(table string, query *SelectStatement, conflictField string, fields ...string) (sql string, values []interface{}, err error) {
	i.table = table
	i.query = query
	i.conflictField = conflictField
	i.fields = fields
	_, sql, values, err = i.create()
	return
}

func (u *UpdateStatement) create() (tSql string, sql string, values []interface{}, err error) {
	var wc string // WHERE clause
	var conditionValue = make([]interface{}, 0)
	var equalFlat string = "="
	var uSql string
	if strings.TrimSpace(u.table) == "" {
		return "", "", nil, model_sqlbuilder_table_none
	}
	if u.fvSet == nil {
		return "", "", nil, model_sqlbuilder_fields_fvset_none
	}

	if strings.TrimSpace(u.joiner) == "" {
		u.joiner = " AND "
	} else if s := strings.ToUpper(u.joiner); s != "AND" || s != "OR" {
		return "", "", nil, model_sqlbuilder_conditions_keyword_none
	}
	_, _, values, kvmask4update := format(u.fvSet, equalFlat)

	if u.conditions == nil {
		uSql = fmt.Sprintf(sql4updaten, u.table, kvmask4update)
	} else {
		wc, conditionValue = u.conditions.create() //createWhereClause(u.conditions,u.joiner)
		uSql = fmt.Sprintf(sql4updatew, u.table, kvmask4update, wc)
	}
	sql = formatSQL(u.dbType, uSql)
	if conditionValue != nil {
		values = append(values, conditionValue...)
	}
	return
}

func (u *UpdateStatement) ByStandard(table string, fvSet map[string]interface{}) (sql string, values []interface{}, err error) {
	u.table = table
	u.fvSet = fvSet
	_, sql, values, err = u.create()
	return
}

func (u *UpdateStatement) ByConditions(table string, fvSet map[string]interface{}, conditions *Conditions) (sql string, values []interface{}, err error) {
	u.table = table
	u.fvSet = fvSet
	u.conditions = conditions
	_, sql, values, err = u.create()
	return
}

func (s *SelectStatement) create() (tSql string, sql string, values []interface{}, err error) {
	var num4Joinon, num4Joinusing int
	var fields string
	var wc string // WHERE clause
	var orderBy string
	var groupBy string

	var joinTxt string
	var template string = "%s%s%s"

	if strings.TrimSpace(s.mainTable) == "" {
		return "", "", nil, model_sqlbuilder_table_none
	}
	if s.targetTables != nil {
		num4Joinon = len(s.targetTables)
	}

	if s.joinUsing != nil {
		num4Joinusing = len(s.joinUsing)

	}

	if s.fieldsList == nil {
		fields = "*"
	} else {
		fields = strings.Join(s.fieldsList, ",")
	}

	if s.conditions != nil {
		wc, values = s.conditions.create()
	}

	if s.fields4Orderby != nil && s.keyword4Orderby != nil {
		orderBy = makeOrderBy(s.fields4Orderby, s.keyword4Orderby)
	}

	if s.fields4Groupby != nil {
		groupBy = makeGroupBy(s.fields4Groupby)
	}
	switch {
	case num4Joinon == 0 && num4Joinusing == 0:
		template = fmt.Sprintf(template, wc, orderBy, groupBy)
		tSql = fmt.Sprintf(sql4select, fields, s.mainTable, template)

	case num4Joinon > 0 || num4Joinusing > 0:
		if s.joinType != "" {
			if s.joinConditions != nil {
				var joinOn = &join{table: s.targetTables, conditions: s.joinConditions}
				joinTxt, err = joinOn.create(s.joinType)
			} else if s.joinUsing != nil {
				var joinUsing = &joinUsing{using: s.joinUsing}
				joinTxt, err = joinUsing.create(s.joinType)
			}
			if err == nil {
				template = fmt.Sprintf(template, joinTxt, orderBy, groupBy)
				tSql = fmt.Sprintf(sql4select, fields, s.mainTable, template)
			}
		}

	}
	sql = formatSQL(s.dbType, tSql)
	return
}

func (s *SelectStatement) ByStandard(table string, fields4OrderBy []string, keyword4OrderBy []string, fields4GroupBy []string, fields ...string) (query *SelectStatement, sql string, values []interface{}, err error) {
	s.mainTable = table
	s.fieldsList = fields
	s.fields4Orderby = fields4OrderBy
	s.keyword4Orderby = keyword4OrderBy
	s.fields4Groupby = fields4GroupBy
	_, sql, values, err = s.create()
	return s, sql, values, err
}

func (s *SelectStatement) ByConditions(table string, conditions *Conditions, fields4OrderBy []string, keyword4OrderBy []string, fields4GroupBy []string, fields ...string) (query *SelectStatement, sql string, values []interface{}, err error) {
	s.mainTable = table
	s.conditions = conditions
	s.fields4Orderby = fields4OrderBy
	s.keyword4Orderby = keyword4OrderBy
	s.fields4Groupby = fields4GroupBy
	s.fieldsList = fields
	_, sql, values, err = s.create()
	return s, sql, values, err

}

func (s *SelectStatement) ByJoinOn(table string, joinType string, targetTables []string, fields4Join map[string]string) (query *SelectStatement, sql string, values []interface{}, err error) {
	s.mainTable = table
	s.joinType = joinType
	s.targetTables = targetTables
	s.joinConditions = fields4Join
	_, sql, values, err = s.create()
	return s, sql, values, err
}

func (s *SelectStatement) ByJoinUsing(table string, joinType string, using map[string][]string) (query *SelectStatement, sql string, values []interface{}, err error) {
	s.mainTable = table
	s.joinType = joinType
	s.joinUsing = using
	_, sql, values, err = s.create()
	return s, sql, values, err
}

func (d *DeleteStatement) create() (tSql string, sql string, values []interface{}, err error) {
	var wc string // Where clause
	var dSql string
	if strings.TrimSpace(d.table) == "" {
		return "", "", nil, model_sqlbuilder_table_none
	}

	if d.joiner == "" {
		d.joiner = " AND "
	}

	if d.conditions == nil {
		sql = fmt.Sprintf(sql4deleteall, d.table)
	} else {
		wc, values = d.conditions.create() //createWhereClause(d.conditions, d.joiner)
		dSql = fmt.Sprintf(sql4deleteone, d.table, wc)
		sql = formatSQL(d.dbType, dSql)
	}
	return
}

func (d *DeleteStatement) All(table string) (sql string, err error) {
	d.table = table
	_, sql, _, err = d.create()
	return
}

func (d *DeleteStatement) ByConditions(table string, conditions *Conditions) (sql string, values []interface{}, err error) {
	d.table = table
	d.conditions = conditions
	_, sql, values, err = d.create()
	return
}

func (j *join) create(joinType string) (joinTxt string, err error) {
	var join string
	var tJoinTxt string = " %s %s ON %s" // Template for Join on
	var tConditions string = "%s=%s"     // Template for join conditions
	var conditions string
	var joinList = make([]string, 0)
	var counter int = 0
	fmt.Println(j.table, j.conditions)
	if j.table == nil {
		return "", model_sqlbuilder_table_none
	}
	if j.conditions == nil {
		return "", model_sqlbuilder_conditions_none

	}

	switch s := strings.ToUpper(joinType); s {
	case "LEFT":
		join = "LEFT JOIN"
	case "RIGHT":
		join = "RIGHT JOIN"
	case "JOIN":
		join = "JOIN"
	case "INNER":
		join = "INNER JOIN"
	case "FULL":
		join = "FULL OUTER JOIN"
	}
	if len(j.table) == len(j.conditions) {
		for k, v := range j.conditions {
			conditions = fmt.Sprintf(tConditions, k, v)
			joinList = append(joinList, fmt.Sprintf(tJoinTxt, join, j.table[counter], conditions))
			counter += 1
		}
		joinTxt = strings.Join(joinList, "")
		err = nil
	} else {
		err = model_sqlbuilder_fields_linked_quantity
	}
	return
}

func (j *joinUsing) create(joinType string) (joinTxt string, err error) {
	var tJoinTxt string = " %s %s USING (%s)"
	var columns string
	var sUsing string
	var usingList []string
	var join string
	if j.using == nil {
		return "", model_sqlbuilder_linked_fields_none
	}

	switch s := strings.ToUpper(joinType); s {
	case "LEFT":
		join = "LEFT JOIN"
	case "RIGHT":
		join = "RIGHT JOIN"
	case "JOIN":
		join = "JOIN"
	case "INNER":
		join = "INNER JOIN"
	case "FULL":
		join = "FULL OUTER JOIN"
	}

	for k, v := range j.using {
		columns = strings.Join(v, ",")
		sUsing = fmt.Sprintf(tJoinTxt, join, k, columns)
		if sUsing != "" {
			usingList = append(usingList, sUsing)
		}
	}
	joinTxt = strings.Join(usingList, "")
	fmt.Println(joinTxt)
	return
}

func (c *Conditions) create() (tSql string, values []interface{}) {
	var sValues []interface{}
	var conditions []string
	if c.s != nil {
		if c.queryKey != nil {
			for i, _ := range c.queryKey {
				func(s *SelectStatement) {
					var cSql string
					tSql, _, values, _ := s.create()
					cSql = fmt.Sprintf("%s (%s)", c.queryKey[i], tSql)
					conditions = append(conditions, cSql) // append sub query to main conditions
					sValues = append(sValues, values...)  // append sub query's value
				}(c.s[i])
			}
		}
		c.s = nil
		c.queryKey = nil
	}

	if sValues != nil {
		values = append(values, sValues...) // append sub query values into main values list
	}

	if c.fvSet != nil {
		for k, v := range c.fvSet {
			conditionItem := c.createConditionItem(k, v)
			if conditionItem != "" {
				conditions = append(conditions, conditionItem)
				values = append(values, v)
			}
		}
	}
	if strings.TrimSpace(c.connector) == "" {
		c.connector = " AND "
	}
	tSql = fmt.Sprintf(" WHERE %s", strings.Join(conditions, c.connector))
	return
}

func (c *Conditions) createConditionItem(k string, v interface{}) (condition string) {
	switch key, _ := getOperator(k); strings.ToUpper(key) {
	case "=", "!=", "<", "<=", ">", ">=":
		condition = fmt.Sprintf("%s%s", k, "%s")
	case "LIKE":
		if _, isStr := v.(string); isStr {
			if checkPs(v.(string)) {
				condition = fmt.Sprintf("%s %s", k, "%s")
			}
		}
	case "IN":
		if _, isArr := v.([]interface{}); isArr {
			condition = fmt.Sprintf("%s(%s)", k, makeStrMask(len(v.([]interface{}))))
		}
	case "BETWEEN":
		condition = fmt.Sprintf("%s %s AND %s", k, "%s", "%s")
	case "NOT IN":
		if _, isArr := v.([]interface{}); isArr {
			condition = fmt.Sprintf("%s(%s)", k, makeStrMask(len(v.([]interface{}))))
		}
	}
	return
}

func (c *Conditions) Append(conditionItems map[string]interface{}, subQuery ...map[string]*SelectStatement) *Conditions {
	if subQuery != nil {
		for _, item := range subQuery {
			for k, v := range item {
				c.queryKey = append(c.queryKey, k)
				c.s = append(c.s, v)
			}
		}
	}
	c.fvSet = conditionItems
	return c
}

// makeOrderBy for create ORDER BY clause in SELECT statement
// f:fields list wants to sort
// w:ORDER BY's keyword it should be asc or desc
// example:
// var fields=[]string{"a","b","c"}
// var sort=[]string{"asc","desc","desc"}
// s:=orderby(fieles,sort)
// print(s)->ORDER BY a asc,b desc,c desc

func makeOrderBy(f, w []string) (clause string) {

	var ts string = " ORDER BY "
	var s []string //template to store，exp:fields DESC/ASC
	var lf int
	var lw int
	var se string
	if f == nil {
		clause = ""
	}
	if w == nil {
		clause = ts + strings.Join(f, ",")
	}
	lf = len(f)
	lw = len(w)
	switch {
	case lf == 1 && lw == 1:
		clause = fmt.Sprintf("%s%s %s", ts, f[0], w[0])
	case lf == 1 && lw > 1:
		clause = ""
	case lf > 1 && lw == 1:
		for i := range f {
			se = fmt.Sprintf("%s %s", f[i], w[0])
			s = append(s, se) //s->[field1 desc,field2 desc]
		}
		clause = fmt.Sprintf("%s%s", ts, strings.Join(s, ",")) //fo->ORDER BY field1 desc,field2 desc
	case lf > 1 && lw > 1:
		if lf != lw {
			clause = ""
		} else {
			for j := range f {
				se = fmt.Sprintf("%s %s", f[j], w[j])
				s = append(s, se) //s->[field1 desc,field2 asc]
			}
			clause = fmt.Sprintf("%s%s", ts, strings.Join(s, ",")) //fo->ORDER BY field1 desc,field2 asc...
		}
	default:
		clause = ""
	}
	return
}

// Create GROUP BY clause for SELECT statement
func makeGroupBy(s []string) string {
	var ts string = " GROUP BY "
	return ts + strings.Join(s, ",")
}

func checkPs(s string) bool {
	//Check "%" in LIKE clause
	return strings.HasPrefix(s, `%`) || strings.HasSuffix(s, `%`)
}

func getOperator(s string) (operator string, had bool) {
	var operatorList = []string{">", "<", ">=", "<=", "=", "<>", "!=", " like", " LIKE", " between", " BETWEEN", " in", " IN", " Exists"}
	ck := func(ts string, t string) bool { return strings.HasSuffix(ts, t) }
	for _, v := range operatorList {
		if ck(s, v) {
			operator = v
			had = true
			break
		}
		operator = "="
		had = true

	}
	return
}

func checkFields(field string, tables []string) (ok bool, err error) {
	var index int
	var tableName string
	if field == "" || tables == nil {
		return false, model_sqlbuilder__nonefield
	}
	if !strings.Contains(field, ".") {
		return false, model_sqlbuilder_join_point_none
	}
	index = strings.Index(field, ".")
	tableName = field[:index]
	for _, v := range tables {
		if v != tableName {
			return false, model_sqlbuilder_join_wrongTableorField
		}
	}
	return true, nil
}

func makeStrMask(num int) (masks string) {
	masks = strings.Repeat(",%s", num)[1:]
	return
}

// makeDBMask create db placeholder for PostgreSQL,MySQL,MSSQL and Oracle
func makeDBMask(dbType string, num int) (masks []interface{}) {
	makeMasks := func(formatTxt string, placeHolder string, qty int) (maskList []interface{}) {
		for i := 1; i <= qty; i++ {
			if strings.ToUpper(dbType) == "MYSQL" {
				maskList = append(maskList, fmt.Sprintf(formatTxt, placeHolder))
			} else {
				maskList = append(maskList, fmt.Sprintf(formatTxt, placeHolder, i))
			}
		}
		return
	}
	switch db := strings.ToUpper(dbType); db {
	case "PG":
		masks = makeMasks("%s%d", "$", num)
	case "MYSQL":
		masks = makeMasks("%s", "?", num)
	case "MSSQL":
		masks = makeMasks("%s%d", "@P", num)
	case "ORACLE":
		masks = makeMasks("%s%d", ":P", num)
	}
	return
}

//DB instance creator
func CreatePGInstance() *PG {
	return &PG{}
}

func CreateMySQLInstance() *MySQL {
	return &MySQL{}
}

func CreateMSSQLInstance() *MSSQL {
	return &MSSQL{}
}

func CreateOracleInstance() *Oracle {
	return &Oracle{}
}

//statement creator
// Postgresql
func (p *PG) NewSelect() *SelectStatement {
	s := &SelectStatement{dbType: "pg"}
	return s
}

func (p *PG) NewInsert() *InsertStatement {
	i := &InsertStatement{dbType: "pg"}
	return i
}

func (p *PG) NewUpdate() *UpdateStatement {
	u := &UpdateStatement{dbType: "pg"}
	return u
}

func (p *PG) NewDeletet() *DeleteStatement {
	d := &DeleteStatement{dbType: "pg"}
	return d
}

func (p *PG) NewConditions() *Conditions {
	return &Conditions{}

}

//MySQL

func (m *MySQL) NewSelect() *SelectStatement {
	s := &SelectStatement{dbType: "mysql"}
	return s
}

func (m *MySQL) NewInsert() *InsertStatement {
	i := &InsertStatement{dbType: "mysql"}
	return i
}

func (m *MySQL) NewUpdate() *UpdateStatement {
	u := &UpdateStatement{dbType: "mysql"}
	return u
}

func (m *MySQL) NewDeletet() *DeleteStatement {
	d := &DeleteStatement{dbType: "mysql"}
	return d
}

func (m *MySQL) NewConditions() *Conditions {
	return &Conditions{}

}

//MSSQL

func (ms *MSSQL) NewSelect() *SelectStatement {
	s := &SelectStatement{dbType: "mssql"}
	return s
}

func (ms *MSSQL) NewInsert() *InsertStatement {
	i := &InsertStatement{dbType: "mssql"}
	return i
}

func (ms *MSSQL) NewUpdate() *UpdateStatement {
	u := &UpdateStatement{dbType: "mssql"}
	return u
}

func (ms *MSSQL) NewDeletet() *DeleteStatement {
	d := &DeleteStatement{dbType: "mssql"}
	return d
}

func (ms *MSSQL) NewConditions() *Conditions {
	return &Conditions{}

}

//Oracle

func (o *Oracle) NewSelect() *SelectStatement {
	s := &SelectStatement{dbType: "oracle"}
	return s
}

func (o *Oracle) NewInsert() *InsertStatement {
	i := &InsertStatement{dbType: "oracle"}
	return i
}

func (o *Oracle) NewUpdate() *UpdateStatement {
	u := &UpdateStatement{dbType: "oracle"}
	return u
}

func (o *Oracle) NewDeletet() *DeleteStatement {
	d := &DeleteStatement{dbType: "oracle"}
	return d
}

func (o *Oracle) NewConditions() *Conditions {
	return &Conditions{}
}
