package summer

import (
	"log"
	"testing"
)

func TestDeleteStatement_All(t *testing.T) {
	// PostgreSQL
	pg := CreatePGInstance()
	log.Println(pg.NewDeletet().All("PGTable4Delete"))
	// MySQL
	mysql := CreateMySQLInstance()
	log.Println(mysql.NewDeletet().All("MysqlTable4Delete"))
	// SQL Server
	mssql := CreateMSSQLInstance()
	log.Println(mssql.NewDeletet().All("MssqlTable4Delete"))
	// Oracle
	oracle := CreateOracleInstance()
	log.Println(oracle.NewDeletet().All("OracleTable4Delete"))
}

func TestDeleteStatement_ByConditions(t *testing.T) {
	// PostgreSQL
	pg := CreatePGInstance()
	conditions := pg.NewConditions().Append(map[string]interface{}{"CField1=": 1, "CField2=": "2"})
	log.Println(pg.NewDeletet().ByConditions("PGTable4DeleteByCondition", conditions))

	// MySQL
	mysql := CreateMySQLInstance()
	mysqlConditions := mysql.NewConditions().Append(map[string]interface{}{"CField1=": 3, "CField2=": "4"})
	log.Println(mysql.NewDeletet().ByConditions("MySQLTable4DeleteByCondition", mysqlConditions))

	// SQL Server
	mssql := CreateMSSQLInstance()
	mssqlConditions := mssql.NewConditions().Append(map[string]interface{}{"CField1=": 5, "CField2=": "6"})
	log.Println(mssql.NewDeletet().ByConditions("MSSQLTable4DeleteByCondition", mssqlConditions))

	// Oracle
	oracle := CreateOracleInstance()
	oracleConditions := oracle.NewConditions().Append(map[string]interface{}{"CField1": 7, "CField2": "8"})
	log.Println(oracle.NewDeletet().ByConditions("OracleTable4DeleteByCondition", oracleConditions))

}

func TestInsertStatement_ByStandard(t *testing.T) {
	// PostgreSQL
	pg := CreatePGInstance()
	log.Println(pg.NewInsert().ByStandard("PGTable4Insert", map[string]interface{}{"Field1": 1, "Field2": "2"}, "RuleField"))
	// MySQL
	mysql := CreateMySQLInstance()
	log.Println(mysql.NewInsert().ByStandard("MYSQLTable4Insert", map[string]interface{}{"Field1": 3, "Field2": 4}, "RuleField"))
	// SQL Server
	mssql := CreateMSSQLInstance()
	log.Println(mssql.NewInsert().ByStandard("MSSQLTable4Insert", map[string]interface{}{"Field1": "5", "Field2": 6}, "RuleField"))
	// Oracle
	oracle := CreateOracleInstance()
	log.Println(oracle.NewInsert().ByStandard("OracleTable4Insert", map[string]interface{}{"Field1": "78", "Field2": 90}, "RuleField"))
}

func TestInsertStatement_ByQuery(t *testing.T) {
	pg := CreatePGInstance()
	subQuery := pg.NewSelect()
	subQuery1, _, _, _ := pg.NewSelect().ByStandard("TargetTable", nil, nil, nil, "F4,F5,F6")
	subQuery.mainTable = "subTable"
	log.Println(pg.NewInsert().ByQuery("PGTable4InsertByQuery", subQuery, "RuleField", "F1", "F2", "F3"))
	log.Println(pg.NewInsert().ByQuery("PGTable4InsertByQuery", subQuery, "RuleField"))
	log.Println(pg.NewInsert().ByQuery("PGTable4InsertByQuery", subQuery1, "RuleField"))
}

func TestSelectStatement_ByStandard(t *testing.T) {
	pg := CreatePGInstance()
	log.Println(pg.NewSelect().ByStandard("PGTable", nil, nil, nil, "Field1", "Field2", "Field3"))
}

func TestSelectStatement_ByConditions(t *testing.T) {
	// PostgreSQL
	pg := CreatePGInstance()
	cd := pg.NewConditions()
	cd.Append(map[string]interface{}{"CF1=": 1, "CF2>": 99, "CF3<": 200})
	log.Println(pg.NewSelect().ByConditions("PGTable4Condition", cd, nil, nil, nil))

	// MySQL
	mysql := CreateMySQLInstance()
	cd1 := mysql.NewConditions()
	cd1.Append(map[string]interface{}{"CF4=": 7, "CF5>": 100, "CF6<": 1000})
	log.Println(mysql.NewSelect().ByConditions("MySQLTable4Condition", cd1, nil, nil, nil, "F1", "F2", "F3", "F4"))

	//SQL Server
	mssql := CreateMSSQLInstance()
	cd2 := mssql.NewConditions()
	cd2.Append(map[string]interface{}{"CF7=": "hello world", "CF8>": 99})
	log.Println(mssql.NewSelect().ByConditions("MSSQLTable4Condition", cd2, nil, nil, nil))

	//Oracle
	oracle := CreateOracleInstance()
	cd3 := oracle.NewConditions()
	cd3.Append(map[string]interface{}{"CF9=": "golang", "CF10<": 99999})
	log.Println(oracle.NewSelect().ByConditions("OracleTable4Condition", cd3, nil, nil, nil))

	// Create by condition with sub query
	pg1 := CreatePGInstance()
	query, _, _, _ := pg1.NewSelect().ByStandard("SubQueryTable", nil, nil, nil, "SF1", "SF2", "SF3") //Create sub query for condition
	// Create condition and init through field-value set and sub query
	cdq := pg1.NewConditions()
	cdq.Append(map[string]interface{}{"CF11=": 1, "CF12=": "ABCA"}, map[string]*SelectStatement{"CF13 IN": query})
	queryMain := pg1.NewSelect()
	log.Println(queryMain.ByConditions("maintable", cdq, nil, nil, nil))
}

func TestSelectStatement_ByJoinOn(t *testing.T) {
	//PostgreSQL
	pg := CreatePGInstance()
	// The linked fields quantity for JOIN ON clause should be same as joined table
	// Be care for the join type it should be left,right inner and full
	log.Println(pg.NewSelect().ByJoinOn("PGTable4JoinOn", "left", []string{"JoinTable1", "JoinTable2", "JoinTable3"}, map[string]string{"id": "id", "id1": "id1", "id2": "id2"}))
}

func TestSelectStatement_ByJoinUsing(t *testing.T) {
	pg := CreatePGInstance()
	joinFields1 := make([]string, 0)
	joinFields1 = append(joinFields1, "JF1", "JF2", "JF3", "JF4")
	joinFields2 := make([]string, 0)
	joinFields2 = append(joinFields2, "JF5", "JF6", "JF7", "JF8")
	log.Println(pg.NewSelect().ByJoinUsing("PGTable4JoinUsing", "right", map[string][]string{"JoinTable4": joinFields1, "JoinTable5": joinFields2}))
}
