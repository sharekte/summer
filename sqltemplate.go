package sqlbuilder

var (
	sql4insert    string = `INSERT INTO %s %s VALUES %s ON CONFLICT (%s) DO NOTHING`
	sql4insertq   string = `INSERT INTO %s%s %s ON CONFLICT (%s）DO NOTHING`
	sql4updaten   string = `UPDATE %s SET %s`
	sql4updatew   string = `UPDATE %s SET %s%s`
	sql4deleteall string = `DELETE * FROM %s`
	sql4deleteone string = `DELETE * FROM %s%s`
	sql4select    string = `SELECT %s FROM %s%s`
)
