package sqlbuilder

import "errors"

var (
	model_sqlbuilder_table_none              = errors.New("Don't find the table or empty table name")
	model_sqlbuilder_fields_linked_quantity  = errors.New("Wrong quantity of joined fields,it should be same as linked table's")
	model_sqlbuilder_fields_fvset_none       = errors.New("Don't find fields and values set")
	model_sqlbuilder_fields_conflict_none    = errors.New("Cannot create Insert without conflict field")
	model_sqlbuilder_conditions_none         = errors.New("Cannot create clause without conditions")
	model_sqlbuilder_linked_fields_none      = errors.New("Cannot create Join Using clause without linked field(s)")
	model_sqlbuilder_conditions_keyword_none = errors.New("Cannot find AND or OR keyword between each condition")
	model_sqlbuilder_join_point_none         = errors.New("Cannot find point(.) between teable and filed in conditions of join clause")
	model_sqlbuilder__nonefield              = errors.New("Cannot find field or tables to check")
	model_sqlbuilder_join_nopoint            = errors.New("Don't find point(.) on the clause")
	model_sqlbuilder_join_wrongTableorField  = errors.New("The table name with fields should be in the joined tables list")
)
