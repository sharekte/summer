# summer
This is the simply SQL assembler for SELECT,INSERT,UPDATE,DELETE.You can create them from some parameter given, in the meantime it can return parameter values also.Save the step to call database funtion on Golang.There are some example as below.

##1.Create single SELECT statement without WHERE clause
```golang
	package main
	import "fmt"
	func main(){
	    pg := CreatePGInstance()
	    fmt.Println(pg.NewSelect().ByStandard("PGTable", nil, nil, nil, "Field1", "Field2", "Field3"))
	}
```
It will return_SELECT Field1,Field2,Field3 FROM PGTable_


##2.Create SELECT statement with WHERE clause
```golang
	package main
	import "fmt"
	func main(){
	    pg := CreatePGInstance()
	    cd := pg.NewConditions()
	    cd.Append(map[string]interface{}{"CF1=": 1, "CF2>": 99, "CF3<": 200})
	    fmt.Println(pg.NewSelect().ByConditions("PGTable4Condition", cd, nil, nil, nil))
	}

```
It will return _SELECT * FROM PGTable4Condition WHERE CF1=$1 AND CF2>$2 AND CF3<$3 [1 99 200]_,you can find the value slice _[1,99,200]_ put it to Golang database funtions that need to call directly.

Most example you can find in builder_test.go.Please let me know if you find some issue or give suggestion.Call me by mail neo_yan@outlook.com
Enjoy!!!
