# sqlite
A pure-go wrapper for database/sql using logoove/sqlite

## Usage

```go
type row struct {
    id int // pk
    name string
}

db := &sql.Sqlite{DBPath: "demo.db"}
err := db.Open()
if err != nil {
    panic(err)
}

err = db.Create("demotable", &row{})
if err != nil {
    panic(err)
}

err = db.Insert("demotable", &row{id: 123, name: "Anna"})
if err != nil {
    panic(err)
}

var r row
err = db.Find("demotable", &r, "WHERE id=123")
if err != nil {
    panic(err)
}
fmt.Println(r)
```