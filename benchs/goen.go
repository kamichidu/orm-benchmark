package benchs

import (
	"database/sql"
	"fmt"

	"github.com/kamichidu/goen"
	_ "github.com/kamichidu/goen/dialect/postgres"
)

var goendb *sql.DB

func init() {
	st := NewSuite("goen")
	st.InitF = func() {
		st.AddBenchmark("Insert", 2000*ORM_MULTI, GoenInsert)
		st.AddBenchmark("MultiInsert 100 row", 500*ORM_MULTI, GoenInsertMulti)
		st.AddBenchmark("Update", 2000*ORM_MULTI, GoenUpdate)
		st.AddBenchmark("Read", 4000*ORM_MULTI, GoenRead)
		st.AddBenchmark("MultiRead limit 100", 2000*ORM_MULTI, GoenReadSlice)

		db, err := sql.Open("postgres", ORM_SOURCE)
		checkErr(err)
		goendb = db
	}
}

func GoenInsert(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
	})
	for i := 0; i < b.N; i++ {
		dbc := goen.NewDBContext("postgres", goendb)
		dbc.Patch(goen.InsertPatch("models", []string{
			"name",
			"title",
			"fax",
			"web",
			"age",
			"right",
			"counter",
		}, []interface{}{
			m.Name,
			m.Title,
			m.Fax,
			m.Web,
			m.Age,
			m.Right,
			m.Counter,
		}))
		if err := dbc.SaveChanges(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GoenInsertMulti(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
	})
	for i := 0; i < b.N; i++ {
		dbc := goen.NewDBContext("postgres", goendb)
		dbc.Compiler = &goen.BulkCompilerOptions{
			MaxPatches: 1000,
		}
		for j := 0; j < 100; j++ {
			dbc.Patch(goen.InsertPatch("models", []string{
				"name",
				"title",
				"fax",
				"web",
				"age",
				"right",
				"counter",
			}, []interface{}{
				m.Name,
				m.Title,
				m.Fax,
				m.Web,
				m.Age,
				m.Right,
				m.Counter,
			}))
		}
		if err := dbc.SaveChanges(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func goenInsert(m *Model) error {
	_, err := goendb.Exec(rawInsertSQL, m.Name, m.Title, m.Fax, m.Web, m.Age, m.Right, m.Counter)
	if err != nil {
		return err
	}
	return nil
}

func GoenUpdate(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		goenInsert(m)
	})
	for i := 0; i < b.N; i++ {
		dbc := goen.NewDBContext("postgres", goendb)
		dbc.Patch(goen.UpdatePatch("models", []string{
			"name",
			"title",
			"fax",
			"web",
			"age",
			"right",
			"counter",
		}, []interface{}{
			m.Name,
			m.Title,
			m.Fax,
			m.Web,
			m.Age,
			m.Right,
			m.Counter,
		}, &goen.MapRowKey{
			Table: "models",
			Key: map[string]interface{}{
				"id": m.Id,
			},
		}))
		if err := dbc.SaveChanges(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GoenRead(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		if err := goenInsert(m); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	})
	for i := 0; i < b.N; i++ {
		dbc := goen.NewDBContext("postgres", goendb)
		var models []Model
		rows, err := dbc.Query(`SELECT id, name, title, fax, web, age, "right", counter FROM models WHERE id = $1 LIMIT 1`, 1)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		err = dbc.Scan(rows, &models)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		if err = rows.Err(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		if err = rows.Close(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}

func GoenReadSlice(b *B) {
	var m *Model
	wrapExecute(b, func() {
		initDB()
		m = NewModel()
		for i := 0; i < 100; i++ {
			if err := goenInsert(m); err != nil {
				fmt.Println(err)
				b.FailNow()
			}
		}
	})
	for i := 0; i < b.N; i++ {
		dbc := goen.NewDBContext("postgres", goendb)
		var models []Model
		rows, err := dbc.Query(`SELECT id, name, title, fax, web, age, "right", counter FROM models WHERE id > 0 LIMIT 100`)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		err = dbc.Scan(rows, &models)
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		if err = rows.Err(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
		if err = rows.Close(); err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
}
