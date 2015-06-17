package main
 
import (
    "database/sql"
    // _ "github.com/go-sql-driver/mysql"
    _ "github.com/lib/pq"
    "log"
    "math/rand"
    // "time"
    "flag"
    // "sort"
    "os"
    "fmt"
    "strings"
    "C"
    "strconv"
    "io/ioutil"
    // . "github.com/ahmetalpbalkan/go-linq"
)


 
//export RsDb
type RsDb struct {
    db *sql.DB
    database_name string
    schema_name string
    owner string
    
}

// func (RD RsDb) in_config(param string, dest string, seed int) bool {
//     return IncludeString(RD.config.params, param) && IncludeString(RD.config.dests, dest) && IncludeInt(RD.config.seeds, seed) 
// }

func (RD RsDb) get_sid(param string, dest string, seed int) (int, error) {
    sql := fmt.Sprintf("SELECT id FROM %[1]s.setting where param = $1 and dest = $2 and seed = $3", RD.schema_name)
    rows, err := RD.db.Query(sql, param, dest, seed)

    if err != nil {
        log.Println(err)
        os.Exit(2)
    }

    if rows.Next() {
        var id int
        err = rows.Scan(&id)
        if err != nil {
            return -1, err
        }

        return id, nil
    }
    return -1, fmt.Errorf("can't find any param=%s, dest=%s, seed=%d", param, dest, seed)

}

func (RD RsDb) insert_cwnd(param string, dest string, seed int, client_id int, time float64, value float64) bool {
    return RD.insert("cwnd", param, dest, seed, client_id, time, value, "")
}

func (RD RsDb) insert_sentbytes(param string, dest string, seed int, client_id int, time float64, value float64) bool {
    return RD.insert("sentbytes", param, dest, seed, client_id, time, value, "")
}

func (RD RsDb) insert_updown(param string, dest string, seed int, client_id int, time float64, value float64, note string) bool {
    return RD.insert("updown", param, dest, seed, client_id, time, value, note)
}

func (RD RsDb) insert_simset(param string, dest string, seed int, note string) bool {
    return RD.insert("simset", param, dest, seed, 0, 0, 0, note)
}

func (RD RsDb) insert(table_name string, param string, dest string, seed int, client_id int, time float64, value float64, note string) bool {
    
    // if ! RD.in_config(param, dest, seed) {
    //     return false
    // }

    sid, err := RD.get_sid(param, dest, seed)

    if err != nil {
        log.Println(err)
        os.Exit(2)
    }


    sql := fmt.Sprintf(`insert into %[1]s.%[2]s(sid, client_id, time, value, note) 
        values ( $1, $2, $3, $4, $5 );
    `, RD.schema_name, table_name)

    stmt, err := RD.db.Prepare(sql)
    defer stmt.Close()
 
    if err != nil {
        log.Println(err)
        os.Exit(2)
    }

    stmt.Exec(sid, client_id, time, value, note)
    return true
}

func (RD RsDb) initialize(config RsConfig) bool {

    RD.create_schema()
    RD.create_table_setting(config)
    RD.create_table_cwnd()
    RD.create_table_updown()
    RD.create_table_sentbytes()
    RD.create_table_simset()
    return true
}



func (RD RsDb) create_schema() bool {
    if RD.exist_schema(RD.schema_name) {
        log.Println("exist schema: ", RD.schema_name)
        os.Exit(2)
    }

    sql := fmt.Sprintf("create schema %s ;", RD.schema_name)
    RD.multiline_prepare_exec(sql)

    return true
}


func (RD RsDb) create_table_cwnd() bool {
    return RD.create_table_template("cwnd")
}

func (RD RsDb) create_table_updown() bool {
    return RD.create_table_template("updown")
}

func (RD RsDb) create_table_sentbytes() bool {
    return RD.create_table_template("sentbytes")
}

func (RD RsDb) create_table_simset() bool {
    return RD.create_table_template("simset")
}

func (RD RsDb) create_table_template(table_name string) bool {

    if RD.exist_table(table_name) {
        return false
    }

    sqls := fmt.Sprintf(
    `DROP TABLE IF EXISTS "%[1]s"."%[3]s";
    CREATE TABLE "%[1]s"."%[3]s" (
        "sid" int4 NOT NULL,
        "client_id" int8 NOT NULL,
        "time" numeric,
        "value" numeric,
        "note" varchar
    )
    WITH (OIDS=FALSE);
    ALTER TABLE "%[1]s"."%[3]s" OWNER TO "%[2]s";`, RD.schema_name, RD.owner, table_name)
    
    RD.multiline_prepare_exec(sqls)

    return true
}

func (RD RsDb) create_table_setting(config RsConfig) bool {

    if RD.exist_table("setting") {
        return false
    }

    sqls := fmt.Sprintf(
    `CREATE TABLE "%[1]s"."setting" (
        "id" int4 NOT NULL,
        "param" varchar NOT NULL COLLATE "default",
        "dest" varchar NOT NULL COLLATE "default",
        "seed" int4 NOT NULL
    )
    WITH (OIDS=FALSE);
    ALTER TABLE "%[1]s"."setting" OWNER TO "%[2]s";
    ALTER TABLE "%[1]s"."setting" ADD PRIMARY KEY ("id") NOT DEFERRABLE INITIALLY IMMEDIATE;`, RD.schema_name, RD.owner)
    
    RD.multiline_prepare_exec(sqls)

    sql := fmt.Sprintf("insert into %s.setting(id, param, dest, seed) values ($1, $2, $3, $4)", RD.schema_name)
    stmt, err := RD.db.Prepare(sql)
    defer stmt.Close()
 
    if err != nil {
        log.Println(err)
        os.Exit(2)
    }

    id := 0
    for _, seed := range config.seeds {
        for _, param := range config.params {
            for _, dest := range config.dests {
                stmt.Exec(id, param, dest, seed)
                id++
            }
        }
    }

    return true
}


func (RD RsDb) multiline_prepare_exec(sqls string) bool {
    tx, err := RD.db.Begin()

    if err != nil {
        log.Println(err)
        os.Exit(2)
    }

    _sqls := strings.Split(sqls, ";")
    for _, sql := range _sqls  {

        // log.Println(sql)
        stmt, err := tx.Prepare(sql)
        defer stmt.Close()
     
        if err != nil {
            log.Println(err)
            os.Exit(2)
        }
        stmt.Exec()
    }
    tx.Commit()
    return true
}


func (RD RsDb) exist_schema(schema_name string) bool {
    rows, err := RD.db.Query("select 1 from information_schema.schemata where schema_name=$1", schema_name)

    if err != nil {
        log.Println(err)
        os.Exit(2)
    }

    if rows.Next() {
        return true
    }
    return false
}

func (RD RsDb) exist_table(table_name string) bool {
    rows, err := RD.db.Query("select 1 from information_schema.tables where table_schema=$1 and table_name=$2", RD.schema_name, table_name)

    // fmt.Printf("select 1 from information_schema.tables where table_schema = %s and table_name = %s\n", RD.schema_name, table_name)

    if err != nil {
        log.Println(err)
        os.Exit(2)
    }

    if rows.Next() {
        return true
    }
    return false
}

func (RD RsDb) create(db_name string, conf RsConfig) {
    // stmt, err := db.Prepare("INSERT INTO user(username, password) VALUES(?, ?)")
    stmt, err := RD.db.Prepare("create database if not exists ")
    defer stmt.Close()
 
    if err != nil {
        log.Println(err)
        return
    }
    stmt.Exec("guotie", rand.Int31n(100))
    // stmt.Exec("guotie", "guotie")
    // stmt.Exec("testuser", "123123")
 
}

func NewRsDB(database_uri string, database_name string, schema_name string) *RsDb {

    // database_name := ""

    // uri := fmt.Sprintf("postgres://postgres:mysim1234@localhost/test_123?sslmode=disable")
    db, err := sql.Open("postgres", database_uri)
    if err != nil {
        log.Fatalf("Open database error: %s\n", err)
    }
    // defer db.Close()
 
    err = db.Ping()
    if err != nil {
        log.Fatal(err)
    }

    return &RsDb{
        db: db,
        database_name: database_name,
        schema_name: schema_name,
        owner: "postgres",
    }
}



// func (t T) to_i() int {
//     if t.type != string {
//         log.Fatal(err)
//     }

//     i, err := strconv.Atoi(s)
//     if err != nil {
//         // handle error
//         fmt.Println(err)
//         os.Exit(2)
//     }
//     return i
// }



type RsConfig struct {
    params []string
    seeds []int
    dests []string
}


// type StringArray []string

func IncludeString (ss []string, s string) bool {
    for _, v := range ss {
        if v == s {
            return true
        }
    }
    return false
}

func IncludeInt (ss []int, s int) bool {
    for _, v := range ss {
        if v == s {
            return true
        }
    }
    return false
}

type StringArray []string
 
func main() {

    var action string
    var schema string
    var param string
    var dest string 
    var seed int
    var client_id int
    var rstime float64
    var rsvalue float64
    var note string 
    // var actions = []string{"init", "insert"}


    var params string
    var seeds string
    var dests string

    flag.StringVar(&params, "params", "", "params")
    flag.StringVar(&seeds, "seeds", "", "seeds")
    flag.StringVar(&dests, "dests", "", "dests")

    flag.StringVar(&action, "action", "", "action of doing on database")
    flag.StringVar(&schema, "schema", "", "schema")
    flag.StringVar(&param, "param", "", "param")
    flag.StringVar(&dest, "dest", "", "dest")
    flag.StringVar(&note, "note", "", "note")
    flag.IntVar(&seed, "seed", -1, "seed")
    flag.IntVar(&client_id, "client_id", -1, "client_id")
    flag.Float64Var(&rstime, "time", -1, "time")
    flag.Float64Var(&rsvalue, "value", -1, "value")
    flag.Parse()

    dat, err := ioutil.ReadFile("godb.conf")
    if err != nil {
        panic(err)
    }

    fmt.Print(string(dat))

    conf := make(map[string]string)
    for _, v := range strings.Split(string(dat), "\n") {
        // fmt.Println(v+"--")
        kv := strings.Split(v, "=")

        conf[ strings.Trim(kv[0], " \r\n") ] = strings.Trim(kv[1], " \r\n")
    }
    conf["database_uri"] = conf["database_uri"]+conf["database_name"]+"?sslmode=disable"
    // fmt.Println(conf["database_uri"])

    

    // config := RsConfig{seeds: []int{0,1}, params: []string{"a", "b"}, dests: []string{"p", "q"}}
    
    RD := *NewRsDB(conf["database_uri"], conf["database_name"], schema)
    // RD.insert_cwnd()
    // log.Println(RD.exist_schema("hi"))
    // log.Println(RD.exist_schema("dddd"))
    // log.Println(RD.exist_table("eee"))

    // log.Println(RD.create_table_setting())
    // log.Println(RD.in_config(config.params[0], config.dests[0], config.seeds[0]))
    // log.Println(RD.in_config(config.params[0], "bb", 1))

    // log.Println(RD.insert_cwnd(config.params[0], config.dests[0], config.seeds[0], 0, 1.1, 2.2))



    log.Println("action =", action)
    switch action {
    case "init":

        if (params == "" || dests == "" || seeds == "") {
            log.Println(`params == "" || dests == "" || seeds == ""`)
            os.Exit(2)
        }

        params_a := strings.Split(params, ",")
        dests_a := strings.Split(dests, ",")
        seeds_a := []int{}
        for _, v := range strings.Split(seeds, ",") {
            i, err := strconv.Atoi(v)
            if err != nil {
                fmt.Println(err)
                os.Exit(2)
            }
            seeds_a = append(seeds_a, i)
        }
        config := RsConfig{seeds: seeds_a, params: params_a, dests: dests_a}

        RD.initialize(config)
        // RD.create_table_cwnd()
        // insert(db)
    case "insert_cwnd":
        if param == "" || dest == "" || seed == -1 || client_id == -1 {
            log.Println(`param == "" || dest == "" || seed == -1 || client_id == -1`)
            os.Exit(2)
        }
        RD.insert_cwnd(param, dest, seed, client_id, rstime, rsvalue)
    case "insert_sentbytes":
        if param == "" || dest == "" || seed == -1 || client_id == -1 {
            log.Println(`param == "" || dest == "" || seed == -1 || client_id == -1`)
            os.Exit(2)
        }
        RD.insert_sentbytes(param, dest, seed, client_id, rstime, rsvalue)
    case "insert_updown":
        if param == "" || dest == "" || seed == -1 || client_id == -1 || note == "" {
            log.Println(`param == "" || dest == "" || seed == -1 || client_id == -1 || note == ""`)
            os.Exit(2)
        }
        RD.insert_updown(param, dest, seed, client_id, rstime, rsvalue, note)
    case "insert_simset":
        if param == "" || dest == "" || seed == -1 || note == "" {
            log.Println(`param == "" || dest == "" || seed == -1 || note == ""`)
            os.Exit(2)
        }
        RD.insert_simset(param, dest, seed, note)
    case "exist_schema":
        if schema == "" {
            log.Println(`schema == ""`)
            os.Exit(2)
        }

        RD.exist_schema(schema)


    default:
        log.Printf("no this action '%s'\n", action)
    }
    
    

 
    // log.Println(time.Now().UTC().UnixNano())
    // rand.Seed( time.Now().UTC().UnixNano() )
    // for i:=1; i<10; i++ {
    //     log.Println(rand.Int31n(100))
    // }

    // insert(db)
 
    // rows, err := db.Query("select id, username from user where id = ?", 1)
    // rows, err := db.Query("select TABLE_NAME from information_schema.tables")
    // if err != nil {
    //     log.Println(err)
    // }
 
    // defer rows.Close()
    // var table_name string

    // for rows.Next() {
    //     err := rows.Scan(&table_name)
    //     if err != nil {
    //         log.Fatal(err)
    //     }
    //     log.Println(table_name)
    // }


    // var id int
    // var name string
    // for rows.Next() {
    //     err := rows.Scan(&id, &name)
    //     if err != nil {
    //         log.Fatal(err)
    //     }
    //     log.Println(id, name)
    // }
 
    // err = rows.Err()
    // if err != nil {
    //     log.Fatal(err)
    // }
}

