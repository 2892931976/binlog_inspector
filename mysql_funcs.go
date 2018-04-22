package main

import (
	"database/sql"
	"encoding/json"
	"strings"

	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/toolkits/file"
	sliceKits "github.com/toolkits/slice"
)

const (
	//PRIMARY_KEY_LABLE = "primary"
	//UNIQUE_KEY_LABLE  = "unique"
	KEY_BINLOG_POS_SEP = "/"
	KEY_DB_TABLE_SEP   = "."
	KEY_NONE_BINLOG    = "_"

	/*
		KEY_DDL_BINLOG = "binlog"
		KEY_DDL_SPOS   = "startpos"
		KEY_DDL_EPOS   = "stoppos"
	*/
)

type DdlPosInfo struct {
	Binlog   string `json:"binlog"`
	StartPos uint32 `json:"start_position"`
	StopPos  uint32 `json:"stop_position"`
	DdlSql   string `json:"ddl_sql"`
}

//type FieldInfo map[string]string //{"name":"col1", "type":"int"}

type FieldInfo struct {
	FieldName string `json:"column_name"`
	FieldType string `json:"column_type"`
}

type KeyInfo []string //{colname1, colname2}

type TblInfoJson struct {
	Database   string      `json:"database"`
	Table      string      `json:"table"`
	Columns    []FieldInfo `json:"columns"`
	PrimaryKey KeyInfo     `json:"primary_key"`
	UniqueKeys []KeyInfo   `json:"unique_keys"`
	DdlInfo    DdlPosInfo  `json:"ddl_info"`
}

type TablesColumnsInfo struct {
	//lock       *sync.RWMutex
	tableInfos map[string]map[string]*TblInfoJson //{db.tb:{binlog/startpos/stoppos:TblInfoJson}}
}

var (
	primaryUniqueKeysSql string = `
		select k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE
		from information_schema.TABLE_CONSTRAINTS as c inner join information_schema.KEY_COLUMN_USAGE as k on
		c.CONSTRAINT_NAME = k.CONSTRAINT_NAME and c.table_schema = k.table_schema and c.table_name=k.table_name
		where c.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE') and c.table_schema=? and c.table_name=?
		order by k.CONSTRAINT_NAME asc, k.ORDINAL_POSITION asc
	`
	primaryUniqueKeysSqlBatch string = `
		select k.table_schema, k.table_name, k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE, k.ORDINAL_POSITION
		from information_schema.TABLE_CONSTRAINTS as c inner join information_schema.KEY_COLUMN_USAGE as k on
		c.CONSTRAINT_NAME = k.CONSTRAINT_NAME and c.table_schema = k.table_schema and c.table_name=k.table_name
		where c.CONSTRAINT_TYPE in ('PRIMARY KEY', 'UNIQUE') and c.table_schema in (%s) and c.table_name in (%s)
		order by k.table_schema asc, k.table_name asc, k.CONSTRAINT_NAME asc, k.ORDINAL_POSITION asc
	`
	columnNamesTypesSql string = `
		select COLUMN_NAME, DATA_TYPE from information_schema.columns
		where table_schema=? and table_name=?
		order by ORDINAL_POSITION asc
	`

	columnNamesTypesSqlBatch string = `
		select table_schema, table_name, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION from information_schema.columns
		where table_schema in (%s) and table_name in (%s)
		order by table_schema asc, table_name asc, ORDINAL_POSITION asc
	`
	KEY_NONE_POS     uint32 = 0
	NoneBinlogPosKey string = GetBinlogPosAsKey(KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)

	G_TablesColumnsInfo TablesColumnsInfo
)

func (this TablesColumnsInfo) GetTableInfoJsonOfBinPos(schema string, table string, binlog string, spos uint32, epos uint32) (*TblInfoJson, error) {
	/*
		如果有非默认的表结构， 则找出所有非默认的表结构中binlogpos小于当前event并且所有非默认表结构中binlogpos最小的来使用。因为这个表结构是相应的DDL前的表结构。
		对于一个binlog中最后一次DDL， 则可以增加一个{binlog+1,4,4}的表结构， 或者使用默认的（之后没有DDL的情况）
	*/
	myPos := mysql.Position{Name: binlog, Pos: epos}
	tbKey := GetAbsTableName(schema, table)
	//binlogKey := GetBinlogPosAsKey()
	tbDefsArr, ok := this.tableInfos[tbKey]

	if !ok {
		return &TblInfoJson{}, fmt.Errorf("table struct not found for %s, maybe it was dropped. Skip it, binlog position info: %s", tbKey, myPos.String())
	}

	var nearestKey string = ""
	var cmpResult int
	for k, oneTbJson := range tbDefsArr {
		if oneTbJson.DdlInfo.Binlog == KEY_NONE_BINLOG || oneTbJson.DdlInfo.StartPos == KEY_NONE_POS {
			continue
		}
		ddlPos := mysql.Position{Name: oneTbJson.DdlInfo.Binlog, Pos: oneTbJson.DdlInfo.StartPos}
		if myPos.Compare(ddlPos) < 1 {
			if nearestKey == "" {
				nearestKey = k
			} else {
				cmpResult = ddlPos.Compare(mysql.Position{Name: tbDefsArr[nearestKey].DdlInfo.Binlog,
					Pos: tbDefsArr[nearestKey].DdlInfo.StartPos})
				if cmpResult == -1 {
					nearestKey = k
				}
			}
		}

	}
	if nearestKey != "" {
		return tbDefsArr[nearestKey], nil
	} else {
		return tbDefsArr[GetBinlogPosAsKey(KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)], nil
	}

}

func GetAndMergeColumnStructFromJsonFileAndDb(cfg ConfCmd, fromFile *TablesColumnsInfo) {
	//get table columns from DB
	sqlUrl := GetMysqlUrl(cfg)
	SqlCon, err := CreateMysqlCon(sqlUrl)
	CheckErr(err, "fail to connect to mysql", ERR_MYSQL_CONNECTION, true)
	allTables := GetAllTableNames(SqlCon, cfg)

	fromFile.GetAllTableFieldsFromDb(SqlCon, allTables, 50)
	fromFile.GetAllTableKeysInfoFromDb(SqlCon, allTables, 50)
	//tbInfo := &TablesColumnsInfo{}
	/*
		for db, tbArr := range allTables {
			for _, tb := range tbArr {
				fromFile.GetTableFieldsFromDb(SqlCon, db, tb)
				fromFile.GetTableKeysInfoFromDb(SqlCon, db, tb)
			}
		}
	*/
	/*
		for _, oneTb := range fromFile.tableInfos {
			for _, oneBin := range oneTb {
				tbKey := GetAbsTableName(oneBin.Database, oneBin.Table)
				binPosKey := GetBinlogPosAsKey(oneBin.DdlInfo.Binlog, oneBin.DdlInfo.StartPos, oneBin.DdlInfo.StopPos)
				tbInfo.CheckAndCreateTblKey(oneBin.Database, oneBin.Table, oneBin.DdlInfo.Binlog, oneBin.DdlInfo.StartPos, oneBin.DdlInfo.StopPos)
				// whatever, overwrite
				tbInfo.tableInfos[tbKey][binPosKey] = oneBin
			}

		}
	*/
	//return *fromFile

}

func GetColIndexFromKey(ki KeyInfo, columns []FieldInfo) []int {
	arr := make([]int, len(ki))
	for j, colName := range ki {
		for i, f := range columns {
			if f.FieldName == colName {
				arr[j] = i
				break
			}
		}
	}
	return arr
}

func (this TblInfoJson) JsonDumpStr() string {
	bArr, err := json.MarshalIndent(this, "", "\t")
	CheckErr(err, "error when dump to json string of "+this.Database+"."+this.Table, ERR_JSON_MARSHAL, false)
	return string(bArr)
}

func (this TblInfoJson) GetOneUniqueKey() KeyInfo {

	if len(this.PrimaryKey) > 0 {
		return this.PrimaryKey
	} else if len(this.UniqueKeys) > 0 {
		return this.UniqueKeys[0]
	} else {
		return KeyInfo{}
	}
}

func (this *TablesColumnsInfo) CheckAndCreateTblKey(schema, table, binlog string, spos, epos uint32) bool {
	if len(this.tableInfos) < 1 {
		this.tableInfos = map[string]map[string]*TblInfoJson{}
	}
	tbKey := GetAbsTableName(schema, table)
	_, ok := this.tableInfos[tbKey]
	if !ok {
		this.tableInfos[tbKey] = map[string]*TblInfoJson{}
	}
	binPosKey := GetBinlogPosAsKey(binlog, spos, epos)
	_, ok = this.tableInfos[tbKey][binPosKey]
	/*
		if !ok {
			this.tableInfos[tbKey][binPosKey] = TblInfoJson{}
		}
	*/
	return ok
}

func (this *TablesColumnsInfo) GetTableFieldsFromDb(db *sql.DB, schema string, table string) error {

	rows, err := db.Query(columnNamesTypesSql, schema, table)
	if err != nil {
		CheckErr(err, "fail to query columns info of "+schema+"."+table, ERR_MYSQL_QUERY, false)
		return err
	}

	defer rows.Close()

	var colName, dataType string
	var tbInfo []FieldInfo
	for rows.Next() {
		err := rows.Scan(&colName, &dataType)

		if err != nil {
			CheckErr(err, "fail to get query result of columns info of "+schema+"."+table, ERR_MYSQL_QUERY, false)
			return err
		}
		tbInfo = append(tbInfo, FieldInfo{FieldName: colName, FieldType: dataType})

	}

	ok := this.CheckAndCreateTblKey(schema, table, KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
	tbKey := GetAbsTableName(schema, table)
	binPosKey := GetBinlogPosAsKey(KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
	if ok {
		this.tableInfos[tbKey][binPosKey].Columns = tbInfo
	} else {
		this.tableInfos[tbKey][binPosKey] = &TblInfoJson{
			Database: schema, Table: table, Columns: tbInfo,
			DdlInfo: DdlPosInfo{Binlog: KEY_NONE_BINLOG, StartPos: KEY_NONE_POS, StopPos: KEY_NONE_POS, DdlSql: ""},
			//DdlInfo: map[string]string{KEY_DDL_BINLOG: KEY_NONE_BINLOG, KEY_DDL_SPOS: KEY_NONE_POS, KEY_DDL_EPOS: KEY_NONE_POS},
		}
	}

	return nil
}

func (this *TablesColumnsInfo) GetAllTableFieldsFromDb(db *sql.DB, dbTbs map[string][]string, batchCnt int) error {
	querySqls := GetFieldOrKeyQuerySqls(columnNamesTypesSqlBatch, dbTbs, batchCnt)
	var dbName, tbName, colName, dataType string
	var colPos int
	var ok bool
	var dbTbFieldsInfo map[string]map[string][]FieldInfo = map[string]map[string][]FieldInfo{}
	for _, oneQuery := range querySqls {

		rows, err := db.Query(oneQuery)
		if err != nil {
			CheckErr(err, "fail to query mysql: "+oneQuery, ERR_MYSQL_QUERY, false)
			return err
		}

		for rows.Next() {
			err := rows.Scan(&dbName, &tbName, &colName, &dataType, &colPos)

			if err != nil {
				CheckErr(err, "fail to read result of query:"+oneQuery, ERR_MYSQL_QUERY, false)
				return err
			}
			_, ok = dbTbFieldsInfo[dbName]
			if !ok {
				dbTbFieldsInfo[dbName] = map[string][]FieldInfo{}
			}
			_, ok = dbTbFieldsInfo[dbName][tbName]
			if !ok {
				dbTbFieldsInfo[dbName][tbName] = []FieldInfo{}
			}
			dbTbFieldsInfo[dbName][tbName] = append(dbTbFieldsInfo[dbName][tbName], FieldInfo{FieldName: colName, FieldType: dataType})

		}
		rows.Close()

	}
	for dbName, _ = range dbTbFieldsInfo {
		for tbName, tbInfo := range dbTbFieldsInfo[dbName] {
			ok = this.CheckAndCreateTblKey(dbName, tbName, KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
			tbKey := GetAbsTableName(dbName, tbName)
			binPosKey := GetBinlogPosAsKey(KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
			if ok {
				this.tableInfos[tbKey][binPosKey].Columns = tbInfo
			} else {
				this.tableInfos[tbKey][binPosKey] = &TblInfoJson{
					Database: dbName, Table: tbName, Columns: tbInfo,
					DdlInfo: DdlPosInfo{Binlog: KEY_NONE_BINLOG, StartPos: KEY_NONE_POS, StopPos: KEY_NONE_POS, DdlSql: ""}}
			}
		}
	}

	return nil
}

func (this *TablesColumnsInfo) GetAllTableKeysInfoFromDb(db *sql.DB, dbTbs map[string][]string, batchCnt int) error {

	querySqls := GetFieldOrKeyQuerySqls(primaryUniqueKeysSqlBatch, dbTbs, batchCnt)
	var dbName, tbName, kName, colName, ktype string
	var colPos int
	var ok bool
	var dbTbKeysInfo map[string]map[string]map[string]KeyInfo = map[string]map[string]map[string]KeyInfo{}
	var primaryKeys map[string]map[string]map[string]bool = map[string]map[string]map[string]bool{}
	for _, oneQuery := range querySqls {

		rows, err := db.Query(oneQuery)
		if err != nil {
			CheckErr(err, "fail to query mysql: "+oneQuery, ERR_MYSQL_QUERY, false)
			return err
		}

		for rows.Next() {
			//select k.table_schema, k.table_name, k.CONSTRAINT_NAME, k.COLUMN_NAME, c.CONSTRAINT_TYPE, k.ORDINAL_POSITION
			err := rows.Scan(&dbName, &tbName, &kName, &colName, &ktype, &colPos)

			if err != nil {
				CheckErr(err, "fail to read result of query:"+oneQuery, ERR_MYSQL_QUERY, false)
				return err
			}
			_, ok = dbTbKeysInfo[dbName]
			if !ok {
				dbTbKeysInfo[dbName] = map[string]map[string]KeyInfo{}
			}
			_, ok = dbTbKeysInfo[dbName][tbName]
			if !ok {
				dbTbKeysInfo[dbName][tbName] = map[string]KeyInfo{}
			}
			_, ok = dbTbKeysInfo[dbName][tbName][kName]
			if !ok {
				dbTbKeysInfo[dbName][tbName][kName] = KeyInfo{}
			}
			if !sliceKits.ContainsString(dbTbKeysInfo[dbName][tbName][kName], colName) {
				dbTbKeysInfo[dbName][tbName][kName] = append(dbTbKeysInfo[dbName][tbName][kName], colName)
			}

			if ktype == "PRIMARY KEY" {
				_, ok = primaryKeys[dbName]
				if !ok {
					primaryKeys[dbName] = map[string]map[string]bool{}
				}
				_, ok = primaryKeys[dbName][tbName]
				if !ok {
					primaryKeys[dbName][tbName] = map[string]bool{}
				}
				primaryKeys[dbName][tbName][kName] = true
			}

		}
		rows.Close()

	}
	var isPrimay bool = false
	for dbName, _ = range dbTbKeysInfo {
		for tbName, _ = range dbTbKeysInfo[dbName] {

			tbKey := GetAbsTableName(dbName, tbName)
			binPosKey := GetBinlogPosAsKey(KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
			ok = this.CheckAndCreateTblKey(dbName, tbName, KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)

			if ok {
				this.tableInfos[tbKey][binPosKey].PrimaryKey = KeyInfo{}
				this.tableInfos[tbKey][binPosKey].UniqueKeys = []KeyInfo{}
			} else {
				this.tableInfos[tbKey][binPosKey] = &TblInfoJson{
					Database: dbName, Table: tbName,
					PrimaryKey: KeyInfo{}, UniqueKeys: []KeyInfo{},
					DdlInfo: DdlPosInfo{Binlog: KEY_NONE_BINLOG, StartPos: KEY_NONE_POS, StopPos: KEY_NONE_POS, DdlSql: ""}}
			}
			for kn, kf := range dbTbKeysInfo[dbName][tbName] {
				isPrimay = false
				_, ok = primaryKeys[dbName]
				if ok {
					_, ok = primaryKeys[dbName][tbName]
					if ok {
						_, ok = primaryKeys[dbName][tbName][kn]
						if ok && primaryKeys[dbName][tbName][kn] {
							isPrimay = true
						}
					}
				}
				if isPrimay {
					this.tableInfos[tbKey][binPosKey].PrimaryKey = kf
				} else {
					this.tableInfos[tbKey][binPosKey].UniqueKeys = append(this.tableInfos[tbKey][binPosKey].UniqueKeys, kf)
				}
			}
		}
	}

	return nil
}

func (this *TablesColumnsInfo) GetTableKeysInfoFromDb(db *sql.DB, schema, table string) error {
	rows, err := db.Query(primaryUniqueKeysSql, schema, table)
	if err != nil {
		CheckErr(err, "fail to query unique keys info of "+schema+"."+table, ERR_MYSQL_QUERY, false)
		return err
	}
	defer rows.Close()

	var kname, col, ktype string
	var kArr map[string]KeyInfo = map[string]KeyInfo{}
	var primaryKeys map[string]bool = map[string]bool{}
	for rows.Next() {
		err := rows.Scan(&kname, &col, &ktype)
		if err != nil {
			CheckErr(err, "fail to get query result of unique index info of "+schema+"."+table, ERR_MYSQL_QUERY, false)
			return err
		}
		_, ok := kArr[kname]
		if !ok {
			kArr[kname] = KeyInfo{}
		}
		kArr[kname] = append(kArr[kname], col)
		if ktype == "PRIMARY KEY" {
			primaryKeys[kname] = true
		}
	}

	tbKey := GetAbsTableName(schema, table)
	binPosKey := GetBinlogPosAsKey(KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
	ok := this.CheckAndCreateTblKey(schema, table, KEY_NONE_BINLOG, KEY_NONE_POS, KEY_NONE_POS)
	if ok {
		this.tableInfos[tbKey][binPosKey].PrimaryKey = KeyInfo{}
		this.tableInfos[tbKey][binPosKey].UniqueKeys = []KeyInfo{}
	} else {
		this.tableInfos[tbKey][binPosKey] = &TblInfoJson{Database: schema, Table: table,
			PrimaryKey: KeyInfo{}, UniqueKeys: []KeyInfo{},
			DdlInfo: DdlPosInfo{Binlog: KEY_NONE_BINLOG, StartPos: KEY_NONE_POS, StopPos: KEY_NONE_POS, DdlSql: ""},
			//DdlInfo: map[string]string{KEY_DDL_BINLOG: KEY_NONE_BINLOG, KEY_DDL_SPOS: KEY_NONE_POS, KEY_DDL_EPOS: KEY_NONE_POS},
		}
	}

	for kn, kf := range kArr {
		if primaryKeys[kn] {
			this.tableInfos[tbKey][binPosKey].PrimaryKey = kf
		} else {
			this.tableInfos[tbKey][binPosKey].UniqueKeys = append(this.tableInfos[tbKey][binPosKey].UniqueKeys, kf)
		}
	}

	return nil
}

/*
func (this *TablesColumnsInfo) DumpTblInfoJsonToFile(fname string) error {
	if len(this.tableInfos) {
		return nil
	}
	var jsonArr []TblInfoJson
	for dbtbKey, tbInfoArr := range this.tableInfos {

		for binKey, tbInfo := range tbInfoArr {

			tbJson := TblInfoJson{Database: tbInfo.Database, Table: tbInfo.Table}
			cols := make([]map[string]string, len(tbInfo.Columns))

			for _, fArr := range tbInfo.Columns {
				cols = append(cols, map[string]string{"name": fArr.FieldName, "type": fArr.FieldType})
			}

			tbJson.Columns = cols

			if len(this.tableInfos.[db][tb]) > 0 {
				for _, kArr := range this.keysInfo[tbInfo.Database][tbInfo.Table] {
					kLen = len(kArr)
					if kLen < 1 {
						continue
					}
					if kArr[kLen-1] == PRIMARY_KEY_LABLE {
						tbJson.PrimaryKey = kArr
					} else if kArr[kLen-1] == UNIQUE_KEY_LABLE {
						tbJson.UniqueKeys = append(tbJson.UniqueKeys, kArr)
					}

				}
			}
			jsonArr = append(jsonArr, tbJson)
		}
	}

	if len(jsonArr) > 0 {
		jsonBytes, err := json.MarshalIndent(jsonArr, "", "\t")
		if err != nil {
			CheckErr(err, "error when dump tables info into json string", ERR_JSON_MARSHAL, false)
			return err
		}
		_, err = file.WriteBytes(fname, jsonBytes)
		CheckErr(err, "Fail to write tables info json into file "+fname, ERR_FILE_WRITE, false)
		return err
	} else {
		return nil
	}
}
*/

func (this *TablesColumnsInfo) DumpTblInfoJsonToFile(fname string) error {
	jsonBytes, err := json.MarshalIndent(this.tableInfos, "", "\t")
	if err != nil {
		CheckErr(err, "error when dump tables info into json string", ERR_JSON_MARSHAL, false)
		return err
	}
	_, err = file.WriteBytes(fname, jsonBytes)
	CheckErr(err, "Fail to write tables info json into file "+fname, ERR_FILE_WRITE, false)
	return err
}

func (this *TablesColumnsInfo) DecodeTblInfoFromJsonFile(fname string) error {
	if !file.IsFile(fname) {
		err := fmt.Errorf("%s not exists nor a file", fname)
		CheckErr(err, "", ERR_FILE_NOT_EXISTS, false)
		return err
	}
	fBytes, err := file.ToBytes(fname)
	if err != nil {
		CheckErr(err, "fail to read content of "+fname, ERR_FILE_READ, false)
		return err
	}
	var tblArr []TblInfoJson
	err = json.Unmarshal(fBytes, tblArr)
	if err != nil {
		CheckErr(err, "fail to unmarshal "+fname, ERR_JSON_UNMARSHAL, false)
		return err
	}

	for _, oneTb := range tblArr {
		this.CheckAndCreateTblKey(oneTb.Database, oneTb.Table, oneTb.DdlInfo.Binlog, oneTb.DdlInfo.StartPos, oneTb.DdlInfo.StopPos)
		dbTbKey := GetAbsTableName(oneTb.Database, oneTb.Table)
		binPosKey := GetBinlogPosAsKey(oneTb.DdlInfo.Binlog, oneTb.DdlInfo.StartPos, oneTb.DdlInfo.StopPos)
		this.tableInfos[dbTbKey][binPosKey] = &oneTb
	}
	return nil
}

func GetMysqlUrl(cfg ConfCmd) string {
	var urlStr string
	if cfg.Socket == "" {
		urlStr = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/?autocommit=true&charset=utf8mb4,utf8&loc=Local&parseTime=true&writeTimeout=30s&readTimeout=30s&timeout=10s",
			cfg.User, cfg.Passwd, cfg.Host, cfg.Port)

	} else {
		urlStr = fmt.Sprintf(
			"%s:%s@unix(%s)/?autocommit=true&charset=utf8mb4,utf8&loc=Local&parseTime=true&writeTimeout=30s&readTimeout=30s&timeout=10s",
			cfg.User, cfg.Passwd, cfg.Socket)
	}

	return urlStr

}

func CreateMysqlCon(mysqlUrl string) (*sql.DB, error) {
	db, err := sql.Open("mysql", mysqlUrl)

	if err != nil {
		//CheckErr(err, "fail to connecto to mysql", ERR_MYSQL_CONNECTION, false)
		return nil, err
	}

	err = db.Ping()

	if err != nil {
		//CheckErr(err, "fail to connecto to mysql", ERR_MYSQL_CONNECTION, false)
		return nil, err
	}

	return db, nil
}

func GetFieldOrKeyQuerySqls(sqlFmt string, dbTbs map[string][]string, batchCnt int) []string {
	var (
		batchDbs  []string
		batchTbs  []string
		querySqls []string
		i         int    = 0
		oneSql    string = ""
		db        string
		tb        string
		tbArr     []string
	)
	for db, tbArr = range dbTbs {
		//fmt.Println(db, tbArr)
		batchDbs = append(batchDbs, db)
		for _, tb = range tbArr {
			batchTbs = append(batchTbs, tb)
			i++
			if i >= batchCnt {
				oneSql = fmt.Sprintf(sqlFmt, GetStrCommaSepFromStrSlice(batchDbs), GetStrCommaSepFromStrSlice(batchTbs))
				//fmt.Printf("in for:\n\t%s\n", oneSql)
				querySqls = append(querySqls, oneSql)
				i = 0
				batchTbs = []string{}
				batchDbs = []string{db}
			}

		}
	}
	if i > 0 && i < batchCnt && len(batchTbs) > 0 {
		oneSql = fmt.Sprintf(sqlFmt, GetStrCommaSepFromStrSlice(batchDbs), GetStrCommaSepFromStrSlice(batchTbs))
		querySqls = append(querySqls, oneSql)
		//fmt.Printf("out for:\n\t%s\n", oneSql)
	}
	//fmt.Println(querySqls)
	return querySqls
}

func GetStrCommaSepFromStrSlice(arr []string) string {
	arrTmp := make([]string, len(arr))
	for i, v := range arr {
		arrTmp[i] = fmt.Sprintf("'%s'", v)
	}
	return strings.Join(arrTmp, ",")
}

func GetAllTableNames(sqlCon *sql.DB, cfg ConfCmd) map[string][]string {
	sqlStr := "select table_schema, table_name from information_schema.tables where "
	var sqlWhereArr []string = []string{"table_type='BASE TABLE'"}
	if len(cfg.Databases) > 0 {
		sqlWhereArr = append(sqlWhereArr, fmt.Sprintf("table_schema in (%s)", GetStrCommaSepFromStrSlice(cfg.Databases)))
	} else {
		sqlWhereArr = append(sqlWhereArr, "table_schema not in ('information_schema', 'mysql', 'performance_schema')")
	}
	if len(cfg.Tables) > 0 {
		sqlWhereArr = append(sqlWhereArr, fmt.Sprintf(" table_name in (%s)", GetStrCommaSepFromStrSlice(cfg.Tables)))
	}
	sqlStr += strings.Join(sqlWhereArr, " and ")
	//fmt.Println(sqlStr)
	rows, err := sqlCon.Query(sqlStr)
	if err != nil {
		CheckErr(err, "fail to query: "+sqlStr, ERR_MYSQL_QUERY, true)
	}
	defer rows.Close()
	var schema, table string
	dbTbs := map[string][]string{}
	for rows.Next() {
		err = rows.Scan(&schema, &table)
		if err != nil {
			CheckErr(err, "fail to get query result of all tables", ERR_MYSQL_QUERY, true)
		}
		_, ok := dbTbs[schema]
		if ok {
			dbTbs[schema] = append(dbTbs[schema], table)

		} else {
			dbTbs[schema] = []string{table}
		}
	}
	return dbTbs

}
