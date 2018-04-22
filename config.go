package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/toolkits/file"
)

const (
	G_Version        = "binlog_inspector V1.04 \t--By danny.lai@vipshop.com | laijunshou@gmail.com\n"
	VALID_OPTS_MSG   = "valid options are: "
	SLICE_TO_STR_SEP = ","

	DATETIME_FORMAT          = "2006-01-02 15:04:05"
	DATETIME_FORMAT_NOSPACE  = "2006-01-02_15:04:05"
	DATETIME_FORMAT_FRACTION = "2006-01-02 15:04:05.000001"
	DATETIME_ZERO            = "0000-00-00 00:00:00.000000"
	DATETIME_ZERO_NO_MS      = "0000-00-00 00:00:00"
	DATETIME_ZERO_UNEXPECTED = "-0001-11-30 00:00:00.000011"

	UNKNOWN_FIELD_NAME_PREFIX = "dropped_column_"
	UNKNOWN_FIELD_TYPE_NAME   = "unknown_type"
	UNKNOWN_FIELD_TYPE_CODE   = mysql.MYSQL_TYPE_NULL

	TABLE_COLUMNS_DEF_JSON_FILE = "table_columns.json"

	TRX_STATUS_BEGIN    = 0
	TRX_STATUS_COMMIT   = 1
	TRX_STATUS_ROLLBACK = 2
	TRX_STATUS_PROGRESS = -1

	//OUTPUT_STATS_FILE_PREFIX = "binlog_stats"

	ERR_INVALID_OPTION  = 11
	ERR_MISSING_OPTION  = 12
	ERR_OPTION_MISMATCH = 13
	ERR_OPTION_OUTRANGE = 14
	ERR_DIR_NOT_EXISTS  = 15
	ERR_NUMBER_PARSE    = 16
	ERR_FILE_NOT_EXISTS = 17
	ERR_FILE_OPEN       = 18
	ERR_FILE_READ       = 19
	ERR_FILE_WRITE      = 20
	ERR_FILE_SEEK       = 21
	ERR_FILE_REMOVE     = 22
	ERR_NOT_BINLOG      = 23

	ERR_JSON_MARSHAL   = 35
	ERR_JSON_UNMARSHAL = 36

	ERR_MYSQL_REPL       = 51
	ERR_MYSQL_CONNECTION = 52
	ERR_MYSQL_QUERY      = 53

	ERR_BINLOG_EVENT    = 81
	ERR_BINEVENT_HEADER = 82
	ERR_BINEVENT_BODY   = 83

	ERR_ERROR = 99

	RE_PROCESS  = 0
	RE_CONTINUE = 1
	RE_BREAK    = 2
	RE_FILE_END = 3
)

type Threads_Finish_Status struct {
	finishedThreadsCnt uint
	threadsCnt         uint
	lock               sync.RWMutex
}

func (this *Threads_Finish_Status) IncreaseFinishedThreadCnt() {
	this.lock.Lock()
	defer this.lock.Unlock()
	this.finishedThreadsCnt++
	fmt.Println("increase finished threads cnt:", this.finishedThreadsCnt)
}

var (
	//options must be set, default value is not suitable
	//Opts_Required []string = []string{""}

	Opts_Valid_Mode      []string = []string{"repl", "file"}
	Opts_Valid_WortType  []string = []string{"tbldef", "stats", "2sql", "rollback"}
	Opts_Valid_MysqlType []string = []string{"mysql", "mariadb"}
	Opts_Valid_FilterSql []string = []string{"insert", "update", "delete"}

	Opts_Value_Range map[string][]int = map[string][]int{
		// lowerlimit, upperlimit, defaultvalue
		"PrintInterval":  []int{1, 600, 30},
		"BigTrxRowLimit": []int{10, 3000, 500},
		"LongTrxSeconds": []int{1, 1200, 300},
		"InsertRows":     []int{1, 100, 30},
		"Threads":        []int{1, 8, 2},
	}
	Stats_Columns []string = []string{
		"StartTime", "StopTime", "Binlog", "PosRange",
		"Database", "Table",
		"BigTrxs", "BiggestTrx", "LongTrxs", "LongestTrx",
		"Inserts", "Updates", "Deletes", "Trxs", "Statements",
		"Renames", "RenamePoses", "Ddls", "DdlPoses",
	}

	Ddl_Str_Print_Header []string = []string{"datetime", "binlog", "startposition", "stopposition", "sql"}

	//G_Table_Columns_Struct_Json TablesColumnsInfo = TablesColumnsInfo{}

	g_threads_finished = &Threads_Finish_Status{finishedThreadsCnt: 0, threadsCnt: 0}
)

type ConfCmd struct {
	Mode      string
	WorkType  string
	MysqlType string

	Host     string
	Port     uint
	User     string
	Passwd   string
	Socket   string
	ServerId uint

	Databases    []string
	Tables       []string
	FilterSql    []string
	FilterSqlLen int

	StartFile         string
	StartPos          uint
	StartFilePos      mysql.Position
	IfSetStartFilePos bool

	StopFile         string
	StopPos          uint
	StopFilePos      mysql.Position
	IfSetStopFilePos bool

	StartDatetime uint32
	StopDatetime  uint32

	IfSetStartDateTime bool
	IfSetStopDateTime  bool

	ToLastLog      bool
	PrintInterval  int
	BigTrxRowLimit int
	LongTrxSeconds int

	IfSetStopParsPoint bool

	OutputDir string

	MinColumns     bool
	InsertRows     int
	KeepTrx        bool
	SqlTblPrefixDb bool
	FilePerTable   bool

	PrintExtraInfo bool

	Threads uint

	TableDefJsonFile string
	OnlyColFromFile  bool
	//OnlyDumpTblDef   bool

	BinlogDir string

	GivenBinlogFile string
}

func (this *ConfCmd) ParseCmdOptions() {
	flag.Usage = func() {
		this.PrintUsageMsg()
	}
	var version bool
	flag.BoolVar(&version, "version", false, "print version")
	flag.StringVar(&this.Mode, "mode", "file", StrSliceToString(Opts_Valid_Mode, SLICE_TO_STR_SEP, VALID_OPTS_MSG)+". repl: as a slave to get binlogs from master. file: get binlogs from local filesystem. default file")
	flag.StringVar(&this.WorkType, "wtype", "stats", StrSliceToString(Opts_Valid_WortType, SLICE_TO_STR_SEP, VALID_OPTS_MSG)+". 2sql: convert binlog to sqls, rollback: generate rollback sqls, stats: analyze transactions. default: stats")
	flag.StringVar(&this.MysqlType, "mtype", "mysql", StrSliceToString(Opts_Valid_MysqlType, SLICE_TO_STR_SEP, VALID_OPTS_MSG)+". server of binlog, mysql or mariadb, default mysql")

	flag.StringVar(&this.Host, "host", "127.0.0.1", "master host, DONOT need to specify when --wtype=stats. if mode is file, it can be slave or other mysql contains same schema and table struct, not only master. default 127.0.0.1")
	flag.UintVar(&this.Port, "port", 3306, "master port, default 3306. DONOT need to specify when --wtype=stats")
	flag.StringVar(&this.User, "user", "", "mysql user. DONOT need to specify when --wtype=stats")
	flag.StringVar(&this.Passwd, "password", "", "mysql user password. DONOT need to specify when --wtype=stats")
	flag.StringVar(&this.Socket, "socket", "", "mysql socket file")
	flag.UintVar(&this.ServerId, "serverid", 3320, "works with --mode=repl, this program replicates from master as slave to read binlogs. Must set this server id unique from other slaves, default 3320")

	var dbs, tbs, sqlTypes string
	flag.StringVar(&dbs, "databases", "", "only parse these databases, comma seperated, default all. Useless when --wtype=stats")
	flag.StringVar(&tbs, "tables", "", "only parse these tables, comma seperated, DONOT prefix with schema, default all. Useless when --wtype=stats")
	flag.StringVar(&sqlTypes, "sqltypes", "", StrSliceToString(Opts_Valid_FilterSql, SLICE_TO_STR_SEP, VALID_OPTS_MSG)+". only parse these types of sql, comma seperated, valid types are: insert, update, delete; default is all(insert,update,delete)")

	flag.StringVar(&this.StartFile, "start-binlog", "", "binlog file to start reading")
	flag.UintVar(&this.StartPos, "start-pos", 0, "start reading the binlog at position")
	flag.StringVar(&this.StopFile, "stop-binlog", "", "binlog file to stop reading")
	flag.UintVar(&this.StopPos, "stop-pos", 0, "Stop reading the binlog at position")

	var startTime, stopTime string
	flag.StringVar(&startTime, "start-datetime", "", "Start reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2004-12-25 11:25:56\"")
	flag.StringVar(&stopTime, "stop-datetime", "", "Stop reading the binlog at first event having a datetime equal or posterior to the argument, it should be like this: \"2004-12-25 11:25:56\"")

	flag.BoolVar(&this.ToLastLog, "to-last-log", false, "works with WorkType='stats', keep analyzing transations to last binlog for mode=file, and keep analyzing for mode=replication")
	flag.IntVar(&this.PrintInterval, "interval", this.GetDefaultValueOfRange("PrintInterval"), "works with WorkType='stats', print stats info each PrintInterval. "+this.GetDefaultAndRangeValueMsg("PrintInterval"))
	flag.IntVar(&this.BigTrxRowLimit, "big-trx-rows", this.GetDefaultValueOfRange("BigTrxRowLimit"), "transaction with affected rows greater or equal to this value is considerated as big transaction. "+this.GetDefaultAndRangeValueMsg("BigTrxRowLimit"))
	flag.IntVar(&this.LongTrxSeconds, "long-trx-seconds", this.GetDefaultValueOfRange("LongTrxSeconds"), "transaction with duration greater or equal to this value is considerated as long transaction. "+this.GetDefaultAndRangeValueMsg("LongTrxSeconds"))

	flag.BoolVar(&this.MinColumns, "min-columns", false, "Works with WorkType=2sql|rollback. for update sql, skip unchanged columns. for update and delete, use primary/unique key to build where condition. default false, contain all columns")
	flag.IntVar(&this.InsertRows, "insert-rows", this.GetDefaultValueOfRange("InsertRows"), "Works with WorkType=2sql|rollback. rows for each insert sql. "+this.GetDefaultAndRangeValueMsg("InsertRows"))
	flag.BoolVar(&this.KeepTrx, "keep-trx", false, "Works with WorkType=2sql|rollback. wrap result statements with 'begin...commit|rollback'")
	flag.BoolVar(&this.SqlTblPrefixDb, "prefix-database", true, "Works with WorkType=2sql|rollback. Prefix table name with database name in sql, ex: insert into db1.tb1 (x1, x1) values (y1, y1). Default true")

	flag.StringVar(&this.OutputDir, "output-dir", "", "result output dir, default current work dir. Attension, result files could be large, set it to a dir with large free space")

	flag.BoolVar(&this.PrintExtraInfo, "extra-info", false, "Works with WorkType=2sql|rollback. Print database/table/datetime/binlogposition...info on the line before sql, default false")

	flag.BoolVar(&this.FilePerTable, "file-each-table", false, "Works with WorkType=2sql|rollback. one file for one table if true, else one file for all tables. default false. Attention, always one file for one binlog")

	flag.UintVar(&this.Threads, "threads", 2, "Works with WorkType=2sql|rollback. threads to run, default 2")

	flag.StringVar(&this.TableDefJsonFile, "table-columns", "", "Works with WorkType=2sql|rollback. json file defines table struct")
	flag.BoolVar(&this.OnlyColFromFile, "only-table-columns", false, "Only use table struct from --table-columns=file, do not find table struct from mysql")
	//flag.BoolVar(&this.OnlyDumpTblDef, "only-dump-table-columns", false, "Only dump table definition to json file and exits, not parsing binlog to get forward/rollback sql nor statistical analysis")

	flag.Parse()
	//flag.PrintDefaults()
	if version {
		fmt.Printf("\n%s\n", G_Version)
		os.Exit(0)
	}
	if this.Mode != "repl" && this.Mode != "file" {

		fmt.Printf("unsupported mode=%s, valid modes: file, repl\n", this.Mode)
		os.Exit(ERR_INVALID_OPTION)
	}

	if this.Mode == "file" && this.WorkType != "tbldef" {
		// the last arg should be binlog file
		if flag.NArg() != 1 {
			fmt.Println("missing binlog file. binlog file as last arg must be specify when --mode=file")
			this.PrintUsageMsg()
			os.Exit(ERR_MISSING_OPTION)
		}
		this.GivenBinlogFile = flag.Args()[0]
		if !file.IsFile(this.GivenBinlogFile) {
			fmt.Println("%s doesnot exists nor a binlog file", this.GivenBinlogFile)
			os.Exit(ERR_FILE_NOT_EXISTS)
		} else {
			this.BinlogDir = filepath.Dir(this.GivenBinlogFile)
		}
	}

	if this.TableDefJsonFile != "" {
		if !file.IsFile(this.TableDefJsonFile) {
			fmt.Println("%s doesnot exists nor a file", this.TableDefJsonFile)
			os.Exit(ERR_FILE_NOT_EXISTS)
		}
		jdat, err := file.ToBytes(this.TableDefJsonFile)
		CheckErr(err, "fail to read file "+this.TableDefJsonFile, ERR_FILE_READ, true)
		err = json.Unmarshal(jdat, &(G_TablesColumnsInfo.tableInfos))
		(&G_TablesColumnsInfo).DumpTblInfoJsonToFile("tmp.json")
		CheckErr(err, "fail to unmarshal file "+this.TableDefJsonFile, ERR_JSON_UNMARSHAL, true)

	}

	if dbs != "" {
		//this.Databases = strings.Split(dbs, ",")
		this.Databases = CommaSeparatedListToArray(dbs)
	}

	if tbs != "" {
		//this.Tables = strings.Split(tbs, ",")
		this.Tables = CommaSeparatedListToArray(tbs)
	}

	if sqlTypes != "" {
		//this.FilterSql = strings.Split(sqlTypes, ",")
		this.FilterSql = CommaSeparatedListToArray(sqlTypes)
		for _, oneSqlT := range this.FilterSql {
			CheckElementOfSliceStr(Opts_Valid_FilterSql, oneSqlT, "invalid sqltypes", true)
		}
		this.FilterSqlLen = len(this.FilterSql)
	} else {
		this.FilterSqlLen = 0
	}

	timeLoc, err := time.LoadLocation("Local")
	CheckErr(err, "fail to time zone", ERR_INVALID_OPTION, true)
	if startTime != "" {
		t, err := time.ParseInLocation(DATETIME_FORMAT, startTime, timeLoc)
		CheckErr(err, "Invalid start-datetime", ERR_INVALID_OPTION, true)
		this.StartDatetime = uint32(t.Unix())
		this.IfSetStartDateTime = true
	} else {
		this.IfSetStartDateTime = false
	}

	if stopTime != "" {
		t, err := time.ParseInLocation(DATETIME_FORMAT, stopTime, timeLoc)
		CheckErr(err, "Invalid stop-datetime", ERR_INVALID_OPTION, true)
		this.StopDatetime = uint32(t.Unix())
		this.IfSetStopDateTime = true
		this.IfSetStopParsPoint = true
	} else {
		this.IfSetStopDateTime = false
	}

	if startTime != "" && stopTime != "" {
		if this.StartDatetime >= this.StopDatetime {
			fmt.Println("--start-datetime muste be ealier than --stop-datetime")
			os.Exit(ERR_OPTION_MISMATCH)
		}
	}

	this.CheckCmdOptions()

}

func (this *ConfCmd) CheckCmdOptions() {
	//check --mode
	CheckElementOfSliceStr(Opts_Valid_Mode, this.Mode, "invalid arg for --mode", true)

	//check --wtype
	CheckElementOfSliceStr(Opts_Valid_WortType, this.WorkType, "invalid arg for --wtype", true)

	//check --mtype
	CheckElementOfSliceStr(Opts_Valid_MysqlType, this.MysqlType, "invalid arg for --mtype", true)

	if this.Mode != "file" && this.WorkType != "stats" {
		//check --user
		this.CheckRequiredOption(this.User, "--user must be set", true)
		//check --password
		this.CheckRequiredOption(this.Passwd, "--password must be set", true)

	}

	//check --start-binlog --start-pos --stop-binlog --stop-pos
	if this.StartFile != "" && this.StartPos != 0 && this.StopFile != "" && this.StopPos != 0 {
		cmpRes := CompareBinlogPos(this.StartFile, this.StartPos, this.StopFile, this.StopPos)
		if cmpRes != -1 {
			fmt.Println("start postion(--start-binlog --start-pos) must less than stop position(--stop-binlog --stop-pos)")
			os.Exit(ERR_OPTION_MISMATCH)
		}
	}

	if this.StartFile != "" && this.StartPos != 0 {
		this.IfSetStartFilePos = true
		this.StartFilePos = mysql.Position{Name: this.StartFile, Pos: uint32(this.StartPos)}

	} else {
		if this.StartFile != "" || this.StartPos != 0 {
			fmt.Printf("--start-pos and --start-binlog must be set together")
			os.Exit(ERR_MISSING_OPTION)
		}
		this.IfSetStartFilePos = false

	}

	if this.StopFile != "" && this.StopPos != 0 {

		this.IfSetStopFilePos = true
		this.StopFilePos = mysql.Position{Name: this.StopFile, Pos: uint32(this.StopPos)}
		this.IfSetStopParsPoint = true

	} else {
		if this.StopFile != "" || this.StopPos != 0 {
			fmt.Printf("--stop-pos and --stop-binlog must set together.")
			os.Exit(ERR_MISSING_OPTION)
		}

		this.IfSetStopFilePos = false
		this.IfSetStopParsPoint = false

	}

	if this.Mode == "repl" && this.WorkType != "tbldef" {
		if this.StartFile == "" || this.StartPos == 0 {
			fmt.Println("when --mode=repl, --start-binlog and --start-pos must be specified")
			os.Exit(ERR_OPTION_MISMATCH)
		}
	}

	// check --interval
	if this.PrintInterval != this.GetDefaultValueOfRange("PrintInterval") {
		this.CheckValueInRange("PrintInterval", this.PrintInterval, "value of --interval out of range", true)
	}

	// check --big-trx-rows
	if this.BigTrxRowLimit != this.GetDefaultValueOfRange("BigTrxRowLimit") {
		this.CheckValueInRange("BigTrxRowLimit", this.BigTrxRowLimit, "value of --big-trx-rows out of range", true)
	}

	// check --long-trx-seconds
	if this.LongTrxSeconds != this.GetDefaultValueOfRange("LongTrxSeconds") {
		this.CheckValueInRange("LongTrxSeconds", this.LongTrxSeconds, "value of --long-trx-seconds out of range", true)
	}

	// check --insert-rows
	if this.InsertRows != this.GetDefaultValueOfRange("InsertRows") {
		this.CheckValueInRange("InsertRows", this.InsertRows, "value of --insert-rows out of range", true)
	}

	// check --threads
	if this.Threads != uint(this.GetDefaultValueOfRange("Threads")) {
		this.CheckValueInRange("Threads", int(this.Threads), "value of --threads out of range", true)
	}

	// check --output-dir
	if this.OutputDir != "" {
		ifExist, errMsg := CheckIsDir(this.OutputDir)
		if !ifExist {
			fmt.Println(errMsg)
			os.Exit(ERR_DIR_NOT_EXISTS)
		}
	} else {
		this.OutputDir, _ = os.Getwd()
	}

	// check --to-last-log
	if this.ToLastLog {
		if this.Mode != "repl" || this.WorkType != "stats" {
			fmt.Println("--to-last-log only works with --mode=repl and --wtype=stats")
			os.Exit(ERR_OPTION_MISMATCH)
		}
		this.IfSetStopParsPoint = true
	}
}

func (this *ConfCmd) CheckValueInRange(opt string, val int, prefix string, ifExt bool) bool {
	valOk := true
	if val < this.GetMinValueOfRange(opt) {
		valOk = false
	} else if val > this.GetMaxValueOfRange(opt) {
		valOk = false
	}

	if !valOk {
		fmt.Printf("%s: %d is specfied, but %s", prefix, val, this.GetDefaultAndRangeValueMsg(opt))
		if ifExt {
			os.Exit(ERR_OPTION_OUTRANGE)
		}
	}
	return valOk
}

func (this *ConfCmd) CheckRequiredOption(v interface{}, prefix string, ifExt bool) bool {
	// options must set, default value is not suitable
	notOk := false
	switch realVal := v.(type) {
	case string:
		if realVal == "" {
			notOk = true
		}
	case int:
		if realVal == 0 {
			notOk = true
		}
	}
	if notOk {
		fmt.Println(prefix)
		if ifExt {
			os.Exit(ERR_INVALID_OPTION)
		}

	}
	return true
}

func (this *ConfCmd) PrintUsageMsg() {
	//flag.Usage()
	fmt.Printf("\n%s", G_Version)
	fmt.Println("\nparse mysql binlog to generate analysis report, forward or rollback sql.\ntwo work mode:")
	fmt.Println("\tread binlog from master, work as a fake slave: ./binlog_inspector --mode=repl opts...")
	fmt.Println("\tread binlog from local filesystem: ./binlog_inspector --mode=file opts... mysql-bin.000010")
	exp := "\n\nusage example:\ngenerate forward sql and analysis report:\n\t" + os.Args[0] + " --mode=repl --wtype=2sql --mtype=mysql --threads=4 --serverid=3331 --host=127.0.0.1 --port=3306 --user=xxx --password=xxx --databases=db1,db2 --tables=tb1,tb2 --start-binlog=mysql-bin.000556 --start-pos=107 --stop-binlog=mysql-bin.000559 --stop-pos=4 --min-columns --file-each-table --insert-rows=20 --keep-trx --big-trx-rows=100 --long-trx-seconds=10 --output-dir=/home/apps/tmp --table-columns tbs_all_def.json"
	exp += "\n\ngenerate rollback sql and analysis report:\n\t" + os.Args[0] + " --mode=file --wtype=rollback --mtype=mysql --threads=4 --host=127.0.0.1 --port=3306 --user=xxx --password=xxx --databases=db1,db2 --tables=tb1,tb2 --start-datetime='2017-09-28 13:00:00' --stop-datetime='2017-09-28 16:00:00' --min-columns --file-each-table --insert-rows=20 --keep-trx --big-trx-rows=100 --long-trx-seconds=10 --output-dir=/home/apps/tmp --table-columns tbs_all_def.json /apps/dbdata/mysqldata_3306/log/mysql-bin.000556"
	exp += "\n\nonly generate analysis report:\n\t" + os.Args[0] + " --mode=repl --wtype=stats --mtype=mysql --host=127.0.0.1 --port=3306 --user=xxx --password=xxx --databases=db1,db2 --tables=tb1,tb2 --start-binlog=mysql-bin.000556 --start-pos=107 --to-last-log --interval=20 --big-trx-rows=100 --long-trx-seconds=10 --output-dir=/home/apps/tmp"
	fmt.Println(exp)
	fmt.Println("\nsuported options:\n")
	flag.PrintDefaults()
}

func (this *ConfCmd) GetMinValueOfRange(opt string) int {
	return Opts_Value_Range[opt][0]
}

func (this *ConfCmd) GetMaxValueOfRange(opt string) int {
	return Opts_Value_Range[opt][1]
}

func (this *ConfCmd) GetDefaultValueOfRange(opt string) int {
	//fmt.Printf("default value of %s: %d\n", opt, Opts_Value_Range[opt][2])
	return Opts_Value_Range[opt][2]
}

func (this *ConfCmd) GetDefaultAndRangeValueMsg(opt string) string {
	return fmt.Sprintf("Valid values range from %d to %d, default %d",
		this.GetMinValueOfRange(opt),
		this.GetMaxValueOfRange(opt),
		this.GetDefaultValueOfRange(opt),
	)
}
