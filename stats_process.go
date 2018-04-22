package main

import (
	"fmt"

	"os"
	"path/filepath"
	"regexp"
	"runtime"

	"strings"

	"sync"

	"github.com/siddontang/go-mysql/replication"
)

var RegexpMatchDdlQuery *regexp.Regexp = regexp.MustCompile(`^\s*(alter|create|rename|truncate|create)\s+table`)

var Stats_Result_Header_Column_names []string = []string{"binlog", "starttime", "stoptime",
	"startpos", "stoppos", "inserts", "updates", "deletes", "database", "table"}
var Stats_DDL_Header_Column_names []string = []string{"datetime", "binlog", "startpos", "stoppos", "sql"}
var Stats_BigLongTrx_Header_Column_names []string = []string{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows", "duration", "tables"}

type BinEventStats struct {
	Timestamp uint32
	Binlog    string
	StartPos  uint32
	StopPos   uint32
	Database  string
	Table     string
	QueryType string // query, insert, update, delete
	RowCnt    uint32
	QuerySql  string // for type=query
}

type BinEventStatsPrint struct {
	Binlog    string
	StartTime uint32
	StopTime  uint32
	StartPos  uint32
	StopPos   uint32
	Database  string
	Table     string
	Inserts   uint32
	Updates   uint32
	Deletes   uint32
}

/*
type DdlStatsInfo struct {
	Timestamp uint32
	Binlog    string
	StartPos  uint32
	StopPos   uint32
	Statement string
}
*/

type BigLongTrxInfo struct {
	//IsBig bool
	//IsLong bool
	StartTime  uint32
	StopTime   uint32
	Binlog     string
	StartPos   uint32
	StopPos    uint32
	RowCnt     uint32                       // total row count for all statement
	Duration   uint32                       // how long the trx lasts
	Statements map[string]map[string]uint32 // rowcnt for each type statment: insert, update, delete. {db1.tb1:{insert:0, update:2, delete:10}}

}

func OpenStatsResultFiles(cfg ConfCmd) (*os.File, *os.File, *os.File) {
	// stat file
	statFile := filepath.Join(cfg.OutputDir, "binlog_stats.log")
	statFH, err := os.OpenFile(statFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		CheckErr(err, "fail to open file "+statFile, ERR_FILE_OPEN, false)
		runtime.Goexit()
	}
	//defer statFH.Close()
	statFH.WriteString(GetStatsPrintHeaderLine(Stats_Result_Header_Column_names))

	// ddl file
	ddlFile := filepath.Join(cfg.OutputDir, "ddl_info.log")
	ddlFH, err := os.OpenFile(ddlFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		CheckErr(err, "fail to open file "+ddlFile, ERR_FILE_OPEN, false)
		statFH.Close()
		runtime.Goexit()
	}
	//defer ddlFH.Close()
	ddlFH.WriteString(GetDdlPrintHeaderLine(Stats_DDL_Header_Column_names))

	// big/long trx info
	biglongFile := filepath.Join(cfg.OutputDir, "big_long_trx.log")
	biglongFH, err := os.OpenFile(biglongFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		CheckErr(err, "fail to open file "+biglongFile, ERR_FILE_OPEN, false)
		statFH.Close()
		ddlFH.Close()
		runtime.Goexit()
	}
	//defer biglongFH.Close()
	biglongFH.WriteString(GetBigLongTrxPrintHeaderLine(Stats_BigLongTrx_Header_Column_names))

	return statFH, ddlFH, biglongFH
	//return bufio.NewWriter(statFH), bufio.NewWriter(ddlFH), bufio.NewWriter(biglongFH)
}

func ProcessBinEventStats(statFH *os.File, ddlFH *os.File, biglongFH *os.File, cfg ConfCmd, statChan chan BinEventStats, wg *sync.WaitGroup) {
	defer wg.Done()

	var lastPrintTime uint32 = 0
	var lastBinlog string = ""

	var statsPrintArr map[string]*BinEventStatsPrint = map[string]*BinEventStatsPrint{} // key=db.tb
	var oneBigLong BigLongTrxInfo
	var ddlInfoStr string
	printInterval := uint32(cfg.PrintInterval)
	bigTrxRowsLimit := uint32(cfg.BigTrxRowLimit)
	longTrxSecs := uint32(cfg.LongTrxSeconds)
	//fmt.Println(cfg)
	//fmt.Println(cfg.PrintInterval, cfg.BigTrxRowLimit, cfg.LongTrxSeconds)
	//fmt.Println(printInterval, bigTrxRowsLimit, longTrxSecs)

	for st := range statChan {

		if lastBinlog != st.Binlog {
			// new binlog
			//print stats
			for _, oneSt := range statsPrintArr {
				statFH.WriteString(GetStatsPrintContentLine(oneSt))
			}
			statsPrintArr = map[string]*BinEventStatsPrint{}

			lastPrintTime = 0
		}
		if lastPrintTime == 0 {
			lastPrintTime = st.Timestamp + printInterval
		}
		if lastBinlog == "" {
			lastBinlog = st.Binlog
		}
		if st.QueryType == "query" {
			//fmt.Print(st.QuerySql)
			querySql := strings.ToLower(st.QuerySql)
			//fmt.Printf("query sql:%s\n", querySql)

			// trx cannot spreads in different binlogs
			if querySql == "begin" {
				oneBigLong = BigLongTrxInfo{Binlog: st.Binlog, StartPos: st.StartPos, StartTime: 0, RowCnt: 0, Statements: map[string]map[string]uint32{}}
			} else if querySql == "commit" || querySql == "rollback" {
				if oneBigLong.StartTime > 0 { // the rows event may be skipped by --databases --tables
					//big and long trx
					oneBigLong.StopPos = st.StopPos
					oneBigLong.StopTime = st.Timestamp
					oneBigLong.Duration = oneBigLong.StopTime - oneBigLong.StartTime
					if oneBigLong.RowCnt >= bigTrxRowsLimit || oneBigLong.Duration >= longTrxSecs {
						biglongFH.WriteString(GetBigLongTrxContentLine(oneBigLong))
					}
				}

			} else if RegexpMatchDdlQuery.MatchString(querySql) {
				// ddl
				ddlInfoStr = GetDdlInfoContentLine(st.Binlog, st.StartPos, st.StopPos, st.Timestamp, st.QuerySql)
				ddlFH.WriteString(ddlInfoStr)
			}
		} else {
			//big and long trx
			oneBigLong.RowCnt += st.RowCnt
			dbtbKey := GetAbsTableName(st.Database, st.Table)

			if _, ok := oneBigLong.Statements[dbtbKey]; !ok {
				oneBigLong.Statements[dbtbKey] = map[string]uint32{"insert": 0, "update": 0, "delete": 0}
			}
			oneBigLong.Statements[dbtbKey][st.QueryType] += st.RowCnt
			if oneBigLong.StartTime == 0 {
				oneBigLong.StartTime = st.Timestamp
			}

			//stats
			if _, ok := statsPrintArr[dbtbKey]; !ok {
				statsPrintArr[dbtbKey] = &BinEventStatsPrint{Binlog: st.Binlog, StartTime: st.Timestamp, StartPos: st.StartPos,
					Database: st.Database, Table: st.Table, Inserts: 0, Updates: 0, Deletes: 0}
			}
			switch st.QueryType {
			case "insert":
				statsPrintArr[dbtbKey].Inserts += st.RowCnt
			case "update":
				statsPrintArr[dbtbKey].Updates += st.RowCnt
			case "delete":
				statsPrintArr[dbtbKey].Deletes += st.RowCnt
			}
			statsPrintArr[dbtbKey].StopTime = st.Timestamp
			statsPrintArr[dbtbKey].StopPos = st.StopPos
			if st.Timestamp >= lastPrintTime {

				//print stats
				for _, oneSt := range statsPrintArr {
					statFH.WriteString(GetStatsPrintContentLine(oneSt))
				}
				statFH.WriteString("\n")
				statsPrintArr = map[string]*BinEventStatsPrint{}
				lastPrintTime = st.Timestamp + printInterval
			}

		}

		lastBinlog = st.Binlog

	}

}

func GetStatsPrintHeaderLine(headers []string) string {
	//[binlog, starttime, stoptime, startpos, stoppos, inserts, updates, deletes, database, table,]
	return fmt.Sprintf("%-17s %-19s %-19s %-10s %-10s %-8s %-8s %-8s %-15s %-20s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
}

func GetStatsPrintContentLine(st *BinEventStatsPrint) string {
	//[binlog, starttime, stoptime, startpos, stoppos, inserts, updates, deletes, database, table]
	return fmt.Sprintf("%-17s %-19s %-19s %-10d %-10d %-8d %-8d %-8d %-15s %-20s\n",
		st.Binlog, GetDatetimeStr(int64(st.StartTime), int64(0), DATETIME_FORMAT_NOSPACE),
		GetDatetimeStr(int64(st.StopTime), int64(0), DATETIME_FORMAT_NOSPACE),
		st.StartPos, st.StopPos, st.Inserts, st.Updates, st.Deletes, st.Database, st.Table)
}

func GetDdlPrintHeaderLine(headers []string) string {
	//{"datetime", "binlog", "startpos", "stoppos", "sql"}
	return fmt.Sprintf("%-19s %-17s %-10s %-10s %s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
}

func GetDdlInfoContentLine(binlog string, spos uint32, epos uint32, timeStamp uint32, sql string) string {
	// datetime, binlog, startpos, stoppos, ddlsql
	tStr := GetDatetimeStr(int64(timeStamp), int64(0), DATETIME_FORMAT_NOSPACE)
	return fmt.Sprintf("%-19s %-17s %-10d %-10d %s\n", tStr, binlog, spos, epos, sql)
}

func GetBigLongTrxPrintHeaderLine(headers []string) string {
	//{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows","duration", "tables"}
	return fmt.Sprintf("%-17s %-19s %-19s %-10s %-10s %-8s %-10s %s\n", ConvertStrArrToIntferfaceArrForPrint(headers)...)
}

func GetBigLongTrxContentLine(blTrx BigLongTrxInfo) string {
	//{"binlog", "starttime", "stoptime", "startpos", "stoppos", "rows", "duration", "tables"}
	return fmt.Sprintf("%-17s %-19s %-19s %-10d %-10d %-8d %-10d %s\n", blTrx.Binlog,
		GetDatetimeStr(int64(blTrx.StartTime), int64(0), DATETIME_FORMAT_NOSPACE),
		GetDatetimeStr(int64(blTrx.StopTime), int64(0), DATETIME_FORMAT_NOSPACE),
		blTrx.StartPos, blTrx.StopPos,
		blTrx.RowCnt, blTrx.Duration, GetBigLongTrxStatementsStr(blTrx.Statements))
}

func GetBigLongTrxStatementsStr(st map[string]map[string]uint32) string {
	strArr := make([]string, len(st))
	var i int = 0
	//var queryTypes []string = []string{"insert", "update", "delete"}
	for dbtb, arr := range st {
		strArr[i] = fmt.Sprintf("%s(inserts=%d, updates=%d, deletes=%d)", dbtb, arr["insert"], arr["update"], arr["delete"])
		i++
	}
	return fmt.Sprintf("[%s]", strings.Join(strArr, " "))
}

func GetDbTbAndQueryAndRowCntFromBinevent(ev *replication.BinlogEvent) (string, string, string, string, uint32) {
	var (
		db      string = ""
		tb      string = ""
		sql     string = ""
		sqlType string = ""
		rowCnt  uint32 = 0
	)

	switch ev.Header.EventType {

	case replication.WRITE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "insert"
		rowCnt = uint32(len(wrEvent.Rows))

	case replication.UPDATE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "update"
		rowCnt = uint32(len(wrEvent.Rows)) / 2

	case replication.DELETE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv2:

		//replication.XID_EVENT,
		//replication.TABLE_MAP_EVENT:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db = string(wrEvent.Table.Schema)
		tb = string(wrEvent.Table.Table)
		sqlType = "delete"
		rowCnt = uint32(len(wrEvent.Rows))

	case replication.QUERY_EVENT:
		queryEvent := ev.Event.(*replication.QueryEvent)
		db = string(queryEvent.Schema)
		sql = string(queryEvent.Query)
		sqlType = "query"

	case replication.MARIADB_GTID_EVENT:
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event, and also to mark stand-alone (ddl).
		//https://mariadb.com/kb/en/library/gtid_event/
		sql = "begin"
		sqlType = "query"

	case replication.XID_EVENT:
		// XID_EVENT represents commit。rollback transaction not in binlog
		sql = "commit"
		sqlType = "query"

	}
	//fmt.Println(db, tb, sqlType, rowCnt, sql)
	return db, tb, sqlType, sql, rowCnt

}
