package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/siddontang/go-mysql/replication"
)

func main() {
	cfg := ConfCmd{}
	cfg.IfSetStopParsPoint = false
	cfg.ParseCmdOptions()
	//fmt.Println(cfg)

	GetTblDefFromDbAndMergeAndDump(cfg)

	if cfg.WorkType != "stats" {
		G_HandlingBinEventIndex = &BinEventHandlingIndx{EventIdx: 1, finished: false}
	}

	eventChan := make(chan MyBinEvent, cfg.Threads*2)
	statChan := make(chan BinEventStats, cfg.Threads*2)
	sqlChan := make(chan ForwardRollbackSqlOfPrint, cfg.Threads*2)
	var wg, wgGenSql sync.WaitGroup

	// stats file
	statFH, ddlFH, biglongFH := OpenStatsResultFiles(cfg)
	defer statFH.Close()
	defer ddlFH.Close()
	defer biglongFH.Close()
	wg.Add(1)
	go ProcessBinEventStats(statFH, ddlFH, biglongFH, cfg, statChan, &wg)

	if cfg.WorkType != "stats" {
		// write forward or rollback sql to file
		wg.Add(1)
		go PrintExtraInfoForForwardRollbackupSql(cfg, sqlChan, &wg)

		// generate forward or rollback sql from binlog
		g_threads_finished.threadsCnt = cfg.Threads
		for i := uint(0); i < cfg.Threads; i++ {
			wgGenSql.Add(1)
			go GenForwardRollbackSqlFromBinEvent(i, cfg, eventChan, sqlChan, &wgGenSql)
		}

	}

	if cfg.Mode == "repl" {
		ParserAllBinEventsFromRepl(cfg, eventChan, statChan)
	} else if cfg.Mode == "file" {
		myParser := BinFileParser{}
		myParser.parser = replication.NewBinlogParser()
		myParser.parser.SetParseTime(true) // go time type for mysql datetime/time column
		myParser.MyParseAllBinlogFiles(cfg, eventChan, statChan)
	}

	//fmt.Println(g_threads_finished.threadsCnt, g_threads_finished.threadsCnt)
	wgGenSql.Wait()
	close(sqlChan)

	wg.Wait()

}

func GetTblDefFromDbAndMergeAndDump(cfg ConfCmd) {

	dFile := filepath.Join(cfg.OutputDir, TABLE_COLUMNS_DEF_JSON_FILE)

	ifNeedGetTblDefFromDb := false
	if cfg.WorkType == "tbldef" {
		ifNeedGetTblDefFromDb = true
	}
	if cfg.WorkType != "stats" && !cfg.OnlyColFromFile {
		ifNeedGetTblDefFromDb = true
	}

	if ifNeedGetTblDefFromDb {
		if (cfg.Socket == "") && (cfg.Host == "" || cfg.Port == 0) {
			fmt.Printf("when (--wtype!=stats and not sepecify --only-table-columns) or --wtype=tbldef, must specify mysql addr and login user/password to get table definition")
			os.Exit(ERR_MISSING_OPTION)
		} else if cfg.User == "" || cfg.Passwd == "" {
			fmt.Printf("when (--wtype!=stats and not sepecify --only-table-columns) or --wtype=tbldef, must specify mysql addr and login user/password to get table definition")
			os.Exit(ERR_MISSING_OPTION)
		}

		// dump table column definition

		GetAndMergeColumnStructFromJsonFileAndDb(cfg, &G_TablesColumnsInfo)
		//fmt.Println("finish getting table struct from db:", time.Now())
		//write table column def json
		if len(G_TablesColumnsInfo.tableInfos) == 0 {
			fmt.Printf("get no table difinition info from mysql, pls check user %s has privileges to read tables in infomation_schema!!!\nError Exits!!", cfg.User)
			os.Exit(ERR_MYSQL_QUERY)
		}

	}

	if cfg.WorkType != "stats" && len(G_TablesColumnsInfo.tableInfos) == 0 {
		fmt.Println("-wtype!=stats, but get no table definition info from mysql or local json file!!!\nError Exits!!")
		os.Exit(ERR_ERROR)
	}

	if !cfg.OnlyColFromFile {
		(&G_TablesColumnsInfo).DumpTblInfoJsonToFile(dFile)
		fmt.Printf("table definition has been dumped to %s\n", dFile)
	}

	if cfg.WorkType == "tbldef" {
		fmt.Printf("--wtype=tbldef, and table definition has been dumped to %s\nExits! Bye!\n", dFile)
		os.Exit(0)
	}
}
