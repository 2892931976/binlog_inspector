package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	//"github.com/davecgh/go-spew/spew"
	SQL "github.com/dropbox/godropbox/database/sqlbuilder"
	sliceKits "github.com/toolkits/slice"
)

var G_Time_Column_Types []string = []string{"timestamp", "datetime"}

type ExtraSqlInfoOfPrint struct {
	schema    string
	table     string
	binlog    string
	startpos  uint32
	endpos    uint32
	datetime  string
	trxIndex  uint64
	trxStatus int
}

type ForwardRollbackSqlOfPrint struct {
	sqls    []string
	sqlInfo ExtraSqlInfoOfPrint
}

var (
	ForwardSqlFileNamePrefix  string = "forward"
	RollbackSqlFileNamePrefix string = "rollback"
)

func PrintExtraInfoForForwardRollbackupSql(cfg ConfCmd, sqlChan chan ForwardRollbackSqlOfPrint, wg *sync.WaitGroup) {
	defer wg.Done()
	rollbackFileName := ""
	tmpFileName := ""
	oneSqls := ""
	fhArr := map[string]*os.File{}
	fhArrBuf := map[string]*bufio.Writer{}
	var FH *os.File
	var bufFH *bufio.Writer
	var err error
	var rollbackFiles []map[string]string //{"tmp":xx, "rollback":xx}
	var lastTrxIndex uint64 = 0
	var trxStr string = "commit;\nbegin;\n"
	//var trxStrLen int = len(trxStr)
	var trxCommitStr string = "commit;\n"
	//var trxCommitStrLen int = len(trxCommitStr)
	bytesCntFiles := map[string][][]int{} //{"file1":{{8, 0}, {8 , 0}}} {length of bytes, trxIndex}

	for sc := range sqlChan {
		//fmt.Println(sc.sqlInfo)
		if cfg.WorkType == "rollback" {
			tmpFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, true)
			rollbackFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, true, sc.sqlInfo.binlog, false)

		} else {
			tmpFileName = GetForwardRollbackSqlFileName(sc.sqlInfo.schema, sc.sqlInfo.table, cfg.FilePerTable, cfg.OutputDir, false, sc.sqlInfo.binlog, false)
		}
		if _, ok := fhArr[tmpFileName]; !ok {

			FH, err = os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			CheckErr(err, "Fail to open file "+tmpFileName, ERR_FILE_OPEN, true) //os.exit if err
			bufFH = bufio.NewWriter(FH)
			fhArrBuf[tmpFileName] = bufFH
			fhArr[tmpFileName] = FH
			if cfg.WorkType == "rollback" {
				rollbackFiles = append(rollbackFiles, map[string]string{"tmp": tmpFileName, "rollback": rollbackFileName})
				bytesCntFiles[tmpFileName] = [][]int{}
			}

		}

		if cfg.KeepTrx {
			if sc.sqlInfo.trxIndex != lastTrxIndex {
				if cfg.WorkType == "2sql" {
					fhArrBuf[tmpFileName].WriteString(trxStr)
				}

				/*
					if cfg.WorkType == "rollback" {
						bytesCntFiles[tmpFileName] = append(bytesCntFiles[tmpFileName], []int{trxStrLen, 1})

					}
				*/

			}
		}

		lastTrxIndex = sc.sqlInfo.trxIndex
		oneSqls = GetForwardRollbackContentLineWithExtra(sc, cfg.PrintExtraInfo)
		fhArrBuf[tmpFileName].WriteString(oneSqls)
		if cfg.WorkType == "rollback" {
			bytesCntFiles[tmpFileName] = append(bytesCntFiles[tmpFileName], []int{len(oneSqls), int(sc.sqlInfo.trxIndex)})
		}
		/*
			if sc.sqlInfo.trxStatus == TRX_STATUS_COMMIT {
				fhArr[tmpFileName].WriteString("commit\n")
			} else if sc.sqlInfo.trxStatus == TRX_STATUS_ROLLBACK {
				fhArr[tmpFileName].WriteString("rollback\n")
			}
		*/

	}
	for fn, bufFH := range fhArrBuf {
		if cfg.KeepTrx && cfg.WorkType == "2sql" {
			bufFH.WriteString(trxCommitStr)
			/*
				if cfg.WorkType == "rollback" {
					bytesCntFiles[fn] = append(bytesCntFiles[fn], []int{trxCommitStrLen, 1})
				}
			*/

		}

		bufFH.Flush()
		fhArr[fn].Close()

	}
	// reverse rollback sql file
	if cfg.WorkType == "rollback" {
		var reWg sync.WaitGroup
		filesChan := make(chan map[string]string, cfg.Threads)
		threadNum := GetMaxValue(int(cfg.Threads), len(rollbackFiles))

		for i := 0; i < threadNum; i++ {
			reWg.Add(1)
			go ReverseFileGo(filesChan, bytesCntFiles, cfg.KeepTrx, &reWg)
		}

		for _, tmpArr := range rollbackFiles {
			filesChan <- tmpArr
		}
		close(filesChan)
		reWg.Wait()
	}

}

func GetForwardRollbackContentLineWithExtra(sq ForwardRollbackSqlOfPrint, ifExtra bool) string {
	if ifExtra {
		return fmt.Sprintf("# datetime=%s database=%s table=%s binlog=%s startpos=%d stoppos=%d\n%s;\n",
			sq.sqlInfo.datetime, sq.sqlInfo.schema, sq.sqlInfo.table, sq.sqlInfo.binlog, sq.sqlInfo.startpos,
			sq.sqlInfo.endpos, strings.Join(sq.sqls, ";\n"))
	} else {

		str := strings.Join(sq.sqls, ";\n") + ";\n"
		//fmt.Println("one sqls", str)
		return str
	}

}

func GetForwardRollbackSqlFileName(schema string, table string, filePerTable bool, outDir string, ifRollback bool, binlog string, ifTmp bool) string {

	_, idx := GetBinlogBasenameAndIndex(binlog)

	if ifRollback {
		if ifTmp {
			if filePerTable {
				return filepath.Join(outDir, fmt.Sprintf(".%s.%s.%s.%d.sql", schema, table, RollbackSqlFileNamePrefix, idx))
			} else {
				return filepath.Join(outDir, fmt.Sprintf(".%s.%d.sql", RollbackSqlFileNamePrefix, idx))
			}

		} else {
			if filePerTable {
				return filepath.Join(outDir, fmt.Sprintf("%s.%s.%s.%d.sql", schema, table, RollbackSqlFileNamePrefix, idx))
			} else {
				return filepath.Join(outDir, fmt.Sprintf("%s.%d.sql", RollbackSqlFileNamePrefix, idx))
			}
		}
	} else {
		if filePerTable {
			return filepath.Join(outDir, fmt.Sprintf("%s.%s.%s.%d.sql", schema, table, ForwardSqlFileNamePrefix, idx))
		} else {
			return filepath.Join(outDir, fmt.Sprintf("%s.%d.sql", ForwardSqlFileNamePrefix, idx))
		}

	}

}

func GenForwardRollbackSqlFromBinEvent(i uint, cfg ConfCmd, evChan chan MyBinEvent, sqlChan chan ForwardRollbackSqlOfPrint, wg *sync.WaitGroup) {
	defer wg.Done()
	//defer g_threads_finished.IncreaseFinishedThreadCnt()
	//fmt.Println("enter thread", i)
	var err error
	//var currentIdx uint64
	var tbInfo *TblInfoJson
	var db, tb string
	var allColNames []FieldInfo
	var colsDef []SQL.NonAliasColumn
	var colsTypeName []string
	var colCnt int
	var sqlArr []string
	var uniqueKeyIdx []int
	var uniqueKey KeyInfo
	var ifRollback bool = false
	if cfg.WorkType == "rollback" {
		ifRollback = true
	}
	var currentSqlForPrint ForwardRollbackSqlOfPrint
	for ev := range evChan {
		db = string(ev.BinEvent.Table.Schema)
		tb = string(ev.BinEvent.Table.Table)
		tbInfo, err = G_TablesColumnsInfo.GetTableInfoJsonOfBinPos(db, tb, ev.MyPos.Name, ev.StartPos, ev.MyPos.Pos)
		if err != nil {
			CheckErr(err, "", ERR_BINLOG_EVENT, false)
			continue
		}
		colCnt = len(ev.BinEvent.Rows[0])
		allColNames = GetAllFieldNamesWithDroppedFields(colCnt, tbInfo.Columns)
		colsDef, colsTypeName = GetSqlFieldsEXpressions(colCnt, allColNames, ev.BinEvent.Table)
		colsTypeNameFromMysql := make([]string, len(colsTypeName))
		// convert datetime/timestamp type to string
		for ci, colType := range colsTypeName {
			colsTypeNameFromMysql[ci] = tbInfo.Columns[ci].FieldType
			if sliceKits.ContainsString(G_Time_Column_Types, colType) {
				for ri, _ := range ev.BinEvent.Rows {
					if ev.BinEvent.Rows[ri][ci] == nil {
						continue
					}
					//fmt.Println(ev.BinEvent.Rows[ri][0], ev.BinEvent.Rows[ri][ci])
					tv, convertOk := ev.BinEvent.Rows[ri][ci].(time.Time)
					if !convertOk {
						//spew.Dump(ev.BinEvent.Rows[ri][ci])
						tStr, tStrOk := ev.BinEvent.Rows[ri][ci].(string)

						if tStrOk {
							tStrArr := strings.Split(tStr, ".")
							if tStrArr[0] == DATETIME_ZERO_NO_MS {
								ev.BinEvent.Rows[ri][ci] = DATETIME_ZERO
							} else {
								CheckErr(fmt.Errorf("fail to convert %s.%s.%s %v to time type, convert to string type OK, but value of it is not %s nor %s",
									db, tb, allColNames[ci].FieldName,
									ev.BinEvent.Rows[ri][ci], DATETIME_ZERO, DATETIME_ZERO_NO_MS), "", ERR_BINEVENT_BODY, false)
							}
						} else {
							CheckErr(fmt.Errorf("fail to convert %s.%s.%s %v to time type nor string type",
								db, tb, allColNames[ci].FieldName, ev.BinEvent.Rows[ri][ci]), "", ERR_BINEVENT_BODY, false)
						}
					} else {

						if tv.IsZero() || tv.Unix() == 0 {
							//fmt.Println("zero datetime")
							ev.BinEvent.Rows[ri][ci] = DATETIME_ZERO
						} else {
							tvStr := tv.Format(DATETIME_FORMAT_FRACTION)
							if tvStr == DATETIME_ZERO_UNEXPECTED {
								tvStr = DATETIME_ZERO
							}
							ev.BinEvent.Rows[ri][ci] = tvStr
						}

					}
				}
			} else if colType == "blob" {
				// text is stored as blob
				if strings.Contains(strings.ToLower(tbInfo.Columns[ci].FieldType), "text") {
					for ri, _ := range ev.BinEvent.Rows {
						if ev.BinEvent.Rows[ri][ci] == nil {
							continue
						}
						txtStr, coOk := ev.BinEvent.Rows[ri][ci].([]byte)
						if !coOk {
							CheckErr(fmt.Errorf("fail to convert %v to []byte type", ev.BinEvent.Rows[ri][ci]), "", ERR_BINEVENT_BODY, false)
						} else {
							ev.BinEvent.Rows[ri][ci] = string(txtStr)
						}

					}
				}
			}
		}
		uniqueKey = tbInfo.GetOneUniqueKey()
		if len(uniqueKey) > 0 {
			uniqueKeyIdx = GetColIndexFromKey(uniqueKey, allColNames)
		} else {
			uniqueKeyIdx = []int{}
		}

		if ev.SqlType == "insert" {
			if ifRollback {
				sqlArr = GenDeleteSqlsForOneRowsEventRollbackInsert(ev.BinEvent, colsDef, uniqueKeyIdx, cfg.MinColumns, cfg.SqlTblPrefixDb)
			} else {
				sqlArr = GenInsertSqlsForOneRowsEvent(ev.BinEvent, colsDef, cfg.InsertRows, false, cfg.SqlTblPrefixDb)
			}

		} else if ev.SqlType == "delete" {
			if ifRollback {
				sqlArr = GenInsertSqlsForOneRowsEventRollbackDelete(ev.BinEvent, colsDef, cfg.InsertRows, cfg.SqlTblPrefixDb)
			} else {
				sqlArr = GenDeleteSqlsForOneRowsEvent(ev.BinEvent, colsDef, uniqueKeyIdx, cfg.MinColumns, false, cfg.SqlTblPrefixDb)
			}
		} else if ev.SqlType == "update" {
			if ifRollback {
				sqlArr = GenUpdateSqlsForOneRowsEvent(colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.MinColumns, true, cfg.SqlTblPrefixDb)
			} else {
				sqlArr = GenUpdateSqlsForOneRowsEvent(colsTypeNameFromMysql, colsTypeName, ev.BinEvent, colsDef, uniqueKeyIdx, cfg.MinColumns, false, cfg.SqlTblPrefixDb)
			}
		} else {
			fmt.Println("unsupported query type %s to generate 2sql|rollback sql, it should one of insert|update|delete. %s", ev.SqlType, ev.MyPos.String())
			continue
		}
		//fmt.Println(sqlArr)
		currentSqlForPrint = ForwardRollbackSqlOfPrint{sqls: sqlArr,
			sqlInfo: ExtraSqlInfoOfPrint{schema: db, table: tb, binlog: ev.MyPos.Name, startpos: ev.StartPos, endpos: ev.MyPos.Pos,
				datetime: GetDatetimeStr(int64(ev.Timestamp), int64(0), DATETIME_FORMAT_NOSPACE),
				trxIndex: ev.TrxIndex, trxStatus: ev.TrxStatus}}

		for {
			//fmt.Println("in thread", i)
			G_HandlingBinEventIndex.lock.Lock()
			//fmt.Println("handing index:", G_HandlingBinEventIndex.EventIdx, "binevent index:", ev.EventIdx)
			if G_HandlingBinEventIndex.EventIdx == ev.EventIdx {
				sqlChan <- currentSqlForPrint
				G_HandlingBinEventIndex.EventIdx++
				G_HandlingBinEventIndex.lock.Unlock()
				//fmt.Println("handing index == binevent index, break")
				break
			}

			G_HandlingBinEventIndex.lock.Unlock()
			time.Sleep(1 * time.Microsecond)

		}

	}
	//fmt.Println("thread", i, "exits")
}
