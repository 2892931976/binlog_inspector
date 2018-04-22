package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

/*
type ReplBinlogStreamer struct {
	cfg ConfCmd
	binSyncCfg replication.BinlogSyncerConfig
	replBinSyncer *replication.BinlogSyncer
	replBinStreamer *replication.BinlogStreamer
}
*/

func ParserAllBinEventsFromRepl(cfg ConfCmd, eventChan chan MyBinEvent, statChan chan BinEventStats) {

	defer close(eventChan)
	defer close(statChan)
	replStreamer := NewReplBinlogStreamer(cfg)
	SendBinlogEventRepl(cfg, replStreamer, eventChan, statChan)

}

func NewReplBinlogStreamer(cfg ConfCmd) *replication.BinlogStreamer {
	replCfg := replication.BinlogSyncerConfig{
		ServerID:        uint32(cfg.ServerId),
		Flavor:          cfg.MysqlType,
		Host:            cfg.Host,
		Port:            uint16(cfg.Port),
		User:            cfg.User,
		Password:        cfg.Passwd,
		Charset:         "utf8",
		SemiSyncEnabled: false,
		ParseTime:       true,
	}

	replSyncer := replication.NewBinlogSyncer(replCfg)

	syncPosition := mysql.Position{Name: cfg.StartFile, Pos: uint32(cfg.StartPos)}
	replStreamer, err := replSyncer.StartSync(syncPosition)
	if err != nil {
		errMsg := fmt.Sprintf("error replication from master %s:%d ", cfg.Host, cfg.Port)
		CheckErr(err, errMsg, ERR_MYSQL_REPL, true)
	}
	return replStreamer
}

func SendBinlogEventRepl(cfg ConfCmd, streamer *replication.BinlogStreamer, eventChan chan MyBinEvent, statChan chan BinEventStats) {
	//defer close(statChan)
	//defer close(eventChan)

	var (
		chkRe         int
		currentBinlog *string = &cfg.StartFile
		binEventIdx   uint64  = 0
		trxIndex      uint64  = 0
		trxStatus     int     = 0
		sqlLower      string  = ""

		db      string = ""
		tb      string = ""
		sql     string = ""
		sqlType string = ""
		rowCnt  uint32 = 0

		tbMapPos uint32 = 0

		justStart bool = true
	)
	//defer g_MaxBin_Event_Idx.SetMaxBinEventIdx()
	for {
		ev, err := streamer.GetEvent(context.Background())
		if err != nil {
			fmt.Println("error to get binlog event: %s\n", err)
			break
		}

		if !cfg.IfSetStopParsPoint && !justStart {
			//just parse one binlog. the first event is rotate event

			if ev.Header.EventType == replication.ROTATE_EVENT {
				break
			}
		}
		justStart = false

		if ev.Header.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = ev.Header.LogPos - ev.Header.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}
		ev.RawData = []byte{} // we donnot need raw data

		chkRe = CheckBinHeaderCondition(cfg, ev.Header, currentBinlog)
		if chkRe == RE_BREAK {
			break
		} else if chkRe == RE_CONTINUE {
			continue
		} else if chkRe == RE_FILE_END {
			continue
		}

		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: *currentBinlog, Pos: ev.Header.LogPos},
			StartPos: tbMapPos}
		//StartPos: ev.Header.LogPos - ev.Header.EventSize}
		chkRe = oneMyEvent.CheckBinEvent(cfg, ev, currentBinlog)

		if chkRe == RE_CONTINUE {
			continue
		} else if chkRe == RE_BREAK {
			break
		} else if chkRe == RE_PROCESS {
			db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(ev)

			if sqlType == "query" {
				sqlLower = strings.ToLower(sql)

				if sqlLower == "begin" {
					trxStatus = TRX_STATUS_BEGIN
					trxIndex++
				} else if sqlLower == "commit" {
					trxStatus = TRX_STATUS_COMMIT
				} else if sqlLower == "rollback" {
					trxStatus = TRX_STATUS_ROLLBACK
				}

			} else {
				trxStatus = TRX_STATUS_PROGRESS
			}

			if cfg.WorkType != "stats" && oneMyEvent.IfRowsEvent {
				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema),
					string(oneMyEvent.BinEvent.Table.Table))
				if _, ok := G_TablesColumnsInfo.tableInfos[tbKey]; ok {
					binEventIdx++
					oneMyEvent.EventIdx = binEventIdx
					oneMyEvent.SqlType = sqlType
					oneMyEvent.Timestamp = ev.Header.Timestamp
					oneMyEvent.TrxIndex = trxIndex
					oneMyEvent.TrxStatus = trxStatus
					eventChan <- *oneMyEvent
				} /* else {
					fmt.Printf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s", tbKey, oneMyEvent.MyPos.String())
				}*/

			}

			// output analysis result whatever the WorkType is
			if sqlType != "" {
				if sqlType == "query" {
					statChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: *currentBinlog, StartPos: ev.Header.LogPos - ev.Header.EventSize, StopPos: ev.Header.LogPos,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				} else {
					statChan <- BinEventStats{Timestamp: ev.Header.Timestamp, Binlog: *currentBinlog, StartPos: tbMapPos, StopPos: ev.Header.LogPos,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				}

			}

		} else if chkRe == RE_FILE_END {
			continue
		} else {
			fmt.Printf("this should not happen: return value of CheckBinEvent() is %d\n", chkRe)
		}

	}

	//g_MaxBin_Event_Idx.SetMaxBinEventIdx(binEventIdx)

}
