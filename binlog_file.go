package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/toolkits/file"
)

var (
	fileBinEventHandlingIndex uint64 = 0
	fileTrxIndex              uint64 = 0
)

type BinFileParser struct {
	parser *replication.BinlogParser
}

func (this BinFileParser) MyParseAllBinlogFiles(cfg ConfCmd, evChan chan MyBinEvent, statChan chan BinEventStats) {

	defer close(evChan)
	defer close(statChan)

	binlog, binpos := GetFirstBinlogPosToParse(cfg)
	binBaseName, binBaseIndx := GetBinlogBasenameAndIndex(binlog)
	for {
		if cfg.IfSetStopFilePos {
			if cfg.StopFilePos.Compare(mysql.Position{Name: filepath.Base(binlog), Pos: 4}) < 1 {
				break
			}
		}
		fmt.Printf("start to parse %s %d\n", binlog, binpos)
		result, err := this.MyParseOneBinlogFile(cfg, binlog, evChan, statChan)
		if err != nil {
			fmt.Println(err)
			break
		}
		if result == RE_BREAK {
			break
		} else if result == RE_FILE_END {
			if !cfg.IfSetStopParsPoint {
				//just parse one binlog
				break
			}
			binlog = filepath.Join(cfg.BinlogDir, GetNextBinlog(binBaseName, binBaseIndx))
			if !file.IsFile(binlog) {
				fmt.Printf("%s not exists nor a file\n", binlog)
				break
			}
			binBaseIndx++
			binpos = 4
		} else {
			fmt.Printf("this should not happen: return value of MyParseOneBinlog is %d\n", result)
			break
		}

	}
	//g_MaxBin_Event_Idx.SetMaxBinEventIdx(fileBinEventHandlingIndex)

}

func (this BinFileParser) MyParseOneBinlogFile(cfg ConfCmd, name string, evChan chan MyBinEvent, statChan chan BinEventStats) (int, error) {
	// process: 0, continue: 1, break: 2
	f, err := os.Open(name)
	if err != nil {
		CheckErr(err, "fail to open "+name, ERR_FILE_OPEN, false)
		return RE_BREAK, errors.Trace(err)
	}
	defer f.Close()
	fileTypeBytes := int64(4)

	b := make([]byte, fileTypeBytes)
	if _, err = f.Read(b); err != nil {
		CheckErr(err, "fail to read "+name, ERR_FILE_READ, false)
		return RE_BREAK, errors.Trace(err)
	} else if !bytes.Equal(b, replication.BinLogFileHeader) {
		CheckErr(err, name+" is not a valid binlog file, head 4 bytes must fe'bin' ", ERR_NOT_BINLOG, false)
		return RE_BREAK, errors.Trace(err)
	}

	// must not seek to other position, otherwise the program may panic because formatevent, table map event is skipped
	if _, err = f.Seek(fileTypeBytes, os.SEEK_SET); err != nil {
		CheckErr(err, fmt.Sprintf("seek %s to %d error %v", name, fileTypeBytes, err), ERR_FILE_SEEK, false)
		return RE_BREAK, errors.Trace(err)
	}
	var binlog string = filepath.Base(name)
	return this.MyParseReader(cfg, f, evChan, &binlog, statChan)
}

func (this BinFileParser) MyParseReader(cfg ConfCmd, r io.Reader, evChan chan MyBinEvent, binlog *string, statChan chan BinEventStats) (int, error) {
	// process: 0, continue: 1, break: 2, EOF: 3
	var err error
	var n int64

	var (
		db        string = ""
		tb        string = ""
		sql       string = ""
		sqlType   string = ""
		rowCnt    uint32 = 0
		trxStatus int    = 0
		sqlLower  string = ""
		tbMapPos  uint32 = 0
	)

	for {
		headBuf := make([]byte, replication.EventHeaderSize)

		if _, err = io.ReadFull(r, headBuf); err == io.EOF {
			return RE_FILE_END, nil
		} else if err != nil {
			CheckErr(err, "fail to read binlog event header of "+*binlog, ERR_FILE_READ, false)
			return RE_BREAK, errors.Trace(err)
		}

		var h *replication.EventHeader
		h, err = this.parser.ParseHeader(headBuf)
		if err != nil {
			CheckErr(err, "fail to parse binlog event header of "+*binlog, ERR_BINEVENT_HEADER, false)
			return RE_BREAK, errors.Trace(err)
		}
		//fmt.Printf("parsing %s %d %s\n", *binlog, h.LogPos, GetDatetimeStr(int64(h.Timestamp), int64(0), DATETIME_FORMAT))

		if h.EventSize <= uint32(replication.EventHeaderSize) {
			err = errors.Errorf("invalid event header, event size is %d, too small", h.EventSize)
			CheckErr(err, "", ERR_BINEVENT_HEADER, false)

			return RE_BREAK, err

		}

		var buf bytes.Buffer
		if n, err = io.CopyN(&buf, r, int64(h.EventSize)-int64(replication.EventHeaderSize)); err != nil {
			err = errors.Errorf("get event body err %v, need %d - %d, but got %d", err, h.EventSize, replication.EventHeaderSize, n)
			CheckErr(err, "", ERR_BINEVENT_BODY, false)
			return RE_BREAK, err
		}

		data := buf.Bytes()
		//rawData := data

		eventLen := int(h.EventSize) - replication.EventHeaderSize

		if len(data) != eventLen {
			err = errors.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
			CheckErr(err, "", ERR_BINEVENT_BODY, false)
			return RE_BREAK, err
		}

		var e replication.Event
		e, err = this.parser.ParseEvent(h, data)
		if err != nil {
			CheckErr(err, "fail to parse binlog event body of "+*binlog, ERR_BINEVENT_BODY, false)
			return RE_BREAK, errors.Trace(err)
		}
		if h.EventType == replication.TABLE_MAP_EVENT {
			tbMapPos = h.LogPos - h.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}

		//can not advance this check, because we need to parse table map event or table may not found. Also we must seek ahead the read file position
		chRe := CheckBinHeaderCondition(cfg, h, binlog)
		if chRe == RE_BREAK {
			return RE_BREAK, nil
		} else if chRe == RE_CONTINUE {
			continue
		} else if chRe == RE_FILE_END {
			return RE_FILE_END, nil
		}

		//binEvent := &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}
		binEvent := &replication.BinlogEvent{Header: h, Event: e} // we donnot need raw data
		oneMyEvent := &MyBinEvent{MyPos: mysql.Position{Name: *binlog, Pos: h.LogPos},
			StartPos: tbMapPos}
		//StartPos: h.LogPos - h.EventSize}
		chRe = oneMyEvent.CheckBinEvent(cfg, binEvent, binlog)
		if chRe == RE_BREAK {
			return RE_BREAK, nil
		} else if chRe == RE_CONTINUE {
			continue
		} else if chRe == RE_FILE_END {
			return RE_FILE_END, nil
		} else if chRe == RE_PROCESS {
			// output analysis result whatever the WorkType is
			db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(binEvent)
			if sqlType == "query" {
				sqlLower = strings.ToLower(sql)

				if sqlLower == "begin" {
					trxStatus = TRX_STATUS_BEGIN
					fileTrxIndex++
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
					fileBinEventHandlingIndex++
					oneMyEvent.EventIdx = fileBinEventHandlingIndex
					oneMyEvent.SqlType = sqlType
					oneMyEvent.Timestamp = h.Timestamp
					oneMyEvent.TrxIndex = fileTrxIndex
					oneMyEvent.TrxStatus = trxStatus
					evChan <- *oneMyEvent
				} /*else {
					fmt.Printf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s", tbKey, oneMyEvent.MyPos.String())
				}*/

			}

			if sqlType != "" {
				if sqlType == "query" {
					statChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: h.LogPos - h.EventSize, StopPos: h.LogPos - h.EventSize,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				} else {
					statChan <- BinEventStats{Timestamp: h.Timestamp, Binlog: *binlog, StartPos: tbMapPos, StopPos: h.LogPos,
						Database: db, Table: tb, QuerySql: sql, RowCnt: rowCnt, QueryType: sqlType}
				}

			}

		}

	}

	return RE_FILE_END, nil
}
