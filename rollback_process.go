package main

import (
	"io"
	"os"
	"strings"
	"sync"
)

func ReverseFileGo(rollbackFileChan chan map[string]string, bytesCntFiles map[string][][]int, keepTrx bool, wg *sync.WaitGroup) {
	defer wg.Done()
	for arr := range rollbackFileChan {
		//ReverseFileToNewFile(arr["tmp"], arr["rollback"], batchLines)
		//ReverseFileToNewFileOneByOneLineAndKeepTrx(arr["tmp"], arr["rollback"])
		ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(arr["tmp"], arr["rollback"], bytesCntFiles[arr["tmp"]], keepTrx)
		err := os.Remove(arr["tmp"])
		if err != nil {
			CheckErr(err, "fail to remove file "+arr["tmp"], ERR_FILE_REMOVE, false)
		}
	}

}

func ReverseFileToNewFileOneByOneLineAndKeepTrxBatchRead(srcFile string, destFile string, trxPoses [][]int, keepTrx bool) error {
	srcFH, err := os.Open(srcFile)
	if err != nil {
		CheckErr(err, "fail to open file "+srcFile, ERR_FILE_OPEN, false)
		return err
	}
	defer srcFH.Close()

	destFH, err := os.OpenFile(destFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		CheckErr(err, "fail to open file "+destFile, ERR_FILE_OPEN, false)
		return err
	}
	defer destFH.Close()

	srcInfo, err := srcFH.Stat()
	if err != nil {
		CheckErr(err, "fail to stat file "+srcFile, ERR_FILE_READ, false)
		return err
	}

	srcSize := srcInfo.Size() //int64

	var readByteCntTotal int64 = 0

	var bufStr string
	var LineSep string = "\n"

	//var ifCommit bool = true

	_, err = srcFH.Seek(0, os.SEEK_END)
	if err != nil {
		CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
		return err
	}

	var lastTrxIdx int = 0

	for batchIdx := len(trxPoses) - 1; batchIdx >= 0; batchIdx-- {

		startPos, err := srcFH.Seek(-int64(trxPoses[batchIdx][0]), os.SEEK_CUR)
		if err != nil {
			CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
			return err
		}
		var buf []byte = make([]byte, trxPoses[batchIdx][0])
		//_, err = srcFH.Read(buf)
		_, err = io.ReadFull(srcFH, buf)
		if err != nil {
			CheckErr(err, "fail to read file "+srcFile, ERR_FILE_READ, false)
			return err
		}

		readByteCntTotal += int64(trxPoses[batchIdx][0])

		bufStr = string(buf)
		strArr := strings.Split(bufStr, LineSep)
		var strArrStrs []string = make([]string, len(strArr))

		for ji, ai := 0, len(strArr)-1; ai >= 0; ai-- {

			if strArr[ai] == "" {
				continue
			}
			/*
				if trxPoses[batchIdx][1] == 1 {
					if strArr[ai] == "commit" {
						ifCommit = true
						if batchIdx == 0 && ai == 0 {
							strArrStrs[ji] = "" // "commit" is written as the first line in the tmp file, so we skip it
						} else {
							strArrStrs[ji] = "begin"
						}

					} else if strArr[ai] == "rollback" {
						ifCommit = false
						strArrStrs[ji] = "begin"
					} else if strArr[ai] == "begin" {
						if ifCommit {
							strArrStrs[ji] = "commit"
						} else {
							strArrStrs[ji] = "rollback"
						}
						ifCommit = true // default is commit
					}
				} else {
					strArrStrs[ji] = strArr[ai]
				}
			*/
			strArrStrs[ji] = strArr[ai]
			ji++

		}
		if keepTrx && lastTrxIdx != trxPoses[batchIdx][1] {
			destFH.WriteString("commit\nbegin\n")
		}
		lastTrxIdx = trxPoses[batchIdx][1]
		_, err = destFH.WriteString(strings.Join(strArrStrs, LineSep))
		if err != nil {
			CheckErr(err, "fail to write file "+destFile, ERR_FILE_WRITE, false)
			return err
		}

		if readByteCntTotal == srcSize || startPos == 0 {
			break // finishing reading
		}
		if batchIdx > 0 {
			_, err := srcFH.Seek(-int64(trxPoses[batchIdx][0]), os.SEEK_CUR)
			if err != nil {
				CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
				return err
			}
		}
	}

	if keepTrx {
		destFH.WriteString("commit\n")
	}

	return nil

}

func ReverseFileToNewFileOneByOneLineAndKeepTrx(srcFile string, destFile string) error {
	srcFH, err := os.Open(srcFile)
	if err != nil {
		CheckErr(err, "fail to open file "+srcFile, ERR_FILE_OPEN, false)
		return err
	}
	defer srcFH.Close()

	destFH, err := os.OpenFile(destFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		CheckErr(err, "fail to open file "+destFile, ERR_FILE_OPEN, false)
		return err
	}
	defer destFH.Close()

	srcInfo, err := srcFH.Stat()
	if err != nil {
		CheckErr(err, "fail to stat file "+srcFile, ERR_FILE_READ, false)
		return err
	}

	srcSize := srcInfo.Size() //int64

	var offset int64 = -1
	var readByteCntTotal int64 = 0
	var buf []byte = make([]byte, 1)
	var bufStr string
	var LineSep string = "\n"
	var printLineStr string

	var byteCntBeforePrint int64 = 0
	var ifCommit bool = true

	_, err = srcFH.Seek(0, os.SEEK_END)
	if err != nil {
		CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
		return err
	}

	for {

		startPos, err := srcFH.Seek(offset, os.SEEK_CUR)
		if err != nil {
			CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
			return err
		}
		_, err = srcFH.Read(buf)
		if err != nil {
			CheckErr(err, "fail to read file "+srcFile, ERR_FILE_READ, false)
			return err
		}

		readByteCntTotal++
		byteCntBeforePrint++

		bufStr = string(buf)
		if bufStr == LineSep && readByteCntTotal == 1 {
			_, err := srcFH.Seek(offset, os.SEEK_CUR)
			if err != nil {
				CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
				return err
			}
			continue // if the end of file is EOL, skip it
		}

		// read a line or we alread finish reading the whole file
		if bufStr == LineSep || readByteCntTotal == srcSize || startPos == 0 {
			printLine := make([]byte, byteCntBeforePrint-1)
			_, err := io.ReadFull(srcFH, printLine)
			if err != nil {
				CheckErr(err, "fail to read file "+srcFile, ERR_FILE_READ, false)
				return err
			}
			printLineStr = string(printLine)

			if printLineStr == "commit\n" {
				ifCommit = true
				printLineStr = "begin\n"
			} else if printLineStr == "rollback\n" {
				ifCommit = false
				printLineStr = "begin\n"
			} else if printLineStr == "begin\n" {
				if ifCommit {
					printLineStr = "commit\n"
				} else {
					printLineStr = "rollback\n"
				}

				ifCommit = true // default is commit
			}
			_, err = destFH.WriteString(printLineStr)
			if err != nil {
				CheckErr(err, "fail to write file "+destFile, ERR_FILE_WRITE, false)
				return err
			}
			if readByteCntTotal == srcSize || startPos == 0 {
				break
			}
			_, err = srcFH.Seek(-byteCntBeforePrint, os.SEEK_CUR)
			if err != nil {
				CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
				return err
			}

			byteCntBeforePrint = 1

		} else {
			_, err := srcFH.Seek(offset, os.SEEK_CUR)
			if err != nil {
				CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
				return err
			}
		}

	}

	return nil

}

func ReverseFileToNewFile(srcFile string, destFile string, batchLines int) error {
	srcFH, err := os.Open(srcFile)
	if err != nil {
		CheckErr(err, "fail to open file "+srcFile, ERR_FILE_OPEN, false)
		return err
	}
	defer srcFH.Close()

	destFH, err := os.Open(destFile)
	if err != nil {
		CheckErr(err, "fail to open file "+destFile, ERR_FILE_OPEN, false)
		return err
	}
	defer destFH.Close()

	srcInfo, err := srcFH.Stat()
	if err != nil {
		CheckErr(err, "fail to stat file "+srcFile, ERR_FILE_READ, false)
		return err
	}

	srcSize := srcInfo.Size() //int64

	var offset int64 = -1
	var readByteCntTotal int64 = 0
	var buf []byte = make([]byte, 1)
	var LineSep string = "\n"
	var lineCnt int = 0
	var byteCntBeforePrint int64 = 0

	_, err = srcFH.Seek(0, os.SEEK_END)
	if err != nil {
		CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
		return err
	}

	for {

		startPos, err := srcFH.Seek(offset, os.SEEK_CUR)
		if err != nil {
			CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
			return err
		}
		_, err = srcFH.Read(buf)
		if err != nil {
			CheckErr(err, "fail to read file "+srcFile, ERR_FILE_READ, false)
			return err
		}
		if string(buf) == LineSep {
			// if the last byte is EOL, donnot count
			if readByteCntTotal != 0 {
				lineCnt++
			}
		}
		readByteCntTotal++
		byteCntBeforePrint++

		// read enough lines or we alread finish reading the whole file
		if lineCnt == batchLines || readByteCntTotal == srcSize || startPos == 0 {
			printLines := make([]byte, byteCntBeforePrint-1)
			_, err := io.ReadFull(srcFH, printLines)
			if err != nil {
				CheckErr(err, "fail to read file "+srcFile, ERR_FILE_READ, false)
				return err
			}
			_, err = destFH.Write(printLines)
			if err != nil {
				CheckErr(err, "fail to write file "+destFile, ERR_FILE_WRITE, false)
				return err
			}
			if readByteCntTotal == srcSize || startPos == 0 {
				break
			}
			_, err = srcFH.Seek(-byteCntBeforePrint, os.SEEK_CUR)
			if err != nil {
				CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
				return err
			}
			lineCnt = 0
			byteCntBeforePrint = 1

		} else {
			_, err := srcFH.Seek(offset, os.SEEK_CUR)
			if err != nil {
				CheckErr(err, "fail to seek file "+srcFile, ERR_FILE_SEEK, false)
				return err
			}
		}

	}

	return nil

}
