package broker

//测试
import (
	"blockEmulator/core"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestBroker(t *testing.T) {
	DatasetFile := "./12000000to12999999_BlockTransaction.csv"
	TotalDataSize := 3000000
	nowDataNum := 0
	txfile, err := os.Open(DatasetFile)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()

	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}
		if tx, ok := data2tx(data, uint64(nowDataNum)); ok {
			txlist = append(txlist, tx)
			nowDataNum++
			if nowDataNum%500000 == 0 {
				fmt.Println("already deal txs:", nowDataNum)
			}
		} else {
			continue
		}
		if nowDataNum == TotalDataSize {
			break
		}

	}
	if err := dumpTopAccountsToTxt(txlist, "top_accounts", 100); err != nil {
		log.Printf("dumpTopAccountsToTxt error: %v\n", err)
	}
}
func data2tx(data []string, nonce uint64) (*core.Transaction, bool) {
	if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
		val, ok := new(big.Int).SetString(data[8], 10)
		if !ok {
			log.Panic("new int failed\n")
		}
		tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce, time.Now())
		return tx, true
	}
	return &core.Transaction{}, false
}
