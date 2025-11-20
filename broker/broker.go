package broker

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/params"
	"bufio"
	"fmt"
	"os"
	"sort"
)

type Broker struct {
	BrokerRawMegs  map[string]*message.BrokerRawMeg
	ChainConfig    *params.ChainConfig
	BrokerAddress  []string
	RawTx2BrokerTx map[string][]string
}

func (b *Broker) NewBroker(pcc *params.ChainConfig) {
	b.BrokerRawMegs = make(map[string]*message.BrokerRawMeg)
	b.RawTx2BrokerTx = make(map[string][]string)
	b.ChainConfig = pcc
	b.BrokerAddress = b.initBrokerAddr(params.BrokerNum)
}

func (b *Broker) IsBroker(address string) bool {
	for _, brokerAddress := range b.BrokerAddress {
		if brokerAddress == address {
			return true
		}
	}
	return false
}

func (b *Broker) initBrokerAddr(num int) []string {
	brokerAddress := make([]string, 0)
	filePath := `./broker/top_accounts`
	readFile, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
	}
	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		brokerAddress = append(brokerAddress, fileScanner.Text())
		num--
		if num == 0 {
			break
		}
	}
	readFile.Close()
	return brokerAddress
}

func dumpTopAccountsToTxt(txlist []*core.Transaction, outPath string, topN int) error {
	// 1. 统计每个账户的交易次数（这里把 from + to 都算进去）
	accCount := make(map[string]int)

	for _, tx := range txlist {
		// 下面两行需要根据你的 Transaction 结构做适配：
		// 假设有 From / To 字段或方法返回 string
		fromAddr := tx.Sender // 如果不是 String()，改成你自己的取地址方式
		toAddr := tx.Recipient

		if fromAddr != "" {
			accCount[fromAddr]++
		}
		if toAddr != "" {
			accCount[toAddr]++
		}
	}

	// 2. map 转切片，用于排序
	type accStat struct {
		Addr  string
		Count int
	}
	stats := make([]accStat, 0, len(accCount))
	for addr, cnt := range accCount {
		stats = append(stats, accStat{Addr: addr, Count: cnt})
	}

	// 3. 按 Count 降序排序
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Count > stats[j].Count
	})

	// 4. 打开输出文件
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	limit := topN
	if len(stats) < topN {
		limit = len(stats)
	}

	for i := 0; i < limit; i++ {
		fmt.Fprintf(w, "%s\n", stats[i].Addr)
	}

	// 刷新缓冲区
	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}
