package committee

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/partition"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// clpa 版本使用pendingtx进行分析
// MALPA committee operations
type MALPACommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int

	// additional variants
	curEpoch            int32
	clpaLock            sync.Mutex
	clpaGraph           *partition.MALPAState
	modifiedMap         map[string]uint64
	clpaLastRunningTime time.Time
	clpaFreq            int

	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss            *signal.StopSignal // to control the stop message sending
	IpNodeTable   map[uint64]map[uint64]string
	PendingTxList map[uint64]*core.Transaction
}

func NewMALPACommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, sl *supervisor_log.SupervisorLog, csvFilePath string, dataNum, batchNum, clpaFrequency int) *MALPACommitteeModule {
	cg := new(partition.MALPAState)
	cg.Init_MALPAState(0.5, 10, params.ShardNum)
	return &MALPACommitteeModule{
		csvPath:             csvFilePath,
		dataTotalNum:        dataNum,
		batchDataNum:        batchNum,
		nowDataNum:          0,
		clpaGraph:           cg,
		modifiedMap:         make(map[string]uint64),
		clpaFreq:            clpaFrequency,
		clpaLastRunningTime: time.Time{},
		IpNodeTable:         Ip_nodeTable,
		Ss:                  Ss,
		sl:                  sl,
		curEpoch:            0,
		PendingTxList:       make(map[uint64]*core.Transaction),
	}
}

func (ccm *MALPACommitteeModule) HandleOtherMessage([]byte) {}

func (ccm *MALPACommitteeModule) fetchModifiedMap(key string) uint64 {
	if val, ok := ccm.modifiedMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	} else {
		return val
	}
}

func (ccm *MALPACommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(params.ShardNum); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				go networks.TcpDial(send_msg, ccm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		sendersid := ccm.fetchModifiedMap(tx.Sender)
		sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

func (ccm *MALPACommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(ccm.csvPath)
	if err != nil {
		log.Panic(err)
	}
	defer txfile.Close()

	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	// MALPA 相关的局部状态（用 atomic 做并发安全）
	var clpaCnt int32     // 已触发的 MALPA epoch 次数
	var clpaRunning int32 // 0: 空闲, 1: 有 MALPA goroutine 在跑

	// 封装一个“按需、异步触发 MALPA”的函数
	runMALPAIfNeeded := func() {
		// 单分片不需要 MALPA
		if params.ShardNum <= 1 {
			return
		}
		// 尚未开始计时，不触发
		if ccm.clpaLastRunningTime.IsZero() {
			return
		}
		// 频率尚未到达，不触发
		if time.Since(ccm.clpaLastRunningTime) < time.Duration(ccm.clpaFreq)*time.Second {
			return
		}
		// 已有一个 MALPA 在运行，避免重复触发
		if !atomic.CompareAndSwapInt32(&clpaRunning, 0, 1) {
			return
		}

		// 记录本次 MALPA 的“序号”，拷贝成局部变量供 goroutine 使用
		myEpoch := atomic.AddInt32(&clpaCnt, 1)

		// 触发时就更新 lastRunningTime，避免 MALPA 过程中被频繁重复触发
		ccm.clpaLastRunningTime = time.Now()

		// 真正的 MALPA 放在后台 goroutine，不阻塞交易注入
		go func(expectEpoch int32) {
			defer atomic.StoreInt32(&clpaRunning, 0) // 结束后允许下一次 MALPA

			ccm.clpaLock.Lock()
			ccm.sl.Slog.Println("pending tx number:", len(ccm.PendingTxList))

			//遍历PendingTxList，更新clpaGraph
			for _, tx := range ccm.PendingTxList {
				ccm.clpaGraph.AddEdge(partition.Vertex{Addr: tx.Sender}, partition.Vertex{Addr: tx.Recipient})
			}

			mmap, _ := ccm.clpaGraph.MALPA_Partition()

			ccm.clpaMapSend(mmap)
			for key, val := range mmap {
				ccm.modifiedMap[key] = val
			}
			ccm.clpaReset()
			ccm.clpaLock.Unlock()

			// 等待链上 epoch 追到当前 MALPA 对应的 epoch
			for atomic.LoadInt32(&ccm.curEpoch) != expectEpoch {
				time.Sleep(time.Second)
			}

			ccm.sl.Slog.Println("Next MALPA epoch begins.", "epoch", expectEpoch)
		}(myEpoch)
	}

	for {
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Panic(err)
		}

		if tx, ok := data2tx(data, uint64(ccm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			ccm.nowDataNum++

		} else {
			continue
		}

		// batch sending condition
		if len(txlist) == int(ccm.batchDataNum) || ccm.nowDataNum == ccm.dataTotalNum {
			// set the algorithm timer begins
			if ccm.clpaLastRunningTime.IsZero() {
				ccm.clpaLastRunningTime = time.Now()
			}
			ccm.clpaLock.Lock()
			for tx := range txlist {
				ccm.PendingTxList[txlist[tx].Nonce] = txlist[tx]
			}
			ccm.clpaLock.Unlock()

			// 正常注入交易（主 goroutine 内顺序执行）
			ccm.txSending(txlist)

			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			ccm.Ss.StopGap_Reset()
			ccm.sl.Slog.Println("already sending tx number:", ccm.nowDataNum)
		}

		// 尝试异步触发 MALPA（不会阻塞交易注入）
		runMALPAIfNeeded()

		if ccm.nowDataNum == ccm.dataTotalNum {
			break
		}
	}

	// all transactions are sent. keep sending partition message...
	for !ccm.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		// 尾部也只做“按需异步触发 MALPA”，不再阻塞等待
		runMALPAIfNeeded()
	}
}

func (ccm *MALPACommitteeModule) clpaMapSend(m map[string]uint64) {
	// send partition modified Map message
	pm := message.PartitionModifiedMap{
		PartitionModified: m,
	}
	pmByte, err := json.Marshal(pm)
	if err != nil {
		log.Panic()
	}
	send_msg := message.MergeMessage(message.CPartitionMsg, pmByte)
	// send to worker shards
	for i := uint64(0); i < uint64(params.ShardNum); i++ {
		go networks.TcpDial(send_msg, ccm.IpNodeTable[i][0])
	}
	ccm.sl.Slog.Println("Supervisor: all partition map message has been sent. ")
}

func (ccm *MALPACommitteeModule) clpaReset() {
	ccm.clpaGraph = new(partition.MALPAState)
	// ccm.clpaGraph.Init_MALPAState(0.5, 10, params.ShardNum)
	ccm.clpaGraph.Init_MALPAState(0.5, 10, params.ShardNum)
	for key, val := range ccm.modifiedMap {
		ccm.clpaGraph.PartitionMap[partition.Vertex{Addr: key}] = int(val)
	}
}

func (ccm *MALPACommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	ccm.sl.Slog.Printf("Supervisor: received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
	if atomic.CompareAndSwapInt32(&ccm.curEpoch, int32(b.Epoch-1), int32(b.Epoch)) {
		ccm.sl.Slog.Println("this curEpoch is updated", b.Epoch)
	}
	if b.BlockBodyLength == 0 {
		return
	}
	//从pendingtx中移除已经执行完的交易

	ccm.clpaLock.Lock()
	for _, tx := range b.InnerShardTxs {
		delete(ccm.PendingTxList, tx.Nonce)
	}
	for _, r2tx := range b.Relay2Txs {
		delete(ccm.PendingTxList, r2tx.Nonce)
	}
	ccm.clpaLock.Unlock()
}
