package partition

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
)

// CLPA算法状态，state of constraint label propagation algorithm
type MALPAState struct {
	NetGraph          Graph          // 需运行CLPA算法的图
	PartitionMap      map[Vertex]int // 记录分片信息的 map，某个节点属于哪个分片
	Edges2Shard       []int          // Shard 相邻接的边数，对应论文中的 total weight of edges associated with label k
	VertexsNumInShard []int          // Shard 内节点的数目
	WeightPenalty     float64        // 权重惩罚，对应论文中的 beta
	MinEdges2Shard    int            // 最少的 Shard 邻接边数，最小的 total weight of edges associated with label k
	MaxIterations     int            // 最大迭代次数，constraint，对应论文中的\tau
	CrossShardEdgeNum int            // 跨分片边的总数
	ShardNum          int            // 分片数目
	GraphHash         []byte
	SchedulMap        map[int]int //迁移到某分片的状态数量
	lambda            float64
}

func (graph *MALPAState) Hash() []byte {
	hash := sha256.Sum256(graph.Encode())
	return hash[:]
}

func (graph *MALPAState) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(graph)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

// 加入节点，需要将它默认归到一个分片中
func (cs *MALPAState) AddVertex(v Vertex) {
	cs.NetGraph.AddVertex(v)
	if val, ok := cs.PartitionMap[v]; !ok {
		cs.PartitionMap[v] = utils.Addr2Shard(v.Addr)
	} else {
		cs.PartitionMap[v] = val
	}
	cs.VertexsNumInShard[cs.PartitionMap[v]] += 1 // 此处可以批处理完之后再修改 VertexsNumInShard 参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

// 加入边，需要将它的端点（如果不存在）默认归到一个分片中
func (cs *MALPAState) AddEdge(u, v Vertex) {
	// 如果没有点，则增加边，权恒定为 1
	if _, ok := cs.NetGraph.VertexSet[u]; !ok {
		cs.AddVertex(u)
	}
	if _, ok := cs.NetGraph.VertexSet[v]; !ok {
		cs.AddVertex(v)
	}
	cs.NetGraph.AddEdge(u, v)
	// 可以批处理完之后再修改 Edges2Shard 等参数
	// 当然也可以不处理，因为 CLPA 算法运行前会更新最新的参数
}

// 复制CLPA状态
func (dst *MALPAState) CopyCLPA(src MALPAState) {
	dst.NetGraph.CopyGraph(src.NetGraph)
	dst.PartitionMap = make(map[Vertex]int)
	for v := range src.PartitionMap {
		dst.PartitionMap[v] = src.PartitionMap[v]
	}
	dst.Edges2Shard = make([]int, src.ShardNum)
	copy(dst.Edges2Shard, src.Edges2Shard)
	dst.VertexsNumInShard = src.VertexsNumInShard
	dst.WeightPenalty = src.WeightPenalty
	dst.MinEdges2Shard = src.MinEdges2Shard
	dst.MaxIterations = src.MaxIterations
	dst.ShardNum = src.ShardNum

}

// 输出CLPA
func (cs *MALPAState) PrintCLPA() {
	cs.NetGraph.PrintGraph()
	println(cs.MinEdges2Shard)
	for v, item := range cs.PartitionMap {
		print(v.Addr, " ", item, "\t")
	}
	for _, item := range cs.Edges2Shard {
		print(item, " ")
	}
	println()
}

// 根据当前划分，计算 Wk，即 Edges2Shard
func (cs *MALPAState) ComputeEdges2Shard() {
	cs.Edges2Shard = make([]int, cs.ShardNum)
	interEdge := make([]int, cs.ShardNum)
	cs.MinEdges2Shard = math.MaxInt

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] = 0
		interEdge[idx] = 0
	}

	for v, lst := range cs.NetGraph.EdgeSet {
		// 获取节点 v 所属的shard
		vShard := cs.PartitionMap[v]
		for _, u := range lst {
			// 同上，获取节点 u 所属的shard
			uShard := cs.PartitionMap[u]
			if vShard != uShard {
				// 判断节点 v, u 不属于同一分片，则对应的 Edges2Shard 加一
				// 仅计算入度，这样不会重复计算
				cs.Edges2Shard[uShard] += 1
			} else {
				interEdge[uShard]++
			}
		}
	}

	cs.CrossShardEdgeNum = 0
	for _, val := range cs.Edges2Shard {
		cs.CrossShardEdgeNum += val
	}
	cs.CrossShardEdgeNum /= 2

	for idx := 0; idx < cs.ShardNum; idx++ {
		cs.Edges2Shard[idx] += interEdge[idx] / 2
	}
	// 修改 MinEdges2Shard, CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}

}

// 在账户所属分片变动时，重新计算各个参数，faster
func (cs *MALPAState) changeShardRecompute(v Vertex, old int) {
	new := cs.PartitionMap[v]
	for _, u := range cs.NetGraph.EdgeSet[v] {
		neighborShard := cs.PartitionMap[u]
		if neighborShard != new && neighborShard != old {
			cs.Edges2Shard[new]++
			cs.Edges2Shard[old]--
		} else if neighborShard == new {
			cs.Edges2Shard[old]--
			cs.CrossShardEdgeNum--
		} else {
			cs.Edges2Shard[new]++
			cs.CrossShardEdgeNum++
		}
	}
	cs.MinEdges2Shard = math.MaxInt
	// 修改 MinEdges2Shard, CrossShardEdgeNum
	for _, val := range cs.Edges2Shard {
		if cs.MinEdges2Shard > val {
			cs.MinEdges2Shard = val
		}
	}
}

// 设置参数
func (cs *MALPAState) Init_MALPAState(wp float64, mIter, sn int) {
	cs.WeightPenalty = wp
	cs.MaxIterations = mIter
	cs.ShardNum = sn
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cs.SchedulMap = make(map[int]int)
	cs.lambda = 0
}

// 初始化划分，使用节点地址的尾数划分，应该保证初始化的时候不会出现空分片
func (cs *MALPAState) Init_Partition() {
	// 设置划分默认参数
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		var va = v.Addr[len(v.Addr)-8:]
		num, err := strconv.ParseInt(va, 16, 64)
		if err != nil {
			log.Panic()
		}
		cs.PartitionMap[v] = int(num) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
}

// 不会出现空分片的初始化划分
func (cs *MALPAState) Stable_Init_Partition() error {
	// 设置划分默认参数
	if cs.ShardNum > len(cs.NetGraph.VertexSet) {
		return errors.New("too many shards, number of shards should be less than nodes. ")
	}
	cs.VertexsNumInShard = make([]int, cs.ShardNum)
	cs.PartitionMap = make(map[Vertex]int)
	cnt := 0
	for v := range cs.NetGraph.VertexSet {
		cs.PartitionMap[v] = int(cnt) % cs.ShardNum
		cs.VertexsNumInShard[cs.PartitionMap[v]] += 1
		cnt++
	}
	cs.ComputeEdges2Shard() // 删掉会更快一点，但是这样方便输出（毕竟只执行一次Init，也快不了多少）
	return nil
}

func (cs *MALPAState) getShard_score(v Vertex, uShard int, oldMap map[Vertex]int) float64 {
	// 计算 将节点 v 放入 uShard 所产生的 score
	var score float64
	// 节点 v 的出度
	v_outdegree := len(cs.NetGraph.EdgeSet[v])
	// 节点 v 所在分片
	vNowShard := cs.PartitionMap[v]
	// uShard 与节点 v 相连的边数
	Edgesto_vShard := 0
	Edgesto_uShard := 0

	for _, item := range cs.NetGraph.EdgeSet[v] {
		if cs.PartitionMap[item] == uShard {
			Edgesto_uShard += 1
		}
		if cs.PartitionMap[item] == vNowShard {
			Edgesto_vShard += 1
		}
	}
	preVNowShardLoad := float64(cs.Edges2Shard[vNowShard]) + cs.lambda*float64(cs.SchedulMap[vNowShard])
	preUShardLoad := float64(cs.Edges2Shard[uShard]) + cs.lambda*float64(cs.SchedulMap[uShard])
	beta := math.Min(preUShardLoad, preVNowShardLoad) / math.Max(preUShardLoad, preVNowShardLoad)
	// //让q迁入先加1
	if oldMap[v] != uShard {
		cs.SchedulMap[uShard] += 1
	}

	// //如果v原来不在p中，则p的schedulMap减1
	if oldMap[v] != vNowShard {
		cs.SchedulMap[vNowShard] -= 1
	}
	vNowShardLoad := float64(cs.Edges2Shard[vNowShard]) + cs.lambda*float64(cs.SchedulMap[vNowShard]) - float64(v_outdegree) + float64(Edgesto_vShard)
	uShardLoad := float64(cs.Edges2Shard[uShard]) + cs.lambda*float64(cs.SchedulMap[uShard]) + float64(v_outdegree) - float64(Edgesto_uShard)

	score = beta*(float64(Edgesto_uShard)-float64(Edgesto_vShard)-cs.lambda) + (1-beta)*(math.Abs(preVNowShardLoad-preUShardLoad)-math.Abs(vNowShardLoad-uShardLoad))
	// //让q迁入先加1
	if oldMap[v] != uShard {
		cs.SchedulMap[uShard] -= 1
	}

	//如果v原来不在p中，则p的schedulMap减1
	if oldMap[v] != vNowShard {
		cs.SchedulMap[vNowShard] += 1
	}
	return score
}

// CLPA 划分算法
func (cs *MALPAState) MALPA_Partition() (map[string]uint64, int) {

	cs.ComputeEdges2Shard()
	// cs.OutputLoad()
	log.Default().Println("Before running ELPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	res := make(map[string]uint64)
	oldPartitionMap := make(map[Vertex]int)
	for v := range cs.NetGraph.VertexSet {
		oldPartitionMap[v] = cs.PartitionMap[v]
	}
	//v优先级计算
	// sortV := cs.PriorCalculate()
	iter := 0
	for { // 第一层循环控制算法次数，constraint
		delta := 0.0

		// for _, vSort := range sortV {
		// 	v := vSort.v
		for v := range cs.NetGraph.VertexSet {
			neighborShardScore := make(map[int]float64)
			max_score := -9999.0
			vNowShard, max_scoreShard := cs.PartitionMap[v], cs.PartitionMap[v]
			for _, u := range cs.NetGraph.EdgeSet[v] {
				uShard := cs.PartitionMap[u]
				// 对于属于 uShard 的邻居，仅需计算一次
				if _, computed := neighborShardScore[uShard]; !computed {
					neighborShardScore[uShard] = cs.getShard_score(v, uShard, oldPartitionMap)
					if max_score < neighborShardScore[uShard] {
						max_score = neighborShardScore[uShard]
						max_scoreShard = uShard
					}
				}
			}
			if vNowShard != max_scoreShard && neighborShardScore[max_scoreShard] > 0 {
				cs.PartitionMap[v] = max_scoreShard
				res[v.Addr] = uint64(max_scoreShard)
				// 重新计算 VertexsNumInShard
				cs.VertexsNumInShard[vNowShard] -= 1
				cs.VertexsNumInShard[max_scoreShard] += 1
				//更新变更后迁移负载变化
				if oldPartitionMap[v] != vNowShard {
					cs.SchedulMap[vNowShard] -= 1
				}
				if oldPartitionMap[v] != max_scoreShard {
					cs.SchedulMap[max_scoreShard] += 1
				}
				// 重新计算Wk
				cs.changeShardRecompute(v, vNowShard)
				delta += neighborShardScore[max_scoreShard]
			}
		}
		iter++
		if delta < 0.00001 || iter >= 10 {
			println("迭代次数:", iter)
			break
		}
	}
	for v := range cs.NetGraph.VertexSet {
		if oldPartitionMap[v] == cs.PartitionMap[v] {
			delete(res, v.Addr)
		}
	}

	cs.ComputeEdges2Shard()
	log.Default().Println("After running ELPA, cross-shard edge number:", cs.CrossShardEdgeNum)
	return res, cs.CrossShardEdgeNum
}

type SortVertex struct {
	prior float64
	v     Vertex
}

func (cs *MALPAState) PriorCalculate() []SortVertex {

	sortV := make([]SortVertex, 0, len(cs.NetGraph.VertexSet))
	for v := range cs.NetGraph.VertexSet {
		prior := 0.5 * float64(len(cs.NetGraph.EdgeSet[v]))
		sortV = append(sortV, SortVertex{prior: float64(prior), v: v})
	}
	sort.Slice(sortV, func(i, j int) bool {
		return sortV[i].prior > sortV[j].prior // 降序排序
	})
	return sortV

}

func (cs *MALPAState) EraseEdges() {
	cs.NetGraph.EdgeSet = make(map[Vertex][]Vertex)
}
func (cs *MALPAState) OutputLoad() {
	allLoad := 0.0
	loads := make([]float64, params.ShardNum)

	for i := 0; i < params.ShardNum; i++ {
		shardLoad := float64(cs.Edges2Shard[i]) + cs.lambda*float64(cs.SchedulMap[i])
		loads[i] = shardLoad
		allLoad += shardLoad
		fmt.Printf("Shard %d tx Load: %d; schedul load: %f; Total: %f\n", i, cs.Edges2Shard[i], cs.lambda*float64(cs.SchedulMap[i]), shardLoad)
	}

	// 计算均值
	meanLoad := allLoad / float64(params.ShardNum)

	// 计算方差
	variance := 0.0
	for _, load := range loads {
		variance += math.Pow(math.Ceil(load/float64(params.MaxBlockSize_global))-math.Ceil(meanLoad/float64(params.MaxBlockSize_global)), 2)
	}
	variance /= float64(params.ShardNum)

	fmt.Printf("All load: %f\n", allLoad)
	fmt.Printf("Load variance: %f\n", variance)
}
func (cs *MALPAState) OutputSchedulMap() {
	schedulNum := 0
	for i := 0; i < params.ShardNum; i++ {
		log.Default().Printf("Shard %d schedulMap: %d\n", i, cs.SchedulMap[i])
		schedulNum += cs.SchedulMap[i]
	}
	log.Default().Printf("Shard total schedulMap: %d\n", schedulNum)
}
