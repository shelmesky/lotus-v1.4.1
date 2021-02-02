package sectorstorage

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/xerrors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0
var SelectorTimeout = 5 * time.Second
var InitWait = 3 * time.Second

var (
	SchedWindows = 2
)

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error) // true if a is preferred over b
}

type scheduler struct {
	workersLk sync.RWMutex
	workers   map[WorkerID]*workerHandle

	schedule       chan *workerRequest
	windowRequests chan *schedWindowRequest
	workerChange   chan struct{} // worker added / changed/freed resources
	workerDisable  chan workerDisableReq

	// owned by the sh.runSched goroutine
	schedQueue  *requestQueue
	openWindows []*schedWindowRequest

	workTracker *workTracker

	info chan func(interface{})

	closing  chan struct{}
	closed   chan struct{}
	testSync chan struct{} // used for testing

	manager *Manager
}

type workerHandle struct {
	workerRpc Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources

	lk sync.Mutex

	wndLk         sync.Mutex
	activeWindows []*schedWindow

	enabled bool

	// for sync manager goroutine closing
	cleanupStarted bool
	closedMgr      chan struct{}
	closingMgr     chan struct{}
}

type schedWindowRequest struct {
	worker WorkerID

	done chan *schedWindow
}

type schedWindow struct {
	allocated activeResources
	todo      []*workerRequest
}

type workerDisableReq struct {
	activeWindows []*schedWindow
	wid           WorkerID
	done          func()
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerRequest struct {
	sector   storage.SectorRef
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	start time.Time

	index int // The index of the item in the heap.

	indexHeap int
	ret       chan<- workerResponse
	ctx       context.Context
}

type workerResponse struct {
	err error
}

func newScheduler() *scheduler {
	return &scheduler{
		workers: map[WorkerID]*workerHandle{},

		schedule:       make(chan *workerRequest),
		windowRequests: make(chan *schedWindowRequest, 20),
		workerChange:   make(chan struct{}, 20),
		workerDisable:  make(chan workerDisableReq),

		schedQueue: &requestQueue{},

		workTracker: &workTracker{
			done:    map[storiface.CallID]struct{}{},
			running: map[storiface.CallID]trackedWork{},
		},

		info: make(chan func(interface{})),

		closing: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

// 扇区任务请求
type SectorRequest struct {
	Sector   storage.SectorRef
	TaskType sealtasks.TaskType
	Selector WorkerSelector
	Prepare  WorkerAction
	Work     WorkerAction
	RetChan  chan workerResponse
}

// 按照Value字段排序
type SortStruct struct {
	Hostname string
	Value    int
}

type ByValue []SortStruct

func (a ByValue) Len() int           { return len(a) }
func (a ByValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByValue) Less(i, j int) bool { return a[i].Value < a[j].Value }

var ErrorNoAvaiableWorker = errors.New("Can not find any worker.")

var taskTypeList []sealtasks.TaskType = []sealtasks.TaskType{
	sealtasks.TTAddPiece,
	sealtasks.TTPreCommit1,
	sealtasks.TTPreCommit2,
	sealtasks.TTCommit1,
	sealtasks.TTCommit2,
	sealtasks.TTFinalize,
	sealtasks.TTFetch,
	sealtasks.TTReadUnsealed,
	sealtasks.TTUnseal,
}

// 定义worker的任务相关属性
type WorkerTaskSpecs struct {
	Hostname        string
	CurrentAP       int
	MaxAP           int
	CurrentPC1      int
	MaxPC1          int
	CurrentPC2      int
	MaxPC2          int
	CurrentC1       int
	MaxC1           int
	CurrentC2       int
	MaxC2           int
	CurrentFinalize int
	MaxFinalize     int
	CurrentFetch    int
	MaxFetch        int
	RequestSignal   chan *SectorRequest                        // 接收任务的队列
	RequestQueueMap map[sealtasks.TaskType]chan *SectorRequest // 每种任务类型对应的队列
	PendingList     *list.List                                 // 收集无法执行的任务，保存到这个队列
	StopChan        chan bool                                  // 接收停止信号的channel
	Locker          *sync.Mutex                                // 互斥锁
	Scheduler       *scheduler                                 // 全局调度器
}

func NewWorkerTaskSpeec(sh *scheduler, hostname string, maxAP, maxPC1, maxPC2, maxC1, maxC2,
	maxFinal, maxFetch int) *WorkerTaskSpecs {

	var workerSpece WorkerTaskSpecs

	workerSpece.Locker = new(sync.Mutex)

	workerSpece.Hostname = hostname
	workerSpece.MaxAP = maxAP
	workerSpece.MaxPC1 = maxPC1
	workerSpece.MaxPC2 = maxPC2
	workerSpece.MaxC1 = maxC1
	workerSpece.MaxC2 = maxC2
	workerSpece.MaxFinalize = maxFinal
	workerSpece.MaxFetch = maxFetch

	workerSpece.RequestSignal = make(chan *SectorRequest, 512)

	workerSpece.RequestQueueMap = make(map[sealtasks.TaskType]chan *SectorRequest, 16)

	for i := 0; i < len(taskTypeList); i++ {
		taskType := taskTypeList[i]
		workerSpece.RequestQueueMap[taskType] = make(chan *SectorRequest, 512)
	}

	workerSpece.PendingList = list.New()

	workerSpece.StopChan = make(chan bool)
	workerSpece.Scheduler = sh

	go workerSpece.runWorkerTaskLoop()

	return &workerSpece
}

func (workerSpec *WorkerTaskSpecs) runWorkerTaskLoop() {
	log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始运行...", workerSpec.Hostname)
	for {
		var sectorReq *SectorRequest
		hasTask := false
		select {
		case sectorReq = <-workerSpec.RequestSignal:
			workerSpec.RequestQueueMap[sectorReq.TaskType] <- sectorReq
			hasTask = true
			log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 获取到任务 [%v]\n",
				workerSpec.Hostname, DumpRequest(sectorReq))

		case <-workerSpec.StopChan:
			log.Warnf("Worker: [%v] runWorkerTaskLoop() 退出!\n", workerSpec.Hostname)
			return

		case <-time.After(10 * time.Second):
			log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 定时器到期...", workerSpec.Hostname)
		}

		workerSpec.Locker.Lock()

		var req *SectorRequest

		// AddPiece
		if workerSpec.CurrentAP < workerSpec.MaxAP {
			queue := workerSpec.RequestQueueMap[sealtasks.TTAddPiece]
			if len(queue) > 0 {
				req = <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentAP += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n", workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] AddPiece 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentAP, workerSpec.MaxAP)
		}

		// PreCommit1
		if workerSpec.CurrentPC1 < workerSpec.MaxPC1 {
			queue := workerSpec.RequestQueueMap[sealtasks.TTPreCommit1]
			if len(queue) > 0 {
				req = <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentPC1 += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n",
					workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] PreCommit1 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentPC1, workerSpec.MaxPC1)
		}

		// PreCommit2
		if workerSpec.CurrentPC2 < workerSpec.MaxPC2 {
			queue := workerSpec.RequestQueueMap[sealtasks.TTPreCommit2]
			if len(queue) > 0 {
				req = <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentPC2 += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n",
					workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] PreCommit2 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentPC2, workerSpec.MaxPC2)
		}

		// Commit1
		if workerSpec.CurrentC1 < workerSpec.MaxC1 {
			queue := workerSpec.RequestQueueMap[sealtasks.TTCommit1]
			if len(queue) > 0 {
				req = <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentC1 += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n",
					workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] Commit1 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentC1, workerSpec.MaxC1)
		}

		// Commit2
		if workerSpec.CurrentC2 < workerSpec.MaxC2 {
			queue := workerSpec.RequestQueueMap[sealtasks.TTCommit2]
			if len(queue) > 0 {
				req = <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentC2 += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n",
					workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] Commit2 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentC2, workerSpec.MaxC2)
		}

		// Finalize
		if workerSpec.CurrentFinalize < workerSpec.MaxFinalize {
			queue := workerSpec.RequestQueueMap[sealtasks.TTFinalize]
			if len(queue) > 0 {
				req := <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentFinalize += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n",
					workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] Finalize 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentFinalize, workerSpec.MaxFinalize)
		}

		// Fetch
		if workerSpec.CurrentFetch < workerSpec.MaxFetch {
			queue := workerSpec.RequestQueueMap[sealtasks.TTFetch]
			if len(queue) > 0 {
				req := <-queue
				go workerSpec.runTask(req)
				workerSpec.CurrentFetch += 1
				log.Debugf("^^^^^^^^ runWorkerTaskLoop() Worker [%v] 开始执行任务 [%v]\n",
					workerSpec.Hostname, DumpRequest(req))
			}
		} else {
			log.Warnf("^^^^^^^^ !!! Worker:[%v] Fetch 达到最大数量: [Currnet: %v -> Max: %v] !!!\n",
				workerSpec.Hostname, workerSpec.CurrentFetch, workerSpec.MaxFetch)
		}

		// 记录扇区在本worker上执行过
		if req != nil {
			lotusSealingWorkers.SaveWorkTaskAssign(req, workerSpec.Hostname)
			log.Debugf("^^^^^^^^ Worker[%v] -> runWorkerTaskLoop() 保存扇区 [%v] 记录\n",
				workerSpec.Hostname, req.Sector.ID)

			var next *list.Element
			for e := workerSpec.PendingList.Front(); e != nil; e = next {
				value := e.Value.(*SectorRequest)
				if value == req {
					log.Debugf("^^^^^^^ Worker[%v] -> runWorkerTaskLoop() -> "+
						"PendingList: 删除任务: [%v]\n", DumpRequest(req))
					workerSpec.PendingList.Remove(e)
				}
				next = e.Next()
			}
		} else {
			if hasTask {
				workerSpec.PendingList.PushBack(sectorReq) // 将任务放在挂起队列中
				log.Warnf("^^^^^^^^ Worker[%v] -> runWorkerTaskLoop() 接收到任务 [%v]，"+
					"但是任务数量已满，放入挂起队列，暂未执行.",
					workerSpec.Hostname, DumpRequest(sectorReq))
			}
		}

		workerSpec.Locker.Unlock()
	}
}

// 运行扇区任务
func (workerSpec *WorkerTaskSpecs) runTask(request *SectorRequest) {
	hostname := workerSpec.Hostname
	sh := workerSpec.Scheduler

	log.Debugf("^^^^^^^^ 任务：[%v] Worker:[%v] -> runTask()：获取到最优的Worker: [%v]\n",
		DumpRequest(request), workerSpec.Hostname, hostname)

	var workerID WorkerID
	var Worker *workerHandle

	// 根据worker名字，找到worker的处理接口。
	for {
		sh.workersLk.Lock()
		for wid, w := range sh.workers {
			log.Debugf("^^^^^^^^ Worker:[%v] -> runTask()：打印所有worker: [%v], [%v]\n", workerSpec.Hostname,
				wid, w.info.Hostname)
			if w.info.Hostname == hostname {
				Worker = w
				sh.workersLk.Unlock()
				log.Debugf("^^^^^^^^ 任务：[%v] Worker:[%v] -> runTask()：最优Worker [%v]　在线!\n",
					DumpRequest(request), workerSpec.Hostname, w.info.Hostname)
				goto Run
			}
		}

		sh.workersLk.Unlock()
		if Worker == nil {
			log.Debugf("^^^^^^^^ 任务: [%v] Worker:[%v] -> runTask()：最优Worker [%v]　离线，等待Worker上线!\n",
				DumpRequest(request), workerSpec.Hostname, hostname)
			time.Sleep(time.Second * 3)
		}
	}

Run:

	log.Debugf("^^^^^^^^ Worker:[%v] -> runTask(): 任务: [%v] 已经调度到 Worker [%v]上执行。\n",
		workerSpec.Hostname, DumpRequest(request), Worker.info.Hostname)

	workFunc := func(ret chan workerResponse) {

		err := request.Prepare(context.TODO(), sh.workTracker.worker(workerID, Worker.workerRpc))
		if err != nil {
			log.Errorf("^^^^^^^^ 任务：[%v] Worker:[%v] -> runTask() Prepare 函数执行失败 [%v]\n",
				DumpRequest(request), workerSpec.Hostname, err)
			ret <- workerResponse{err: err}
		}

		err = request.Work(context.TODO(), sh.workTracker.worker(workerID, Worker.workerRpc))
		if err != nil {
			log.Errorf("^^^^^^^^ 任务：[%v] Worker:[%v] -> runTask() Work 函数执行失败 [%v]\n",
				DumpRequest(request), workerSpec.Hostname, err)
		}

		// 任务完成后，计数器减一
		workerSpec.Locker.Lock()
		switch request.TaskType {
		case sealtasks.TTAddPiece:
			workerSpec.CurrentAP -= 1
		case sealtasks.TTPreCommit1:
			workerSpec.CurrentPC1 -= 1
		case sealtasks.TTPreCommit2:
			workerSpec.CurrentPC2 -= 1
		case sealtasks.TTCommit1:
			workerSpec.CurrentC1 -= 1
		case sealtasks.TTCommit2:
			workerSpec.CurrentC2 -= 1
		case sealtasks.TTFinalize:
			workerSpec.CurrentFinalize -= 1
		case sealtasks.TTFetch:
			workerSpec.CurrentFetch -= 1
		}
		workerSpec.Locker.Unlock()

		ret <- workerResponse{err: err}

	}

	log.Debugf("^^^^^^^^ 任务：[%v] Worker:[%v] -> runTask() 执行扇区任务。\n",
		DumpRequest(request), workerSpec.Hostname)

	go workFunc(request.RetChan)
}

// 调度器依赖的数据
type SealingWorkers struct {
	WorkerList    map[string]*WorkerTaskSpecs // 保存所有worker属性
	Locker        *sync.RWMutex               // 读写锁
	WorkAssignMap map[abi.SectorID]string     // 保存已分配的，扇区ID对应的worker
	ScheduleQueue chan *SectorRequest         // 任务队列
	StopChan      chan struct{}               // 接收停止信号的队列
	DefaultNode   *WorkerTaskSpecs            // 默认的worker
}

// 检查某个worker的任务类型是否已满
func (workers *SealingWorkers) GetWorkerCount(request *SectorRequest, hostname string) bool {
	if worker, ok := workers.WorkerList[hostname]; ok {
		switch request.TaskType {
		case sealtasks.TTAddPiece:
			if worker.CurrentAP >= worker.MaxAP {
				return true
			}
		case sealtasks.TTPreCommit1:
			if worker.CurrentPC1 >= worker.MaxPC1 {
				return true
			}
		case sealtasks.TTPreCommit2:
			if worker.CurrentPC2 >= worker.MaxPC2 {
				return true
			}
		case sealtasks.TTCommit1:
			if worker.CurrentC1 >= worker.MaxC1 {
				return true
			}
		case sealtasks.TTCommit2:
			if worker.CurrentC2 >= worker.MaxC2 {
				return true
			}
		case sealtasks.TTFinalize:
			if worker.CurrentFinalize >= worker.MaxFinalize {
				return true
			}
		case sealtasks.TTFetch:
			if worker.CurrentFetch >= worker.MaxFetch {
				return true
			}
		}
	}

	return false
}

// 将分配给扇区的worker的hostname保存
func (workers *SealingWorkers) SaveWorkTaskAssign(request *SectorRequest, hostname string) {
	workers.WorkAssignMap[request.Sector.ID] = hostname
}

// 根据扇区ID，获取曾经执行过该扇区的worker.
func (workers *SealingWorkers) GetSectorWorker(request *SectorRequest) string {
	if hostname, ok := workers.WorkAssignMap[request.Sector.ID]; ok {
		return hostname
	}

	return ""
}

/*
1. 过滤掉任务数量是0的worker.
2. 过滤掉不满足条件的worker.
*/
func (workers *SealingWorkers) GetWorkerList(request *SectorRequest, filterList []string) []*WorkerTaskSpecs {
	var workerList []*WorkerTaskSpecs

	checkFilterFunc := func(hostname string) bool {
		for idx := range filterList {
			if hostname == filterList[idx] {
				return true
			}
		}
		return false
	}

	for _, v := range workers.WorkerList {
		switch request.TaskType {
		case sealtasks.TTAddPiece:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxAP == 0 {
				continue
			}
		case sealtasks.TTPreCommit1:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxPC1 == 0 {
				continue
			}
		case sealtasks.TTPreCommit2:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxPC2 == 0 {
				continue
			}
		case sealtasks.TTCommit1:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxC1 == 0 {
				continue
			}
		case sealtasks.TTCommit2:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxC2 == 0 {
				continue
			}
		case sealtasks.TTFinalize:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxFinalize == 0 {
				continue
			}
		case sealtasks.TTFetch:
			if checkFilterFunc(v.Hostname) {
				continue
			}
			if v.MaxFetch == 0 {
				continue
			}
		}

		workerList = append(workerList, v)
	}

	return workerList
}

// 根据任务类型，取所有woker中任务数量最小的那个worker，返回它的hostname。
func (workers *SealingWorkers) GetMinTaskWorker(request *SectorRequest, filterList []string) (string, error) {
	var sortByValue ByValue

	// 将某些workeru过滤掉
	workerList := workers.GetWorkerList(request, filterList)

	for idx := range workerList {
		v := workerList[idx]

		var sortItem SortStruct
		sortItem.Hostname = v.Hostname

		switch request.TaskType {
		case sealtasks.TTAddPiece:
			sortItem.Value = v.CurrentAP
		case sealtasks.TTPreCommit1:
			sortItem.Value = v.CurrentPC1
		case sealtasks.TTPreCommit2:
			sortItem.Value = v.CurrentPC2
		case sealtasks.TTCommit1:
			sortItem.Value = v.CurrentC1
		case sealtasks.TTCommit2:
			sortItem.Value = v.CurrentC2
		case sealtasks.TTFinalize:
			sortItem.Value = v.CurrentFinalize
		case sealtasks.TTFetch:
			sortItem.Value = v.CurrentFetch
		}

		sortByValue = append(sortByValue, sortItem)
	}

	sort.Sort(sortByValue)
	if len(sortByValue) > 0 {
		hostname := sortByValue[0].Hostname
		return hostname, nil
	}

	return "", ErrorNoAvaiableWorker
}

var lotusSealingWorkers *SealingWorkers

func InitWorerList(scheduler *scheduler) {
	lotusSealingWorkers = new(SealingWorkers)

	lotusSealingWorkers.Locker = new(sync.RWMutex)

	lotusSealingWorkers.Locker.Lock()
	defer lotusSealingWorkers.Locker.Unlock()

	lotusSealingWorkers.WorkerList = make(map[string]*WorkerTaskSpecs, 512)

	node1 := NewWorkerTaskSpeec(scheduler, "miner-node-1", 0, 4, 2, 2,
		2, 4, 4)
	//node2 := NewWorkerTaskSpeec(scheduler, "worker-node-1", 4, 4, 2, 2, 2, 2)
	node3 := NewWorkerTaskSpeec(scheduler, "worker-node-2", 1, 4, 2, 2,
		2, 4, 4)

	lotusSealingWorkers.WorkerList[node1.Hostname] = node1
	//lotusSealingWorkers.WorkerList[node2.Hostname] = node2
	lotusSealingWorkers.WorkerList[node3.Hostname] = node3

	lotusSealingWorkers.Locker = new(sync.RWMutex)
	lotusSealingWorkers.WorkAssignMap = make(map[abi.SectorID]string, 512)
	lotusSealingWorkers.ScheduleQueue = make(chan *SectorRequest, 512)
	lotusSealingWorkers.StopChan = make(chan struct{})

	lotusSealingWorkers.DefaultNode = node1
}

func DumpRequest(req *SectorRequest) string {
	return fmt.Sprintf("扇区: %v, 类型: %v", req.Sector.ID, req.TaskType)
}

func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType,
	sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	request := &SectorRequest{
		Sector:   sector,
		TaskType: taskType,
		Selector: sel,
		Prepare:  prepare,
		Work:     work,
		RetChan:  ret,
	}

	select {
	case lotusSealingWorkers.ScheduleQueue <- request:
		log.Infof("^^^^^^^^ Schedule() 将任务 [%v] 发送到调度队列.\n", DumpRequest(request))
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		log.Infof("^^^^^^^^ Schedule() 任务 [%v] 返回结果.\n", DumpRequest(request))
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sh *scheduler) getSectorWorker(sectorNumber abi.SectorNumber) string {
	var stats []WorkState
	if err := sh.manager.work.List(&stats); err != nil {
		log.Error("getting work IDs")
	}

	for idx := range stats {
		stat := stats[idx]
		var args_list [][]interface{}
		err := json.Unmarshal([]byte(stat.ID.Params), &args_list)
		if err != nil {
			log.Debugf("^^^^^^^^ getSectorWorker() 反序列化扇区参数错误: [%v]\n", err)
			return ""
		}

		if len(args_list) > 0 {
			if len(args_list[0]) > 0 {
				sector := args_list[0][0]

				if sectorObj, ok := sector.(map[string]interface{}); ok {
					if WorkID, ok := sectorObj["ID"].(map[string]interface{}); ok {
						if sectorNumberFloat, ok := WorkID["Number"].(float64); ok {
							if int(sectorNumberFloat) == int(sectorNumber) {
								return stat.WorkerHostname
							}
						}
					}
				}
			}
		}
	}

	return ""
}

func (sh *scheduler) doSched() {

	for {

		var workeRequest *SectorRequest
		select {
		case workeRequest = <-lotusSealingWorkers.ScheduleQueue:
			log.Infof("^^^^^^^^ doSched() -> 接收到任务请求 [%v].", DumpRequest(workeRequest))
		case <-lotusSealingWorkers.StopChan:
			return
		}

		for {
			if len(sh.workers) != len(lotusSealingWorkers.WorkerList) {
				time.Sleep(3 * time.Second)
				log.Debugf("^^^^^^^^ doSched() -> 等待所有Worker上线...\n")
			} else {
				log.Debugf("^^^^^^^^ doSched() -> 所有Worker已上线...\n")
				break
			}
		}

		var bestWorkerName string
		var err error

		lotusSealingWorkers.Locker.Lock()
		// 如果是AddPiece任务，直接选择一个任务数量最小的worker执行.
		// 如果数量最小的worker超过了任务最大限制，或者没有可用的worker，
		// 就直接把扇区任务PUSH到等待队列中。
		if workeRequest.TaskType == sealtasks.TTAddPiece {

			// 超找add piece最小任务数量的worker
			bestWorkerName, err = lotusSealingWorkers.GetMinTaskWorker(workeRequest, []string{})
			if err == ErrorNoAvaiableWorker {
				log.Errorf("^^^^^^^^ doSched() -> AddPiece任务无法找到合适的worker\n")
			}

		} else if workeRequest.TaskType == sealtasks.TTFetch {

			// 如果是Fetch类型任务，让miner执行。
			// TODO: 执行sector selector选择worker.
			bestWorkerName = lotusSealingWorkers.DefaultNode.Hostname

			log.Infof("^^^^^^^^ doSched() 任务 [%v] 是Fetch类型的任务，选择在默认Worker [%v] 运行.\n",
				DumpRequest(workeRequest), bestWorkerName)

		} else {

			// 根据selector过滤器选择正确的worker
			sh.workersLk.RLock()
			log.Debugf("^^^^^^^^ doSched() -> 任务[%v] 开始执行过滤器，Worker数量：[%d]\n",
				DumpRequest(workeRequest), len(sh.workers))

			var invalidWorkerList []string
			for idx := range sh.workers {
				workerHander := sh.workers[idx]
				log.Debugf("^^^^^^^^ doSched() -> 任务[%v] 在Worker: [%v] 上应用过滤规则\n",
					DumpRequest(workeRequest), workerHander.info.Hostname)

				ok, err := workeRequest.Selector.Ok(context.TODO(), workeRequest.TaskType, workeRequest.Sector.ProofType,
					workerHander)
				if err != nil {
					log.Errorf("^^^^^^^^  doSched() -> 任务: [%v] Woker [%v] 执行 Selector.OK() 错误 [%v]\n",
						DumpRequest(workeRequest), workerHander.info.Hostname, err)
					invalidWorkerList = append(invalidWorkerList, workerHander.info.Hostname)
					continue
				}
				if !ok {
					log.Debugf("^^^^^^^^ doSched() -> 任务: [%v] Worker [%v] 不符合过滤器\n",
						DumpRequest(workeRequest), workerHander.info.Hostname)
					invalidWorkerList = append(invalidWorkerList, workerHander.info.Hostname)
					continue
				}
			}
			sh.workersLk.RUnlock()

			// 如果是其他类型：PC1、PC2、C1、C2、Finalize
			// 查找扇区是否曾经在某个worker上执行
			hostname := lotusSealingWorkers.GetSectorWorker(workeRequest)

			// 如果任务曾经在worker上执行
			if len(hostname) > 0 {

				workerList := lotusSealingWorkers.GetWorkerList(workeRequest, []string{})

				// 如果任务是PC1，并且该worker上的任务数量已满，就寻找一个未满的worker.
				// 如果未找到，就寻找所有worker中数量最小的那个.
				if lotusSealingWorkers.GetWorkerCount(workeRequest, hostname) &&
					workeRequest.TaskType == sealtasks.TTPreCommit1 {

					log.Warnf("^^^^^^^^ doSched() -> 任务 [%v] 找到了之前曾经运行的 Worker[%v],"+
						"但是该Worker任务数量过多，尝试寻找未超过数量的Worker.\n", DumpRequest(workeRequest), hostname)

					var tempName string
					foundAvaiable := false
					for i := 0; i < len(lotusSealingWorkers.WorkerList); i++ {
						tempName = workerList[i].Hostname
						overLoad := lotusSealingWorkers.GetWorkerCount(workeRequest, tempName)
						if !overLoad {
							foundAvaiable = true
							break
						}
					}

					if foundAvaiable {
						hostname = tempName
						log.Warnf("^^^^^^^^ doSched() -> 任务 [%v] 找到了数量更少的Worker[%v].\n",
							DumpRequest(workeRequest), hostname)
					} else {
						minTaskWorker, err := lotusSealingWorkers.GetMinTaskWorker(workeRequest, []string{})
						if err == ErrorNoAvaiableWorker {
							log.Errorf("^^^^^^^^ doSched() -> 任务 [%v] 无法找到合适的worker\n",
								DumpRequest(workeRequest))
						}
						hostname = minTaskWorker
						log.Warnf("^^^^^^^^ doSched() -> 任务 [%v] 所有的Worker数量已满"+
							"只能寻找其中最小数量的Worker[%v]\n", DumpRequest(workeRequest), hostname)
					}
				}

				// 找到合适的worker
				bestWorkerName = hostname
				log.Infof("^^^^^^^^ doSched() -> 任务 [%v] 在Worker [%v] 上曾经运行过，继续选择该Worker.\n",
					DumpRequest(workeRequest), bestWorkerName)
			} else {
				// 任务从来未在任何worker上运行过
				// 尝试在m.work中查找之前保存过的worker和任务分配记录
				oldWorker := sh.getSectorWorker(workeRequest.Sector.ID.Number)
				if len(oldWorker) > 0 {
					bestWorkerName = oldWorker
					log.Infof("^^^^^^^^ doSched() -> 任务 [%v] 在数据库中记录在Worker [%v] 上运行过，继续选择该Worker.\n",
						DumpRequest(workeRequest), bestWorkerName)
				} else {
					log.Infof("^^^^^^^^ oSched() -> 任务 [%v] 使用过滤器: [%v] 查询数量最小的Worker. ",
						DumpRequest(workeRequest), invalidWorkerList)
					// 如果找不到就找最小任务数量的worker，同时应用过滤器，过滤掉不符合规则的worker.
					bestWorkerName, err = lotusSealingWorkers.GetMinTaskWorker(workeRequest, invalidWorkerList)
					if err == ErrorNoAvaiableWorker {
						log.Errorf("^^^^^^^^ doSched() -> 任务 [%v] 无法找到合适的worker\n",
							DumpRequest(workeRequest))
					}
					log.Infof("^^^^^^^^ doSched() -> 任务 [%v] 从未在任何Worker上运行过，选择任务数最小的Worker [%v] 运行.\n",
						DumpRequest(workeRequest), bestWorkerName)
				}
			}
		}

		log.Infof("^^^^^^^^ doSched() -> 任务 [%v] 在worker [%v]上执行.\n",
			DumpRequest(workeRequest), bestWorkerName)

		// 找到worker，把任务请求发送到worker的任务队列中。
		if worker, ok := lotusSealingWorkers.WorkerList[bestWorkerName]; ok {
			worker.RequestSignal <- workeRequest
			log.Infof("^^^^^^^^ doSched() -> 任务 [%v] 发送到了 Worker [%v] 的任务队列中.\n",
				DumpRequest(workeRequest), bestWorkerName)
		}

		lotusSealingWorkers.Locker.Unlock()
	}
}

/*
func (sh *scheduler) Schedule(ctx context.Context, sector storage.SectorRef, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		start: time.Now(),

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}
*/

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type SchedDiagRequestInfo struct {
	Sector   abi.SectorID
	TaskType sealtasks.TaskType
	Priority int
}

type SchedDiagInfo struct {
	Requests    []SchedDiagRequestInfo
	OpenWindows []string
}

func (sh *scheduler) runSched() {
	InitWorerList(sh)
	go sh.doSched()

	defer close(sh.closed)

	iw := time.After(InitWait)
	var initialised bool

	for {
		var doSched bool
		var toDisable []workerDisableReq

		select {
		case <-sh.workerChange:
			doSched = true
		case dreq := <-sh.workerDisable:
			toDisable = append(toDisable, dreq)
			doSched = true
		case req := <-sh.schedule:
			sh.schedQueue.Push(req)
			doSched = true

			if sh.testSync != nil {
				sh.testSync <- struct{}{}
			}
		case req := <-sh.windowRequests:
			sh.openWindows = append(sh.openWindows, req)
			doSched = true
		case ireq := <-sh.info:
			ireq(sh.diag())

		case <-iw:
			initialised = true
			iw = nil
			doSched = true
		case <-sh.closing:
			sh.schedClose()
			return
		}

		if doSched && initialised {
			// First gather any pending tasks, so we go through the scheduling loop
			// once for every added task
		loop:
			for {
				select {
				case <-sh.workerChange:
				case dreq := <-sh.workerDisable:
					toDisable = append(toDisable, dreq)
				case req := <-sh.schedule:
					sh.schedQueue.Push(req)
					if sh.testSync != nil {
						sh.testSync <- struct{}{}
					}
				case req := <-sh.windowRequests:
					sh.openWindows = append(sh.openWindows, req)
				default:
					break loop
				}
			}

			for _, req := range toDisable {
				for _, window := range req.activeWindows {
					for _, request := range window.todo {
						sh.schedQueue.Push(request)
					}
				}

				openWindows := make([]*schedWindowRequest, 0, len(sh.openWindows))
				for _, window := range sh.openWindows {
					if window.worker != req.wid {
						openWindows = append(openWindows, window)
					}
				}
				sh.openWindows = openWindows

				sh.workersLk.Lock()
				sh.workers[req.wid].enabled = false
				sh.workersLk.Unlock()

				req.done()
			}

			sh.trySched()
		}

	}
}

func (sh *scheduler) diag() SchedDiagInfo {
	var out SchedDiagInfo

	for sqi := 0; sqi < sh.schedQueue.Len(); sqi++ {
		task := (*sh.schedQueue)[sqi]

		out.Requests = append(out.Requests, SchedDiagRequestInfo{
			Sector:   task.sector.ID,
			TaskType: task.taskType,
			Priority: task.priority,
		})
	}

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	for _, window := range sh.openWindows {
		out.OpenWindows = append(out.OpenWindows, uuid.UUID(window.worker).String())
	}

	return out
}

func (sh *scheduler) trySched() {
	/*
		This assigns tasks to workers based on:
		- Task priority (achieved by handling sh.schedQueue in order, since it's already sorted by priority)
		- Worker resource availability
		- Task-specified worker preference (acceptableWindows array below sorted by this preference)
		- Window request age

		1. For each task in the schedQueue find windows which can handle them
		1.1. Create list of windows capable of handling a task
		1.2. Sort windows according to task selector preferences
		2. Going through schedQueue again, assign task to first acceptable window
		   with resources available
		3. Submit windows with scheduled tasks to workers

	*/

	sh.workersLk.RLock()
	defer sh.workersLk.RUnlock()

	windowsLen := len(sh.openWindows)
	queuneLen := sh.schedQueue.Len()

	log.Debugf("SCHED %d queued; %d open windows", queuneLen, windowsLen)

	if windowsLen == 0 || queuneLen == 0 {
		// nothing to schedule on
		return
	}

	windows := make([]schedWindow, windowsLen)
	acceptableWindows := make([][]int, queuneLen)

	// Step 1
	throttle := make(chan struct{}, windowsLen)

	var wg sync.WaitGroup
	wg.Add(queuneLen)
	for i := 0; i < queuneLen; i++ {
		throttle <- struct{}{}

		go func(sqi int) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			task := (*sh.schedQueue)[sqi]
			needRes := ResourceTable[task.taskType][task.sector.ProofType]

			task.indexHeap = sqi
			for wnd, windowRequest := range sh.openWindows {
				worker, ok := sh.workers[windowRequest.worker]
				if !ok {
					log.Errorf("worker referenced by windowRequest not found (worker: %s)", windowRequest.worker)
					// TODO: How to move forward here?
					continue
				}

				if !worker.enabled {
					log.Debugw("skipping disabled worker", "worker", windowRequest.worker)
					continue
				}

				// TODO: allow bigger windows
				if !windows[wnd].allocated.canHandleRequest(needRes, windowRequest.worker, "schedAcceptable", worker.info.Resources) {
					continue
				}

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				ok, err := task.sel.Ok(rpcCtx, task.taskType, task.sector.ProofType, worker)
				cancel()
				if err != nil {
					log.Errorf("trySched(1) req.sel.Ok error: %+v", err)
					continue
				}

				if !ok {
					continue
				}

				acceptableWindows[sqi] = append(acceptableWindows[sqi], wnd)
			}

			if len(acceptableWindows[sqi]) == 0 {
				return
			}

			// Pick best worker (shuffle in case some workers are equally as good)
			rand.Shuffle(len(acceptableWindows[sqi]), func(i, j int) {
				acceptableWindows[sqi][i], acceptableWindows[sqi][j] = acceptableWindows[sqi][j], acceptableWindows[sqi][i] // nolint:scopelint
			})
			sort.SliceStable(acceptableWindows[sqi], func(i, j int) bool {
				wii := sh.openWindows[acceptableWindows[sqi][i]].worker // nolint:scopelint
				wji := sh.openWindows[acceptableWindows[sqi][j]].worker // nolint:scopelint

				if wii == wji {
					// for the same worker prefer older windows
					return acceptableWindows[sqi][i] < acceptableWindows[sqi][j] // nolint:scopelint
				}

				wi := sh.workers[wii]
				wj := sh.workers[wji]

				rpcCtx, cancel := context.WithTimeout(task.ctx, SelectorTimeout)
				defer cancel()

				r, err := task.sel.Cmp(rpcCtx, task.taskType, wi, wj)
				if err != nil {
					log.Errorf("selecting best worker: %s", err)
				}
				return r
			})
		}(i)
	}

	wg.Wait()

	log.Debugf("SCHED windows: %+v", windows)
	log.Debugf("SCHED Acceptable win: %+v", acceptableWindows)

	// Step 2
	scheduled := 0
	rmQueue := make([]int, 0, queuneLen)

	for sqi := 0; sqi < queuneLen; sqi++ {
		task := (*sh.schedQueue)[sqi]
		needRes := ResourceTable[task.taskType][task.sector.ProofType]

		selectedWindow := -1
		for _, wnd := range acceptableWindows[task.indexHeap] {
			wid := sh.openWindows[wnd].worker
			wr := sh.workers[wid].info.Resources

			log.Debugf("SCHED try assign sqi:%d sector %d to window %d", sqi, task.sector.ID.Number, wnd)

			// TODO: allow bigger windows
			if !windows[wnd].allocated.canHandleRequest(needRes, wid, "schedAssign", wr) {
				continue
			}

			log.Debugf("SCHED ASSIGNED sqi:%d sector %d task %s to window %d", sqi, task.sector.ID.Number, task.taskType, wnd)

			windows[wnd].allocated.add(wr, needRes)
			// TODO: We probably want to re-sort acceptableWindows here based on new
			//  workerHandle.utilization + windows[wnd].allocated.utilization (workerHandle.utilization is used in all
			//  task selectors, but not in the same way, so need to figure out how to do that in a non-O(n^2 way), and
			//  without additional network roundtrips (O(n^2) could be avoided by turning acceptableWindows.[] into heaps))

			selectedWindow = wnd
			break
		}

		if selectedWindow < 0 {
			// all windows full
			continue
		}

		windows[selectedWindow].todo = append(windows[selectedWindow].todo, task)

		rmQueue = append(rmQueue, sqi)
		scheduled++
	}

	if len(rmQueue) > 0 {
		for i := len(rmQueue) - 1; i >= 0; i-- {
			sh.schedQueue.Remove(rmQueue[i])
		}
	}

	// Step 3

	if scheduled == 0 {
		return
	}

	scheduledWindows := map[int]struct{}{}
	for wnd, window := range windows {
		if len(window.todo) == 0 {
			// Nothing scheduled here, keep the window open
			continue
		}

		scheduledWindows[wnd] = struct{}{}

		window := window // copy
		select {
		case sh.openWindows[wnd].done <- &window:
		default:
			log.Error("expected sh.openWindows[wnd].done to be buffered")
		}
	}

	// Rewrite sh.openWindows array, removing scheduled windows
	newOpenWindows := make([]*schedWindowRequest, 0, windowsLen-len(scheduledWindows))
	for wnd, window := range sh.openWindows {
		if _, scheduled := scheduledWindows[wnd]; scheduled {
			// keep unscheduled windows open
			continue
		}

		newOpenWindows = append(newOpenWindows, window)
	}

	sh.openWindows = newOpenWindows
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()
	log.Debugf("closing scheduler")

	for i, w := range sh.workers {
		sh.workerCleanup(i, w)
	}
}

func (sh *scheduler) Info(ctx context.Context) (interface{}, error) {
	ch := make(chan interface{}, 1)

	sh.info <- func(res interface{}) {
		ch <- res
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (sh *scheduler) Close(ctx context.Context) error {
	close(sh.closing)
	select {
	case <-sh.closed:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
