package stimer

import (
	"reflect"
	"sync"
	"time"
)

const (
	CRON_NUM_MAX   = 100
	CRON_ITEM_NUM  = 1000
	CRON_PRECISION = 1
	CRON_INTERVAL  = 100

	MAX_TIMER_SEP = 1 * 1000 * 1000 * 1000 * 1000

	MAX_TIMER_ROUTINE  = 63
	MAX_SLEEP_INTERVAL = 50
)

type node struct {
	sync.Mutex
	seq        uint64
	taskF      reflect.Value
	args       []reflect.Value
	sep        int64
	order      bool
	once       bool
	cronIndexs []int64
	realIndexs []int64
	rmcron     int64

	stop    bool
	preT    time.Time
	sepPlus int64
}

type cron struct {
	sync.Mutex
	nodes    [CRON_ITEM_NUM + 1]map[uint64]*node
	nowIndex int64
}

type Manager struct {
	sync.Mutex
	wg      sync.WaitGroup
	seq     uint64
	crons   [CRON_NUM_MAX]cron
	preT    time.Time
	now     time.Time
	ticker  *time.Ticker
	nodes   map[uint64]*node
	ccron   chan *node
	cremove chan *node

	cstopCron   chan int
	cstopWorker chan int
	stopped     bool

	mworkers [MAX_TIMER_ROUTINE]worker
}

func (m *Manager) Init() {
	m.preT = time.Now()
	m.ticker = time.NewTicker(CRON_INTERVAL * time.Millisecond)
	m.nodes = make(map[uint64]*node)
	m.ccron = make(chan *node, MAX_TIMER_ROUTINE)
	m.cremove = make(chan *node, MAX_TIMER_ROUTINE)
	m.cstopCron = make(chan int)
	m.cstopWorker = make(chan int)
	m.seq = 0
	m.stopped = false
	for i := 0; i < len(m.mworkers); i++ {
		m.mworkers[i].init()
	}
	m.wg.Add(1)
	go m.loop()
	m.wg.Add(1)
	go m.work()
}

func (m *Manager) AddTimer(sepMs int64, f interface{}, args ...interface{}) uint64 {
	if sepMs > MAX_TIMER_SEP {
		return 0
	}

	var n node
	m.Lock()
	defer m.Unlock()
	if m.stopped == true {
		return 0
	}
	m.seq++
	if m.seq == 0 {
		m.seq++
	}
	n.seq = m.seq
	n.sep = sepMs
	n.taskF = reflect.ValueOf(f)
	n.args = make([]reflect.Value, 0, len(args))
	for _, v := range args {
		n.args = append(n.args, reflect.ValueOf(v))
	}
	n.once = false
	if calcuIndexs(&n) < 0 {
		return 0
	}
	m.now = time.Now()
	n.preT = m.now
	m.addNode(&n)
	m.nodes[n.seq] = &n
	return m.seq
}

func (m *Manager) Remove(seq uint64) {
	m.Lock()
	defer m.Unlock()
	if m.stopped == true {
		return
	}
	n, ok := m.nodes[seq]
	if !ok {
		return
	}
	n.stop = true
	delete(m.nodes, seq)
	m.cremove <- n
}

func (m *Manager) Stop() {
	m.Lock()
	defer m.Unlock()
	m.stopped = true
	m.cstopCron <- 1
	m.cstopWorker <- 1
	m.wg.Wait()
	for i, _ := range m.mworkers {
		m.mworkers[i].stop()
	}
}

func (m *Manager) loop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.ticker.C:
			m.tick()
		case n := <-m.cremove:
			m.removeCronTask(n)
		case <-m.cstopCron:
			return
		}
	}
}

func (m *Manager) work() {
	defer m.wg.Done()
	for {
		select {
		case n := <-m.ccron:
			m.mworkers[n.seq%uint64(len(m.mworkers))].addNode(n)
		case <-m.cstopWorker:
			return
		}
	}
}

func (m *Manager) removeCronTask(n *node) {
	n.stop = true
	for i := 0; i < len(n.realIndexs); i++ {
		m.crons[i].Lock()
		if m.crons[i].nodes[n.realIndexs[i]] != nil {
			delete(m.crons[i].nodes[n.realIndexs[i]], n.seq)
		}
		m.crons[i].Unlock()
	}
}

func calcuIndexs(n *node) int {
	n.cronIndexs = nil
	num := (n.sep + n.sepPlus) / CRON_PRECISION
	//fmt.Printf("sep %+v\n", n.sep+n.sepPlus)
	for {
		i := num % CRON_ITEM_NUM
		n.cronIndexs = append(n.cronIndexs, i)
		num = num / CRON_ITEM_NUM
		if num == 0 {
			break
		}
	}
	if len(n.cronIndexs) > CRON_ITEM_NUM-1 {
		return -1
	}
	n.realIndexs = make([]int64, len(n.cronIndexs)+1)
	return 0
}

func (m *Manager) addNode(n *node) {
	var plus int64 = 0
	n.rmcron = 0
	n.realIndexs = n.realIndexs[:0]
	n.preT = m.now
	for i := 0; i < len(n.cronIndexs); i++ {
		m.crons[i].Lock()
		var realIndex int64 = 0
		realIndex = m.crons[i].nowIndex + n.cronIndexs[i] + plus
		//fmt.Printf("m.crons[i].nowIndex i, %+v, nowIndex %+v\n", realIndex, m.crons[0].nowIndex)
		if realIndex >= CRON_ITEM_NUM {
			plus = realIndex / CRON_ITEM_NUM
			realIndex = realIndex % CRON_ITEM_NUM
		} else {
			plus = 0
		}
		if m.crons[i].nodes[realIndex] == nil {
			m.crons[i].nodes[realIndex] = make(map[uint64]*node)
		}
		m.crons[i].nodes[realIndex][n.seq] = n
		n.realIndexs = append(n.realIndexs, realIndex)
		m.crons[i].Unlock()
	}

	// 进位处理
	for i := len(n.realIndexs); i < len(m.crons); i++ {
		if plus == 0 {
			break
		}
		m.crons[i].Lock()
		var realIndex int64 = 0
		realIndex = m.crons[i].nowIndex + plus
		if realIndex >= CRON_ITEM_NUM {
			plus = realIndex / CRON_ITEM_NUM
			realIndex = realIndex % CRON_ITEM_NUM
		} else {
			plus = 0
		}
		if m.crons[i].nodes[realIndex] == nil {
			m.crons[i].nodes[realIndex] = make(map[uint64]*node)
		}
		m.crons[i].nodes[realIndex][n.seq] = n
		n.realIndexs = append(n.realIndexs, realIndex)
		m.crons[i].Unlock()
	}
	//fmt.Printf("nodes %+v now %+v\n", n.realIndexs, time.Now().Unix())
}

func (m *Manager) tryAddNode(n *node) {
	if n.once == true || n.stop == true {
		return
	}
	// 精度校正
	sepPlus := n.preT.UnixNano()/1000/1000 + n.sep - m.now.UnixNano()/1000/1000
	n.sepPlus += sepPlus
	if n.sepPlus+n.sep < 0 {
		n.sepPlus = 0
	}
	calcuIndexs(n)
	m.addNode(n)
}

func (m *Manager) tickNodes(c *cron, index int64) {
	c.Lock()
	var um = c.nodes[index]
	c.nodes[index] = nil
	c.Unlock()
	for _, v := range um {
		v.rmcron++
		//fmt.Printf("tickNodes %v %v\n", int(v.rmcron), len(v.realIndexs))
		if int(v.rmcron) == len(v.realIndexs) {
			//v.taskF.Call(v.args)
			//
			m.ccron <- v
			m.tryAddNode(v)
		}
	}
}

func (m *Manager) tickCron(c *cron, tick int64) int64 {
	nowIndex := c.nowIndex
	c.nowIndex = c.nowIndex + tick
	var ret int64 = 0
	if c.nowIndex >= CRON_ITEM_NUM {
		ret = c.nowIndex / CRON_ITEM_NUM
		c.nowIndex = c.nowIndex % CRON_ITEM_NUM
	}
	for i := int64(1); i <= tick; i++ {
		index := (nowIndex + i) % CRON_ITEM_NUM
		// TODO handle real timer
		m.tickNodes(c, index)
	}
	return ret
}

func (m *Manager) tick() {
	now := time.Now()
	if now.UnixNano() < m.preT.UnixNano() {
		return
	}
	m.now = now
	sum := now.UnixNano()/1000/1000 - m.preT.UnixNano()/1000/1000
	tick := sum / CRON_PRECISION
	if tick <= 0 {
		return
	}
	for i := 0; i < CRON_NUM_MAX; i++ {
		//fmt.Printf(" i %+v tick time %+v\n", i, m.crons[i].nowIndex)
		tick = m.tickCron(&(m.crons[i]), tick)
		if tick <= 0 {
			break
		}
	}
	m.preT = now
}
