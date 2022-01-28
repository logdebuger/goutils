package stimer

import (
	"sync"
)

type worker struct {
	sync.Mutex
	wg     sync.WaitGroup
	nodes  []*node
	status bool
	isstop bool
}

func (w *worker) init() {
	w.isstop = false
}

func (w *worker) stop() {
	w.Lock()
	w.isstop = true
	w.Unlock()
	w.wg.Wait()
}

func (w *worker) handleNode() {
	defer func() {
		w.Lock()
		w.status = false
		w.Unlock()
		w.wg.Done()
	}()
	w.Lock()
	nodes := w.nodes
	w.nodes = nil
	w.Unlock()
	for _, v := range nodes {
		v.Lock()
		if v.stop == true {
			v.Unlock()
			continue
		}
		v.Unlock()
		v.taskF.Call(v.args)
	}
}

func (w *worker) addNode(n *node) {
	w.Lock()
	defer w.Unlock()
	if w.isstop == true {
		return
	}
	w.nodes = append(w.nodes, n)
	if w.status == true {
		return
	}
	w.status = true
	w.wg.Add(1)
	go w.handleNode()
}
