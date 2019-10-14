package raft

import "log"
import "time"
import "sync"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type TimedClosure struct {
	mtx      sync.Mutex
	duration time.Duration
	timer    *time.Timer
	args     []interface{}
	fn       func(...interface{})
}

func (t *TimedClosure) Stop() {
    i := 0
	t.mtx.Lock()
	if t.timer != nil {
        i++
        if !t.timer.Stop() {
            i++
            select {
            case <-t.timer.C:
            default:
            }
        }
	}
	t.timer = nil
	t.mtx.Unlock()
    log.Printf("TimedClosure.Stop(%v) path %d", t, i)
}

func (t *TimedClosure) getTimer() *time.Timer {
	fn := t.fn
	args := t.args
	return time.AfterFunc(t.duration, func() {
		//log.Printf("TimedClosure.getTimer() in AfterFunc")
		t.mtx.Lock()
		t.timer = nil
		t.mtx.Unlock()
		fn(args...)
	})
}
func (t *TimedClosure) reset(d time.Duration) (ret bool) {
	var i int
	now := time.Now()
	t.duration = d
	if t.timer != nil {
		i++
		if !t.timer.Stop() {
			i++
			select {
			case <-t.timer.C:
			default:
			}
		}
		t.timer = t.getTimer()
		ret = true
	} else {
        ret = false
    }
    _, _ = now, i
	//log.Printf("TimedClosure.reset() took %s path %d", time.Since(now), i)
	return
}

func (t *TimedClosure) Reset(d time.Duration) {
	t.mtx.Lock()
	t.reset(d)
	t.mtx.Unlock()
}

func (t *TimedClosure) Delay(f func(...interface{}), args ...interface{}) {
	t.mtx.Lock()
	t.loadArgs(f, args...)
	t.timer = t.getTimer()
	t.mtx.Unlock()
}

func (t *TimedClosure) DelayFor(d time.Duration, f func(...interface{}), args ...interface{}) {
	t.mtx.Lock()
	t.loadArgs(f, args...)
	if !t.reset(d) {
		t.timer = t.getTimer()
	}
	t.mtx.Unlock()
}

func (t *TimedClosure) loadArgs(f func(...interface{}), args ...interface{}) {
	t.fn = f
	var a []interface{}
	t.args = append(a, args...)
}

//func DummyTest(c ...interface{}) {
//	for _, ch := range c {
//		ch := ch.(chan string)
//		ch <- "hi"
//	}
//}
//func main() {
//	ch := make(chan string)
//	t := &TimedClosure{}
//	t.DelayFor(1000*time.Millisecond, DummyTest, ch)
//	time.Sleep(990 * time.Millisecond)
//	t.Reset(2000 * time.Millisecond)
//	fmt.Println(<-ch)
//	return
//}
