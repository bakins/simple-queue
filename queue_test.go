package queue_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bakins/simple-queue"
)

func withQ(t *testing.T, fn func(q *queue.Queue, t *testing.T)) {
	file := tempfile()
	q, err := queue.New(file, 4, 3)
	ok(t, err)
	defer os.Remove(file)
	defer q.Close()
	fn(q, t)
}

func TestNew(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
	})
}

func TestPut(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 600, []byte("testing"))
		ok(t, err)
	})
}

func TestReserve(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 600, []byte("testing"))
		ok(t, err)
		j, err := q.Reserve("test", 0)
		ok(t, err)
		assert(t, j != nil, "job is nil")
		equals(t, []byte("testing"), j.Data)
	})
}

func TestJobs(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 600, []byte("testing"))
		ok(t, err)
		jobs, err := q.Jobs("test")
		ok(t, err)
		equals(t, 1, len(jobs))
	})
}

func TestJobDelete(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 600, []byte("testing"))
		ok(t, err)
		j, err := q.Reserve("test", 0)
		ok(t, err)
		assert(t, j != nil, "job is nil")
		equals(t, []byte("testing"), j.Data)
		err = j.Delete()
		ok(t, err)
		j, err = q.Reserve("test", 0)
		ok(t, err)
		assert(t, j == nil, "job is not nil")
	})
}

func TestExpire(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 1, []byte("testing"))
		ok(t, err)
		j, err := q.Reserve("test", 0)
		ok(t, err)
		assert(t, j != nil, "job is nil")
		equals(t, []byte("testing"), j.Data)

		// job should be set back to ready
		sleep(10)

		j, err = q.Reserve("test", 0)
		ok(t, err)
		assert(t, j != nil, "job is nil")
		equals(t, []byte("testing"), j.Data)

	})
}

func TestEmpty(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 600, []byte("testing"))
		ok(t, err)
		j, err := q.Reserve("test", 0)
		ok(t, err)
		assert(t, j != nil, "job is nil")
		equals(t, []byte("testing"), j.Data)

		j, err = q.Reserve("test", 0)
		ok(t, err)
		assert(t, j == nil, "job is not nil")

	})
}

func TestJobTouch(t *testing.T) {
	withQ(t, func(q *queue.Queue, t *testing.T) {
		err := q.Put("test", 0, 2, []byte("testing"))
		ok(t, err)
		j, err := q.Reserve("test", 0)
		ok(t, err)
		assert(t, j != nil, "job is nil")
		equals(t, []byte("testing"), j.Data)

		err = j.Touch(0)
		ok(t, err)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				err = j.Touch(0)
				ok(t, err)
				sleep(1)
			}
		}()
		sleep(3)

		j2, err := q.Reserve("test", 0)
		ok(t, err)
		assert(t, j2 == nil, "job is not nil")
		wg.Wait()
	})
}

func sleep(delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
}

func tempfile() string {
	f, _ := ioutil.TempFile("", "queue-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

// Thanks to https://github.com/benbjohnson/testing

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
