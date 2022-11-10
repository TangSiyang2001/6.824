package mr

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
)

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

type Id int
type IdGenerator interface {
	GenerateId() Id
}

type IncreasingIdGen struct {
	seed Id
	mu   sync.Mutex
}

func (gen *IncreasingIdGen) GenerateId() Id {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	gen.seed++
	return gen.seed
}

type ChanListener struct {
	cond sync.Cond
}

func (c *ChanListener) Subscribe() {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	c.cond.Wait()
}

func (c *ChanListener) Publish() {
	c.cond.Broadcast()
}

func (c *ChanListener) WakeupOne() {
	c.cond.Signal()
}

func MakeChanListener() ChanListener {
	locker := new(sync.Mutex)
	return ChanListener{
		cond: *sync.NewCond(locker),
	}
}

func ReadFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		return nil
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil
	}
	file.Close()
	return content
}

func CreateTmpFile(dir string, filename string) *os.File {
	file, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
		return nil
	}
	return file
}
