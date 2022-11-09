package mr

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
}

func (gen *IncreasingIdGen) GenerateId() Id {
	gen.seed++
	return gen.seed
}

type ChanListener struct {
	chans []*chan bool
}

func (c *ChanListener) Subscribe() {
	ch := make(chan bool, 1)
	c.chans = append(c.chans, &ch)
	<-ch
}

func (c *ChanListener) Publish() {
	for _, ch := range c.chans {
		*ch <- true
	}
}

func MakeChanListener() ChanListener {
	return ChanListener{
		chans: make([]*chan bool, 64),
	}
}
