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
