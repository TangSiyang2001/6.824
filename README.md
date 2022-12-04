# 6.824
A personal implementation of [MIT 6.824](https://pdos.csail.mit.edu/6.824/schedule.html).

## environment
centos 7.9 + go1.17.6

## Lab1
### Key points
#### Implementation
-  Mind the shared data structure,they had better to be currency protected.
-  Use sync.cond to control the execute phase.
-  Use chan to produce and consume tasks.
-  Use goroutine to follow each task's state as soon as it is consumed.(By Sleeping 10s and then wake up to check.)

#### Testing(passed all tests)
- command "wait - n" seems does not exist on centos 7,use the following command to replace it in "early exists test".
```bash
 while [ ! -e $DF ]
  do
    sleep 0.2
  done
```

## Lab2A
### Key points
#### Debugging
-  It's proved that the clear log can truely make debugging process much easier.I built a debug util under the [guidance](https://blog.josejg.com/debugging-pretty/).

#### Implementation
-  Be sure that election timeout is random enough.(Don't use time.Now() as a random seed,it's proved to be an awful choice when several nodes reset the election timeout at the same time).

#### Testing(passed more than 3000+ times)
-  It's proved that testing with script automatically is a great choice.I built [Serial Script](https://github.com/TangSiyang2001/6.824/blob/master/src/raft/serial-test.sh) and [Parallel Script](https://github.com/TangSiyang2001/6.824/blob/master/src/raft/test-many.sh).

