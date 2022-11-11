# 6.824
A personal implementation of MIT 6.824.

## environment
centos 7.9 + go1.17.6

## Lab1
### Key points
#### Implementation
-  Mind the shared data structure,they had better to be currency protected.
-  Use sync.cond to control the execute phase.
-  Use chan to produce and consume tasks.
-  Use goroutine to follow each task's state as soon as it is consumed.(By Sleeping 10s and then wake up to check.)

#### Testing
- command "wait - n" seems does not exist on centos 7,use the following command to replace it in "early exists test".
```bash
 while [ ! -e $DF ]
  do
    sleep 0.2
  done
```
