# HbaseWQ
A tool for testing QPS of write to measure the performance of HBASE cluster
We can adjust properties to test performance in some scenarios.

### Output

```bash
...
Job 1: numberOfInsert=10000, spendingTime=3593 ms, QPS=2783.1895352073475
Job 2: numberOfInsert=10000, spendingTime=1684 ms, QPS=5938.242280285036
Job 3: numberOfInsert=10000, spendingTime=3783 ms, QPS=2643.4047052603755
Job 4: numberOfInsert=10000, spendingTime=1614 ms, QPS=6195.786864931846
Job 5: numberOfInsert=10000, spendingTime=2579 ms, QPS=3877.471888328809
Total: numberOfInsert=50000, spendingTime=13253 ms, QPS=3772.7307024824568
```



#Requirements

* Mac OS X or Linux
* Scala 2.10.5
* Java 1.8+
* sbt (for building)

sbt http://www.scala-sbt.org/
<br/>
Scala IDE http://scala-ide.org/

#Run

```
$ git clone https://github.com/del680202/HbaseWQ.git
$ cd HbaseWQ
$ cp conf/wq.conf.template conf/wq.conf
$ vim conf/wq.conf  #Add hbase cluster setting to config file
$ sbt test
```

#Config

```
#Hbase connection setting
hbase.zookeeper.quorum=
hbase.zookeeper.property.clientPort=
zookeeper.znode.parent=

#test suite setting
testTableName=QPSTest
autoCreateTable=true
numberOfTest=5
numberOfInsert=10000
batchSize=2000
numberOfWorker=1
cleanTableBeforeTest=false
cleanTableAfterTest=false
preCreateRegion=false
numberOfRegions=8
writeWAL=true
autoFlush=true
```

