seWQ
A tool for testing QPS of write to measure the performance of HBASE cluster
We can adjust properties to test performance in some scenarios.

### Output

```bash
...
Job 1: avg. numberOfInsert=2000.0, avg. spendingTime=320.0 ms, avg. QPS=1250.0
Job 2: avg. numberOfInsert=2000.0, avg. spendingTime=219.8 ms, avg. QPS=1819.8362147406733
Job 3: avg. numberOfInsert=2000.0, avg. spendingTime=559.8 ms, avg. QPS=714.5409074669525
Job 4: avg. numberOfInsert=2000.0, avg. spendingTime=458.6 ms, avg. QPS=872.219799389446
Job 5: avg. numberOfInsert=2000.0, avg. spendingTime=255.2 ms, avg. QPS=1567.398119122257
Total: numberOfInsert=50000, spendingTime=9067 ms, QPS=5514.503143266792
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
$ git clone https://github.com/del680202/komono.git
$ cd komono
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
batchMode=true
batchSize=2000
cleanTableAfterTest=false
preCreateRegion=false
numberOfRegions=8
writeWAL=true
autoFlush=true
```

