

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.hbase.client.HTable
import java.util.Properties
import java.io.FileInputStream
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.conf.Configuration
import scala.util.Random
import org.apache.hadoop.hbase.client.Durability
import java.math.BigInteger
import java.util.concurrent.Executors

case class Config(zooKeeperQuorum: String,
                  zooKeeperPort: Int,
                  zooKeeperZnodeParent: String,
                  testTableName: String,
                  autoCreateTable: Boolean,
                  numberOfTest: Int,
                  numberOfInsert: Int,
                  batchSize: Int,
                  numberOfWorker: Int,
                  cleanTableBeforeTest: Boolean,
                  cleanTableAfterTest: Boolean,
                  preCreateRegion: Boolean,
                  numberOfRegions: Int,
                  writeWAL: Boolean,
                  autoFlush: Boolean)

case class TestResult(spendingMilliseconds: Long,
                      numberOfInsert: Int)

class QPSMetric extends Configured {

  private var _config: Config = _
  def getTestConfig: Config = _config

  val HBASE_ZOOKEEPER_NAME = "hbase.zookeeper.quorum"
  val HBASE_ZOOKEEPER_PORT_NAME = "hbase.zookeeper.property.clientPort"
  val HBASE_ZOOKEEPER_ZNODE_PARENT_NAME = "zookeeper.znode.parent"
  val COLUMN_NAME = Bytes.toBytes("cf")
  val EMPTY_VALUE = Bytes.toBytes("")

  def loadConfig(confPath: String) {
    val properties = new Properties()
    properties.load(new FileInputStream(confPath))
    _config = new Config(
      zooKeeperQuorum = properties.getProperty(HBASE_ZOOKEEPER_NAME),
      zooKeeperPort = properties.getProperty(HBASE_ZOOKEEPER_PORT_NAME).toInt,
      zooKeeperZnodeParent = properties.getProperty(HBASE_ZOOKEEPER_ZNODE_PARENT_NAME),
      testTableName = properties.getProperty("testTableName", "QPSTest"),
      autoCreateTable = properties.getProperty("autoCreateTable", "false").toBoolean,
      numberOfTest = properties.getProperty("numberOfTest", "5").toInt,
      numberOfInsert = properties.getProperty("numberOfInsert", "10000").toInt,
      batchSize = properties.getProperty("batchSize", "1000").toInt,
      numberOfWorker = properties.getProperty("numberOfWorker", "1").toInt,
      cleanTableBeforeTest = properties.getProperty("cleanTableBeforeTest", "false").toBoolean,
      cleanTableAfterTest = properties.getProperty("cleanTableAfterTest", "false").toBoolean,
      preCreateRegion = properties.getProperty("preCreateRegion", "false").toBoolean,
      numberOfRegions = properties.getProperty("numberOfRegions", "8").toInt,
      writeWAL = properties.getProperty("writeWAL", "true").toBoolean,
      autoFlush = properties.getProperty("autoFlush", "true").toBoolean)
  }

  private lazy val getHbaseConfig: Configuration = {
    val config = HBaseConfiguration.create()
    config.set(HBASE_ZOOKEEPER_NAME, getTestConfig.zooKeeperQuorum)
    config.setInt(HBASE_ZOOKEEPER_PORT_NAME, getTestConfig.zooKeeperPort)
    config.set(HBASE_ZOOKEEPER_ZNODE_PARENT_NAME, getTestConfig.zooKeeperZnodeParent)
    setConf(config)
    config
  }

  private def buildHTables(): java.util.ArrayList[HTable] = {
    if (getTestConfig.autoCreateTable) {
      resetTable(getTestConfig.testTableName)
    }
    val tables = new java.util.ArrayList[HTable]()
    for (i <- 1 to getTestConfig.numberOfWorker) {
      val table = new HTable(getHbaseConfig, getTestConfig.testTableName)
      table.setAutoFlush(getTestConfig.autoFlush)
      tables.add(table)
    }
    tables
  }

  private def resetTable(tableName: String) {
    deleteTable(tableName)
    createTable(tableName)
  }

  private def deleteTable(tableName: String) {
    val admin = new HBaseAdmin(getHbaseConfig)
    if (getTestConfig.cleanTableBeforeTest && admin.isTableAvailable(getTestConfig.testTableName)) {
      admin.disableTable(getTestConfig.testTableName)
      admin.deleteTable(getTestConfig.testTableName)
    }
  }

  private def createTable(tableName: String) {
    val desc = new HTableDescriptor(Bytes.toBytes(tableName))
    val family = new HColumnDescriptor(COLUMN_NAME)
    desc.addFamily(family)
    if (!getTestConfig.writeWAL) {
      desc.setDurability(Durability.SKIP_WAL)
    }

    val admin = new HBaseAdmin(getHbaseConfig)
    if (!admin.isTableAvailable(getTestConfig.testTableName)) {
      if (getTestConfig.preCreateRegion) {
        //We make row key by hex format md5( 00~ff)
        val splits = getHexSplits("0", "f", getTestConfig.numberOfRegions)
        admin.createTable(desc, splits)
      } else {
        admin.createTable(desc)
      }
    }
  }

  def getHexSplits(startKey: String, endKey: String, numRegions: Int): Array[Array[Byte]] = {
    val splits = new Array[Array[Byte]](numRegions - 1);
    var lowestKey = new BigInteger(startKey, 16);
    val highestKey = new BigInteger(endKey, 16);
    val range = highestKey.subtract(lowestKey);
    val regionIncrement = range.divide(BigInteger.valueOf(numRegions));
    lowestKey = lowestKey.add(regionIncrement);
    for (i <- 0 until (numRegions - 1)) {
      val key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
      val b = String.format("%016x", key).getBytes();
      splits(i) = b;
    }
    splits;
  }

  private def buildTestInsertData: java.util.ArrayList[Put] = {
    val dataset = new java.util.ArrayList[Put]
    for (i <- 1 to getTestConfig.numberOfInsert) {
      val randomKey = Random.nextInt()
      val rowKey = s"someRow|$randomKey|$i"
      val md5 = java.security.MessageDigest.getInstance("MD5").digest(rowKey.map(_.toChar.toByte).toArray).map("%02x".format(_)).mkString
      val p = new Put(Bytes.toBytes(md5));
      p.addColumn(COLUMN_NAME, EMPTY_VALUE, Bytes.toBytes(s"test_$i"))
      p.setWriteToWAL(getTestConfig.writeWAL)
      dataset.add(p)
    }
    dataset
  }

  private def insertDataToHbase(tables:java.util.ArrayList[HTable],
                                executor:java.util.concurrent.Executor,
                                dataset: java.util.ArrayList[Put]): TestResult = {
    
    import collection.JavaConversions._
    var result: List[TestResult] = List.empty
    var jobs = new java.util.ArrayList[PushWorker]()
    var table_index = 0
    val totalTestResult = Util.measureSpendingTime(getTestConfig.numberOfInsert) {
      for (chunk <- dataset.grouped(getTestConfig.batchSize)) {
        val job = new PushWorker(tables(table_index), chunk)
        jobs.add(job)
        executor.execute(job)
        table_index = if (table_index >= getTestConfig.numberOfWorker - 1) 0 else table_index + 1
      }
      while(jobs.forall(!_.done)){
        //wait for all job done
        Thread.sleep(10)
      }
    }
    totalTestResult
  }

  def test() {

    val testResult: Array[TestResult] = new Array(getTestConfig.numberOfTest)
    val tables = buildHTables
    val executor = Executors.newFixedThreadPool(getTestConfig.numberOfWorker)
    println(s"start workers: num=${getTestConfig.numberOfWorker}")
    for (c <- 1 to getTestConfig.numberOfTest) {
      val dataset = buildTestInsertData
      testResult(c - 1) = insertDataToHbase(tables, executor, dataset)
    }
    if (getTestConfig.cleanTableAfterTest) {
      deleteTable(getTestConfig.testTableName)
    }
    report(testResult)
    executor.shutdown()
  }

  def report(testResult: Array[TestResult]) {
    var totalSpendingTime = 0L
    var totalNumberOfInsert = 0L
    for (eachResultIndex <- 1 to testResult.length) {
      val eachResult = testResult(eachResultIndex - 1)
      val spendingTime = eachResult.spendingMilliseconds
      val numberOfInsert = eachResult.numberOfInsert
      totalSpendingTime = totalSpendingTime + spendingTime
      totalNumberOfInsert = totalNumberOfInsert + numberOfInsert
      println(s"Job ${eachResultIndex}: " +
        s"numberOfInsert=${numberOfInsert}, " +
        s"spendingTime=${spendingTime} ms, " +
        s"QPS=${numberOfInsert / (spendingTime / 1000.0)}")
    }
    println(s"Total: " +
      s"numberOfInsert=${totalNumberOfInsert}, " +
      s"spendingTime=${totalSpendingTime} ms, " +
      s"QPS=${totalNumberOfInsert / (totalSpendingTime / 1000.0)}")
  }
}