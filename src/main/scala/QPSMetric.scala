

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

case class Config(zooKeeperQuorum: String,
                  zooKeeperPort: Int,
                  zooKeeperZnodeParent: String,
                  testTableName: String,
                  autoCreateTable: Boolean,
                  numberOfTest: Int,
                  numberOfInsert: Int,
                  batchMode: Boolean,
                  batchSize: Int,
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
      batchMode = properties.getProperty("batchMode", "true").toBoolean,
      batchSize = properties.getProperty("batchSize", "1000").toInt,
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

  private def buildHTable(): HTable = {
    if (getTestConfig.autoCreateTable) {
      resetTable(getTestConfig.testTableName)
    }
    new HTable(getHbaseConfig, getTestConfig.testTableName)
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

  private def insertDataToHbase(table: HTable, dataset: java.util.ArrayList[Put]): List[TestResult] = {
    import collection.JavaConversions._
    table.setAutoFlush(getTestConfig.autoFlush)
    var result: List[TestResult] = List.empty
    if (getTestConfig.batchMode) {
      result = dataset.grouped(getTestConfig.batchSize).map(chunk => measureSpendingTime(chunk.size) {
        table.put(chunk)
      }).toList
    } else {
      result = dataset.map(put => measureSpendingTime(1) {
        table.put(put)
      }).toList
    }
    table.flushCommits()
    result
  }

  def measureSpendingTime(numberOfInsert: Int)(executingBody: => Unit): TestResult = {
    val before = System.currentTimeMillis()
    executingBody
    val spendingTime = System.currentTimeMillis() - before
    if (numberOfInsert > 1) {
      println(s"Handle block data, batchSize=${numberOfInsert}, spendTime=${spendingTime} ms, QPS=${numberOfInsert / (spendingTime / 1000.0)}")
    }
    new TestResult(spendingTime, numberOfInsert)
  }

  def test() {
    var table = buildHTable
    val testResult: Array[List[TestResult]] = new Array(getTestConfig.numberOfTest)
    for (c <- 1 to getTestConfig.numberOfTest) {
      val dataset = buildTestInsertData
      testResult(c - 1) = insertDataToHbase(table, dataset)
    }
    if (getTestConfig.cleanTableAfterTest) {
      deleteTable(getTestConfig.testTableName)
    }
    report(testResult)
  }

  def report(testResult: Array[List[TestResult]]) {
    var totalSpendingTime = 0L
    var totalNumberOfInsert = 0L
    for (eachResultIndex <- 1 to testResult.length) {
      val eachResult = testResult(eachResultIndex - 1)
      val spendingTime = eachResult.map(_.spendingMilliseconds).sum
      val numberOfInsert = eachResult.map(_.numberOfInsert).sum
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