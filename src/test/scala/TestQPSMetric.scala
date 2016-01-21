
import org.scalatest.FunSuite

class TestQPSMetric extends FunSuite {
  
  test("Test QPS of Hbase Write") {
    val qps = new QPSMetric
    qps.loadConfig("conf/wq.conf")
    qps.test()
  }
}