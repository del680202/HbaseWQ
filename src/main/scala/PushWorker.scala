
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.HTableInterface
import org.apache.hadoop.hbase.client.HTablePool

class PushWorker(table:HTableInterface, chunk:java.util.List[Put], pool:HTablePool) extends Runnable{

   var done: Boolean = false
  
   override def run():Unit = {
     
     table.put(chunk)
     table.flushCommits()
     pool.putTable(table)
     done = true
   }
}