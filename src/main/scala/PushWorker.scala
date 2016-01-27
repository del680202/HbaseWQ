
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.HTable

class PushWorker(table:HTable, chunk:java.util.List[Put]) extends Runnable{

   var done: Boolean = false
  
   override def run():Unit = {
     table.put(chunk)
     table.flushCommits()
     done = true
   }
}