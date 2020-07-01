import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-05-29 17:37:06
 */
object AdStatisticsByGeo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    val stream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("AdClickLog.csv").getPath)
      .map(data=>{
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.ts * 1000)
      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(15))
      .aggregate(new CountAgg(),new CountResult())
      .print()
    env.execute("ad ")
    
  }
}
case class AdClickLog(userId:Long,addId:Long,province:String,city:String,ts:Long)
case  class CountByProvince(WindowStart:String,windowEnd:String,province:String,count:Long)

class CountAgg extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L
  
  override def add(in: AdClickLog, acc: Long): Long = acc +1L
  
  override def getResult(acc: Long): Long = acc
  
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class CountResult extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Long],
                     out: Collector[CountByProvince]): Unit = {
    val start_ts = window.getStart
    val end_ts = window.getEnd
    out.collect(new CountByProvince(format_ts(start_ts),
      format_ts(end_ts),
      key,
      input.iterator.next))
  }
  private def format_ts(ts:Long) ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }
}