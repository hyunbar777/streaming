import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-05-29 17:15:17
 */
object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior!="UNINSTALL")
      .map(data=>{
        (data.behavior,1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .process(new MarketingCount())
      .print()
    
    env.execute()
  }
}

class MarketingCount
  extends ProcessWindowFunction[(String,Long),MarketingCountView,String,TimeWindow]{
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long)],
                       out: Collector[MarketingCountView]): Unit = {
    val start_ts = context.window.getStart
    val end_ts = context.window.getEnd
    out.collect(new MarketingCountView(format_ts(start_ts),format_ts(end_ts),"全部渠道",key,elements.size))
  }
  private def format_ts(ts:Long) ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }
}