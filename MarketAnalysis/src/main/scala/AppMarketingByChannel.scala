import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-05-26 14:22:48
 */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    val stream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(1))
      .process(new MarketingCountByChannel())
        .print()
    env.execute()
  }
}

case class MarketingCountView(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

class MarketingCountByChannel
  extends ProcessWindowFunction[((String, String), Long), MarketingCountView, (String, String), TimeWindow] {
  override def process(key: (String, String),
                       context: Context,
                       elements: Iterable[((String, String), Long)],
                       out: Collector[MarketingCountView]): Unit = {
    val start_ts = context.window.getStart
    val end_ts = context.window.getEnd
    out.collect(MarketingCountView(format_ts(start_ts),format_ts(end_ts),key._1,key._2,elements.size))
  }
  
  private def format_ts(ts:Long) ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }
}