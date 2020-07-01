import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-05-29 17:37:06
 */
object AdStatisticsByGeoBlacklist {
  val blackListOutputTag = new OutputTag[BlackListWarning]("blacklist")
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //转换类型
    val stream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("AdClickLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.ts * 1000L)
    // 过滤黑名单 用户
    val filterBlackListStream = stream
      .keyBy(log => (log.userId, log.addId))
      .process(new FilterBlackListUser(100))
    // 计算结果
    val countStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(15))
      .aggregate(new CountAgg(), new CountResult())
      .print
    
    filterBlackListStream.getSideOutput(blackListOutputTag).print("black list")
    
    env.execute("ad ")
    
  }
}

case class AdClickLog(userId: Long, addId: Long, province: String, city: String, ts: Long)

case class CountByProvince(WindowStart: String, windowEnd: String, province: String, count: Long)

case class BlackListWarning(userId: Long, adId: Long, msg: String)

class CountAgg extends AggregateFunction[AdClickLog, Long, Long] {
  override def createAccumulator(): Long = 0L
  
  override def add(in: AdClickLog, acc: Long): Long = acc + 1L
  
  override def getResult(acc: Long): Long = acc
  
  override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class CountResult extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
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
  
  private def format_ts(ts: Long) = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(new Date(ts))
  }
}



class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {
  // 保存当前用户对当前广告的点击量
  private var countState: ValueState[Long] = _
  // 标记当前（用户，广告）作为key是否第一次发送到黑名单
  private var firstSend: ValueState[Boolean] = _
  // 保存定时器触发的时间戳，到时清空重置状态
  private var resetTime: ValueState[Long] = _
  
  override def open(parameters: Configuration): Unit = {
    val countStateDesc = new ValueStateDescriptor[Long]("countState", classOf[Long])
    countState = getRuntimeContext.getState(countStateDesc)
    val firstSendDesc = new ValueStateDescriptor[Boolean]("firstSend", classOf[Boolean])
    firstSend = getRuntimeContext.getState(firstSendDesc)
    val resetTimeDesc = new ValueStateDescriptor[Long]("resetTime", classOf[Long])
    resetTime = getRuntimeContext.getState(resetTimeDesc)
  }
  
  override def processElement(i: AdClickLog,
                              context: KeyedProcessFunction[(Long, Long),
                                AdClickLog,
                                AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()
    // 如果是第一次处理，注册一个定时器，每天 00：00触发清除
    if (curCount == 0) {
      //第二天凌晨
      val ts = (context.timerService.currentProcessingTime() / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000)
      //更新定时器状态
      resetTime.update(ts)
      //注册定时器
      context.timerService().registerProcessingTimeTimer(ts)
    }
    // 如果计数已经超过上限，则加入黑名单，用侧输出流输出报警信息
    if (curCount > maxCount) {
      if (!firstSend.value) {
        firstSend.update(true)
        context.output(AdStatisticsByGeoBlacklist.blackListOutputTag,
          BlackListWarning(i.userId, i.addId, "Click over " + maxCount + " times today."))
      }
      return
    }
    countState.update(curCount + 1L)
    collector.collect(i)
  }
  
  /**
   * 是一个回调函数
   *
   * @param timestamp 为定时器所设定的触发的时间戳
   * @param ctx       context 可以访问元素的时间戳，元素的key，以及 TimerService 时间服务
   * @param out       输出结果集
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTime.value) {
      firstSend.clear()
      countState.clear()
    }
  }
}