import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Author z
 * Date 2020-06-02 11:19:00
 */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    env.readTextFile(ClassLoader.getSystemClassLoader.getResource("LoginLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
          override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
        })
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()
    
    env.execute()
  }
}

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

class MatchFunction extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
  
  private var loginState: ListState[LoginEvent] = _
  
  override def open(parameters: Configuration): Unit = {
    val loginStateDesc = new ListStateDescriptor[LoginEvent]("loginState", classOf[LoginEvent])
    loginState = getRuntimeContext.getListState(loginStateDesc)
  }
  
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, collector: Collector[LoginEvent]): Unit = {
    if (i.eventType == "fail") {
      loginState.add(i)
    }
    context.timerService.registerEventTimeTimer(i.eventTime *1000 +2*1000)
  }
  
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    val allLogins:ListBuffer[LoginEvent]=ListBuffer()
    import scala.collection.JavaConversions._
    for(login <- loginState.get){
      allLogins += login
    }
    loginState.clear()
    if (allLogins.length >1) {
      out.collect(allLogins.head)
    }
  }
}