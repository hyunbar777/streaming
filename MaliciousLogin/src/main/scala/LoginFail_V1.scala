import java.util

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
object LoginFail_V1 {
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
      .process(new MatchFunction1())
      .print()
    
    env.execute()
  }
}


case class Warning(userId: Long, eventTime: Long, valueTime: Long, msg: String)

class MatchFunction1 extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  private var loginState: ListState[LoginEvent] = _
  
  override def open(parameters: Configuration): Unit = {
    val loginStateDesc = new ListStateDescriptor[LoginEvent]("loginState", classOf[LoginEvent])
    loginState = getRuntimeContext.getListState(loginStateDesc)
  }
  
  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    //按照type 做筛选，如果success 直接清空 ，如果fail在做处理
    if (value.eventType == "fail") {
      //如果已经有登录失败的数据，那么就判断是否在两秒内
      val iter = loginState.get.iterator
      if (iter.hasNext) {
        val firstFail = iter.next
        //如果两次登录失败时间间隔小于2秒，输出报警
        if (value.eventTime < firstFail.eventTime + 2) {
          collector.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds."))
        }
        import scala.collection.JavaConversions._
        // 把最近一次的登录失败数据，更新写入state中
        val failList = new util.ArrayList[LoginEvent]()
        failList += value
        loginState.update(failList)
      } else {
        //如果state中没有登录失败的数据，那就添加
        loginState.add(value)
      }
    } else {
      loginState.clear
    }
  }
  
}