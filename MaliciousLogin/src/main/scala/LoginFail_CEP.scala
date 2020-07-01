import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Author z
 * Date 2020-06-02 11:19:00
 */
object LoginFail_CEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    val stream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("LoginLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
          override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
        })
    // 定义匹配模式
    val pattern = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))
    // 在数据流中匹配出定义好的模式
    val patternStream = CEP
      .pattern(stream.keyBy(_.userId), pattern)
    
    patternStream
      .select(p => {
        val first = p.getOrElse("begin", null).iterator.next
        val second = p.getOrElse("next", null).iterator.next
        (first.userId, second.userId, second.eventType)
      })
      .print()
    
    env.execute()
  }
}
