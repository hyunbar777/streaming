import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Author z
 * Date 2020-06-03 13:45:46
 */
object OrderTimeout_CEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val stream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("OrderLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000)
    
    val orderPattern = Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))
    
    val orderTimeoutOutput = OutputTag[OrderResult]("OrderTimeout")
    
    // 订单时间流根据orderId 分流，然后在每一条流中匹配出定义好的模式
    val patternStream = CEP.pattern(stream.keyBy(_.orderId), orderPattern)
    val completedResult = patternStream
      .select(orderTimeoutOutput) {
        // 对于已超时的部分模式匹配的时间序列，会调用这个函数
        case (pattern: collection.Map[String, Iterable[OrderEvent]], timestamp: Long) => {
          val createOrder = pattern.get("begin")
          OrderResult(createOrder.get.iterator.next.orderId, "timeout")
        }
      } {
        //检测到定义好的模式序列时，就会调用这个函数
        case pattern: collection.Map[String, Iterable[OrderEvent]] => {
          val payOrder = pattern.get("follow")
          OrderResult(payOrder.get.iterator.next.orderId, "success")
        }
      }
    val timeoutResult = completedResult.getSideOutput(orderTimeoutOutput)
    
    completedResult.print
    timeoutResult.print
    
    env.execute()
  }
}

case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, eventType: String)