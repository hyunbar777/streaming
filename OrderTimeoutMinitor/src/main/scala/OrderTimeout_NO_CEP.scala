import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-06-03 13:45:46
 */
object OrderTimeout_NO_CEP {
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
      .keyBy(_.orderId)
      .process(new OrderTimeOutAlert())
      .print()
    env.execute()
  }
}

class OrderTimeOutAlert extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  private var isPayedState: ValueState[Boolean] = _
  
  override def open(parameters: Configuration): Unit = {
    val isPayedState_desc = new
        ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean])
    isPayedState = getRuntimeContext.getState(isPayedState_desc)
  }
  
  override def processElement(value: OrderEvent,
                              context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context,
                              collector: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    
    if (value.eventType == "create" && !isPayed) {
      context.timerService.registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    } else if (value.eventType == "pay") {
      isPayedState.update(true)
    }
  }
  
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                       out: Collector[OrderResult]): Unit = {
    val isPayed = isPayedState.value()
    if (isPayed) {
      isPayedState.clear()
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order timeout"))
    }
  }
}