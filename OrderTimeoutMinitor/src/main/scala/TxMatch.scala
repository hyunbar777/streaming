import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-06-03 13:45:46
 */
object TxMatch {
  val unMatchedPays = new OutputTag[OrderEvent]("unMatchedPays")
  val unMatchedReceipts = new OutputTag[OrderEvent]("unMatchedReceipts")
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val orderStream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("OrderLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent_txId(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)
    
    val receiptStream = env
      .readTextFile(ClassLoader.getSystemClassLoader.getResource("ReceiptLog.csv").getPath)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.txId)
    
    val processedStream = orderStream
      .connect(receiptStream)
      .process(new TxMatchDetection)
    processedStream.getSideOutput(unMatchedPays).print
    processedStream.getSideOutput(unMatchedReceipts).print
    processedStream.print
    
    env.execute()
  }
}

case class OrderEvent_txId(orderId: Long, eventType: String, txId: String, eventTime: Long)

case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)

class TxMatchDetection extends CoProcessFunction[OrderEvent_txId, ReceiptEvent, (OrderEvent_txId, ReceiptEvent)] {
  private var payState: ValueState[OrderEvent_txId] = _
  private var receiptState: ValueState[ReceiptEvent] = _
  
  override def open(parameters: Configuration): Unit = {
    val payDesc = new
        ValueStateDescriptor[OrderEvent_txId]("payState", classOf[OrderEvent_txId])
    payState = getRuntimeContext.getState(payDesc)
    val receiptDesc = new
        ValueStateDescriptor[ReceiptEvent]("receiptState", classOf[ReceiptEvent])
    receiptState = getRuntimeContext.getState(receiptDesc)
  }
  
  override def processElement1(pay: OrderEvent_txId,
                               context: CoProcessFunction[OrderEvent_txId, ReceiptEvent, (OrderEvent_txId, ReceiptEvent)]#Context,
                               collector: Collector[(OrderEvent_txId, ReceiptEvent)]): Unit = {
    val receipt = receiptState.value()
    if (receipt != null) {
      receiptState.clear()
      collector.collect(pay, receipt)
    } else {
      payState.update(pay)
      context.timerService.registerEventTimeTimer(pay.eventTime * 1000L)
    }
  }
  
  override def processElement2(receipt: ReceiptEvent,
                               context: CoProcessFunction[OrderEvent_txId, ReceiptEvent, (OrderEvent_txId, ReceiptEvent)]#Context,
                               collector: Collector[(OrderEvent_txId, ReceiptEvent)]): Unit = {
    val pay = payState.value()
    if (pay != null) {
      payState.clear()
      collector.collect(pay, receipt)
    } else {
      receiptState.update(receipt)
      context.timerService.registerEventTimeTimer(receipt.eventTime * 1000L)
    }
  }
  
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent_txId, ReceiptEvent, (OrderEvent_txId, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent_txId, ReceiptEvent)]): Unit = {
    if (payState.value != null) {
      ctx.output(TxMatch.unMatchedPays, payState.value)
    }
    if (receiptState.value != null) {
      ctx.output(TxMatch.unMatchedReceipts, receiptState.value
    }
    payState.clear
    receiptState.clear
  }
}