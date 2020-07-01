import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-07-01 15:06:16
 */
class Top2TempAcc {
  var highestTemp = Double.MinValue
  var secondHighestTemp = Double.MinValue
}

class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc] {
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc
  
  def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
    if (temp > acc.highestTemp) {
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp > acc.secondHighestTemp) {
      acc.secondHighestTemp = temp
    }
  }
  def emitValue(acc:Top2TempAcc,out:Collector[(Double,Int)]): Unit ={
    out.collect(acc.highestTemp,1)
    out.collect(acc.secondHighestTemp,2)
  }
}
class TableAgg_test{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  
    val stream = env.readTextFile("sensor.txt")
  
    val dataStream = stream
      .map(
        data => {
          val arrs = data.split(",")
          Sensor(arrs(0), arrs(1).toLong, arrs(2).toDouble)
        })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(10)) {
          override def extractTimestamp(element: Sensor): Long = element.ts * 1000L
        }
      )
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val table_env = StreamTableEnvironment.create(env, settings)
  
    //将dataStream转换为table，并指定时间字段
  
    val student_table = table_env
      .fromDataStream(dataStream, 'id, 'data, 'ts.rowtime)
    
    val top2Temp = new Top2Temp
    student_table.groupBy('id)
      .flatAggregate(top2Temp('data) as ('temp,'rank))
      .select('id,'temp,'rank)
      .toRetractStream[(String,Double,Int)].print("agg")
  }
}