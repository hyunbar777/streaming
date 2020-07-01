import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction
import java.lang.{Integer => JInteger, Long => JLong}

import org.apache.flink.api.java.tuple.{Tuple, Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

/**
 * Author z
 * Date 2020-07-01 14:43:58
 */
class AvgTempAcc {
  var sum = 0L
  var count = 0
  
}

class AvgTemp extends AggregateFunction[Long, AvgTempAcc] {
  override def getValue(acc: AvgTempAcc): Long = acc.sum / acc.count
  
  override def createAccumulator(): AvgTempAcc = new AvgTempAcc
  
  def accumulate(acc: AvgTempAcc, temp: Long) = {
    acc.sum += temp
    acc.count += 1
  }
}

class AvgTempacc_test {
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
    
    //创建聚合函数实例
    val avgTemp = new AvgTemp
    
    student_table.groupBy('id)
      .aggregate(avgTemp('data) as 'avgTemp)
      .select('id, 'avgTemp)
      //转换成流打印
      .toRetractStream[(String, Long)].print("agg temp")
    
    //sql实现
    table_env.createTemporaryView("student", student_table)
    table_env.registerFunction("avgTemp", avgTemp)
    
    table_env.sqlQuery(
      """
        |select id,avgTemp(data) as avgT
        |from student
        |group by id
        |""".stripMargin
    )
      .toRetractStream[Row].print("agg")
    
  }
}