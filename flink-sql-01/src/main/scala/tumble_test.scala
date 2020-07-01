import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

/**
 * Author z
 * Date 2020-06-30 13:43:21
 */
class tumble_test {
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
    
    val dataTable = table_env
      .fromDataStream(dataStream, 'id, 'ts.rowtime, 'data)
    val resultTable = dataTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count)
    
    val sqlDatatable = dataTable.select('id, 'data, 'ts)
    
    val resultSqlTable = table_env.sqlQuery(
      "select id,count(id) from " + sqlDatatable +
        " group  by id,tumble(ts,interval '10' second)"
    )
    
    val resultDStream = resultSqlTable
      .toRetractStream[(String,Long)]
    
    resultDStream.filter(_._1).print
    
    env.execute()
  }
}

case class Sensor(id: String, ts: Long, data: Double)