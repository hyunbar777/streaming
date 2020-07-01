import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * Author z
 * Date 2020-06-30 15:12:15
 * 自定义标量函数
 */
class func_test {
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

    val sensorTable = table_env
      .fromDataStream(dataStream,'id,'data,'ts.rowtime)
   //Table Api 中使用
    val hashCode = new HashCode(10)
    val resultTable = sensorTable
      .select('id,hashCode('id))
    //Sql中使用
    table_env.createTemporaryView("sensor",sensorTable)
    table_env.registerFunction("hashCode",hashCode)
    val resultSqlTable = table_env
      .sqlQuery("select id,hashCode(id) from sensor")
    
    resultTable.toAppendStream[Row].print("table")
    resultSqlTable.toAppendStream[Row].print("sql")
    
    env.execute()
  }
}
class HashCode(factor:Int) extends ScalarFunction{
  def eval(s:String)={
    s.hashCode * factor
  }
}
