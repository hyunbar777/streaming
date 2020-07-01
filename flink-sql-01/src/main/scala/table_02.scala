import org.apache.calcite.rel.core.Window.Group
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}

/**
 * Author z
 * Date 2020-06-22 16:31:55
 */
object table_02 {
  def main(args: Array[String]): Unit = {
    /*1.0  老版本的流式查询（flink-Streaming-Query）*/
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val fs_table_env = StreamTableEnvironment.create(env)
    
    
    /*2.0  基于老版本的批处理环境（Flink-Batch-Query）*/
    val fbenv = ExecutionEnvironment.getExecutionEnvironment
    val fb_table_env = BatchTableEnvironment.create(fbenv)
    
    
    /*3.0  基于blink版本的流式处理环境（Blink-Streaming-Query）*/
    val bs_setting = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bs_stream_env = StreamTableEnvironment.create(env, bs_setting)
    
    /*4.0  基于Blink版本的批处理环境（Blink-Batch-Query）*/
    val bb_setting = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bb_table_env = TableEnvironment.create(bb_setting)
    
    /*5 datastream转化为table时指定处理时间（process time）*/
    val dataStream = env
      .readTextFile("student.txt")
      .map(data => {
        val dataArray = data.split(",")
        Student(dataArray(0), dataArray(1), dataArray(2).toInt)
      })
    bs_stream_env.fromDataStream(dataStream, 'id, 'name, 'age, 'pt.proctime)
    
    /*6 定义table Schema时指定处理时间（process time）*/
    bs_stream_env.connect(
      new FileSystem().path("student.txt")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("name", DataTypes.STRING())
        .field("id", DataTypes.INT())
        .proctime()
      )
      .createTemporaryTable("inputtable")
    
    
    /*7 创建表的DDL中指定处理时间（process time）*/
    val sinkDDL =
      """
        |create table dataTable(
        | id varchar(20) not null,
        | name varchar(20),
        | age int,
        | pt as PROCTIME()
        |)with(
        | 'connector.type' = 'filesystem',
        | 'connector.path' = 'file:///D:\\..\\student.txt',
        | 'format.type' = 'csv'
        |)
        |""".stripMargin
    bs_stream_env.sqlUpdate(sinkDDL)
    
    
    /*8 datastream转化为table时指定事件时间（Even Time）*/
    val ds_order = env
      .readTextFile("order.txt")
      .map(data => {
        val dataArray = data.split(",")
        Order(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.ts)
    //将DataStream转换为table，并指定时间字段
    val orderTable = bs_stream_env
      .fromDataStream(ds_order, 'id, 'ts.rowtime, 'name)
    //或者，直接追加字段
    bs_stream_env.fromDataStream(ds_order, 'id, 'name, 'ts, 'tp.rowtime)
    
    /*9 定义table Schema时指定处理时间（Even time）*/
    bs_stream_env.connect(
      new FileSystem().path("order.txt")
    )
      .withFormat(new Csv())
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("name", DataTypes.STRING())
          .field("ts", DataTypes.BIGINT())
          .rowtime(
            new Rowtime()
              //从字段中提取时间戳
              .timestampsFromField("ts")
              //watermark延迟1秒
              .watermarksPeriodicBounded(1000L)
          )
      )
      .createTemporaryTable("inputtable")
    
    
    /*10 创建表的DDL中指定处理时间（Even time）*/
    bs_stream_env.sqlUpdate(
      """
        |create table dataTable(
        | id varchar(20) not null,
        | name varchar(20),
        | ts bigint,
        | rt as TO_TIMESTAMP(FROM_UNIXTIME(ts)),
        | watermark for rt as rt - interval '5' second
        |)with(
        | 'connector.type' = 'filesystem',
        | 'connector.path' = 'file:///D:\\..\\order.txt',
        | 'format.type' = 'csv'
        |)
        |""".stripMargin)
    
    
    /*11 Group windows*/
    
    /*12 滚动窗口*/
    orderTable.window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'tw.end)
    //事件时间字段rowtime
    orderTable.window(Tumble over 10.seconds on 'proctime as 'w)
    //处理时间字段proctime
    orderTable.window(Tumble over 10.seconds on 'rowtime as 'w)
    //类似于计算窗口，按事件时间排序，10行一组
    orderTable.window(Tumble over 10.rows on 'rowtime as 'w)
    
    
    /*13 滑动窗口*/
    orderTable.window(Slide over 10.hours every 5.minutes on 'rowtime as 'w)
    orderTable.window(Slide over 10.hours every 5.minutes on 'proctime as 'w)
    orderTable.window(Slide over 10.rows every 5.rows on 'rowtime as 'w)
    
    
    /*14 会话窗口*/
    orderTable.window(Session withGap 10.minutes on 'rowtime as 'w)
    orderTable.window(Session withGap 10.minutes on 'proctime as 'w)
    
   
    /*15 无界 Over window*/
    orderTable.window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE as 'w)
    orderTable.window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_RANGE as 'w)
    orderTable.window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_ROW as 'w)
    orderTable.window(Over partitionBy 'a orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
    
    
    /*16 有界的over window*/
    orderTable.window(Over partitionBy 'a orderBy 'rowtime preceding 1.minutes as 'w)
    orderTable.window(Over partitionBy 'a orderBy 'proctime preceding 1.minutes as 'w)
    orderTable.window(Over partitionBy 'a orderBy 'rowtime preceding 10.rows as 'w)
    orderTable.window(Over partitionBy 'a orderBy 'proctime preceding 10.rows as 'w)
    
    
    
  }
}

case class Order(id: String, name: String, ts: Long)
