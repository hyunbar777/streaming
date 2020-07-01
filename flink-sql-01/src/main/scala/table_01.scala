import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Elasticsearch, FileSystem, Json, Kafka, OldCsv, Schema}
import org.apache.flink.types.Row


/**
 * Author z
 * Date 2020-06-22 16:14:19
 */
object table_01 {
  def main(args: Array[String]): Unit = {
    /*1.0  老版本的流式查询（flink-Streaming-Query）*/
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner() //使用老版本planner
      .inStreamingMode() //流处理模式
      .build()
    val table_env = StreamTableEnvironment.create(env, settings)
    
    
    /*2.0  基于老版本的批处理环境（Flink-Batch-Query）*/
    val batch_env = ExecutionEnvironment.getExecutionEnvironment
    val batchTable_env = BatchTableEnvironment.create(batch_env)
    
    
    /*3.0  基于blink版本的流式处理环境（Blink-Streaming-Query）*/
    val bs_settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bstable_env = StreamTableEnvironment.create(env, bs_settings)
    
    
    /*4.0  基于Blink版本的批处理环境（Blink-Batch-Query）*/
    val bb_settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbtable_env = TableEnvironment.create(bb_settings)
    
    
    /*5.0 连接到文件系统（CSV格式）（旧版本）*/
    bstable_env
      // 定义表数据来源，外部链接
      .connect(new FileSystem().path("hello.txt"))
      // 定义从外部系统读取数据之后的格式化方法
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("price", DataTypes.DOUBLE())
      )
      // 定义表结构（创建临时表）
      .createTemporaryTable("inputtable")
    
    
    /*6.0 连接到文件系统（CSV格式）（新版本）*/
    bstable_env
      // 定义表数据来源，外部链接
      .connect(new FileSystem().path("hello.txt"))
      // 定义从外部系统读取数据之后的格式化方法
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("price", DataTypes.DOUBLE())
      )
      // 定义表结构（创建临时表）
      .createTemporaryTable("inputtable")
    
    
    /*7.0  链接到kafka*/
    table_env
      .connect(
        new Kafka()
          .version("0.11") //kafka版本
          .topic("test") //定义主题
          .property("zookeeper.connect", "localhost:2181")
          .property("bootstrap.servers", "localhost:9092")
      )
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("price", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkainputtable")
    
    
    /*8.0 tableapi的调用*/
    table_env.from("inputable")
      .select("id,price")
      .filter("id='1'")
    
    val table = table_env.from("inputable")
    
    /*9.0 sql查询*/
    //9.1
    table_env.sqlQuery("select id,price from inputable where id = '1'")
    //9.2
    table_env.sqlQuery(
      """
        |select id,price
        |from inputable
        |where id = '1'
        |""".stripMargin
    )
    //9.3
    table
      .groupBy('id)
      .select('id, 'id.count as 'cnt)
      .toDataSet[Row]
      .print()
    //9.4
    val order = table_env.from("order") //schema(a,b,c,rowtime)
    order
      .filter('a.isNotNull && 'b.isNotNull && 'c.isNotNull)
      .select('a.lowerCase() as 'a, 'b, 'rowtime)
      .window(Tumble over 1.hour on 'rowtime as 'hourlywindow)
      .groupBy('hourlywindow, 'a)
      .select('a, 'hourlywindow.end as 'hour, 'b.avg as 'avgBillingAmount)
    
    /*10.0 创建临时视图（Temporary View）*/
    //10.1 基于datastream
    val dataStream = env
      .readTextFile("student.txt")
      .map(data => {
        val dataArray = data.split(",")
        Student(dataArray(0), dataArray(1), dataArray(2).toInt)
      })
    table_env.createTemporaryView("myview", dataStream)
    table_env.createTemporaryView("myview", dataStream, 'id, 'name as 'a, 'age)
    //10.2 基于table
    table_env.createTemporaryView("myview", order)
    
    /*11.0 输出到文件*/
    table_env
      //定义到文件系统的连接
      .connect(new FileSystem().path("out.txt"))
      //定义格式化方法，csv
      .withFormat(new Csv)
      //定义表结构
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("age", DataTypes.INT())
      )
      //创建临时表
      .createTemporaryTable("outputtable")
    
    order.insertInto("outputtable")
    
    /*12 输出到kafka*/
    table_env.connect(
      new Kafka()
        .version("0.11")
        .topic("test")
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    )
      //定义格式化方法，csv
      .withFormat(new Csv)
      //定义表结构
      .withSchema(
        new Schema()
          .field("id", DataTypes.STRING())
          .field("age", DataTypes.INT())
      )
      //创建临时表
      .createTemporaryTable("kafkaoutputtable")
    order.insertInto("kafkaoutputtable")
    
    
    /*13 输出到es*/
    table_env.connect(
      new Elasticsearch()
        .version("6")
        .host("localhost",9200,"http")
        .index("student")
        .documentType("_doc")
    )
      //指定是upsert模式
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("age", DataTypes.INT())
      )
      .createTemporaryTable("esoutputtable")
    order.insertInto("esoutputtable")
    
    
    /*14 输出到mysql*/
    val sinkDDL =
      """
        |create table jdbcOutputTable(
        |id varchar(20) not null,
        |age int not null
        |)with(
        |'connector.type'='jdbc,
        |'connector.url'='jdbc:mysql://localhost:3306/test',
        |'connector.table'='student',
        |'connector.driver'='com.mysql.jdbc.Driver',
        |'connector.username'='root',
        |'connector.password'='123456'
        |)
        |""".stripMargin
    table_env.sqlUpdate(sinkDDL)
    order.insertInto("jdbcOutputTable")
    
    /*15 将表转换为datastream*/
    val resultStream = table_env
      .toAppendStream[Row](order)
      .print()
    val aggResultStream = table_env
      .toRetractStream[(String,Int)](order)
      .print()
    
    
  
  }
}
case class Student(id: String, name: String, age: Int)