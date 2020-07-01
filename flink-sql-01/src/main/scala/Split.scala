
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row


/**
 * Author z
 * Date 2020-07-01 13:47:56
 */
class Split_test {
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
    val table = bs_stream_env.fromDataStream(dataStream, 'id, 'name, 'age, 'pt.proctime)
    val split = new Split("#")
    table.joinLateral(split('id) as ('word, 'length))
      .select('id, 'word, 'length)
        .toAppendStream[Row].print("1")
    
    table.leftOuterJoinLateral(split('id) as ('word,'length))
        .select('id,'word,'length)
        .toAppendStream[Row].print("2")
    
    
    //sql 方式
    fb_table_env.registerFunction("split",new Split("#"))
    fb_table_env.sqlQuery("select id,word,length from " +
      "student," +
      "lateral table(split(id)) as new_stu(word,lenght)")
        .toAppendStream[Row].print("3")
    
    fb_table_env.sqlQuery(
      """
        |select id,word,length
        |from student
        |left join
        |lateral table(split(id)) as new_stu(word,length)
        |on true
        |""".stripMargin)
      .toAppendStream[Row].print("4")
    
    env.execute()
  }
  //自定义tablefunction
  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      str.split(separator).foreach(
        word => collect((word, word.length))
      )
    }
  }
  
}

