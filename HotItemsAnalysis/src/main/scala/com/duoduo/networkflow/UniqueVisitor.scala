package com.duoduo.utils.networkflow

import com.duoduo.hotitems.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Author z
 * Date 2020-05-15 09:43:57
 */
case class UvCount(windowEnd: Long, count: Long)
object UniqueVisitor {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
    
        val stream = env.readTextFile(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath)
            .map(line => {
                val linearray = line.split(",")
                UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
            })
            .assignAscendingTimestamps(_.timestamp * 1000)
            .filter(_.behavior == "pv")
            .timeWindowAll(Time.hours(1))
            .apply(new UvCountByWindow())
            .print
        env.execute("Unique Visitor Job")
    }
}

class UvCountByWindow extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
    override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
        var ids = Set[Long]()
        
        for(item <- input){
            ids += item.userId
        }
        out.collect(UvCount(window.getEnd +1,ids.size))
    }
}
