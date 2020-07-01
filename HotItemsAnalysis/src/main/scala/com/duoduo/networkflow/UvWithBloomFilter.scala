package com.duoduo.utils.networkflow

import com.duoduo.hotitems.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * Author z
 * Date 2020-05-15 14:33:43
 */
object UvWithBloomFilter {
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
            .map(data=>("dummykey",data.userId))
            .keyBy(_._1)
            .timeWindow(Time.hours(1))
            .trigger(new MyTrigger())
            .process(new UvCountWithBloomProcess())
            .print
        env.execute("Unique Visitor BloomFilter Job")
    }
}

class MyTrigger() extends Trigger[(String,Long),TimeWindow]{
    override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        //每来一条数据，就触发窗口操作并清空
       TriggerResult.FIRE_AND_PURGE
    }
    
    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }
    
    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }
    
    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = ???
}

class UvCountWithBloomProcess extends ProcessWindowFunction[(String,Long),UvCount,String,TimeWindow]{
    lazy val jedis = new Jedis("localhost",6379)
    lazy val bloom = new Bloom(1<<29)
    
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    
        
        val storeKey = context.window.getEnd.toString
        var count =0L
        if (jedis.hget("count",storeKey)!=null) {
            count=jedis.hget("count",storeKey).toLong
        }
        val userId = elements.last._2.toString
        val offset = bloom.hash(userId,61)
        
        val isExist = jedis.getbit(storeKey,offset)
        if (!isExist){
            jedis.setbit(storeKey,offset,true)
            jedis.hset("count",storeKey,(count +1).toString)
            out.collect(UvCount(storeKey.toLong,count +1))
        }else{
            out.collect(UvCount(storeKey.toLong,count))
        }
        
    }
}

class  Bloom(size:Long) extends  Serializable{
    private val cap = size
    def hash(value:String,seed:Int):Long={
        //最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做重叠
        var result = 0
        for (i <- 0 until value.length){
            result = result * seed + value.charAt(i)
        }
    (cap -1) & result
    }
}