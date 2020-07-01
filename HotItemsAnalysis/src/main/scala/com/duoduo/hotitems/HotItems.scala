package com.duoduo.hotitems

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Author z
 * Date 2020-05-13 15:27:19
 */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long) {
    override def equals(that: Any): Boolean = ???
}

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long) {
    override def equals(that: Any): Boolean = ???
}

object HotItems {
    def main(args: Array[String]): Unit = {
        val environment = StreamExecutionEnvironment.getExecutionEnvironment
        //设定time类型为EventTime
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //为了打印到控制台的结果不乱序，配置全局并发为1，改变并发对结果正确性没有影响
        environment.setParallelism(1)
        val result = environment.readTextFile(this.getClass.getClassLoader.getResource("UserBehavior.csv").getPath)
            .map(line => {
                val arr = line.split(',')
                UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
            })
            //指定时间戳和watermark
            //有序的数据
            // .assignAscendingTimestamps(_.timestamp * 1000)
            //无序的数据，延迟1s
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1000)) {
                override def extractTimestamp(t: UserBehavior): Long = {
                    t.timestamp * 1000
                }
            })
            //过滤pv数据
            .filter(_.behavior == "pv")
            //按商品id进行分组
            .keyBy("itemId")
            //设置窗口大小1小时，步长5分钟
            .timeWindow(Time.minutes(1), Time.seconds(10))
            //得到每个商品在每个窗口的点击量
            .aggregate(new CountAgg(), new WindowResultFunction())
            .keyBy("windowEnd")
            .process(new TopNHostItems(3))
            .print()
        environment.execute("Hot Items Job")
        
    }
}

class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L
    
    override def add(in: UserBehavior, acc: Long): Long = acc + 1
    
    override def getResult(acc: Long): Long = acc
    
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemid = key.asInstanceOf[Tuple1[Long]].f0
        val count = input.iterator.next
        out.collect(ItemViewCount(itemid, window.getEnd, count))
    }
}

//求某个窗口中前N名的热门点击商品，key为窗口时间戳，out为TopN的结果
class TopNHostItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    
    private var itemState: ListState[ItemViewCount] = _
    
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //命名状态变量的名字和状态变量的类型
        val itemStateDesc = new ListStateDescriptor[ItemViewCount](
            "itemState-sate", classOf[ItemViewCount])
        //定义状态变量
        itemState = getRuntimeContext.getListState(itemStateDesc)
        
    }
    
    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
        //将每条数据保存到状态中
        itemState.add(i)
        //注册windowEnd + 1 的eventTime timer
        //当触发时，说明收齐了属于windowEnd窗口的所有商品数据
        //也就是当程序看到windowEnd + 1 的水位线watermark时，触发onTimer回调函数
        context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }
    
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        //获取收到的所有商品点击量
        val allItem: ListBuffer[ItemViewCount] = ListBuffer()
        import scala.collection.JavaConversions._
        for (item <- itemState.get) {
            allItem += item
        }
        //提前清除状态中的数据，释放空间
        itemState.clear()
        //按照点击量从大到小排序
        val sortItems = allItem.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
        //将排名信息格式化成string，便于打印
        val sb = new StringBuilder
        sb.append("===========================\n")
            .append("时间：")
            .append(new Timestamp(timestamp - 1))
            .append("\n")
            .append("=========================\n\n")
        for (i <- sortItems.indices) {
            val currentItem = sortItems(i)
            sb.append("NO").append(i + 1).append(":")
                .append(" 商品 ID=").append(currentItem.itemId)
                .append("  浏览量=").append(currentItem.count).append("\n")
        }
        //控制输出频率，模拟实时滚动结果
        Thread.sleep(1000)
        out.collect(sb.toString())
    }
}