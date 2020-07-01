package com.duoduo.utils.networkflow

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Author z
 * Date 2020-05-14 09:51:05
 */
//日志输入类型
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

//日志输出类型
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlow {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val stream = env.readTextFile(this.getClass.getClassLoader.getResource("apache.log").getPath)
            .map(line => {
                val items = line.split(" ")
                val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val timestamp = format.parse(items(3)).getTime
                ApacheLogEvent(items(0), items(2), timestamp, items(5), items(6))
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds(1000)) {
                override def extractTimestamp(t: ApacheLogEvent): Long = {
                    t.eventTime
                }
            })
            .filter(data => {
                val p = "^((?!\\.(css|js)$).)*$".r
                (p findAllIn data.url).nonEmpty
            })
            .keyBy("url")
            .timeWindow(Time.minutes(10), Time.seconds(5))
            .aggregate(new CountAgg1(), new WindowResultFunction1())
            .keyBy(1)
            .process(new TopNHostUrl(5))
            .print()
        env.execute("Network Flow Job")
        
    }
}

class CountAgg1() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def createAccumulator(): Long = 0L
    
    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1
    
    override def getResult(acc: Long): Long = acc
    
    override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
}

class WindowResultFunction1 extends WindowFunction[Long, UrlViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
        val url = key.asInstanceOf[Tuple1[String]].f0
        val count = input.iterator.next
        out.collect(UrlViewCount(url, window.getEnd, count))
        
    }
}

class TopNHostUrl(topSize: Int) extends KeyedProcessFunction[Tuple, UrlViewCount, String] {
    
    private var urlState: ListState[UrlViewCount] = _
    
    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val urlStateDesc =
            new ListStateDescriptor[UrlViewCount]("urlState-sate", classOf[UrlViewCount])
        urlState = getRuntimeContext.getListState(urlStateDesc)
    }
    
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
        urlState.add(i)
        context.timerService().registerEventTimeTimer(i.windowEnd + 1)
    }
    
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
        import scala.collection.JavaConversions._
        for (urlView <- urlState.get) {
            allUrlViews += urlView
        }
        urlState.clear()
        
        val sortedUrlViews = allUrlViews
            .sortBy(_.count)(Ordering.Long.reverse).take(topSize)
        var result: StringBuilder = new StringBuilder
        result.append("====================================\n")
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
        
        for (i <- sortedUrlViews.indices) {
            val currentUrlView: UrlViewCount = sortedUrlViews(i)
            // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
            result.append("No").append(i + 1).append(":")
                .append("  URL=").append(currentUrlView.url)
                .append("  流量=").append(currentUrlView.count).append("\n")
        }
        result.append("====================================\n\n")
        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000)
        out.collect(result.toString)
    }
    
}
