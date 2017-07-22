package com.modusbox.metrics

import com.datastax.spark.connector.{SomeColumns, _}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

abstract class Metric extends Serializable {
  def environment: String

  def application: String

  def metricName: String

  def metricTimestamp: Long
}

case class ReceivedCounter(val environment: String, val application: String, val metricName: String, val metricTimestamp: Long,
                           count: Long, receivedInCurrentInterval: Boolean) extends Metric

case class Counter(val environment: String, val application: String, val metricName: String, val metricTimestamp: Long,
                   count: Long, previousMetricTimestamp: Long, previousCount: Long) extends Metric

case class ReceivedTimer(val environment: String, val application: String, val metricName: String, val metricTimestamp: Long,
                         count: Long, max: Float, mean: Float, min: Float, stdDev: Float, median: Float, p75: Float, p95: Float,
                         p98: Float, p99: Float, p999: Float, meanRate: Float, oneMinRate: Float, fiveMinRate: Float, fifteenMinRate: Float,
                         rateUnit: String, durationUnit: String, receivedInCurrentInterval: Boolean) extends Metric

case class Timer(val environment: String, val application: String, val metricName: String, val metricTimestamp: Long, count: Long,
                 max: Float, mean: Float, min: Float, stdDev: Float, median: Float, p75: Float, p95: Float, p98: Float, p99: Float,
                 p999: Float, meanRate: Float, oneMinRate: Float, fiveMinRate: Float, fifteenMinRate: Float, previousMetricTimestamp: Long,
                 previousCount: Long, rateUnit: String, durationUnit: String) extends Metric

object MetricPersister {

  val topics = Array("bmgf.metric.20170221", "bmgf.metric.pi4")
  val batchInterval = 20
  val windowLength = batchInterval * 2
  val windowSlideLength = batchInterval * 1

  def main(args: Array[String]): Unit = {

//    val sparkConf = new SparkConf().setAppName("metrics-collector")
//      .setMaster("local[*]")
//      .set("spark.cassandra.connection.host", "35.164.199.6")

    val sparkContext = SparkContext.getOrCreate()
    val ssc = new StreamingContext(sparkContext, Seconds(batchInterval))

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    print("starting job")
    process(kafkaStream)
    commitKafkaOffsets(kafkaStream)

    ssc.start()
    ssc.awaitTermination()

/*
    // This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
    ssc.awaitTerminationOrTimeout(20 * 1000)

    StreamingContext.getActive.foreach {
      _.stop(stopSparkContext = false)
    }
*/
  }

  def process(incomingMetricsStream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    val metricLineStream: DStream[((String, String, Long), String)] = incomingMetricsStream.map(metricLine => {
      val tokens = metricLine.value().split(',')
      ((metricLine.key(), tokens(0) /*type*/, tokens(4).toLong /*timestamp*/), metricLine.value())
    })
    metricLineStream.print()
    process(metricLineStream)
  }

  def process(metricLineStream: DStream[((String, String, Long), String)]): Unit = {
    // create a tuple ((fullyQualifiedMetricName, type, timestamp), metricLine)
    val metricLineWindowedStream = metricLineStream.window(Seconds(windowLength), Seconds(windowSlideLength))
    val joinedMetricLineStream = metricLineWindowedStream.leftOuterJoin(metricLineStream)
    val joinedCounterStream = joinedMetricLineStream.filter(pair => pair._1._2 == "COUNTER").transform((rdd: RDD[((String, String, Long), (String /* from windowed stream */, Option[String] /* from current batch*/))], time: Time) => {
      rdd.map {
        case (key, (windowedMetricLine, Some(currentIntervalLIne))) => val tokens = windowedMetricLine.split(','); ReceivedCounter(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, true)
        case (key, (windowedMetricLine, None)) => val tokens = windowedMetricLine.split(','); ReceivedCounter(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, false)
      }})

    val joinedTimerStream = joinedMetricLineStream.filter(pair => pair._1._2 == "TIMER").transform((rdd: RDD[((String, String, Long), (String /* from windowed stream */, Option[String] /* from current batch*/))], time: Time) => {
      rdd.map {
        case (key, (windowedMetricLine, Some(currentIntervalLine))) => val tokens = windowedMetricLine.split(','); ReceivedTimer(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, tokens(6).toFloat, tokens(7).toFloat, tokens(8).toFloat, tokens(9).toFloat, tokens(10).toFloat, tokens(11).toFloat, tokens(12).toFloat, tokens(13).toFloat, tokens(14).toFloat, tokens(15).toFloat, tokens(16).toFloat, tokens(17).toFloat, tokens(18).toFloat, tokens(19).toFloat, tokens(20), tokens(21), true)
        case (key, (windowedMetricLine, None)) => val tokens = windowedMetricLine.split(','); ReceivedTimer(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, tokens(6).toFloat, tokens(7).toFloat, tokens(8).toFloat, tokens(9).toFloat, tokens(10).toFloat, tokens(11).toFloat, tokens(12).toFloat, tokens(13).toFloat, tokens(14).toFloat, tokens(15).toFloat, tokens(16).toFloat, tokens(17).toFloat, tokens(18).toFloat, tokens(19).toFloat, tokens(20), tokens(21), false)
      }})

    joinedCounterStream.foreachRDD { (rdd: RDD[ReceivedCounter], time: Time) =>
      val sparkSession = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._

      println("Processing Counters")
      val counterDf = rdd.toDF()
      counterDf.createOrReplaceTempView("counter")
      sparkSession.sql("select * from (select environment, application, metricName, metricTimestamp, count, lag(metricTimestamp, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousMetricTimestamp, lag(count, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousCount from counter) where receivedInCurrentInterval = true and previousMetricTimestamp is not null")
        .as[Counter]
        .rdd
        .saveToCassandra("metric_data", "raw_counter_with_interval", SomeColumns("environment", "application", "metric_name" as "metricName", "metric_timestamp" as "metricTimestamp", "count", "previous_metric_timestamp" as "previousMetricTimestamp", "previous_count" as "previousCount"))
    }

    joinedTimerStream.foreachRDD { (rdd: RDD[ReceivedTimer], time: Time) =>
      val sparkSession = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._

      println("Processing Timers")
      val timerDf = rdd.toDF()
      timerDf.createOrReplaceTempView("timer")

      sparkSession.sql("select * from (select environment, application, metricName, metricTimestamp, count, max, mean, min, stdDev, median, p75, p95, p98, p99, p999, meanRate, oneMinRate, fiveMinRate, fifteenMinRate, rateUnit, durationUnit, lag(metricTimestamp, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousMetricTimestamp, lag(count, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousCount from timer) where receivedInCurrentInterval = true and previousMetricTimestamp is not null")
        .as[Timer]
        .rdd
        .saveToCassandra("metric_data", "raw_timer_with_interval", SomeColumns("environment", "application", "metric_name", "metric_timestamp", "count", "max", "mean", "min", "std_dev", "median", "p75", "p95", "p98", "p99", "p999", "mean_rate", "one_min_rate", "five_min_rate", "fifteen_min_rate", "rate_unit", "duration_unit", "previous_metric_timestamp", "previous_count"))
    }

/*
    joinedMetricLineStream.foreachRDD { (rdd: RDD[((String, String, Long), (String /* from windowed stream */, Option[String] /* from current batch*/))], time: Time) =>
      val sparkSession = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._

      val count = rdd.count()
      println(s"=========* $time, $count =========")

      val counterDf = rdd.filter(pair => pair._1._2 == "COUNTER").map(pair => pair match {
        case (key, (windowedMetricLine, Some(currentIntervalLine))) => val tokens = windowedMetricLine.split(','); ReceivedCounter(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, true)
        case (key, (windowedMetricLine, None)) => val tokens = windowedMetricLine.split(','); ReceivedCounter(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, false)
      }).toDF()

      counterDf.createOrReplaceTempView("counter")

      sparkSession.sql("select * from (select environment, application, metricName, metricTimestamp, count, lag(metricTimestamp, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousMetricTimestamp, lag(count, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousCount from counter) where receivedInCurrentInterval = true and previousMetricTimestamp is not null")
        .as[Counter]
        .rdd
        .saveToCassandra("metric_data", "raw_counter_with_interval", SomeColumns("environment", "application", "metric_name" as "metricName", "metric_timestamp" as "metricTimestamp", "count", "previous_metric_timestamp" as "previousMetricTimestamp", "previous_count" as "previousCount"))

      val timerDf = rdd.filter(pair => pair._1._2 == "TIMER").map(pair => pair match {
        case (key, (windowedMetricLine, Some(currentIntervalLine))) => val tokens = windowedMetricLine.split(','); ReceivedTimer(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, tokens(6).toFloat, tokens(7).toFloat, tokens(8).toFloat, tokens(9).toFloat, tokens(10).toFloat, tokens(11).toFloat, tokens(12).toFloat, tokens(13).toFloat, tokens(14).toFloat, tokens(15).toFloat, tokens(16).toFloat, tokens(17).toFloat, tokens(18).toFloat, tokens(19).toFloat, tokens(20), tokens(21), true)
        case (key, (windowedMetricLine, None)) => val tokens = windowedMetricLine.split(','); ReceivedTimer(tokens(1), tokens(2), tokens(3), tokens(4).toLong, tokens(5).toLong, tokens(6).toFloat, tokens(7).toFloat, tokens(8).toFloat, tokens(9).toFloat, tokens(10).toFloat, tokens(11).toFloat, tokens(12).toFloat, tokens(13).toFloat, tokens(14).toFloat, tokens(15).toFloat, tokens(16).toFloat, tokens(17).toFloat, tokens(18).toFloat, tokens(19).toFloat, tokens(20), tokens(21), false)
      }).toDF()

      timerDf.createOrReplaceTempView("timer")

      sparkSession.sql("select * from (select environment, application, metricName, metricTimestamp, count, max, mean, min, stdDev, median, p75, p95, p98, p99, p999, meanRate, oneMinRate, fiveMinRate, fifteenMinRate, rateUnit, durationUnit, lag(metricTimestamp, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousMetricTimestamp, lag(count, 1) OVER (PARTITION BY environment, application, metricName ORDER BY metricTimestamp ASC) as previousCount from timer) where receivedInCurrentInterval = true and previousMetricTimestamp is not null")
        .as[Timer]
        .rdd
        .saveToCassandra("metric_data", "raw_timer_with_interval", SomeColumns("environment", "application", "metric_name", "metric_timestamp", "count", "max", "mean", "min", "std_dev", "median", "p75", "p95", "p98", "p99", "p999", "mean_rate", "one_min_rate", "five_min_rate", "fifteen_min_rate", "rate_unit", "duration_unit", "previous_metric_timestamp", "previous_count"))
    }
*/

  }

  def commitKafkaOffsets(kafkaStream: InputDStream[ConsumerRecord[String, String]]) = {
    kafkaStream.foreachRDD {
      rdd =>
        val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsets.foreach((offset: OffsetRange) => println(s"overall: ${offset.topic} ${offset.partition} ${offset.fromOffset} ${offset.untilOffset} ${offset.count}"))
        rdd.foreachPartition {
          iter =>
            val o: OffsetRange = offsets(TaskContext.get.partitionId)
            println(s"parition: ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }

        // some time later, after outputs have completed
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
    }

  }
}

