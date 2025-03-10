package experiment
 
import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
 
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
 
sealed trait WindowType
 
object WindowType {
  case object Tumbling extends WindowType
 
  case object Sliding extends WindowType
 
  case object Session extends WindowType
 
  def fromString(s: String): WindowType = s.toLowerCase match {
    case "tumbling" => Tumbling
    case "sliding" => Sliding
    case "session" => Session
    case _ => throw new IllegalArgumentException(s"Invalid window type: $s")
  }
}
 
object StreamingDeequWindowsReddit {
 
  // Define case class for metrics including window information
  case class PerformanceMetrics(
    batchId: Long,
    blastId: String,
    windowType: String,
    windowDuration: String,
    numCores: String,
    windowStart: String,
    windowEnd: String,
    time: Long, // time stands for ingestion timestamp. Using "time" for uniformity with pathway
    numFailedChecks: Int,
    processEndTimestamp: Long,
    batchSize: Long,
    dataRetrievalTimeMs: Long,
    windowingTimeMs: Long,
    checkingTimeMs: Long,
    totalExecutionTimeMs: Long
)
 
  def createWindowedStream(
    df: DataFrame,
    windowType: WindowType,
    windowDuration: String,
    slideDuration: Option[String],
    gapDuration: Option[String]
): DataFrame = {
    windowType match {
      case WindowType.Tumbling =>
        df.withWatermark("created_utc", "0 seconds")
          .groupBy(col("blast_id"), window(col("created_utc"), windowDuration))
          .agg(collect_list(struct(df.columns.map(col): _*)).alias("records"))
 
      case WindowType.Sliding =>
        val slide = slideDuration.getOrElse(
          throw new IllegalArgumentException("Slide duration must be provided for sliding windows")
        )
        df.withWatermark("created_utc", "0 seconds")
          .groupBy(col("blast_id"), window(col("created_utc"), windowDuration, slide))
          .agg(collect_list(struct(df.columns.map(col): _*)).alias("records"))
 
      case WindowType.Session =>
        val gap = gapDuration.getOrElse(
          throw new IllegalArgumentException("Gap duration must be provided for session windows")
        )
        df.withWatermark("created_utc", "0 seconds")
          .groupBy(col("blast_id"), session_window(col("created_utc"), gap))
          .agg(collect_list(struct(df.columns.map(col): _*)).alias("records"))
    }
  }
 
  def main(args: Array[String]): Unit = {
 
    // Read environment variables with defaults
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaInputTopic = sys.env.getOrElse("INPUT_TOPIC", "data_input")
    val kafkaOutputTopic = sys.env.getOrElse("OUTPUT_TOPIC", "data_output")
    val sparkMaster = sys.env.getOrElse("SPARK_MASTER", "local")
    val sparkNumCores = sys.env.getOrElse("SPARK_NUM_CORES", "*")
    val windowTypeStr = sys.env.getOrElse("WINDOW_TYPE", "tumbling").toLowerCase()
    val windowDuration = sys.env.getOrElse("WINDOW_DURATION", "10 seconds")
    val slideDuration = sys.env.getOrElse("SLIDE_DURATION", "5 seconds")
    val gapDuration = sys.env.getOrElse("GAP_DURATION", "5 seconds")
 
 
    // Log the configurations
//    println(s"Kafka Bootstrap Servers: $kafkaBootstrapServers")
//    println(s"Kafka Input Topic: $kafkaInputTopic")
//    println(s"Kafka Output Topic: $kafkaOutputTopic")
//    println(s"Spark Master: $sparkMaster")
//    println(s"Spark Num Cores: $sparkNumCores")
//    println(s"Window Type: $windowTypeStr")
//    println(s"Window Duration: $windowDuration")
//    println(s"Slide Duration: $slideDuration")
//    println(s"Gap Duration: $gapDuration")
 
    // spark master and number of cores are configured from the environment variables in the dockerfile(s)
    val spark = SparkSession.builder()
      .appName("Streaming Deequ Example with Windowing")
      .getOrCreate()
 
    // Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("OFF")
 
    // Import implicits for Encoder
    import spark.implicits._
 
    // Needed for .asJava
    import scala.collection.JavaConverters._
 
    // Define the schema
    val itemSchema = StructType(Array(
      StructField("created_utc", LongType, nullable = true),
      StructField("ups", IntegerType, nullable = true),
      StructField("subreddit_id", StringType, nullable = true),
      StructField("link_id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("score_hidden", IntegerType, nullable = true),
      StructField("author_flair_css_class", StringType, nullable = true),
      StructField("author_flair_text", StringType, nullable = true),
      StructField("subreddit", StringType, nullable = true),
      StructField("id_", StringType, nullable = true),
      StructField("removal_reason", StringType, nullable = true),
      StructField("gilded", IntegerType, nullable = true),
      StructField("downs", IntegerType, nullable = true),
      StructField("archived", IntegerType, nullable = true),
      StructField("author", StringType, nullable = true),
      StructField("score", IntegerType, nullable = true),
      StructField("retrieved_on", LongType, nullable = true),
      StructField("body", StringType, nullable = true),
      StructField("distinguished", StringType, nullable = true),
      StructField("edited", IntegerType, nullable = true),
      StructField("controversiality", IntegerType, nullable = true),
      StructField("parent_id", StringType, nullable = true),
 
      StructField("blast_id", StringType, nullable = true),
      StructField("blast_start_time", StringType, nullable = true),
    ))
 
    // Create an empty Dataset[MetricRecordWithWindow] to store metrics
    var metricsDF = spark.emptyDataset[PerformanceMetrics]
    metricsDF.cache()
 
    // Parse WindowType from string
    val windowType = WindowType.fromString(windowTypeStr)
 
    // Prepare optional durations
    val slideDurationOpt = if (slideDuration.nonEmpty) Some(slideDuration) else None
    val gapDurationOpt = if (gapDuration.nonEmpty) Some(gapDuration) else None
 
    // Read streaming data from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaInputTopic)
      .option("startingOffsets", "latest")
      .load()
 
    // Extract and parse the JSON messages
    val messagesDF = kafkaDF.selectExpr("CAST(value AS STRING) as json_str")
 
    // Define a UDF to handle both string and UNIX timestamp inputs
    val parseEventTime = udf((eventTime: Any) => eventTime match {
      case s: String =>
        // Try to parse the string timestamp
        val formatter = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        new java.sql.Timestamp(formatter.parse(s).getTime)
      case l: Long =>
        // Convert UNIX timestamp in milliseconds to Timestamp
        new java.sql.Timestamp(l * 1000)
      case _ => null
    })
 
    val parsedMessagesDF = messagesDF
      .withColumn("jsonData", from_json(col("json_str"), itemSchema))
      .select("jsonData.*")
      // If eventTime is a StringType, parse it to TimestampType
      .withColumn("created_utc", parseEventTime(col("created_utc")))
      .withColumn("retrieved_on", parseEventTime(col("retrieved_on")))
 
    val windowedStream = createWindowedStream(
      parsedMessagesDF,
      windowType,
      windowDuration,
      slideDurationOpt,
      gapDurationOpt
    )
 
    // Process each windowed batch and apply Deequ checks
    val query = windowedStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val time = System.currentTimeMillis()
 
        // Ensure the batch data is cached to prevent multiple computations
        batchDF.cache()
 
        if (!batchDF.isEmpty) {
          // Collect the windowed data
          val windowedData = batchDF.collect()
          val startTime = System.currentTimeMillis()
          val dataRetrievalTime = startTime - time
          // Process each window separately
          windowedData.foreach { row =>
            val blastId: String = row.getAs[String]("blast_id")
            val window = row.getAs[Row]("window")
            val windowStart = window.getAs[Timestamp]("start")
            val windowEnd = window.getAs[Timestamp]("end")
            val records = row.getAs[Seq[Row]]("records")
 
            // Convert records back to DataFrame
            val windowDF = spark.createDataFrame(records.asJava, parsedMessagesDF.schema)
            val batchSize = windowDF.count()
 
            val rowCount = windowDF.count()
//            println(s"[Batch $batchId] -> Blast: $blastId | Window: [$windowStart to $windowEnd] " +
//              s"| #Records: $rowCount")
 
            if (batchSize > 0) {
              // Record the start time
              val windowingFinishTime = System.currentTimeMillis()
              val windowingTime = windowingFinishTime - time
 
              // Define the data quality checks
              val check = Check(CheckLevel.Error, "Reddit checks")
                .isComplete("id_")
                .isUnique("id_")
                .hasMin("downs", _ >= 0)
                .hasMean("controversiality", _ < 0.05)
                .hasCompleteness("author", _ > 0.9)
 
              val checkingStartTime = System.currentTimeMillis()
              val verificationResult: VerificationResult = VerificationSuite()
                .onData(windowDF)
                .addCheck(check)
                .run()
 
              // Record the end time
              val checkingEndTime = System.currentTimeMillis()
 
 
              val numFailedChecks = verificationResult.checkResults
                .flatMap { case (_, checkResult) => checkResult.constraintResults }
                .count {
                  _.status != ConstraintStatus.Success
                }
 
              // Calculate total execution time and time required only for data quality checks
              val checkingTime = checkingEndTime - checkingStartTime
              val totalExecutionTime = checkingEndTime - time
 
              // Get current timestamp as a string
              val batchEndTime = System.currentTimeMillis()
 
              // Create a MetricRecord
              val performanceMetrics = PerformanceMetrics(
                batchId,
                blastId,
                windowTypeStr,
                windowDuration,
                sparkNumCores,
                windowStart.toString,
                windowEnd.toString,
                time,
                numFailedChecks,
                batchEndTime,
                batchSize,
                dataRetrievalTime,
                windowingTime,
                checkingTime,
                totalExecutionTime,
              )
 
              // Convert MetricRecord to Dataset
              val metricDS = Seq(performanceMetrics).toDS()
 
              // Next, produce metricDS to Kafka's output topic
              // We'll create a small DF of key/value columns
              val toKafkaDF = metricDS
                .selectExpr(
                  "CAST('deequ' AS STRING) as key", // or define your own key
                  "to_json(struct(*)) as value" // entire PerformanceMetrics as JSON
                )
 
              // Write to Kafka in batch mode (each foreachBatch is still micro-batch)
              toKafkaDF
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                .option("topic", kafkaOutputTopic)
                .save()
 
              // Optionally, log to console
              //println(s"Sent performance metrics of blast_id=$blastId to Kafka metrics topic.")
            }
          }
        }
 
        // Unpersist the batch data
        batchDF.unpersist()
        ()
      }
      .option("truncate", "false")
      .start()
 
    query.awaitTermination()
  }
}