package org.cscie88c.week10

import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.typesafe.scalalogging.{LazyLogging}
import org.joda.time.{DateTimeZone, Duration, Instant}
import java.time.ZoneId
import java.time.ZoneOffset
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PBegin, PCollection}
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.Watch
import com.spotify.scio.io.ClosedTap
import com.twitter.algebird.CMS
import com.spotify.scio.io.Tap
import cats.instances.duration
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.transforms.windowing.AfterWatermark
import shapeless.record
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


/*
 * run with
 sbt "runMain org.cscie88c.week10.BeamHourlyTotalApplication \
 --input-file=./docker/beam/data/taxi_tripdata_sample.csv \
 --output-file=./target/hourly-total.csv"
 */
object BeamHourlyTotalApplication extends LazyLogging {

  def main(cmdLineArgs: Array[String]): Unit = {
    // 1. Create context
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    // 2. Read command line arguments
    val inputFile: String = args("inputFile")
    val outputFile: String = args("outputFile")
    // 3. Run pipeline
    // uncomment line below to run the beam pipeline
    runPipeline(inputFile: String, outputFile: String)
    sc.run().waitUntilFinish()
  }

  def runPipeline(inputFile: String, outputFile: String)(implicit sc: ScioContext): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val lines: SCollection[String] = sc.textFile(inputFile)

    val records = lines
      .map { line =>
        val fields = line.split(",")  // assuming CSV is comma-separated
        val timestampStr = fields(1)
        val amount = fields(15).toDouble           // Parse amount from column 3
        val localDateTime = LocalDateTime.parse(timestampStr, formatter)
        val timestamp = localDateTime.toInstant(ZoneOffset.UTC)
        (timestamp, amount)
      }
      .withFixedWindows(Duration.standardHours(1)) // Set 1-hour window
      .map { case (_, amount) => amount }           // Extract amount for summation
      .sum

    records.saveAsTextFile(outputFile)
    
      
  }

  
}
