package org.cscie88c.week9

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

/*
 * input stream: docker/beam/apps/data-streamer.sh after modifying input and output locations
 * output stream: watch -n1 ls <output folder>
 * run with
 sbt "runMain org.cscie88c.week9.BeamSimpleStreamApplication \
 --input-folder=
 --output-folder="
 */

object BeamSimpleStreamApplication extends LazyLogging {

  def main(cmdLineArgs: Array[String]): Unit = {
    // 1. Create context
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc
    // 2. Read command line arguments
    val inputPath: String = args("input-folder")
    val outputPath: String = args("output-folder")
    // 3. Run pipeline
    // uncomment line below to run beam pipeline
    runPipeline(inputPath, outputPath)

  }

  def runPipeline(inputPath: String, outputPath: String)(implicit sc: ScioContext): Unit = {
    // 4. Read input data
    val lines: SCollection[String] = readTransactions(inputPath)

    // 5. Transform and aggregate data
    val result: SCollection[Double] = calculateWindowAggregate(lines)

    // 6. Write results
    writeAggregateToFile(outputPath, result)
    val ec = sc.run()
    ec.waitUntilFinish()
  }

  def readTransactions(inputFile: String)(implicit sc: ScioContext): SCollection[String] = {
    val lines: SCollection[String] = sc.textFile(inputFile)
    lines
  }

  def calculateWindowAggregate(lines: SCollection[String]): SCollection[Double] = {
    // Parse the transactions, assuming each line contains a numeric value and timestamp
    val transactionsWithTimestamp: SCollection[(Double, Long)] = lines.map { line =>
      val parts = line.split(",")  // Assuming CSV format: amount,timestamp
      val amount = parts(0).toDouble
      val timestamp = parts(1).toLong  // Assuming timestamp is in milliseconds
      (amount, timestamp)
    }

    // Apply windowing: Fixed windows of 1 minute
    val windowedTransactions = transactionsWithTimestamp
      .withFixedWindows(Duration.standardMinutes(1))  // Window every 1 minute

    // Sum the transactions within each window
    val windowSums: SCollection[(Long, Double)] = windowedTransactions
      .groupBy(_._2)  // Group by window (Instant represents event time, so this groups by the timestamp)
      .mapValues(_.map(_._1).sum)  // Sum the amounts within each window

    windowSums.map(_._2)
  }

  def writeAggregateToFile(outputPath: String, results: SCollection[Double]): Unit = {
    results
      .map(_.toString) // Convert each Double to a String so we can write it as text
      .saveAsTextFile(outputPath) // Save the results to the specified output path
  }
}
