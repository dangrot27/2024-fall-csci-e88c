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


/*
 * run with
 sbt "runMain org.cscie88c.week10.BeamQuartilesApplication \
 --input-file=./docker/beam/data/taxi_tripdata_sample.csv \
 --output-file=./target/approx-quartile"
 */
object BeamQuartilesApplication extends LazyLogging {

  def main(cmdLineArgs: Array[String]): Unit = {
    // 1. Create context
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    // 2. Read command line arguments
    val inputFile: String = args("inputFile")
    val outputFile: String = args("outputFile")
    // 3. Run pipeline
    runPipeline(inputFile: String, outputFile: String)
    sc.run().waitUntilFinish()
  }

  def runPipeline(inputFile: String, outputFile: String)(implicit sc: ScioContext): Unit = {

    // Read input CSV file and extract the `total_amount` column as Double
    val lines: SCollection[String] = sc.textFile(inputFile)

    val quartiles = approximateQuantiles( lines)

    quartiles.map(println)

    quartiles.saveAsTextFile(path = outputFile, numShards = 1)
  }

  def approximateQuantiles(lines: SCollection[String]): SCollection[String] = {
  lines
    .flatMap(TaxiTripData(_))             // Parse each line into a TaxiTripData instance
    .map(_.total_amount.toDouble)         // Extract trip_distance as Double
    .filter(_ > 0)
    .quantilesApprox(3)                    // Compute approximate quartiles for Double values
    .map(_.mkString("\n"))                 // Convert the result into a String for output
  
  }
}
