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
 sbt "runMain org.cscie88c.week10.BeamCMSApplication \
 --input-file=./docker/beam/data/taxi_tripdata_sample.csv \
 --output-file=./target/frequency-estimate"
 */
object BeamCMSApplication extends LazyLogging {

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
  }

  def runPipeline(inputFile: String, outputFile: String)(implicit sc: ScioContext): Unit = {
    
    // Read input file
    val lines: SCollection[String] = sc.textFile(inputFile)

    val result: ClosedTap[CMS[String]] = approximateFareTypes(lines)

    val ec = sc.run()
    val scioResult = ec.waitUntilDone()
    val cmsResult: Tap[CMS[String]] = scioResult.tap(result)
    val cmsInstance = cmsResult.value.next()
    val count49 = cmsInstance.frequency("49").estimate
    val count74 = cmsInstance.frequency("74").estimate
    
    val Frequencies = s"Frequency estimate for PULocationID 49: $count49\nFrequency estimate for PULocationID 74: $count74"

    println(Frequencies)
    }

    def approximateFareTypes(lines: SCollection[String]) : ClosedTap[CMS[String]] = {
      import  com.twitter.algebird._
      import  com.twitter.algebird.CMSHasherImplicits._

      val PULocationIDs: SCollection[String] = 
        lines
          .flatMap(TaxiTripData(_))
          .map(_.PULocationID.toString())

      val cms = 
          PULocationIDs
            .aggregate(CMS.aggregator[String](0.001, 1E-10, 1))
            .materialize

      cms
    }

}
