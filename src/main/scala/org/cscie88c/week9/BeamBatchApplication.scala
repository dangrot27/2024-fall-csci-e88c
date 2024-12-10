package org.cscie88c.week9
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.typesafe.scalalogging.{LazyLogging}


object BeamBatchApplication extends LazyLogging {

  def main(cmdLineArgs: Array[String]): Unit = {
    // 1. Create context
    val (sc: ScioContext, args: Args) = ContextAndArgs(cmdLineArgs)
    implicit val scImplicit: ScioContext = sc

    // 2. Read command line arguments
    val inputFile: String = args("input-file")
    val outputFile: String = args("output-dir")

    // 3. Run pipeline
    // uncomment line below to run the beam pipeline
    runPipeline(inputFile: String, outputFile: String)
  }

  def runPipeline(inputFile: String, outputFile: String)(implicit sc: ScioContext): Unit = {
    //val totalAmounts: SCollection[Double] = sc.textFile(inputFile)
    val totalAmounts: SCollection[Double] = sc.textFile(inputFile)
      .filter(!_.contains("total_amount"))
      .map { line =>
        val fields = line.split(",")
        // Extract total_amount by index and convert to Double
        fields(16).toDouble
      }

    sumTransactions(totalAmounts, outputFile)
    aveTransactions(totalAmounts, outputFile)

    sc.run().waitUntilFinish()
  }

  def sumTransactions(amounts: SCollection[Double], outputFile: String)(implicit sc: ScioContext): Unit = {
    val sum: SCollection[Double] = amounts.sum
    
    sum.map(println)
    // Write the sum to the output file
    sum.map(total => s"Total Sum of Transactions: $total")
      .saveAsTextFile(outputFile)
  }

  def aveTransactions(amounts: SCollection[Double], outputFile: String)(implicit sc: ScioContext): Unit = {
     // Calculate the sum and count of the transactions
    val sumAndCount = amounts
      .aggregate((0.0, 0))(
        (acc, value) => (acc._1 + value, acc._2 + 1),    // Sequential aggregation: add value to sum, increment count
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2) // Combine results from partitions
      )

  // Calculate the average by dividing sum by count
    val average = sumAndCount.map { case (sum, count) => 
      if (count > 0) sum / count else 0.0 
    }
    average.map(println)

    // Write the result to the output directory
    sc.parallelize(Seq(s"Average transaction amount: $average"))
      .saveAsTextFile(s"$outputFile/average_transaction.txt")
  }
}
