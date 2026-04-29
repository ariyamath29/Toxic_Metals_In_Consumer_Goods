/*
 * Profiling Script: Counts records, identifies distinct categories, 
 * and calculates the range and average for numeric columns (Concentration).
 */


val firstRDD = sc.textFile("/user/aam9811_nyu_edu/consumer_metals.csv")

val header = firstRDD.first()
val NoHeader = firstRDD.filter(line => !line.equals(header))

println("total records using count")
println(NoHeader.count())
val recordList = NoHeader.map(line => ("records", 1)).reduceByKey(_+_)
println("total records using map")
recordList.collect().foreach(println)

def parseCSV(line: String): Array[String] = {
     line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map(_.trim.stripPrefix("\"").stripSuffix("\""))
}

// OPTIMIZATION: Parse the CSV once and cache the result as an Array[String]. 
// This avoids running the expensive regex split over and over again for each column check!
val safeRDD = NoHeader.map(parseCSV).filter(cols => cols.length > 7).cache()


println("Distinct product types")
val productTypes = safeRDD.map(cols => cols(1)).distinct()
val otherCount = safeRDD.filter(cols => cols(1).equalsIgnoreCase("Other")).count()
productTypes.collect().foreach(println)

println(s"Count of 'Other' product types: $otherCount")

println("Distinct Metal types")
val metalTypes = safeRDD.map(cols => cols(3)).distinct()
metalTypes.collect().foreach(println)

println("Distinct manufacturers")
val manufacturers = safeRDD.map(cols => cols(6)).distinct()
//manufacturers.collect().foreach(println)

println("Distinct countries")
val countries = safeRDD.map(cols => cols(7)).distinct()
countries.collect().foreach(println)


// --- Profiling Numeric Columns ---

// 1. Extract valid numeric values from the Concentration column (index 4), ignoring 'Unknown' or invalid text
val validConcentrations = safeRDD.
  map(cols => cols(4).trim.stripPrefix("\"").stripSuffix("\"").replaceAll(",", "")).
  filter(v => v.nonEmpty && !v.toLowerCase.contains("unknown") && !v.toLowerCase.contains("na")).
  map(v => {
    try {
      Some(v.toDouble)
    } catch {
      case _: NumberFormatException => None
    }
  }).
  filter(_.isDefined).
  map(_.get)

validConcentrations.cache()
val countValid = validConcentrations.count()

// 2. Calculate and print the range (Min and Max) to understand the spread of the data
val maxConcentration = if (countValid > 0) validConcentrations.max() else 0.0
val minConcentration = if (countValid > 0) validConcentrations.min() else 0.0
println(s"\nRange of Concentration values: $minConcentration to $maxConcentration")

// 3. Calculate and print the overall average of the valid concentration values
val sumValid = if (countValid > 0) validConcentrations.sum() else 0.0
val avgConcentration = if (countValid > 0) sumValid / countValid else 0.0
println(s"Average Concentration value: $avgConcentration")
