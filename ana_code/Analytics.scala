/*
This program performs analysis on a dataset of consumer products tested for toxic metal contamination. 
After cleaning, the script computes mean, median, mode, and standard deviation—across product groups, countries of origin, and manufacturers. 

In addition to basic statistics, the analysis identifies broader trends in toxicity. It calculates the distribution of metals across product categories, 
evaluates hazard rates by excluding ambiguous values (yes/Yes+no), and flags product groups and countries where more than 50% of valid observations are classified as hazardous. 
The program also ranks products by total hazardous occurrences to highlight items most frequently associated with unsafe metal levels.
*/

val firstRDD = sc.textFile("/user/aam9811_nyu_edu/output/clean_consumer_metals")


val parsedRDD = firstRDD.map(line => line.split("\t"))

// average and median, mode, standard deviation of all concentrations per product type
println("Basic Statistics by Product Type\n")

println("--------Mean by Product----------")
val byProduct = parsedRDD.
  filter(row => row(4) != "Unknown").
  map(row => (row(1), row(4).toDouble)).
  groupByKey()

val meanByProduct = byProduct.
  mapValues(values => values.sum / values.size).
  sortBy(_._2, ascending = false)

// OPTIMIZATION: Scalable output for screenshots
// REPLACED: 
// println(meanByProduct.collect().mkString("\n"))
// WHY: take(20) avoids sending immense amounts of records to the master Node
meanByProduct.take(20).foreach(println)

println("--------Median by Product----------")
val medianByProduct = byProduct.
  mapValues(values => {
    val sortedVals = values.toSeq.sorted
    val n = sortedVals.size
    if (n % 2 == 1) {
      sortedVals(n / 2)
    } else {
      (sortedVals(n / 2 - 1) + sortedVals(n / 2)) / 2.0
    }
  }).
  sortBy(_._2, ascending = false)

// REPLACED: println(medianByProduct.collect().mkString("\n"))
medianByProduct.take(20).foreach(println)

println("--------Mode by Product----------")
val modeByProduct = byProduct.
  mapValues(values => values.groupBy(identity).maxBy(_._2.size)._1).
  sortBy(_._2, ascending = false)

// REPLACED: println(modeByProduct.collect().mkString("\n"))
modeByProduct.take(20).foreach(println)

println("--------Standard Deviation by Product----------")
val stddevByProduct = byProduct.
  mapValues(values => {
    val mean = values.sum / values.size
    math.sqrt(values.map(x => math.pow(x - mean, 2)).sum / values.size)
  }).
  sortBy(_._2, ascending = false)

// REPLACED: println(stddevByProduct.collect().mkString("\n"))
stddevByProduct.take(20).foreach(println)


// average + median, mode concentration by country
println("\nBasic Statistics by Country\n")

val byCountry = parsedRDD.
  filter(row => row(4) != "Unknown").
  map(row => (row(7), row(4).toDouble)).
  groupByKey()

println("--------Mean by Country----------")
val meanByCountry = byCountry.
  mapValues(values => {
    val seq = values.toSeq
    seq.sum / seq.size
  }).
  sortBy(_._2, ascending = false)

// REPLACED: println(meanByCountry.collect().mkString("\n"))
meanByCountry.take(20).foreach(println)

println("--------Median by Country----------")
val medianByCountry = byCountry.
  mapValues(values => {
    val sortedVals = values.toSeq.sorted
    val n = sortedVals.size
    if (n % 2 == 1) {
      sortedVals(n / 2)
    } else {
      (sortedVals(n / 2 - 1) + sortedVals(n / 2)) / 2.0
    }
  }).
  sortBy(_._2, ascending = false)

// REPLACED: println(medianByCountry.collect().mkString("\n"))
medianByCountry.take(20).foreach(println)

println("--------Mode by Country----------")
val modeByCountry = byCountry.
  mapValues(values => values.groupBy(identity).maxBy(_._2.size)._1).
  sortBy(_._2, ascending = false)

// REPLACED: println(modeByCountry.collect().mkString("\n"))
modeByCountry.take(20).foreach(println)


// average + median, mode concentration by manufacturer
println("\nBasic Statistics by Manufacturer\n")

val byManufacturer = parsedRDD.
  filter(row => row(4) != "Unknown").
  map(row => (row(6), row(4).toDouble)).
  groupByKey()

println("--------Mean by Manufacturer----------")
val meanByManufacturer = byManufacturer.
  mapValues(values => {
    val seq = values.toSeq
    seq.sum / seq.size
  }).
  sortBy(_._2, ascending = false)

// REPLACED: println(meanByManufacturer.collect().mkString("\n"))
meanByManufacturer.take(20).foreach(println)

println("--------Median by Manufacturer----------")
val medianByManufacturer = byManufacturer.
  mapValues(values => {
    val sortedVals = values.toSeq.sorted
    val n = sortedVals.size
    if (n % 2 == 1) {
      sortedVals(n / 2)
    } else {
      (sortedVals(n / 2 - 1) + sortedVals(n / 2)) / 2.0
    }
  }).
  sortBy(_._2, ascending = false)

// REPLACED: println(medianByManufacturer.collect().mkString("\n"))
medianByManufacturer.take(20).foreach(println)

println("--------Mode by Manufacturer----------")
val modeByManufacturer = byManufacturer.
  mapValues(values => values.groupBy(identity).maxBy(_._2.size)._1).
  sortBy(_._2, ascending = false)

// REPLACED: println(modeByManufacturer.collect().mkString("\n"))
modeByManufacturer.take(20).foreach(println)


// Other analytics

println("\n--------Metal Distribution----------")
val metalCounts = parsedRDD.
  map(row => (row(3), 1)).
  reduceByKey(_ + _).
  sortBy(_._2, ascending = false)

// REPLACED: println(metalCounts.collect().mkString("\n"))
metalCounts.take(20).foreach(println)


println("\n--------Product Group Distribution per Metal----------")
val productPerMetal = parsedRDD.
  map(row => ((row(3), row(1)), 1)).
  reduceByKey(_ + _).
  map { case ((metal, productGroup), count) => (metal, (productGroup, count)) }.
  groupByKey()

// REPLACED: println(productPerMetal.collect().mkString("\n"))
productPerMetal.take(20).foreach(println)


// product groups where metals are hazardous (over 50% of binary values are YES)
val productHazard = parsedRDD.
  filter(row => row(11).equalsIgnoreCase("yes") || row(11).equalsIgnoreCase("no")).
  map(row => {
    val productGroup = row(1)
    val isHazard = if (row(11).equalsIgnoreCase("yes")) 1 else 0
    (productGroup, (isHazard, 1))
  })

val productHazardRate = productHazard.
  reduceByKey((a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2)).
  mapValues(x => x._1.toDouble / x._2)

val hazardousProducts = productHazardRate.sortBy(_._2, ascending = false)

println("\n--------Product Groups Hazard Rate----------")
// REPLACED: println(hazardousProducts.collect().mkString("\n"))
hazardousProducts.take(20).foreach(println)


// countries where metals are hazardous (over 50% of binary values are YES)
val countryHazard = parsedRDD.
  filter(row => row(11).equalsIgnoreCase("yes") || row(11).equalsIgnoreCase("no")).
  map(row => {
    val country = row(7)
    val isHazard = if (row(11).equalsIgnoreCase("yes")) 1 else 0
    (country, (isHazard, 1))
  })

val countryHazardRate = countryHazard.
  reduceByKey((a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2)).
  mapValues(x => x._1.toDouble / x._2)

val hazardousCountries = countryHazardRate.
  filter(x => x._2 > 0.5).
  sortBy(_._2, ascending = false)

println("\n--------Hazardous Countries (>50%)----------")
// REPLACED: println(hazardousCountries.collect().mkString("\n"))
hazardousCountries.take(20).foreach(println)

// manufacturers where metals are hazardous (over 50% of binary values are YES)
val manufacturerHazard = parsedRDD.
  filter(row => row(11).equalsIgnoreCase("yes") || row(11).equalsIgnoreCase("no")).
  map(row => {
    val manufacturer = row(6)
    val isHazard = if (row(11).equalsIgnoreCase("yes")) 1 else 0
    (manufacturer, (isHazard, 1))
  })

val manufacturerHazardRate = manufacturerHazard.
  reduceByKey((a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2)).
  mapValues(x => x._1.toDouble / x._2)

val hazardousManufacturers = manufacturerHazardRate.
  filter(x => x._2 > 0.5).
  sortBy(_._2, ascending = false)

println("\n--------Hazardous Manufacturers (>50%)----------")
// REPLACED: println(hazardousManufacturers.collect().mkString("\n"))
hazardousManufacturers.take(20).foreach(println)


//manufactuers with the most toxic entries ordered from highest to lowest total YES values
val manufacturerToxic = parsedRDD.
  map(row => {
    val manufacturer = row(6)
    val isHazard = if (row(11).equalsIgnoreCase("yes")) 1 else 0
    (manufacturer, isHazard)
  })
  .reduceByKey(_ + _)
  .sortBy(_._2, ascending = false) 

println("\n--------Most Toxic Manufacturers----------")
// REPLACED TO IMPROVE: println(manufacturerToxic.take(20).mkString("\n"))
manufacturerToxic.take(20).foreach(println)

// products with the most toxic entries ordered from highest to lowest total YES values
val productToxic = parsedRDD.
  map(row => {
    val productName = row(2)
    val isHazard = if (row(11).equalsIgnoreCase("yes")) 1 else 0
    (productName, isHazard)
  })

val toxicProducts = productToxic.reduceByKey(_ + _).sortBy(_._2, ascending = false)

println("\n--------Most Toxic Products----------")
//REPLACED: println(toxicProducts.take(20).mkString("\n"))
toxicProducts.take(20).foreach(println)