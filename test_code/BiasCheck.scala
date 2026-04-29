/*
Checks whether the cleaning and hazard classification
may affect some groups more than other

1. Unknown concentration rate by product type
2. Unknown concentration rate by country
3. NA hazard flag rate by product type
4. NA hazard flag rate by country
*/

val firstRDD = sc.textFile("/user/aam9811_nyu_edu/output/clean_consumer_metals")


val parsedRDD = firstRDD.map(line => line.split("\t")).filter(row => row.length > 11)
println("--------Unknown Concentration Rate by Product Type----------")
val unknownByProduct = parsedRDD.
  map(row => {
    val productType = row(1)
    val isUnknown = if (row(4).equalsIgnoreCase("Unknown")) 1 else 0
    (productType, (isUnknown, 1))
  }).
  reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).
  mapValues(x => x._1.toDouble / x._2).
  sortBy(_._2, ascending = false)

unknownByProduct.take(20).foreach(println)

println("--------Unknown Concentration Rate by Country----------")
val unknownByCountry = parsedRDD.
  map(row => {
    val country = row(8)
    val isUnknown = if (row(4).equalsIgnoreCase("Unknown")) 1 else 0
    (country, (isUnknown, 1))
  }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(x => x._1.toDouble / x._2).sortBy(_._2, ascending = false)

unknownByCountry.take(20).foreach(println)

println("--------NA Hazard Rate by Product Type----------")
val naByProduct = parsedRDD.
  map(row => {
    val productType = row(1)
    val isNA = if (row(11).equalsIgnoreCase("NA")) 1 else 0
    (productType, (isNA, 1))
  }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(x => x._1.toDouble / x._2).sortBy(_._2, ascending = false)

naByProduct.take(20).foreach(println)

println("--------NA Hazard Rate by Country----------")
val naByCountry = parsedRDD.
  map(row => {
    val country = row(8)
    val isNA = if (row(11).equalsIgnoreCase("NA")) 1 else 0
    (country, (isNA, 1))
  }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).mapValues(x => x._1.toDouble / x._2).sortBy(_._2, ascending = false)

naByCountry.take(20).foreach(println)