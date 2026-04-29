
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD

/*
Code Cleaning:
  1: Formatted text in the product column to reduce the number of distinct product types by mapping similar product names 
  to a normalized form ("turmeric haldi", "tumeric", "turmaric" all mapped to "turmeric").

  2: Added a binary column that determines if the toxic metal threshold is higher than limits set by the U.S and is hazardous (YES if so, NO if not)
  *Due to variability in regulatory standards, some ppm-based screening thresholds were used as approximations. 
*/

def parseCSV(line: String): Array[String] =
  line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1).map(_.trim.stripPrefix("\"").stripSuffix("\""))

def clean(value: String): String = {
  val v = Option(value).getOrElse("").trim
  if (v.isEmpty || v.equalsIgnoreCase("null") || v.equalsIgnoreCase("unknown") ||
      v.equalsIgnoreCase("unknown or not stated") || v.equalsIgnoreCase("n/a") ||
      v.equalsIgnoreCase("na")) "Unknown"
  else v
}

def cleanConcentration(value: String): String = {
  val v = value.trim.stripPrefix("\"").stripSuffix("\"").replaceAll(",", "").trim
  if (v.isEmpty || v.equalsIgnoreCase("unknown") || v.equalsIgnoreCase("unknown or not stated")) return "Unknown"
  try {
    val d = v.toDouble
    if (d < 0) "Unknown" else v
  } catch { case _: NumberFormatException => "Unknown" }
}

//Normalization of product names

// OPTIMIZATION: Broadcast large lookup map
// By declaring this map globally and broadcasting it, we avoid sending a massive map to every task across the cluster individually.
val synonymsBC = sc.broadcast(Map(
  //TURMERIC
  "turmeric haldi"                               -> "turmeric",
  "tumeric"                                      -> "turmeric",
  "turmeic"                                      -> "turmeric",
  "turmaric"                                     -> "turmeric",
  "curcuma"                                      -> "turmeric",
  "cuzcuma"                                      -> "turmeric",
  "haldi"                                        -> "turmeric",
  "yellow turmeric"                              -> "turmeric",
  "spice turmeric"                               -> "turmeric",
  "homemade turmeric"                            -> "turmeric",
  "curcuma turmeric"                             -> "turmeric",
  "fijan turmeric"                               -> "turmeric",
  // TURMERIC POWDER
  "turmeric powder"                              -> "turmeric powder",
  "tumeric powder"                               -> "turmeric powder",
  "turmaric powder"                              -> "turmeric powder",
  "powdered turmeric"                            -> "turmeric powder",
  "ground turmeric"                              -> "turmeric powder",
  "ground turmeric powder"                       -> "turmeric powder",
  "turmeric powder haldi powder"                 -> "turmeric powder",
  "turmaric powder haldi powder curcuma enpolvo" -> "turmeric powder",
  //CHILI POWDER
  "chilli powder"                                -> "chili powder",
  "chilly powder"                                -> "chili powder",
  "chile powder"                                 -> "chili powder",
  "chili ppowder"                                -> "chili powder",
  "red chili powder"                             -> "chili powder",
  "red chilli powder"                            -> "chili powder",
  "red chilli powdered"                          -> "chili powder",
  "red chillies powder"                          -> "chili powder",
  "powdered red pepper"                          -> "chili powder",
  "powdered red chili"                           -> "chili powder",
  "powdered chili"                               -> "chili powder",
  "red pepper powder"                            -> "chili powder",
  "hot pepper powder"                            -> "chili powder",
  "red hot pepper powder"                        -> "chili powder",
  "extra hot chilli powder"                      -> "chili powder",
  "dark chilli powder"                           -> "chili powder",
  "deshi hot chili powder"                       -> "chili powder",
  "deshi hot chili power"                        -> "chili powder",
  "kashmiri chili powder"                        -> "chili powder",
  "kashmiri chilli powder"                       -> "chili powder",
  "kashmiri chilly powder"                       -> "chili powder",
  "kashmiri mirch"                               -> "chili powder",
  "kashmiri red chilli spice"                    -> "chili powder",
  "100 kashmiri chilli powder"                   -> "chili powder",
  "red chill powder"                             -> "chili powder",
  "red chili"                                    -> "chili powder",
  "red chilli"                                   -> "chili powder",
  //CHILI PEPPER
  "chilli"                                       -> "chili pepper",
  "chile"                                        -> "chili pepper",
  "red pepper"                                   -> "chili pepper",
  "red peppers"                                  -> "chili pepper",
  "cayenne pepper"                               -> "chili pepper",
  "cayenne"                                      -> "chili pepper",
  "cayenne powder"                               -> "chili pepper",
  "red chili pepper"                             -> "chili pepper",
  "red chilli pepper"                            -> "chili pepper",
  "dried chili pepper"                           -> "chili pepper",
  "dried chili"                                  -> "chili pepper",
  "dried chilli peppers"                         -> "chili pepper",
  "dried red hot chili pepper"                   -> "chili pepper",
  "dried whole red chilli pepper"                -> "chili pepper",
  "dried whole red chilli"                       -> "chili pepper",
  "whole red pepper"                             -> "chili pepper",
  "hot pepper"                                   -> "chili pepper",
  "piquin pepper"                                -> "chili pepper",
  "habanero pepper"                              -> "chili pepper",
  "jalapeno pepper"                              -> "chili pepper",
  "jalapeno"                                     -> "chili pepper",
  "guajillo pepper"                              -> "chili pepper",
  "ancho"                                        -> "chili pepper",
  "chili flakes"                                 -> "chili pepper",
  "red chili flakes"                             -> "chili pepper",
  "crushed red pepper"                           -> "chili pepper",
  "crushed red chili"                            -> "chili pepper",
  "crushed chilli"                               -> "chili pepper",
  "roasted chili flakes"                         -> "chili pepper",
  "red pepper balls"                             -> "chili pepper",
  "red cholls"                                   -> "chili pepper",
  //CUMIN
  "cummin"                                       -> "cumin",
  "comino"                                       -> "cumin",
  "dzira"                                        -> "cumin",
  "zira"                                         -> "cumin",
  "jeera"                                        -> "cumin",
  "jira"                                         -> "cumin",
  "geera"                                        -> "cumin",
  "trainer cumin"                                -> "cumin",
  "morning cumin"                                -> "cumin",
  "bobo zra cumin"                               -> "cumin",
  "daytime zra cumin"                            -> "cumin",
  "home cumin powder"                            -> "cumin",
  "comin"                                        -> "cumin",
  // CUMIN SEEDS
  "cumin seeds"                                  -> "cumin seeds",
  "cumin seed"                                   -> "cumin seeds",
  "cumin seed whole"                             -> "cumin seeds",
  "whole cumin"                                  -> "cumin seeds",
  "black cumin"                                  -> "cumin seeds",
  "dried cumin seed"                             -> "cumin seeds",
  "comino cumin seeds"                           -> "cumin seeds",
  // CUMIN POWDER
  "cumin powder"                                 -> "cumin powder",
  "cumin power"                                  -> "cumin powder",
  "ground cumin"                                 -> "cumin powder",
  "powdered cumin"                               -> "cumin powder",
  "jeera powder"                                 -> "cumin powder",
  "comino molido"                                -> "cumin powder",
  "comino molino"                                -> "cumin powder",
  // GARAM MASALA
  "garam masala"                                 -> "garam masala",
  "garam mashala"                                -> "garam masala",
  "garamasala"                                   -> "garam masala",
  "garaham masala"                               -> "garam masala",
  "graham masala"                                -> "garam masala",
  "garam masala powder"                          -> "garam masala",
  "mixed gorom masala"                           -> "garam masala",
  "roasted garam masala"                         -> "garam masala",
  "garson masala"                                -> "garam masala",
  "homemade garam masala"                        -> "garam masala",
  // MASALA
  "masala"                                       -> "masala",
  "marsala"                                      -> "masala",
  "masala powder"                                -> "masala",
  // CORIANDER POWDER
  "coriander powder"                             -> "coriander powder",
  "coriandor powder"                             -> "coriander powder",
  "corriander powder"                            -> "coriander powder",
  "corriander seed powder"                       -> "coriander powder",
  "corriander"                                   -> "coriander powder",
  "coriandor"                                    -> "coriander powder",
  "dhania"                                       -> "coriander powder",
  "dhaniya powder"                               -> "coriander powder",
  "dhana jeera powder"                           -> "coriander powder",
  "dhana jiru"                                   -> "coriander powder",
  "powdered coriander"                           -> "coriander powder",
  "ground corinander"                            -> "coriander powder",
  "corinder"                                     -> "coriander powder",
  "corrainder"                                   -> "coriander powder",
  "kinzi"                                        -> "coriander powder",
  // CORIANDER SEEDS
  "coriander seeds"                              -> "coriander seeds",
  "coriander"                                    -> "coriander seeds",
  // CURRY POWDER
  "curry powder"                                 -> "curry powder",
  "madras curry powder"                          -> "curry powder",
  "madras curry"                                 -> "curry powder",
  "indian curry powder"                          -> "curry powder",
  "mixed curry powder"                           -> "curry powder",
  "hot curry powder"                             -> "curry powder",
  "jamaican curry powder"                        -> "curry powder",
  "yellow curry powder"                          -> "curry powder",
  "sambar powder"                                -> "curry powder",
  // SURMA / KOHL
  "surma"                                        -> "surma",
  "surma hashmi"                                 -> "surma",
  "surma special"                                -> "surma",
  "surma nargisi"                                -> "surma",
  "surma powder"                                 -> "surma",
  "surma gel"                                    -> "surma",
  "surma kohl"                                   -> "surma",
  "hashmi surma"                                 -> "surma",
  "hashmi surma special"                         -> "surma",
  "kohl"                                         -> "surma",
  // KAJAL
  "kajal"                                        -> "kajal",
  "kajol"                                        -> "kajal",
  "kajal eyeliner"                               -> "kajal",
  "eyeliner"                                     -> "kajal",
  // AMULET
  "amulet"                                       -> "amulet",
  "amulet tabeez"                                -> "amulet",
  "amulet charm"                                 -> "amulet",
  "tabeez"                                       -> "amulet",
  // TAMARIND CANDY
  "tamarind candy"                               -> "tamarind candy",
  "tamarindo candy"                              -> "tamarind candy",
  "tamarindo"                                    -> "tamarind candy",
  "tamarind toffee"                              -> "tamarind candy",
  "dulce de tamarido"                            -> "tamarind candy",
  // CINNAMON
  "cinnamon"                                     -> "cinnamon",
  "cinnamon stick"                               -> "cinnamon",
  "cinnamon sticks"                              -> "cinnamon",
  "ceylon cinnamon"                              -> "cinnamon",
  "saigon cinnamon"                              -> "cinnamon",
  // CINNAMON POWDER
  "cinnamon powder"                              -> "cinnamon powder",
  "ground cinnamon"                              -> "cinnamon powder",
  "cinammon powder"                              -> "cinnamon powder",
  "chinnamon powder"                             -> "cinnamon powder",
  // SINDOOR
  "sindoor"                                      -> "sindoor",
  "sindur"                                       -> "sindoor",
  "kumkum"                                       -> "sindoor",
  "kumkum powder"                                -> "sindoor",
  "kum kum"                                      -> "sindoor",
  "roli kumkum"                                  -> "sindoor",
  "sindoor powder"                               -> "sindoor",
  // CLAY POT
  "clay pot"                                     -> "clay pot",
  "clay pot with cover"                          -> "clay pot",
  "clay pots"                                    -> "clay pot",
  "claypot"                                      -> "clay pot",
  "glazed clay pot"                              -> "clay pot",
  // GINGER
  "ginger"                                       -> "ginger",
  "ginger root"                                  -> "ginger",
  "ginger powder"                                -> "ginger powder",
  "ground ginger"                                -> "ginger powder",
  "zinger powder"                                -> "ginger powder",
  // PAPRIKA
  "paprika"                                      -> "paprika",
  "paprika powder"                               -> "paprika",
  "sweet paprika"                                -> "paprika",
  "hot paprika"                                  -> "paprika",
  "smoked paprika"                               -> "paprika",
  "ground paprika"                               -> "paprika",
  "piment doux"                                  -> "paprika",
  // BLACK PEPPER
  "black pepper"                                 -> "black pepper",
  "black pepper powder"                          -> "black pepper",
  "black peppercorns"                            -> "black pepper",
  "ground black pepper"                          -> "black pepper",
  "whole black pepper"                           -> "black pepper",
  // FENUGREEK
  "fenugreek"                                    -> "fenugreek",
  "fenugreek powder"                             -> "fenugreek",
  "fenugreek leaves"                             -> "fenugreek",
  "methi"                                        -> "fenugreek",
  "methi seeds"                                  -> "fenugreek",
  "utskho suneli"                                -> "fenugreek",
  "dry fenugreek"                                -> "fenugreek",
  "blue fenugreek"                               -> "fenugreek",
  // CARDAMOM
  "cardamom"                                     -> "cardamom",
  "cardamon"                                     -> "cardamom",
  "elach"                                        -> "cardamom",
  "cardamom powder"                              -> "cardamom",
  "black cardamom"                               -> "cardamom",
  "green cardamom"                               -> "cardamom",
  "large cardamom"                               -> "cardamom",
  "small cardamom"                               -> "cardamom",
  "cinrdamom"                                    -> "cardamom",
  // FIVE SPICE
  "five spice powder"                            -> "five spice powder",
  "five spice"                                   -> "five spice powder",
  "five spices powder"                           -> "five spice powder",
  "5 spice powder"                               -> "five spice powder",
  // BIRYANI MASALA
  "biryani masala"                               -> "biryani masala",
  "biryani seasoning"                            -> "biryani masala",
  "biryani spice"                                -> "biryani masala",
  "bombay biryani masala"                        -> "biryani masala",
  "pilau biryani"                                -> "biryani masala",
  "kachchi biryani masala"                       -> "biryani masala",
  // CLOVES
  "cloves"                                       -> "cloves",
  "whole cloves"                                 -> "cloves",
  "clove powder"                                 -> "cloves",
  "ground clove"                                 -> "cloves",
  // KHMELI SUNELI
  "khmeli suneli"                                -> "khmeli suneli",
  "khmeli-suneli"                                -> "khmeli suneli",
  "khemeli suneli"                               -> "khmeli suneli",
  "khemli suneli"                                -> "khmeli suneli",
  "suneli"                                       -> "khmeli suneli",
  "xmeli suneli"                                 -> "khmeli suneli",
  "humeli-suneli"                                -> "khmeli suneli",
  "tmt khmeli suneli"                            -> "khmeli suneli",
  // SOIL
  "potting soil"                                 -> "potted soil",
  "potted soil from balcony"                     -> "potted soil",
  "soil from indoor plant"                       -> "potted soil",
  "soil from potted plant"                       -> "potted soil"
))

//map product names to a normalized form to reduce the number of distinct product types
def normalizeProduct(raw: String): String = {
  val stripped = raw.toLowerCase.replaceAll("\\(.*?\\)", "").replaceAll("[^a-z0-9\\s]", "").trim.replaceAll("\\s+", " ")
  //REPLACED CODE
  // val synonyms: Map[String, String] = Map(...)
  // synonyms.getOrElse(stripped, stripped)
  
  // Uses the broadcasted value to reduce network I/O
  synonymsBC.value.getOrElse(stripped, stripped)
}

//Adding a binary column that determines if the toxic metal threshold is higher than the saftey limit (YES if so, NO if not)

//OPTIMIZATION: Broadcast large lookup map
val thresholdsBC = sc.broadcast(Map(
  // Jewelry / default children products
  ("jewelry", "lead") -> 100.0, ("jewelry", "cadmium") -> 75.0, ("jewelry", "chromium") -> 60.0, ("jewelry", "mercury") -> 1.0, ("jewelry", "arsenic") -> 25.0,
  // Food - Candy
  ("food-candy", "lead") -> 0.1, ("food-candy", "cadmium") -> 0.1, ("food-candy", "chromium") -> 0.5, ("food-candy", "mercury") -> 1.0, ("food-candy", "arsenic") -> 0.01,
  // Cosmetics
  ("cosmetics", "lead") -> 10.0, ("cosmetics", "cadmium") -> 3.0, ("cosmetics", "chromium") -> 5.0, ("cosmetics", "mercury") -> 1.0, ("cosmetics", "arsenic") -> 3.0,
  // Religious Powder
  ("religious powder", "lead") -> 10.0, ("religious powder", "cadmium") -> 3.0, ("religious powder", "chromium") -> 1.0, ("religious powder", "mercury") -> 1.0, ("religious powder", "arsenic") -> 3.0,
  // Food Other
  ("food other", "lead") -> 0.1, ("food other", "cadmium") -> 0.1, ("food other", "chromium") -> 0.1, ("food other", "mercury") -> 1.0, ("food other", "arsenic") -> 0.1,
  // Toys
  ("toys/children's products", "lead") -> 100.0, ("toys/children's products", "cadmium") -> 75.0, ("toys/children's products", "chromium") -> 60.0, ("toys/children's products", "mercury") -> 60.0, ("toys/children's products", "arsenic") -> 25.0,
  // Food Spice
  ("food-spice", "lead") -> 2.0, ("food-spice", "cadmium") -> 0.3, ("food-spice", "chromium") -> 0.1, ("food-spice", "mercury") -> 1.0, ("food-spice", "arsenic") -> 0.1,
  // Supplements
  ("dietary supplement/medications/remedy", "lead") -> 0.5, ("dietary supplement/medications/remedy", "cadmium") -> 0.5, ("dietary supplement/medications/remedy", "chromium") -> 2.5, ("dietary supplement/medications/remedy", "mercury") -> 1.5, ("dietary supplement/medications/remedy", "arsenic") -> 1.5,
  // Tableware
  ("tableware/pottery", "lead") -> 90.0, ("tableware/pottery", "cadmium") -> 0.25, ("tableware/pottery", "chromium") -> 10.0, ("tableware/pottery", "mercury") -> 1.0, ("tableware/pottery", "arsenic") -> 1.0,
  // Paint
  ("paint supplies", "lead") -> 90.0, ("paint supplies", "cadmium") -> 75.0, ("paint supplies", "chromium") -> 5.0, ("paint supplies", "mercury") -> 15.0, ("paint supplies", "arsenic") -> 25.0,
  // Other
  ("other", "lead") -> 90.0, ("other", "cadmium") -> 0.25, ("other", "chromium") -> 10.0, ("other", "mercury") -> 1.0, ("other", "arsenic") -> 1.0
))

def getThreshold(productType: String, metal: String): Option[Double] = {
  val p = productType.toLowerCase
  val m = metal.toLowerCase

  //REPLACED CODE
  //val thresholds: Map[(String, String), Double] = Map(...)
  //thresholds.get((p, m))
  
  thresholdsBC.value.get((p, m))
}

//General Cleaning

val firstRDD = sc.textFile("/user/aam9811_nyu_edu/consumer_metals.csv")
val header   = firstRDD.first()
val noHeader = firstRDD.filter(line => !line.equals(header))

val parsedRDD: RDD[Array[String]] = noHeader.map(line => parseCSV(line))

val safeRDD: RDD[Array[String]] = parsedRDD.filter(cols => cols.length > 10)
//drop null rows where key fields are unknown
val notNullRDD: RDD[Array[String]] = safeRDD.filter(cols =>
//REPLACED CODE: Seq(1, 2, 3, 4).exists(i => cols(i).trim.nonEmpty && !cols(i).equalsIgnoreCase("unknown") && !cols(i).equalsIgnoreCase("unknown or not stated")))
  Seq(1, 2, 3, 4).forall(i => cols(i).trim.nonEmpty && !cols(i).equalsIgnoreCase("unknown") && !cols(i).equalsIgnoreCase("unknown or not stated")))



val cleanRDD: RDD[Array[String]] = notNullRDD.map(cols => Array(
  clean(cols(0)),
  clean(cols(1)),
  normalizeProduct(clean(cols(2))),
  clean(cols(3)),
  cleanConcentration(cols(4)),
  clean(cols(5)),
  clean(cols(6)),
  clean(cols(7)),
  clean(cols(8)),
  clean(cols(9)),
  clean(cols(10))
))

//OPTIMIZATION: Caching repeatedly used RDD
cleanRDD.cache()

// Add a new column that checks if the concentration exceeds the safety threshold
val enrichedRDD: RDD[Array[String]] = cleanRDD.map(row => {

  val productType = row(1)   // PRODUCT_TYPE
  val metal = row(3)         // METAL
  val concentration = row(4) // CONCENTRATION

  // get threshold for this product + metal combination
  val thresholdOpt = getThreshold(productType, metal)

  var exceeds = "NA" // default if we don't have enough info

  if (thresholdOpt.isDefined && concentration != "Unknown") {
    try {
      val value = concentration.toDouble
      val threshold = thresholdOpt.get

      if (value > threshold) {
        exceeds = "YES"
      } else {
        exceeds = "NO"
      }

    } catch {
      case _: Exception => exceeds = "NA"
    }
  }

  // append new column to the row
  row :+ exceeds
})

//OPTIMIZATION: Caching repeatedly used RDD
enrichedRDD.cache()

val total      = noHeader.count()
val afterClean = cleanRDD.count()
val unknown    = cleanRDD.filter(row => row(4).equals("Unknown")).count()

println(s"Raw records             : $total")
println(s"After cleaning          : $afterClean")
println(s"Dropped      : ${total - afterClean}")
println(s"Number of rows with where presence of a metal is detected but cannot be quantified   : $unknown")
val percent = ((unknown.toDouble / afterClean.toDouble) * 100).toInt
println(s"percent of data where concentration is unknown: $percent%")

//println("Sample clean records")
//enrichedRDD.take(5).foreach(row => println(row.mkString(",")))

//println("\nnormalized product names")
//cleanRDD.map(row => row(2)).distinct().collect().sorted.foreach(println)

val outputRDD = enrichedRDD.map(row => row.mkString("\t"))

// check how many rows could not be evaluated
val naCount = enrichedRDD.filter(row => row.last == "NA").count()
println(s"Number of rows with NA threshold: $naCount")

//put output in hadoop ecosystem
val fs = FileSystem.get(sc.hadoopConfiguration)
val out = new Path("/user/aam9811_nyu_edu/output/clean_consumer_metals")
if (fs.exists(out)) {
  fs.delete(out, true)
}

outputRDD.saveAsTextFile("/user/aam9811_nyu_edu/output/clean_consumer_metals")
