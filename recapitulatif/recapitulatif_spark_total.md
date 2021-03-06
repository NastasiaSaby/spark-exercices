# Exemple API Spark SQL

## Scala variable

```scala
val age = 12
println(age)
```
12

## Scala fonction

```scala
def speak() = {
  println("salut")
}

speak()
```
salut

```scala
def speak(word: String) = {
  println(word)
}

speak("hello ")
```
hello

```scala
def sendWord(word: String) = {
  "hello " + word
}

println(sendWord(" world"))
```
hello world

## Case class

```scala
case class Personne(age: Int, name: String)
val personne1 = Personne(12, "Lucien")

println(personne1.age)
```
12

## Lire un fichier avec Spark
```scala
val simpleDiamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
simpleDiamonds.show
```

```
+----+-----+---------+-----+-------+-----+-----+-----+----+----+----+
| _c0|  _c1|      _c2|  _c3|    _c4|  _c5|  _c6|  _c7| _c8| _c9|_c10|
+----+-----+---------+-----+-------+-----+-----+-----+----+----+----+
|null|carat|      cut|color|clarity|depth|table|price|   x|   y|   z|
|   1| 0.23|    Ideal|    E|    SI2| 61.5|   55|  326|3.95|3.98|2.43|
|   2| 0.21|  Premium|    E|    SI1| 59.8|   61|  326|3.89|3.84|2.31|
|   3| 0.23|     Good|    E|    VS1| 56.9|   65|  327|4.05|4.07|2.31|
|   4| 0.29|  Premium|    I|    VS2| 62.4|   58|  334| 4.2|4.23|2.63|
|   5| 0.31|     Good|    J|    SI2| 63.3|   58|  335|4.34|4.35|2.75|
|   6| 0.24|Very Good|    J|   VVS2| 62.8|   57|  336|3.94|3.96|2.48|
|   7| 0.24|Very Good|    I|   VVS1| 62.3|   57|  336|3.95|3.98|2.47|
|   8| 0.26|Very Good|    H|    SI1| 61.9|   55|  337|4.07|4.11|2.53|
|   9| 0.22|     Fair|    E|    VS2| 65.1|   61|  337|3.87|3.78|2.49|
|  10| 0.23|Very Good|    H|    VS1| 59.4|   61|  338|   4|4.05|2.39|
|  11|  0.3|     Good|    J|    SI1|   64|   55|  339|4.25|4.28|2.73|
|  12| 0.23|    Ideal|    J|    VS1| 62.8|   56|  340|3.93| 3.9|2.46|
|  13| 0.22|  Premium|    F|    SI1| 60.4|   61|  342|3.88|3.84|2.33|
|  14| 0.31|    Ideal|    J|    SI2| 62.2|   54|  344|4.35|4.37|2.71|
|  15|  0.2|  Premium|    E|    SI2| 60.2|   62|  345|3.79|3.75|2.27|
|  16| 0.32|  Premium|    E|     I1| 60.9|   58|  345|4.38|4.42|2.68|
|  17|  0.3|    Ideal|    I|    SI2|   62|   54|  348|4.31|4.34|2.68|
|  18|  0.3|     Good|    J|    SI1| 63.4|   54|  351|4.23|4.29| 2.7|
|  19|  0.3|     Good|    J|    SI1| 63.8|   56|  351|4.23|4.26|2.71|
+----+-----+---------+-----+-------+-----+-----+-----+----+----+----+
```

## Lire un fichier CSV avec Spark en prenant en compte le header

```scala
val diamondsWithHeader = spark.read.option("header", "true").csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
diamondsWithHeader.show
```

```
+---+-----+---------+-----+-------+-----+-----+-----+----+----+----+
|_c0|carat|      cut|color|clarity|depth|table|price|   x|   y|   z|
+---+-----+---------+-----+-------+-----+-----+-----+----+----+----+
|  1| 0.23|    Ideal|    E|    SI2| 61.5|   55|  326|3.95|3.98|2.43|
|  2| 0.21|  Premium|    E|    SI1| 59.8|   61|  326|3.89|3.84|2.31|
|  3| 0.23|     Good|    E|    VS1| 56.9|   65|  327|4.05|4.07|2.31|
|  4| 0.29|  Premium|    I|    VS2| 62.4|   58|  334| 4.2|4.23|2.63|
|  5| 0.31|     Good|    J|    SI2| 63.3|   58|  335|4.34|4.35|2.75|
|  6| 0.24|Very Good|    J|   VVS2| 62.8|   57|  336|3.94|3.96|2.48|
|  7| 0.24|Very Good|    I|   VVS1| 62.3|   57|  336|3.95|3.98|2.47|
|  8| 0.26|Very Good|    H|    SI1| 61.9|   55|  337|4.07|4.11|2.53|
|  9| 0.22|     Fair|    E|    VS2| 65.1|   61|  337|3.87|3.78|2.49|
| 10| 0.23|Very Good|    H|    VS1| 59.4|   61|  338|   4|4.05|2.39|
| 11|  0.3|     Good|    J|    SI1|   64|   55|  339|4.25|4.28|2.73|
| 12| 0.23|    Ideal|    J|    VS1| 62.8|   56|  340|3.93| 3.9|2.46|
| 13| 0.22|  Premium|    F|    SI1| 60.4|   61|  342|3.88|3.84|2.33|
| 14| 0.31|    Ideal|    J|    SI2| 62.2|   54|  344|4.35|4.37|2.71|
| 15|  0.2|  Premium|    E|    SI2| 60.2|   62|  345|3.79|3.75|2.27|
| 16| 0.32|  Premium|    E|     I1| 60.9|   58|  345|4.38|4.42|2.68|
| 17|  0.3|    Ideal|    I|    SI2|   62|   54|  348|4.31|4.34|2.68|
| 18|  0.3|     Good|    J|    SI1| 63.4|   54|  351|4.23|4.29| 2.7|
| 19|  0.3|     Good|    J|    SI1| 63.8|   56|  351|4.23|4.26|2.71|
| 20|  0.3|Very Good|    J|    SI1| 62.7|   59|  351|4.21|4.27|2.66|
+---+-----+---------+-----+-------+-----+-----+-----+----+----+----+
```

## Imprimer le schéma de data

```scala
diamondsWithHeader.printSchema
```

```
root
 |-- _c0: string (nullable = true)
 |-- carat: string (nullable = true)
 |-- cut: string (nullable = true)
 |-- color: string (nullable = true)
 |-- clarity: string (nullable = true)
 |-- depth: string (nullable = true)
 |-- table: string (nullable = true)
 |-- price: string (nullable = true)
 |-- x: string (nullable = true)
 |-- y: string (nullable = true)
 |-- z: string (nullable = true)
```

## Inférer le schéma de data dans un fichier csv

```scala
val completeDiamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
completeDiamonds.printSchema
```

```
root
 |-- _c0: integer (nullable = true)
 |-- carat: double (nullable = true)
 |-- cut: string (nullable = true)
 |-- color: string (nullable = true)
 |-- clarity: string (nullable = true)
 |-- depth: double (nullable = true)
 |-- table: double (nullable = true)
 |-- price: integer (nullable = true)
 |-- x: double (nullable = true)
 |-- y: double (nullable = true)
 |-- z: double (nullable = true)
```

## Lire un fichier json

```scala
val people = spark.read.json("/FileStore/tables/people.json")
people.show
```

```
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```

## Créer une table dans Spark

```scala
people.createOrReplaceTempView("people")

spark.sql("""
select * from people
""").show
```

```
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
```

## Filtrer

```scala
val filteredPeople = spark.sql("""
select * from people where age > 20
""")

filteredPeople.show
```

```
+---+----+
|age|name|
+---+----+
| 30|Andy|
+---+----+
```

## Sélectionner
```scala
spark.sql("""
select name from people where age > 20
""").show
```

```
+----+
|name|
+----+
|Andy|
+----+
```

## Réaliser quelques statistiques

```scala
spark.sql("""
select max(age), min(age), round(avg(age))
from people
""").show
```

```
+--------+--------+------------------+
|max(age)|min(age)|round(avg(age), 0)|
+--------+--------+------------------+
|      30|      19|              25.0|
+--------+--------+------------------+
```

# Exemple API DataFrame

## Filtrer

```scala
people.
  filter("age > 20").
  show
```

```
+---+----+
|age|name|
+---+----+
| 30|Andy|
+---+----+
```

## Sélectionner

```scala
people.
  select("name").
  filter("age > 20").
  show
```

```
+----+
|name|
+----+
|Andy|
+----+
```

## Statistiques

```scala
import org.apache.spark.sql.functions._

people.
  agg(max("age"), min("age"), round(avg("age"), 2)).
  show
```

```
+--------+--------+------------------+
|max(age)|min(age)|round(avg(age), 2)|
+--------+--------+------------------+
|      30|      19|              24.5|
+--------+--------+------------------+
```

## Statistiques par rapport à une colonne

```scala
people.
  groupBy("name").
  agg(max("age"), min("age"), round(avg("age"))).
  show
```

```
+-------+--------+--------+------------------+
|   name|max(age)|min(age)|round(avg(age), 0)|
+-------+--------+--------+------------------+
|Michael|    null|    null|              null|
|   Andy|      30|      30|              30.0|
| Justin|      19|      19|              19.0|
+-------+--------+--------+------------------+
```

# Exemple dataset

## Lier une dataFrame à une case class

```scala
case class Personne(age: Option[Long], name: String)
val peoplesDS = people.as[Personne]
peoplesDS.show
```

```
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+

```

## Filtrer une dataset

```scala
val filteredPeople = peoplesDS.filter(people => {
  people.age.isDefined && people.age.get > 20
})

filteredPeople.show
```

## Sélectionner un champ

```scala
peoplesDS.map(_.age).show
```

```
+-----+
|value|
+-----+
| null|
|   30|
|   19|
+-----+
```

## Effectuer des statistiques

```scala
val peopleStatistics = peoplesDS.groupBy("name").agg(min("age").alias("minAge"), max("age").alias("maxAge"), round(avg("age")).alias("avgAge"))

case class PeopleStatistics(name: String, minAge: Long, maxAge: Long, avgAge: Double)
peopleStatistics.as[PeopleStatistics].show
```

```
+-------+------+------+------+
|   name|minAge|maxAge|avgAge|
+-------+------+------+------+
|Michael|  null|  null|  null|
|   Andy|    30|    30|  30.0|
| Justin|    19|    19|  19.0|
+-------+------+------+------+
```

# Exemple jointure

```scala
case class Personne(name: String, experience: Int)
case class Revenue(experience: Int, revenue: Float)

val personneDS = Seq(
Personne("Sarah", 3),
Personne("Selim", 2)
).toDS

val revenueDS = Seq(
Revenue(3, 40.6F),
Revenue(2, 56.7F)
).toDS

personneDS.join(revenueDS, personneDS.col("experience") === revenueDS.col("experience"), "inner").show
```

```
+-----+----------+----------+-------+
| name|experience|experience|revenue|
+-----+----------+----------+-------+
|Sarah|         3|         3|   40.6|
|Selim|         2|         2|   56.7|
+-----+----------+----------+-------+
```

# Exemple Machine Learning

```scala

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

case class Wine(rating: Int, price: Int)

val wines = Seq(
Wine(10, 100),
Wine(20, 200),
Wine(30, 300)
).toDS

val assembler = new VectorAssembler().
   setInputCols(Array("rating")).
   setOutputCol("features")

val linearRegression = new LinearRegression().setLabelCol("price")

val stages = Array(
 assembler,
 linearRegression
)

 //Construct the pipeline
val pipeline = new Pipeline().setStages(stages)

//We fit our DataFrame into the pipeline to generate a model
val model = pipeline.fit(wines)

val winesTest = Seq(
Wine(12, 120),
Wine(22, 250),
Wine(40, 400)
).toDS

val predictions = model.transform(winesTest)

predictions.show

```

```

+------+-----+--------+----------+
|rating|price|features|prediction|
+------+-----+--------+----------+
|    12|  120|  [12.0]|     120.0|
|    22|  250|  [22.0]|     220.0|
|    40|  400|  [40.0]|     400.0|
+------+-----+--------+----------+

```
