# 2. Prendre en main Scala

## Exercice 1
case class Dog(age: Int, name: String, color: String)

def printDog(dog: Dog) = {
    println("Le chien " + dog.name + " a " + dog.age + " ans et il est de couleur " + dog.color)
}

val hachiko = Dog(15, "Hachiko", "marron")

printDog(hachiko)

## Exercice 2

case class Dog(age: Int, name: String, color: String)

def printDog(dog: Dog) = {
    println("Le chien " + dog.name + " a " + dog.age + " ans et il est de couleur " + dog.color)
}

val hachiko = Dog(15, "Hachiko", "marron")
val medor = Dog(7, "Medor", "white")

val dogs = Seq(hachiko, medor)

dogs.foreach(dog => {
    printDog(dog)

# 3. Découverte des APIS haut niveau

## Spark SQL : l'API ouverte à tous

### Exercice 1

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.show
diamonds.printSchema

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT * FROM diamonds
    WHERE color != 'E'
""")

result.show

### Exercice 2

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT cut, clarity, depth
    FROM diamonds
    WHERE color != 'E'
""")

result.show

### Exercice 3

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT min(price) as minPrice, max(price) as maxPrice, round(avg(price), 2) as avgPrice
    FROM diamonds
""")

result.show

### Exercice 4

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT min(price) as minPrice, max(price) as maxPrice, round(avg(price), 2) as avgPrice, color
    FROM diamonds
    GROUP BY color
    ORDER By avgPrice
""")

result.show

### Exercice 5

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT min(price) as minPrice, max(price) as maxPrice, round(avg(price), 2) as avgPrice, carat
    FROM diamonds
    GROUP BY carat
    ORDER BY avgPrice
""")

result.show(1000)

Le carat a beaucoup plus d'influence que la couleur du diamant.

## Transformations et actions : le coeur de Spark

### Exercice 1

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

println(diamonds.count)

### Exercice 2

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT cut
    FROM diamonds """)

println(result.distinct.count)

## DataFrame : une deuxième API haut niveau

### Exercice 1
val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")
val result = diamonds.filter("color != 'E'")

result.show

### Exercice 2

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")
val result = diamonds.select("cut", "clarity", "depth")

result.show

### Exercice 3
val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

import org.apache.spark.sql.functions._

val result = diamonds.
    agg(max("price").as("maxPrice"), min("price").as("minPrice"), round(avg("price"), 2).as("avgPrice"))

result.show

### Exercice 4
val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

import org.apache.spark.sql.functions._

val result = diamonds.
    groupBy("color").
    agg(max("price").as("maxPrice"), min("price").as("minPrice"), round(avg("price"), 2).as("avgPrice")).
    orderBy("avgPrice")

result.show

## Dataset : une troisième API haut niveau

### Exercice 1
case class Diamond(_c0: Int, carat: Double, cut: String, color: String, clarity: String, depth: Double, table: Double, price: Int, x: Double, y: Double, z: Double)

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv").as[Diamond]

val result = diamonds.filter(diamond => {
    diamond.color != "E"
})

result.show

### Exercice 2

case class Diamond(_c0: Int, carat: Double, cut: String, color: String, clarity: String, depth: Double, table: Double, price: Int, x: Double, y: Double, z: Double)

case class DiamondSelection(cut: String, clarity: String, depth: Double)

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv").as[Diamond]

val result = diamonds.map(diamond => {
    DiamondSelection(diamond.cut, diamond.clarity, diamond.depth)
})

result.show

### Exercice 3

case class Diamond(_c0: Int, carat: Double, cut: String, color: String, clarity: String, depth: Double, table: Double, price: Int, x: Double, y: Double, z: Double)

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv").as[Diamond]

import org.apache.spark.sql.functions._

val result = diamonds.
    agg(max("price").as("maxPrice"), min("price").as("minPrice"), round(avg("price"), 2).as("avgPrice"))

result.show

### Exercice 4

case class Diamond(_c0: Int, carat: Double, cut: String, color: String, clarity: String, depth: Double, table: Double, price: Int, x: Double, y: Double, z: Double)

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv").as[Diamond]

import org.apache.spark.sql.functions._

val result = diamonds.
    groupBy("color").
    agg(max("price").as("maxPrice"), min("price").as("minPrice"), round(avg("price"), 2).as("avgPrice")).
    orderBy("avgPrice")

result.show

# 4. Manipulation de données plus avancées

## Créer des dataframes

### Exercice 1

val characters = Seq(
    ("Batman", "Super Héros", 32),
    ("Le Pingouin", "Super Méchant", 45),
    ("Catwoman", "Difficile à dire", 32)
).toDF("name", "type", "age")

characters.show

### Exercice 2

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val rawCharacters = Seq(
    Row("Batman", "Super Héros", 32),
    Row("Le Pingouin", "Super Méchant", 45),
    Row("Catwoman", "Difficile à dire", 32)
)

val schema = Seq(
  StructField("name", StringType, true),
  StructField("type", StringType, true),
  StructField("age", IntegerType, true)
)

val characters = spark.createDataFrame(
  spark.sparkContext.parallelize(rawCharacters),
  StructType(schema)
)

characters.show

## Créer des datasets

### Exercice 1

case class Character(name: String, characterType: String, age: Integer)

val characters = Seq(
    Character("Batman", "Super Héros", 32),
    Character("Le Pingouin", "Super Méchant", 45),
    Character("Catwoman", "Difficile à dire", 32)
).toDS

characters.show

### Exercice 2

case class Character(name: String, characterType: String, age: Integer)

val characters = spark.emptyDataset[Character]

characters.show

## Joindre les données

### Exercice 1

case class Customer(id: Int, firstName: String)
case class Order(productId: Int, customerId: Int)
case class Product(id: Int, name: String, price: Float)

val customers = Seq(
    Customer(1, "Sophie"),
    Customer(2, "Julien"),
    Customer(3, "Sarah"),
    Customer(4, "Irina"),
    Customer(5, "Renzo")
).toDS

customers.show

val orders = Seq(
    Order(1, 3),
    Order(2, 4),
    Order(4, 1)
).toDS

orders.show

val products = Seq(
    Product(1, "Lego", 230.70F),
    Product(2, "Dixit", 45.60F),
    Product(3, "Batman figurine", 19.6F),
    Product(4, "Livre de coloriage", 3.5F)
).toDS

products.show

### Exercice 2

customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")

val result = spark.sql("""
    SELECT firstName, name
    FROM customers
    JOIN orders ON customers.id == orders.customerId
    JOIN products ON products.id == orders.productId
    WHERE name = "Livre de coloriage"
""")

result.show

### Exercice 3

val allJoined = customers.
                    join(orders, customers.col("id") === orders.col("customerId"), "inner").
                    join(products, orders.col("productId") === products.col("id"), "inner")

val drawingBookCustomer = allJoined.
                            filter("name == 'Livre de coloriage'").
                            select("firstName", "name")

drawingBookCustomer.show

### Exercice 4

customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")
products.createOrReplaceTempView("products")

val result = spark.sql("""
    SELECT firstName, name, price
    FROM customers
    JOIN orders ON customers.id == orders.customerId
    JOIN products ON products.id == orders.productId
    WHERE price > 200
""")

result.show


### Exercice 5

val allJoined = customers.
                    join(orders, customers.col("id") === orders.col("customerId"), "inner").
                    join(products, orders.col("productId") === products.col("id"), "inner")

val customerBuyingProductWithHighCost = allJoined.
                            filter("price > 200").
                            select("price", "firstName", "name")

customerBuyingProductWithHighCost.show

## Ajouter une colonne

### Exercice

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

spark.sql("""
    SELECT *, upper(cut) as upperCut
    FROM diamonds
""").show

import org.apache.spark.sql.functions.upper

diamonds.withColumn("upperCut", upper(col("cut"))).show

## Renommer une colonne

### Exercice

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

val result = diamonds.withColumnRenamed("cut", "renamedCut")

result.show
result.printSchema

## Supprimer une colonne

### Exercice

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

val diamondsWithoutCut = diamonds.drop("cut")

diamondsWithoutCut.show
diamondsWithoutCut.printSchema

# 5. Petit récapitulatif

### Exercice 1

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")
diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
    SELECT COUNT(DISTINCT color) as nbColor, cut
    FROM diamonds
    GROUP BY cut
""")

result.show

import org.apache.spark.sql.functions._

val result = diamonds.groupBy("cut").agg(countDistinct("color").as("nbColor"))

result.show

### Exercice 2

val teaConsumers = Seq(
    ("Marie", "thé vert", "France"),
    ("Andrea", "tisane canelle", "Italie"),
    ("Yijia", "thé oolong", "Chine"),
    ("Pi-Yuan", "thé rose", "Chine"),
    ("Ao", "thé litchi", "Chine"),
    ("Elena", "thé noir", "UK"),
    ("Cory", "thé earl grey", "UK")
   ).toDF("consumer", "product", "country")

teaConsumers.createOrReplaceTempView("teaConsumers")

val result = spark.sql("""
    SELECT count(distinct product) as nbProduct, country
    FROM
    teaConsumers
    GROUP BY country
    ORDER BY nbProduct DESC
    LIMIT 1
""")

result.show

import org.apache.spark.sql.functions._

val result = teaConsumers.groupBy("country").agg(countDistinct("product").as("nbProduct")).orderBy(desc("nbProduct")).limit(1)

result.show


### Exercice 3

val employees = Seq(
    ("Johnathan", "Développeur", "WEB"),
    ("Lou", "Développeur", "WEB"),
    ("Solène", "PO", "DATA"),
    ("Selim", "Développeur", "DATA"),
    ("Soraya", "Développeur", "WEB")
).toDF("name", "job", "department")

val departments = Seq(
    ("WEB", "S'occupe du site web coorporate"),
    ("DATA", "S'occupe des grosses données")
).toDF("department", "description")

employees.createOrReplaceTempView("employees")
departments.createOrReplaceTempView("departments")

val result = spark.sql("""
    SELECT name, job, lower(employees.department) as department, description
    FROM employees
    JOIN departments ON departments.department = employees.department
""")

result.show

val employeesWithDepartments = employees.join(departments, Seq("department"), "inner")

import org.apache.spark.sql.functions._

val result = employeesWithDepartments.select($"name", lower($"department").alias("department"), $"description")

result.show

### Exercice 4 : Cas de calcul de résultats d'AB Testing

val viewsByUser = Seq(
    ("user1","blue",2),
    ("user2","green",5),
    ("user1","blue",3),
    ("user1","blue",1),
    ("user3","blue",1)
).toDF("user", "color", "view")

viewsByUser.createOrReplaceTempView("viewsByUser")

val resultColorAndNbView = spark.sql("""
    SELECT SUM(view) as nbView, color
    FROM viewsByUser
    GROUP BY color
    ORDER BY nbView DESC
    LIMIT 1
""")

resultColorAndNbView.createOrReplaceTempView("resultColorAndNbView")

val result = spark.sql("""
    SELECT color as winner FROM resultColorAndNbView
""")

result.show

import org.apache.spark.sql.functions._

val resultColorAndNbView = viewsByUser.groupBy("color").agg(sum("view").as("nbView")).orderBy(desc("nbView")).limit(1)

val result = resultColorAndNbView.select("color")

result.show

# 6. UDF (User Defined Function)

## Exercice

val groupCutAndColor: (String, String) => String = (cut, color) => cut(0) + color

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

import org.apache.spark.sql.functions.udf
val groupCutAndColorUDF = udf(groupCutAndColor)

val result = diamonds.withColumn("cutColor", groupCutAndColorUDF(col("cut"), col("color")))

result.show

# 7. Les Window functions

## Principes de base

### Exercice 1

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
SELECT avg(price) OVER(PARTITION BY color) as avgPrice, *
FROM diamonds
""")

result.show(10000)

import org.apache.spark.sql.expressions.Window
val byColor = Window.partitionBy("color")

val result = diamonds.withColumn("avgPrice", avg("price") over byColor)

result.show(10000)

### Exercice 2

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
SELECT
max(price) OVER(PARTITION BY carat) as maxPrice,
min(price) OVER(PARTITION BY carat) as minPrice,
*
FROM diamonds
""")

result.show(100)

import org.apache.spark.sql.expressions.Window
val byCarat = Window.partitionBy("carat")

val result = diamonds.
                withColumn("maxPrice", max("price") over byCarat).
                withColumn("minPrice", min("price") over byCarat)

result.show(100)

## Utiliser les window functions pour faire du ranking

### Exercice 1

val diamonds = spark.read.option("header", "true").option("inferSchema", "true").csv("diamonds.csv")

diamonds.createOrReplaceTempView("diamonds")

val result = spark.sql("""
SELECT
rank() OVER(PARTITION BY color ORDER BY price DESC) as maxPriceByColor,
*
FROM diamonds
HAVING maxPriceByColor = 1
""")

result.show

import org.apache.spark.sql.expressions.Window

val maxPriceByColor = Window.partitionBy("color").orderBy(desc("price"))

val diamondsWithMaxPriceByColor = diamonds.withColumn("maxPriceByColor", rank() over maxPriceByColor)

val result = diamondsWithMaxPriceByColor.filter("maxPriceByColor == 1")

result.show


### Exercice 2

val sails = Seq(
("Thin", "Cell phone", 6000),
("Normal", "Tablet", 1500),
("Mini", "Tablet", 5500),
("Ultra Thin", "Cell phone", 6000),
("Very Thin", "Cell phone", 5000),
("Big", "Tablet", 2500),
("Pro", "Tablet", 4500)
).toDF("name", "category", "totalAmount")

sails.createOrReplaceTempView("sails")

val result = spark.sql("""
SELECT
rank() OVER (PARTITION BY category ORDER BY totalAmount DESC) as totalAmountByCategory,
*
FROM sails
HAVING totalAmountByCategory = 1
""")

result.show

import org.apache.spark.sql.expressions.Window

val totalAmountByCategory = Window.partitionBy("category").orderBy(desc("totalAmount"))

val sailsWithTotalAmountByCategory = sails.withColumn("totalAmountByCategory", rank() over totalAmountByCategory)

val result = sailsWithTotalAmountByCategory.filter("totalAmountByCategory == 1")

result.show
