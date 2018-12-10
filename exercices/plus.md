# Exercice 1

- Récupérer les données de "customers" en CSV avec Sqoop
- Avec Spark, trouver quel est l'Etat qui consomme le plus*
- Créer une table Hive avec le résultat de ces données

* Dans ce Spark, vous devez ajouter un schéma aux données entrantes pour avoir des noms autres que "_c1",...

```scala

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val schema = StructType(
      Array(
          StructField("inputType",StringType,true), 
          StructField("originalRating",IntegerType,true), 
          StructField("processed",BooleanType,true), 
          StructField("rating",LongType,true), 
          StructField("score",DoubleType,true), 
          StructField("methodId",StringType,true)
      )
  )
            
val  records = sqlContext.read.schema(schema).json("testData")

```
