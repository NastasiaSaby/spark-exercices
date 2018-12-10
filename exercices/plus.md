# Exercice 1

- Récupérer les données de "customers" en CSV avec Sqoop
- Avec Spark, trouver quel est l'Etat qui consomme le plus* et enregistrer le résultat en parquet
- Créer une table Hive avec le résultat de ces données

*Dans Spark, vous devez ajouter un schéma aux données entrantes pour avoir des noms autres que "_c1",...

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

# Exercice 2

- Importer categories et departments en parquet avec Sqoop
- Les joindre de manière à n'avoir que "department_name" et "category_name" côte à côte avec Spark
- Enregistrer en parquet
- Créer une table Hive dessus

# Exercice 3

Récupérer ces data (CSV)  :

https://archive.ics.uci.edu/ml/machine-learning-databases/zoo/zoo.data

Les mettre dans HDFS.

Créer un schéma avec : 
1. animal name: string 
2. hair: boolean 
3. feathers: boolean 
4. eggs: boolean 
5. milk: boolean 
6. airborne: boolean 
7. aquatic:	Boolean 
8. predator: boolean 
9. toothed:	boolean 
10. backbone: boolean 
11. breathes: boolean 
12. venomous: boolean 
13. fins: boolean 
14. legs: Numeric (set of values: {0,2,4,5,6,8}) 
15. tail: boolean 
16. domestic: boolean 
17. catsize: boolean 
18. type: Numeric (integer values in range [1,7])

Et définir quel animal non aquatique pond des œufs.
