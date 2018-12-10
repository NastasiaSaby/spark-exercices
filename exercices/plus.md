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

wget -O zoo.csv https://archive.ics.uci.edu/ml/machine-learning-databases/zoo/zoo.data

Les mettre dans HDFS.

Dans Spark, créer un schéma avec : 
1. animal name: string 
2. hair: integer 
3. feathers: integer 
4. eggs: integer 
5. milk: integer 
6. airborne: integer 
7. aquatic:	integer 
8. predator: integer 
9. toothed:	integer 
10. backbone: integer 
11. breathes: integer 
12. venomous: integer 
13. fins: integer 
14. legs: integer
15. tail: integer 
16. domestic: integer 
17. catsize: integer 
18. type: integer

Et définir combien d'animaux sont des animaux aquatiques et prédateur qui pondent des œufs.
