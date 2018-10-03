# Récpatulatif jointure

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
