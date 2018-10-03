# Récapitulatif dataset

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

