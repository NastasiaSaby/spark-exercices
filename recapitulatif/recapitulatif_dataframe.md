# Récapitulatif API DataFrame

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
