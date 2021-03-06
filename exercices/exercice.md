# Spark SQL

## Exercice 1
Data Diamonds : /databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv
Après avoir loadé les diamants et dans l'API Spark SQL, on veut :
1. Filtrer en excluant la couleur E
2. Calculer le prix minimum, le prix maximum, le prix moyen en arrondissant à 2 après la virgule pour l'ensemble des diamants
3. Calculer le prix minimum, le prix maximum, le prix moyen en arrondissant à 2 après la virgule par couleur

# Actions

## Exercice 2
1. Enregistrer son dernier résultat (Exercice 1.3)
2. On veut savoir le nombre de diamants
3. On veut savoir le nombre distint de diamant

# DataFrame

## Exercice 3
Après avoir loadé les diamants et dans l'API DataFrame, on veut :
1. Filtrer en excluant la couleur E
2. Calculer le prix minimum, le prix maximum, le prix moyen en arrondissant à 2 après la virgule pour l'ensemble des diamants
3. Calculer le prix minimum, le prix maximum, le prix moyen en arrondissant à 2 après la virgule par couleur

# Dataset

## Exercice 4
Aide : case class Diamond(_c0: Int, carat: Double, cut: String, color: String, clarity: String, depth: Double, table: Double, price: Int, x: Double, y: Double, z: Double)
Après avoir loadé les diamants et dans l'API Dataset, on veut :
1. Filtrer en excluant la couleur E
2. Calculer le prix minimum, le prix maximum, le prix moyen en arrondissant à 2 après la virgule pour l'ensemble des diamants
3. Calculer le prix minimum, le prix maximum, le prix moyen en arrondissant à 2 après la virgule par couleur
4. Remapper la sortie dans une autre case class

# Jointures et manipulation de colonnes

## Exercice 5
1. Créer trois datasets pour correspondre aux case class suivantes :

```scala
case class Customer(id: Int, firstName: String)
case class Order(productId: Int, customerId: Int)
case class Product(id: Int, name: String, price: Float)
```

Les valeurs pour les datasets sont les suivantes :
Customer :

```
1, "Sophie"
2, "Julien"
3, "Sarah"
4, "Irina"
5, "Renzo"
```

Order :

```
1, 3
2, 4,
4, 1
```

Product :

```
1, "Lego", 230.70F
2, "Dixit", 45.60F
3, "Batman figurine", 19.6F,
4, "Livre de coloriage", 3.5F
```

2. On veut savoir qui a acheté un livre de coloriage (nom du customer, nom du produit)
3. On veut savoir qui achète des produits valant plus de 200 euros (nom du customer, nom du produit, prix)

## Exercice 6

Au résultat de l'exercice 6.3, on souhaite ajouter une colonne (du nom que vous voulez) pour avoir le prix arrondi à l'unité

## Exercice 7

Au résultat de l'exercice 6.3, renommer une colonne (celle de votre choix)

## Exercice 8

Au résultat de l'exercice 6.3, Supprimer la colonne prix (celui qui n'est pas arrondi)
