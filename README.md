# Spark — Transformations, Actions et Jointures

Ce document explique de manière claire et pratique **les transformations**, **les actions** et **les jointures** dans Apache Spark (PySpark & Scala). Il est pensé pour être placé dans un dépôt GitHub comme guide de référence pour les développeurs et data engineers.

---

## Table des matières

1. Introduction
2. Transformations vs Actions — concept
3. Transformations courantes (exemples PySpark + Scala)
4. Actions courantes (exemples PySpark + Scala)
5. Jointures (joins) — types et usages
6. Bonnes pratiques & performance
7. Tests et validation
8. Exemple de pipeline complet
9. Ressources & références

---

## 1. Introduction

Apache Spark est un moteur distribué pour le traitement de données à grande échelle. Sa programmation repose principalement sur deux types d'opérations sur les RDD/DataFrame/Dataset : **transformations** (lazy) et **actions** (évaluées).

Ce guide illustre ces concepts, propose des exemples de code réutilisables et donne des conseils pour optimiser les performances, surtout lors d'opérations de jointure.

---

## 2. Transformations vs Actions — concept

* **Transformation** : opère sur un DataFrame/RDD et retourne un nouvel objet DataFrame/RDD. **Lazy** — elles ne sont pas exécutées immédiatement, elles construisent un plan d'exécution (DAG).

  * Exemples : `map`, `filter`, `select`, `withColumn`, `groupBy`, `join`, `flatMap`.

* **Action** : force l'exécution du DAG et retourne un résultat au driver ou écrit des données sur le stockage.

  * Exemples : `collect`, `count`, `show`, `write`, `take`, `save`.

Pourquoi c'est important : comprendre la distinction permet d'éviter des aller-retours coûteux entre driver et executors et de composer les opérations pour réduire le coût global.

---

## 3. Transformations courantes (avec exemples)

### PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("exemple").getOrCreate()
df = spark.read.csv("/data/input.csv", header=True, inferSchema=True)

# select + filter
df2 = df.select("id", "age", "salary").filter(col("age") > 30)

# withColumn (nouvelle colonne)
df3 = df2.withColumn("salary_k", col("salary") / 1000)

# groupBy + agg
from pyspark.sql.functions import avg
res = df3.groupBy("age").agg(avg("salary_k").alias("avg_salary_k"))
```

### Scala (Spark SQL/Dataset API)

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("exemple").getOrCreate()
val df = spark.read.option("header", "true").csv("/data/input.csv")

val df2 = df.select("id","age","salary").filter(col("age") > 30)
val df3 = df2.withColumn("salary_k", col("salary") / 1000)
val res = df3.groupBy("age").agg(avg("salary_k").alias("avg_salary_k"))
```

---

## 4. Actions courantes (avec exemples)

* `show()` : affiche des lignes pour debug
* `collect()` : ramène toutes les lignes au driver (danger si dataset volumineux)
* `count()` : retourne le nombre de lignes
* `write` : écrit sur disque (parquet/csv/etc.)

```python
# action: show
res.show(10)

# action: count
n = res.count()

# action: write
res.write.mode("overwrite").parquet("/data/output/avg_salary_by_age")
```

Conseil : évitez `collect()` sur de grands ensembles ; préférez `limit` + `collect` pour des aperçus.

---

## 5. Jointures (joins) — types et usages

### Types de jointures

* `inner` (intersection)
* `left` / `left_outer` (garde toutes les lignes du left)
* `right` / `right_outer` (garde toutes les lignes du right)
* `full_outer` (union complète)
* `left_semi` (garde les lignes du left qui ont correspondance dans right, sans ramener les colonnes du right)
* `left_anti` (garde les lignes du left qui **n'ont pas** correspondance dans right)
* `cross` (produit cartésien)

### Exemples PySpark

```python
# dataframes dfA et dfB
joined = dfA.join(dfB, dfA["id"] == dfB["id"], how="inner")

# project columns (éviter collisions de noms)
joined = joined.select(dfA["id"], dfA["name"], dfB["score"])

# left anti example
only_in_A = dfA.join(dfB, "id", how="left_anti")
```

### Astuce sur les clés et noms de colonnes

* Renommez les colonnes si nécessaire avant la jointure pour éviter collisions : `dfB = dfB.withColumnRenamed("id","id_b")`.

### Stratégies de performance pour les joins

1. **Broadcast join** : broadcastez le petit DataFrame pour éviter le shuffle massif.

   * PySpark : `from pyspark.sql.functions import broadcast` puis `dfA.join(broadcast(dfB), "id")`.
2. **Partitionnement** : repartitionner les DataFrames sur la clé de jointure (`repartition("id")`) pour réduire le shuffle réseau.
3. **Filtrage précoce** : appliquez `filter`/`select` avant la jointure pour réduire la taille des données à joindre.
4. **Utiliser `left_semi`/`left_anti` pour filtrer** quand on n'a pas besoin d'apporter les colonnes du second dataset.
5. **Eviter les `cartesian` joins** sauf si nécessaire.

### Exemple Broadcast

```python
from pyspark.sql.functions import broadcast
big = spark.read.parquet("/data/big")
small = spark.read.parquet("/data/small")

joined = big.join(broadcast(small), "user_id")
```

---

## 6. Bonnes pratiques & performance

* **Empire laziness** : composez vos transformations avant d'appeler une action.
* **Evitez collect()** : utilisez `show`, `take`, `limit` pour l'exploration.
* **Format columnar** : préférez Parquet/ORC pour stockage et I/O efficaces.
* **Cache judicieusement** : `df.cache()` quand on réutilise un DataFrame plusieurs fois. Appelez `unpersist()` quand ce n'est plus nécessaire.
* **Gestion des partitions** : surveillez la taille (recommandation 128MB - 1GB par partition selon cluster). Utilisez `repartition` ou `coalesce` selon le besoin.
* **Skew** : pour des clés très populaires, pensez à salting (ajouter prefix random sur clé) ou à techniques de pré-agrégation.
* **Monitoring** : utilisez l'UI Spark (port 4040) pour observer le DAG, les tasks, les stades et les shuffles.

---

## 7. Tests et validation

* Intégrez des tests unitaires avec `pytest` + `pyspark.sql.SparkSession.builder.master("local[2]")` pour PySpark.
* Validez les résultats (counts, checksums, échantillons) et surveillez les transformations non désirées (NULLs, duplicates).

Exemple simple de test PySpark (pytest):

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_filter(spark):
    df = spark.createDataFrame([(1, 10), (2, 40)], ["id","age"])
    res = df.filter(df.age > 20).collect()
    assert len(res) == 1
```

---

## 8. Exemple de pipeline complet (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col

spark = SparkSession.builder.appName("pipeline_exemple").getOrCreate()

# lecture
users = spark.read.parquet("/data/users")
transactions = spark.read.parquet("/data/transactions")

# transformation: filtrage + colonnes utiles
users_small = users.select("user_id", "country").filter(col("country") == "TG")
transactions_small = transactions.select("user_id", "amount", "ts").filter(col("amount") > 0)

# broadcast join (si users_small est petit)
joined = transactions_small.join(broadcast(users_small), on="user_id", how="inner")

# agrégation
from pyspark.sql.functions import sum as _sum
res = joined.groupBy("user_id").agg(_sum("amount").alias("total_amount"))

# write
res.write.mode("overwrite").parquet("/data/output/total_by_user")
```

---

## 9. Ressources & références

* Documentation officielle Spark : [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/)
* Guide PySpark : [https://spark.apache.org/docs/latest/api/python/](https://spark.apache.org/docs/latest/api/python/)
* Articles sur le Broadcast Join, Salting, repartitioning (rechercher selon version Spark)

---

## Licence

Ce document est publié sous licence MIT — vous pouvez le copier et l'adapter pour votre dépôt GitHub.

---

### Remarques finales

* Ajoute dans le dépôt : exemples `notebooks/`, `tests/` et `examples/` pour rendre le guide immédiatement exploitable.
* Si tu veux, je peux aussi générer des notebooks Jupyter PySpark prêts à l'emploi ou une version en anglais.
