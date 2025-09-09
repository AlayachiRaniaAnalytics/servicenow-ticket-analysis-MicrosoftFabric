#!/usr/bin/env python
# coding: utf-8

# ## From Bronze to Silver
# 
# New notebook

# ## Documentation du Notebook : Transformation des données de Bronze à Silver
# ###  Contexte
# ###### Ce notebook s'inscrit dans une architecture Medallion mise en place dans Microsoft Fabric, et assure la transformation des données brutes (bronze) vers un format nettoyé et structuré (silver), en préparation pour leur exploitation ultérieure (gold / rapport Power BI).
# 
# 

# ### 📦 Importation des bibliothèques nécessaires
# 
# Dans cette section, nous importons l'ensemble des bibliothèques requises pour effectuer les traitements de données entre les couches Bronze et Silver :
# 
# - `pyspark.sql.types` : permet de définir manuellement les schémas des DataFrames (types de colonnes comme `StringType`, `TimestampType`, etc.).
# - `delta.tables` : fournit les outils pour manipuler les tables Delta (création, mise à jour, merge…), indispensables pour assurer la fiabilité, la traçabilité et la gestion des versions dans les architectures Lakehouse.
# - `pyspark.sql` : utilisé pour interagir avec l’environnement Spark, créer des sessions ou manipuler des structures de données.
# - `pyspark.sql.functions` : contient les fonctions utiles pour transformer et enrichir les données (`col`, `when`, `lit`, `date_format`, etc.).
# 
# Ces bibliothèques sont essentielles pour la lecture des données, leur transformation et leur écriture dans les différentes couches du Lakehouse (Silver, Gold).
# 

# In[7]:


from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import *
from pyspark.sql.functions import *


# ### 🚀 Initialisation de la session Spark
# 
# Avant d’effectuer toute opération sur les données, il est nécessaire d’initialiser une session Spark.  
# Cette session constitue le point d’entrée principal pour interagir avec Spark, lire des fichiers, transformer des données et manipuler des tables Delta.
# 
# Dans cet exemple, nous nommons l'application `LakehouseData` pour faciliter son identification dans l'interface Spark UI.
# 

# In[8]:


# Initialize Spark Session
spark = SparkSession.builder.appName("LakehouseData").getOrCreate()


# ### 🧱 Définition du schéma pour les incidents de mise en production
# 
# Dans cette étape, nous définissons manuellement le schéma (`StructType`) des incidents liés à la **mise en production**.  
# Cela permet de structurer correctement les données brutes (couche Bronze) et d'éviter que Spark infère automatiquement les types de colonnes, ce qui pourrait générer des erreurs ou des types non cohérents.
# 
# Le schéma contient les colonnes suivantes :
# 
# - `Numéro`, `Société`, `Priorité`, `État`, `Catégorie`, etc. → informations descriptives des incidents (type `StringType`).
# - `Ouvert`, `Mis à jour` → dates importantes de suivi des incidents (type `DateType`).
# - `Description brève`, `Groupe d'affectation`, `Ouvert par` → éléments utiles au diagnostic et au suivi opérationnel.
# - `N° de ticket externe` → lien éventuel avec d'autres outils de supervision.
# - `Ovrt_h`, `Mis_h` → champs horaires spécifiques, utiles pour calculer la durée ou effectuer des analyses HO/HNO.
# 
# ➡️ Ce schéma est essentiel pour assurer un chargement structuré et fiable des données avant leur transformation dans la couche Silver.
# 

# In[9]:


from pyspark.sql.types import StructType, StructField, StringType

# Schéma pour la table Incident
Incdtexplt_schema = StructType([
    StructField("Numéro", StringType(), True),
    StructField("Ouvert", DateType(), True),
    StructField("Mis à jour", DateType(), True),
    StructField("Société", StringType(), True),
    StructField("Priorité", StringType(), True),
    StructField("Description brève", StringType(), True),
    StructField("Groupe d'affectation", StringType(), True),
    StructField("État", StringType(), True),
    StructField("Ouvert par", StringType(), True),
    StructField("Catégorie", StringType(), True),
    StructField("N° de ticket externe", StringType(), True),
    StructField("Ovrt_h", StringType(), True),
    StructField("Mis_h", StringType(), True)
])


# ### 📥 Chargement des données brutes depuis la couche Bronze (extrait ServiceNow)
# 
# Nous chargeons ici le fichier `incident_modified.csv` situé dans la couche `raw_bronze` du Lakehouse, via un chemin ABFS (Azure Blob File System).  
# Ce fichier est un **extrait exporté depuis ServiceNow**, contenant les incidents liés aux mises en production, sous format CSV.
# 
# 🔍 Points clés :
# - Le format de lecture est `csv`.
# - L’option `header` permet de prendre la première ligne comme en-têtes de colonnes.
# - Le séparateur est `,` (virgule).
# - Le schéma `Incdtexplt_schema` est utilisé pour imposer la structure correcte dès l’ingestion, sans inférence automatique.
# 
# Ce fichier brut constitue la **source initiale** des données avant traitement : il sera transformé, nettoyé et structuré dans la couche Silver afin d’être exploité dans les analyses et tableaux de bord.
# 

# In[10]:


# Chargement du fichier incident.csv depuis le bronze Lakehouse (ABFS path)
df_br = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(Incdtexplt_schema) \
    .load("abfss://e979a68f-0c82-481e-9cfa-3a24201ddb86@onelake.dfs.fabric.microsoft.com/69ba2d01-2315-4a5e-9ba4-67be20fe4a65/Files/raw/MEP.csv")


# ### 🧪 Vérification de la structure du DataFrame
# 
# Après le chargement des données brutes, nous utilisons la commande `printSchema()` pour afficher la structure (types et noms des colonnes) du DataFrame `df_br`.
# 
# Cela permet de :
# 
# - Valider que le schéma appliqué (`Incdtexplt_schema`) a bien été pris en compte par Spark.
# - Vérifier qu’aucune colonne ne présente de type incorrect (par exemple une date lue comme une chaîne).
# - Faciliter les étapes de transformation à venir en ayant une vision claire de la structure des données.
# 
# Cette vérification est cruciale dans une démarche de fiabilité des pipelines de traitement.
# 

# In[11]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_br.printSchema()


# ### ✏️ Nettoyage des noms de colonnes : suppression des espaces, accents et caractères spéciaux
# 
# À cette étape, nous renommons les colonnes du DataFrame `df_br` afin de garantir leur compatibilité avec les traitements ultérieurs (requêtes SQL, stockage Delta, dashboards, etc.).
# 
# Pourquoi faire cela ?
# 
# - 🔤 **Enlever les accents** : certains moteurs ou outils ne les gèrent pas correctement.
# - 🚫 **Supprimer les espaces et caractères spéciaux** : cela évite des erreurs lors de l'accès aux colonnes, notamment dans les requêtes SQL ou lors des jointures.
# - 🧼 **Uniformiser le style** : utiliser un format clair, lisible et cohérent (souvent `snake_case` ou `camelCase` sans caractères spéciaux).
# 
# Par exemple :
# - `"Mis à jour"` devient `"Mis_a_jour"`
# - `"Groupe d'affectation"` devient `"Groupe_affectation"`
# - `"N° de ticket externe"` devient `"Numero_ticket_externe"`
# 
# ⚠️ Ce renommage est essentiel pour assurer la robustesse et la portabilité du pipeline dans les couches Silver et Gold.
# 

# In[12]:


# Étape 3 : Renommer les colonnes pour enlever les espaces, caractères spéciaux, accents
df_br = df_br.withColumnRenamed("Numéro", "Numero") \
       .withColumnRenamed("Ouvert", "Ouvert") \
       .withColumnRenamed("Mis à jour", "Mis_a_jour") \
       .withColumnRenamed("Société", "Societe") \
       .withColumnRenamed("Priorité", "Priorite") \
       .withColumnRenamed("Description brève", "Description_breve") \
       .withColumnRenamed("Groupe d'affectation", "Groupe_affectation") \
       .withColumnRenamed("État", "Etat") \
       .withColumnRenamed("Ouvert par", "Ouvert_par") \
       .withColumnRenamed("Catégorie", "Categorie") \
       .withColumnRenamed("Description", "Description") \
       .withColumnRenamed("N° de ticket externe", "Numero_ticket_externe")\
       .withColumnRenamed("Ovrt_h", "Ovrt_H") \
       .withColumnRenamed("Mis_h", "Mis_H") 


# ### 🔍 Vérification de la structure après renommage des colonnes
# 
# Nous utilisons `df_br.printSchema()` une seconde fois pour **vérifier que le renommage des colonnes a bien été appliqué**.
# 
# Cette étape permet de :
# 
# - ✅ S'assurer que **tous les noms de colonnes sont désormais propres** : sans accents, sans espaces, ni caractères spéciaux.
# - 🧩 Confirmer que le **schéma reste conforme** avec les types de données attendus (`StringType`, `DateType`, etc.).
# - 🛠️ Préparer sereinement les prochaines étapes de transformation dans la couche Silver, en évitant tout risque d’erreur lié à un nom de colonne mal formaté.
# 
# C'est une **bonne pratique systématique** après toute opération de modification de structure sur un DataFrame.
# 

# In[13]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_br.printSchema()


# In[14]:


# Affichage des 10 premières lignes
df_br.show(truncate=False)  # Par défaut coupe à 20 caractères


# ### 🕒 Gestion de la compatibilité des dates : activation du mode LEGACY
# 
# Cette configuration permet d'éviter des erreurs lors du **parsing (interprétation)** de dates dans Spark, notamment lorsque les données d’entrée contiennent :
# 
# - des **formats de dates non standards** (ex : dates mal formatées dans un export CSV),
# - ou des dates qui ne respectent pas strictement les règles du format ISO.
# 
# 👉 En définissant la propriété `spark.sql.legacy.timeParserPolicy` à `"LEGACY"`, on active un **mode de compatibilité** avec les anciennes versions de Spark, plus tolérant vis-à-vis des formats de dates hétérogènes.
# 
# Cela permet :
# 
# - d’éviter des erreurs de type `DateTimeParseException`,
# - de **continuer le traitement sans blocage** même si certaines valeurs ne sont pas parfaitement formatées,
# - et d'effectuer un nettoyage ou une normalisation dans une étape ultérieure, si nécessaire.

# In[15]:


spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


# ### 🧹 Suppression des colonnes non nécessaires à l’analyse
# 
# Dans cette étape, nous supprimons plusieurs colonnes du DataFrame `df_br` à l’aide de la méthode `drop()`.  
# Ces colonnes, bien qu’existantes dans la donnée brute, ne sont **pas nécessaires pour les traitements ou analyses** que nous souhaitons effectuer dans la couche Silver.
# 
# Colonnes supprimées :
# - `Priorite`  
# - `Societe`  
# - `Groupe_affectation`  
# - `Numero_ticket_externe`  
# - `Categorie`  
# 
# Pourquoi cette suppression ?
# - 🔎 Alléger le jeu de données pour améliorer les performances.
# - 🧠 Se concentrer uniquement sur les colonnes utiles pour les KPI ou les analyses métiers ciblées.
# - 📊 Préparer un schéma plus simple et exploitable pour la création de la table Silver.
# 
# La commande `df_br.show(30)` permet ensuite de **visualiser les premières lignes** du DataFrame et de **vérifier que les colonnes ont bien été supprimées.**
# 

# In[16]:


# Suppression des colonnes non nécessaires à l’analyse
df_br = df_br.drop("Priorite", "Societe", "Groupe_affectation", "Numero_ticket_externe", "Categorie")
# Affichage des 30 premières lignes
df_br.show(30)


# ### ❓ Analyse de la complétude des données : détection des valeurs nulles
# 
# Cette étape consiste à vérifier la **présence de valeurs nulles** dans chaque colonne du DataFrame `df_br`.
# 
# Pourquoi c’est important ❓
# - 🔍 Identifier les colonnes contenant des **données manquantes**, souvent synonymes de qualité de données à surveiller.
# - ✅ S’assurer que les champs critiques (comme `Numéro`, `Ouvert`, `Mis_a_jour`, etc.) sont bien renseignés.
# - 🧼 Déterminer s’il est nécessaire de faire un **nettoyage, un filtrage ou un remplacement** (imputation) de ces valeurs avant de charger les données en Silver.
# 
# 📌 La syntaxe utilisée :
# - `col(c).isNull()` : teste si la valeur est nulle.
# - `count(when(...))` : compte le nombre de lignes où la condition est vraie pour chaque colonne.
# - `.alias(c)` : donne à chaque résultat le nom de la colonne correspondante.
# 
# Cette vérification est essentielle pour garantir la **qualité et la fiabilité** des données en aval.
# 

# In[17]:


from pyspark.sql.functions import col

# Vérifier les valeurs nulles dans chaque colonne
df_br.select([count(when(col(c).isNull(), c)).alias(c) for c in df_br.columns]).show()


# In[18]:


from pyspark.sql.functions import col, count, when

# Affiche le nombre de valeurs nulles par colonne
df_br.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_br.columns
]).show()


# ### 🔍 Vérification des doublons dans le DataFrame
# 
# Avant de poursuivre le traitement, il est important de s'assurer que les données ne contiennent pas de doublons.
# 
# Dans cette étape, nous comparons :
# 
# - **Le nombre total de lignes** dans le DataFrame (`df_br`),
# - **Le nombre de lignes distinctes**, en supprimant les doublons.
# 
# L’objectif est de détecter si des enregistrements identiques sont présents plusieurs fois dans la source.
# 
# 👉 Si le nombre total est supérieur au nombre de lignes distinctes, cela signifie que des **doublons sont présents** dans les données.
# 
# Ce diagnostic permet d'assurer la **qualité** et **l'unicité** des enregistrements avant de passer à l'étape **Gold**.
# 

# In[19]:


from pyspark.sql.functions import count, col

# Compter les lignes totales
total_count = df_br.count()

# Compter les lignes distinctes (uniques)
distinct_count = df_br.distinct().count()

# Afficher les résultats
print(f"Nombre total de lignes      : {total_count}")
print(f"Nombre de lignes distinctes : {distinct_count}")

if total_count == distinct_count:
    print("✅ Aucun doublon détecté.")
else:
    print(f"⚠️ Il y a {total_count - distinct_count} doublons dans le DataFrame.")


# In[20]:


# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType

# # Créer une fonction pour attribuer 'HO' ou 'HNO' en fonction de l'heure
# def assign_shift(hour):
#     if hour is None:
#         return "Inconnu"
#     elif 8.5 <= hour < 18.5:
#         return "HO"
#     else:
#         return "HNO"

# # Enregistrer la fonction comme une fonction UDF (User Defined Function) dans PySpark
# assign_shift_udf = F.udf(assign_shift, StringType())

# # Ajouter la nouvelle colonne 'Plages_horaires' en fonction de l'heure de la colonne 'Ouvert'
# df_bronze = df_bronze.withColumn('Plages_horaires', 
#                                  assign_shift_udf(F.hour('Ouvert') + F.minute('Ouvert') / 60))

# # Afficher les résultats pour vérifier si la nouvelle colonne a été ajoutée correctement
# df_bronze.select('Ouvert', 'Plages_horaires').show(truncate=False)


# ### 🕐 Classification des horaires d’ouverture : HO vs HNO avec gestion des jours fériés
# 
# Dans cette étape, nous créons une nouvelle colonne `Plages_horaires` pour catégoriser chaque incident selon le **plage horaire de son ouverture** :
# 
# - **HO (Heures Ouvrées)** : entre 08h30 et 18h30, du lundi au vendredi, hors jours fériés.
# - **HNO (Heures Non Ouvrées)** : toutes les autres périodes (nuits, week-ends, jours fériés).
# 
# #### 🔧 Étapes clés de cette logique :
# 1. **Liste des jours fériés 2025** définie manuellement.
# 2. Création d’une fonction Python (`assign_shift_with_holidays`) :
#    - Combine la date (`Ouvert`) et l’heure (`Ovrt_h`).
#    - Vérifie si la date est un **week-end ou un jour férié**.
#    - Analyse l’heure d’ouverture pour assigner la bonne catégorie (`HO` ou `HNO`).
# 3. Transformation de cette fonction en **UDF** (`User Defined Function`) via `pyspark.sql.functions.udf`.
# 4. Application de l’UDF à chaque ligne du DataFrame pour créer une nouvelle colonne `Plages_horaires`.
# 
# #### ✅ Objectif :
# Faciliter les analyses sur la **distribution temporelle des incidents** : savoir combien sont ouverts en dehors des horaires standards.
# 
# 🧪 L'affichage final permet de contrôler que l'attribution HO/HNO est correcte selon les données source.
# 

# In[21]:


from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

# Liste des jours fériés
jours_feries = {
    "2025-01-01", "2025-04-21", "2025-05-01", "2025-05-08", "2025-05-29",
    "2025-06-09", "2025-07-14", "2025-08-15", "2025-11-01", "2025-11-11", "2025-12-25"
}

def assign_shift_with_holidays(date_obj, time_str):
    if date_obj is None or time_str is None:
        return "Inconnu"
    try:
        date_str = date_obj.strftime("%Y-%m-%d")  # conversion depuis DateType
        if date_str in jours_feries or date_obj.weekday() >= 5:
            return "HNO"

        h, m, s = map(int, time_str.split(":"))
        hour_decimal = h + m / 60
        if 8.5 <= hour_decimal < 18.5:
            return "HO"
        else:
            return "HNO"
    except Exception as e:
        return "Invalide"

# Déclaration UDF
assign_shift_udf = F.udf(assign_shift_with_holidays, StringType())

# Application
df_br = df_br.withColumn("Plages_horaires", assign_shift_udf(F.col("Ouvert"), F.col("Ovrt_h")))
df_br.select("Ouvert", "Ovrt_h", "Plages_horaires").show(truncate=False)


# In[22]:


# Étape 1 : Ajouter la colonne Plages_horaires
df_br = df_br.withColumn("Plages_horaires", assign_shift_udf(F.col("Ouvert"), F.col("Ovrt_h")))

# Étape 2 : Filtrer uniquement les lignes en HNO
df_hno = df_br.filter(F.col("Plages_horaires") == "HNO")

# Étape 3 : Afficher le résultat trié
df_hno.orderBy("Ouvert", "Ovrt_h").select("Ouvert", "Ovrt_h", "Plages_horaires").show(truncate=False)


# In[23]:


# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType

# # Fonction Python pour assigner HO ou HNO à partir de 'HH:mm:ss'
# def assign_shift_from_string(time_str):
#     if time_str is None:
#         return "Inconnu"
#     try:
#         h, m, s = map(int, time_str.split(":"))
#         hour_decimal = h + m / 60
#         if 8.5 <= hour_decimal < 18.5:
#             return "HO"
#         else:
#             return "HNO"
#     except:
#         return "Invalide"

# # Déclaration de la UDF
# assign_shift_udf = F.udf(assign_shift_from_string, StringType())

# # Application de la UDF sur la colonne 'Ovrt_H'
# df_br = df_br.withColumn("Plages_horaires", assign_shift_udf("Ovrt_H"))

# # Affichage de vérification
# df_br.select("Ovrt_H", "Plages_horaires").show(truncate=False)


# In[24]:


# Compter le nombre de lignes pour chaque valeur de 'Plages_horaires' (HO et HNO)
count_plages_horaires = df_br.groupBy('Plages_horaires').count()

# Afficher les résultats
count_plages_horaires.show(truncate=False)


# In[25]:


# Compter le nombre de valeurs uniques dans la colonne 'Description_breve'
unique_count = df_br.select("Description_breve").distinct().count()

# Afficher le nombre de valeurs uniques
print("Nombre de valeurs uniques dans 'Description_breve':", unique_count)


# In[26]:


# Afficher les valeurs uniques dans la colonne 'Description_breve'
df_br.select("Description_breve").distinct().show(truncate=False)


# In[27]:


# Affichage des 20 premières lignes
df_br.show()


# In[28]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_br.printSchema()


# In[29]:


# Affichage des 10 premières lignes

df_br.show(10)


# In[30]:


# Compter le nombre total de lignes dans le DataFrame
total_lignes = df_br.count()

# Compter le nombre de lignes distinctes (uniques)
lignes_distinctes = df_br.distinct().count()

# Vérifier si le nombre de lignes distinctes est inférieur au nombre total de lignes
if total_lignes > lignes_distinctes:
    print("Il y a des lignes dupliquées dans le DataFrame.")
else:
    print("Aucune ligne dupliquée dans le DataFrame.")


# In[31]:


from pyspark.sql.functions import count, when

# Compter les valeurs nulles ou vides pour chaque colonne
df_nulls_vides = df_br.select(
    [count(when(col(c).isNull() | (col(c) == ""), 1)).alias(c) for c in df_br.columns]
)

# Afficher les résultats
df_nulls_vides.show(truncate=False)


# In[32]:


print(type(df_br))
df_br.limit(10).show()


# ### 🏷️ Ajout d'une colonne d'information contextuelle
# 
# Dans cette étape, nous ajoutons une nouvelle colonne nommée **`Type`** au DataFrame.
# 
# - Cette colonne contient une **valeur constante** : `"Mise en production"`.
# - Elle permet d’**identifier clairement l’origine ou le contexte des incidents** analysés (dans ce cas, ceux extraits de ServiceNow liés à la mise en production).
# 
# Ce champ peut être utile pour :
# - filtrer ou regrouper les données par type d’incident,
# - faciliter les analyses futures ou croisements avec d’autres jeux de données.
# 
# Enfin, nous affichons cette colonne pour confirmer son ajout correct dans le DataFrame.
# 

# In[33]:


from pyspark.sql.functions import lit

# Ajouter une colonne 'Type' avec la valeur constante 'Mise en production'
df_br = df_br.withColumn("Type", lit("Mise en production"))

# Vérifier l'ajout de la colonne dans le DataFrame
df_br.select("Type").show(truncate=False)


# ### 🗂️ Création (ou vérification) de la table Delta dans la couche Silver
# 
# Dans cette étape, nous créons une **table Delta** nommée `cleansed_Silver.explt_silv` si elle n'existe pas encore.
# 
# - La table est conçue pour accueillir les données nettoyées provenant de la couche Bronze.
# - Elle contient les colonnes principales nécessaires à l'analyse des incidents de mise en production.
# 
# Pourquoi une **table Delta** ?
# - ✅ Permet de bénéficier des fonctionnalités avancées comme la gestion des versions (time travel), les mises à jour en place (merge), la gestion des métadonnées et la fiabilité des données.
# - 📊 Représente la **couche Silver** dans l'architecture Medallion, où les données sont nettoyées, enrichies et prêtes pour l'analyse.
# 
# Cette table sera ensuite utilisée pour les opérations de mise à jour et d'insertion de données.
# 

# In[34]:


# 1. Création (ou vérification) de la table Delta
DeltaTable.createIfNotExists(spark) \
    .tableName("cleansed_Silver.MEP_silver") \
    .addColumn("Numero", StringType()) \
    .addColumn("Ouvert", DateType()) \
    .addColumn("Mis_a_jour", DateType()) \
    .addColumn("Description_breve", StringType()) \
    .addColumn("Etat", StringType()) \
    .addColumn("Ouvert_par", StringType()) \
    .addColumn("Ovrt_h", StringType()) \
    .addColumn("Mis_h", StringType()) \
    .addColumn("Plages_horaires", StringType()) \
    .addColumn("Type", StringType()) \
    .execute()


# ### 🔄 Fusion des données dans la table Delta (MERGE)
# 
# Cette étape consiste à effectuer une **opération de type MERGE** entre le DataFrame `df_br` (contenant les incidents de mise en production) et la table Delta `cleansed_Silver.explt_silv`.
# 
# Objectifs de cette opération :
# - 🔄 **Mettre à jour** les lignes existantes si un incident avec le même `Numero` et la même date `Ouvert` est déjà présent.
# - ➕ **Insérer** de nouvelles lignes si l'incident n'existe pas encore dans la table.
# 
# Ce mécanisme permet d'assurer :
# - L'**intégrité** des données (évite les doublons),
# - Une **synchronisation continue** entre la source (bronze) et la table nettoyée (silver),
# - Une gestion efficace de l'**historique et des mises à jour**.
# 
# ℹ️ Les colonnes ajoutées récemment (`Plages_horaires`, `Type`) sont également prises en compte dans cette opération de fusion.
# 

# In[35]:


from delta.tables import DeltaTable

# 3. MERGE dans la table Delta
deltaTable = DeltaTable.forName(spark, "cleansed_Silver.MEP_silver")

deltaTable.alias("silver") \
    .merge(
        df_br.alias("updates"),
        "silver.Numero = updates.Numero AND silver.Ouvert = updates.Ouvert"
    ) \
    .whenMatchedUpdate(set={
        "Mis_a_jour": "updates.Mis_a_jour",
        "Description_breve": "updates.Description_breve",
        "Etat": "updates.Etat",
        "Ouvert_par": "updates.Ouvert_par",
        "Ovrt_h": "updates.Ovrt_h",
        "Mis_h": "updates.Mis_h",
        "Plages_horaires": "updates.Plages_horaires",  # ajout ici
        "Type": "updates.Type"  # ajouter ici

        
    }) \
    .whenNotMatchedInsert(values={
        "Numero": "updates.Numero",
        "Ouvert": "updates.Ouvert",
        "Mis_a_jour": "updates.Mis_a_jour",
        "Description_breve": "updates.Description_breve",
        "Etat": "updates.Etat",
        "Ouvert_par": "updates.Ouvert_par",
        "Ovrt_h": "updates.Ovrt_h",
        "Mis_h": "updates.Mis_h",
        "Plages_horaires": "updates.Plages_horaires",  # ajout ici
        "Type": "updates.Type"  # ajouter ici

    }) \
    .execute()


# ### 📥 Lecture des données depuis la table Delta Silver
# 
# À cette étape, nous lisons les données stockées dans la table Delta `cleansed_Silver.explt_silv` afin de :
# 
# - ✅ Vérifier que les données ont bien été insérées ou mises à jour,
# - 🔍 Visualiser un échantillon (les 10 premières lignes) du résultat final,
# - 📊 S'assurer que la transformation depuis la couche Bronze vers la Silver s'est déroulée correctement.
# 
# Cette opération permet de **valider le pipeline de traitement** et de préparer les données pour la suite du processus, notamment vers la couche Gold ou pour des analyses en dashboard.
# 

# In[36]:


df = spark.read.table("cleansed_Silver.MEP_silver")
display(df.head(10))


# In[37]:


# Charger le DataFrame
df = spark.read.table("cleansed_Silver.MEP_silver")

# Filtrer les lignes où 'Type' est 'Mise en production' et compter le nombre de ces lignes
count_mise_en_production = df.filter(df["Type"] == "Mise en production").count()

# Afficher le résultat
print(f"Nombre de 'Mise en production' dans la colonne 'Type' : {count_mise_en_production}")


# In[38]:


# Charger le DataFrame
df = spark.read.table("cleansed_Silver.MEP_silver")

# Filtrer les lignes où 'Type' est 'HNO'
df_hno = df.filter(df["Plages_horaires"] == "HNO")

# Sélectionner les colonnes à afficher
df_hno.select("Plages_horaires", "Ouvert", "Ovrt_h").show(truncate=False)


# In[39]:


# abfss://Exploitation_PFE@onelake.dfs.fabric.microsoft.com/raw_bronze.Lakehouse/Files/raw/sctaskN.csv


# ### 🗂️ Définition du schéma pour les demandes (Demande_schema)
# 
# Avant de charger le fichier contenant les demandes, nous définissons explicitement le schéma (`Demande_schema`). Cela permet à Spark :
# 
# - 📌 D'interpréter correctement le type de chaque colonne (dates, chaînes, entiers, etc.),
# - 🚫 D'éviter que Spark devine automatiquement les types, ce qui peut provoquer des erreurs ou incohérences,
# - ✅ D'assurer un chargement structuré et fiable des données dans le DataFrame.
# 
# Voici les colonnes définies :
# - `Créé` et `Fermé` : dates de création et de clôture de la demande,
# - `Numéro` : identifiant unique de la demande,
# - `Description brève` : courte description du besoin,
# - `Groupe d'affectation` et `Affecté à` : informations de responsabilité,
# - `Task Duration` : durée de traitement en secondes,
# - `Créé_h` et `Fermé_h` : heures correspondantes (format texte) pour analyse horaire.
# 
# Cette étape est cruciale pour une ingestion propre dans la couche Silver.
# 

# In[40]:


from pyspark.sql.types import StructType, StructField, StringType

# Schéma pour la table Incident
Demande_schema = StructType([
    StructField("Créé", DateType(), True),
    StructField("Fermé", DateType(), True),
    StructField("Numéro", StringType(), True),
    StructField("Description brève", StringType(), True),
    StructField("Groupe d'affectation", StringType(), True),
    StructField("Affecté à", StringType(), True),
    StructField("Task Duration", LongType(), True),
    StructField("Créé_h", StringType(), True),
    StructField("Fermé_h", StringType(), True)
])


# ### 📥 Chargement des données des demandes depuis la couche Bronze
# 
# Dans cette étape, nous chargeons le fichier **`sctaskN.csv`** depuis le **Bronze Lakehouse** en utilisant le chemin ABFS (Azure Blob File System). Ce fichier contient les demandes extraites de la plateforme ServiceNow.
# 
# Voici les options et paramètres utilisés :
# 
# - `.format("csv")` : le fichier source est au format CSV,
# - `.option("header", "true")` : la première ligne du fichier contient les noms des colonnes,
# - `.option("delimiter", ",")` : les colonnes sont séparées par des virgules,
# - `.schema(Demande_schema)` : on applique un schéma explicite défini au préalable pour garantir une structuration correcte des données,
# - `.load(...)` : chemin du fichier dans le Lakehouse, dans le dossier *raw/bronze*.
# 
# ✅ Cette étape permet d’importer proprement les données de demande dans un DataFrame Spark pour les futures transformations dans la couche Silver.
# 

# In[42]:


# Chargement du fichier incident.csv depuis le bronze Lakehouse (ABFS path)
df_De = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(Demande_schema) \
    .load("abfss://e979a68f-0c82-481e-9cfa-3a24201ddb86@onelake.dfs.fabric.microsoft.com/69ba2d01-2315-4a5e-9ba4-67be20fe4a65/Files/raw/demandes.csv")


# ### 🧾 Vérification du schéma du DataFrame
# 
# Après le chargement du fichier **`sctaskN.csv`**, nous affichons la structure du DataFrame à l'aide de `printSchema()`.
# 
# Cette commande permet de :
# 
# - ✅ Confirmer que les colonnes ont bien été reconnues,
# - ✅ Vérifier que les types de données (`StringType`, `DateType`, `LongType`, etc.) correspondent bien à ceux définis dans le schéma (`Demande_schema`),
# - 🛠️ Identifier d’éventuelles erreurs de typage ou de lecture avant d’appliquer les transformations en couche Silver.
# 
# C’est une étape de contrôle essentielle dans le processus de data cleansing, pour assurer la qualité et la fiabilité du pipeline.
# 

# In[43]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_De.printSchema()


# ### 🔄 Modification des noms de colonnes dans le DataFrame
# 
# Dans cette étape, nous procédons à la modification des noms de colonnes pour éliminer les espaces, caractères spéciaux et accents. Cette transformation permet d'obtenir des noms de colonnes plus compatibles avec les pratiques standards en programmation, tout en améliorant la lisibilité du DataFrame.
# 
# Les modifications suivantes ont été appliquées aux colonnes :
# - **Créé** → `Cree`
# - **Fermé** → `Ferme`
# - **Numéro** → `Numero`
# - **Description brève** → `Description_breve`
# - **Groupe d'affectation** → `Groupe_affectation`
# - **Affecté à** → `Affecte_a`
# - **Task Duration** → `Task_Duration`
# - **Créé_h** → `Cree_h`
# - **Fermé_h** → `Ferme_h`
# 
# Cela garantit une meilleure gestion des données et des opérations ultérieures sur les colonnes.
# 

# In[44]:


# Étape 3 : Renommer les colonnes pour enlever les espaces, caractères spéciaux, accents
df_De = df_De.withColumnRenamed("Créé", "Cree") \
       .withColumnRenamed("Fermé", "Ferme") \
       .withColumnRenamed("Numéro", "Numero") \
       .withColumnRenamed("Description brève", "Description_breve") \
       .withColumnRenamed("Groupe d'affectation", "Groupe_affectation") \
       .withColumnRenamed("Affecté à", "Affecte_a") \
       .withColumnRenamed("Task Duration", "Task_Duration") \
       .withColumnRenamed("Créé_h", "Cree_h") \
       .withColumnRenamed("Fermé_h", "Ferme_h")


# ### 🔍 Vérification de la structure après renommage des colonnes
# 
# Nous utilisons `df_De.printSchema()` une seconde fois pour **vérifier que le renommage des colonnes a bien été appliqué**.
# 
# Cette étape permet de :
# 
# - ✅ S'assurer que **tous les noms de colonnes sont désormais propres** : sans accents, sans espaces, ni caractères spéciaux.
# - 🧩 Confirmer que le **schéma reste conforme** avec les types de données attendus (`StringType`, `DateType`, etc.).
# - 🛠️ Préparer sereinement les prochaines étapes de transformation dans la couche Silver, en évitant tout risque d’erreur lié à un nom de colonne mal formaté.
# 
# C'est une **bonne pratique systématique** après toute opération de modification de structure sur un DataFrame.
# 

# In[45]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_De.printSchema()


# In[46]:


# Affichage des 20 premières lignes
df_De.show(truncate=False)  # Par défaut coupe à 20 caractères


# ### 🕒 Gestion de la compatibilité des dates : activation du mode LEGACY
# 
# Cette configuration permet d'éviter des erreurs lors du **parsing (interprétation)** de dates dans Spark, notamment lorsque les données d’entrée contiennent :
# 
# - des **formats de dates non standards** (ex : dates mal formatées dans un export CSV),
# - ou des dates qui ne respectent pas strictement les règles du format ISO.
# 
# 👉 En définissant la propriété `spark.sql.legacy.timeParserPolicy` à `"LEGACY"`, on active un **mode de compatibilité** avec les anciennes versions de Spark, plus tolérant vis-à-vis des formats de dates hétérogènes.
# 
# Cela permet :
# 
# - d’éviter des erreurs de type `DateTimeParseException`,
# - de **continuer le traitement sans blocage** même si certaines valeurs ne sont pas parfaitement formatées,
# - et d'effectuer un nettoyage ou une normalisation dans une étape ultérieure, si nécessaire.

# In[47]:


spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


# ### 🧹 Suppression des colonnes non nécessaires à l’analyse
# Dans cette étape, nous supprimons la colonne **`Groupe_affectation`** du DataFrame `df_De` à l'aide de la méthode **`drop()`**. Cette opération permet de se débarrasser des informations non nécessaires à l'analyse actuelle.
# 

# In[48]:


# Suppression des colonnes non nécessaires à l’analyse
df_De = df_De.drop("Groupe_affectation")
# Affichage des 30 premières lignes
df_De.show(30)


# ### ❓ Analyse de la complétude des données : détection des valeurs nulles
# 
# Dans cette étape, nous vérifions la **présence de valeurs nulles** dans chaque colonne du DataFrame `df_De`.
# 
# #### Pourquoi c’est important ❓
# - 🔍 Identifier les colonnes qui contiennent des **données manquantes**. Cela peut avoir un impact sur la qualité des analyses et des décisions prises sur la base des données.
# - ✅ S'assurer que les champs cruciaux, comme `Numéro`, `Description_breve`, `Affecte_a`, etc., sont correctement remplis et ne contiennent pas de valeurs nulles.
# - 🧼 Déterminer les colonnes nécessitant un **nettoyage ou un traitement particulier** (comme le remplacement des valeurs nulles par des valeurs par défaut ou des moyennes).
# 
# #### 📌 La syntaxe utilisée :
# - **`col(c).isNull()`** : Cette fonction teste si une valeur d'une colonne est nulle.
# - **`count(when(...))`** : Cette fonction permet de compter combien de lignes contiennent une valeur nulle pour chaque colonne.
# - **`.alias(c)`** : Utilisé pour attribuer le nom de la colonne à chaque résultat.
# 
# Le résultat de cette opération est un tableau qui affiche pour chaque colonne le nombre de valeurs nulles qu’elle contient, permettant ainsi une évaluation rapide de la **qualité des données** avant d’entamer d’autres traitements.
# 

# In[49]:


from pyspark.sql.functions import col

# Vérifier les valeurs nulles dans chaque colonne
df_De.select([count(when(col(c).isNull(), c)).alias(c) for c in df_De.columns]).show()


# In[50]:


# Appliquer un filtre pour supprimer les lignes où les colonnes 'Ferme' et 'Ferme_h' contiennent des valeurs nulles
df_De = df_De.filter(col("Ferme").isNotNull() & col("Ferme_h").isNotNull())

# Afficher les premières lignes du DataFrame après filtrage
df_De.show()


# In[51]:


from pyspark.sql.functions import col

# Vérifier les valeurs nulles dans chaque colonne
df_De.select([count(when(col(c).isNull(), c)).alias(c) for c in df_De.columns]).show()


# In[52]:


from pyspark.sql.functions import col, count, when

# Affiche le nombre de valeurs nulles par colonne
df_De.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_De.columns
]).show()


# ### 🔍 Vérification des doublons dans le DataFrame
# 
# Avant de poursuivre le traitement, il est important de s'assurer que les données ne contiennent pas de doublons.
# 
# Dans cette étape, nous comparons :
# 
# - **Le nombre total de lignes** dans le DataFrame (`df_De`),
# - **Le nombre de lignes distinctes**, en supprimant les doublons.
# 
# L’objectif est de détecter si des enregistrements identiques sont présents plusieurs fois dans la source.
# 
# 👉 Si le nombre total est supérieur au nombre de lignes distinctes, cela signifie que des **doublons sont présents** dans les données.
# 
# Ce diagnostic permet d'assurer la **qualité** et **l'unicité** des enregistrements avant de passer à l'étape **Gold**.
# 

# In[53]:


from pyspark.sql.functions import count, col

# Compter les lignes totales
total_count = df_De.count()

# Compter les lignes distinctes (uniques)
distinct_count = df_De.distinct().count()

# Afficher les résultats
print(f"Nombre total de lignes      : {total_count}")
print(f"Nombre de lignes distinctes : {distinct_count}")

if total_count == distinct_count:
    print("✅ Aucun doublon détecté.")
else:
    print(f"⚠️ Il y a {total_count - distinct_count} doublons dans le DataFrame.")


# ### 🕒 Classification des plages horaires : Assignation des shifts à partir de l’heure d’ouverture
# 
# Dans cette étape, nous créons une nouvelle colonne `Plages_horaires` pour catégoriser chaque incident selon son horaire de création (`Cree_h`) :
# 
# - **Shift 1** : entre 07h00 et 14h59.
# - **Shift 2** : entre 15h00 et 22h59.
# - **Shift 3** : entre 23h00 et 06h59.
# 
# 🔧 **Étapes clés de cette logique** :
# 
# 1. **Fonction Python `assign_shift_from_string`** :
#     - La fonction prend en entrée une chaîne de caractères au format `HH:mm:ss`.
#     - Elle convertit l'heure en heure décimale pour faciliter la comparaison.
#     - En fonction de l'heure décimale, la fonction attribue un shift spécifique :
#       - **Shift 1** : pour les heures entre 07:00 et 14:59.
#       - **Shift 2** : pour les heures entre 15:00 et 22:59.
#       - **Shift 3** : pour les heures entre 23:00 et 06:59.
#     - Si l'heure est invalide, la fonction retourne "Invalide" ou "Inconnu".
# 
# 2. **Création de l’UDF `assign_shift_udf`** :
#     - La fonction Python est transformée en **UDF** (User Defined Function) avec un type de retour `StringType()`.
# 
# 3. **Application de l’UDF** :
#     - L’UDF est appliquée sur la colonne `Cree_h` pour créer une nouvelle colonne `Plages_horaires` qui contient le shift attribué à chaque ligne.
# 
# ✅ **Objectif** :
# Permettre une analyse plus fine des incidents en fonction de l'heure de leur création, ce qui est utile pour les études de charge de travail ou d'optimisation des ressources en fonction des horaires.
# 

# In[54]:


from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Fonction Python pour assigner les shifts en fonction de 'HH:mm:ss'
def assign_shift_from_string(time_str):
    if time_str is None:
        return "Inconnu"
    try:
        h, m, s = map(int, time_str.split(":"))
        hour_decimal = h + m / 60

        # Shift 1: entre 7h et 15h
        if 7 <= hour_decimal < 15:
            return "Shift 1"
        # Shift 2: entre 15h et 23h
        elif 15 <= hour_decimal < 23:
            return "Shift 2"
        # Shift 3: entre 23h et 7h
        else:
            return "Shift 3"
    except:
        return "Invalide"

# Déclaration de la UDF
assign_shift_udf = F.udf(assign_shift_from_string, StringType())

# Application de la UDF sur la colonne 'Cree_h' de df_De (et non 'Créé_h')
df_De = df_De.withColumn("Plages_horaires", assign_shift_udf("Cree_h"))

# Affichage de vérification
df_De.select("Cree_h", "Plages_horaires").show(truncate=False)


# In[55]:


# Compter le nombre de lignes pour chaque valeur de 'Plages_horaires' 
count_plages_horaires = df_De.groupBy('Plages_horaires').count()

# Afficher les résultats
count_plages_horaires.show(truncate=False)


# In[56]:


# Compter le nombre de valeurs uniques dans la colonne 'Description_breve'
unique_count = df_De.select("Description_breve").distinct().count()

# Afficher le nombre de valeurs uniques
print("Nombre de valeurs uniques dans 'Description_breve':", unique_count)


# In[57]:


# Afficher les valeurs uniques dans la colonne 'Description_breve'
df_De.select("Description_breve").distinct().show(truncate=False)


# In[58]:


# Affichage des 20 premières lignes
df_De.show()


# In[59]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_De.printSchema()


# In[60]:


# Compter le nombre total de lignes dans le DataFrame
total_lignes = df_De.count()

# Compter le nombre de lignes distinctes (uniques)
lignes_distinctes = df_De.distinct().count()

# Vérifier si le nombre de lignes distinctes est inférieur au nombre total de lignes
if total_lignes > lignes_distinctes:
    print("Il y a des lignes dupliquées dans le DataFrame.")
else:
    print("Aucune ligne dupliquée dans le DataFrame.")


# In[61]:


from pyspark.sql.functions import count, when

# Compter les valeurs nulles ou vides pour chaque colonne
df_nulls_vides = df_De.select(
    [count(when(col(c).isNull() | (col(c) == ""), 1)).alias(c) for c in df_De.columns]
)

# Afficher les résultats
df_nulls_vides.show(truncate=False)


# ### ➕ Ajout d'une colonne 'Type' avec une valeur constante
# 
# Dans cette étape, nous ajoutons une nouvelle colonne `Type` à notre DataFrame `df_De`, et nous lui attribuons la valeur constante `"Demande"` pour toutes les lignes.
# 
# 🔧 **Étapes clés de cette logique** :
# 
# 1. **Ajout de la colonne** :
#     - Utilisation de la fonction `lit()` pour créer une colonne contenant une valeur constante (ici, `"Demande"`).
#     - La colonne est ajoutée au DataFrame avec `withColumn()`.
# 
# 2. **Vérification de l'ajout** :
#     - L'ajout de la colonne est validé en affichant les premières lignes de la colonne `Type`.
# 
# ✅ **Objectif** :
# Ajouter une colonne supplémentaire permettant de catégoriser ou d'annoter les lignes avec une valeur statique, utile pour les analyses futures ou pour distinguer les types de données dans le DataFrame.

# In[62]:


from pyspark.sql.functions import lit

# Ajouter une colonne 'Type' avec la valeur constante 'Mise en production'
df_De = df_De.withColumn("Type", lit("Demande"))

# Vérifier l'ajout de la colonne dans le DataFrame
df_De.select("Type").show(truncate=False)


# In[63]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_De.printSchema()


# ### 🗂️ Création (ou vérification) de la table Delta dans la couche Silver
# 
# Dans cette étape, nous créons une **table Delta** nommée `cleansed_Silver.demande_silv` si elle n'existe pas encore.
# 
# - La table est conçue pour accueillir les données nettoyées provenant de la couche Bronze.
# - Elle contient les colonnes principales nécessaires à l'analyse des demande.
# 
# Pourquoi une **table Delta** ?
# - ✅ Permet de bénéficier des fonctionnalités avancées comme la gestion des versions (time travel), les mises à jour en place (merge), la gestion des métadonnées et la fiabilité des données.
# - 📊 Représente la **couche Silver** dans l'architecture Medallion, où les données sont nettoyées, enrichies et prêtes pour l'analyse.
# 
# Cette table sera ensuite utilisée pour les opérations de mise à jour et d'insertion de données.
# 

# In[64]:


from pyspark.sql.types import StringType, DateType, LongType

# 1. Création (ou vérification) de la table Delta
DeltaTable.createIfNotExists(spark) \
    .tableName("cleansed_Silver.demandes_silver") \
    .addColumn("Cree", DateType()) \
    .addColumn("Ferme", DateType()) \
    .addColumn("Numero", StringType()) \
    .addColumn("Description_breve", StringType()) \
    .addColumn("Affecte_a", StringType()) \
    .addColumn("Task_Duration", LongType()) \
    .addColumn("Cree_h", StringType()) \
    .addColumn("Ferme_h", StringType()) \
    .addColumn("Plages_horaires", StringType()) \
    .addColumn("Type", StringType(), nullable=False) \
    .execute()


# ### 🔄 Fusion des données dans la table Delta (MERGE)
# 
# Cette étape consiste à effectuer une **opération de type MERGE** entre le DataFrame `df_De` (contenant les demande) et la table Delta `cleansed_Silver.demande_silv`.
# 
# Objectifs de cette opération :
# - 🔄 **Mettre à jour** les lignes existantes si un incident avec le même `Numero` et la même date `Ouvert` est déjà présent.
# - ➕ **Insérer** de nouvelles lignes si l'incident n'existe pas encore dans la table.
# 
# Ce mécanisme permet d'assurer :
# - L'**intégrité** des données (évite les doublons),
# - Une **synchronisation continue** entre la source (bronze) et la table nettoyée (silver),
# - Une gestion efficace de l'**historique et des mises à jour**.
# 
# ℹ️ Les colonnes ajoutées récemment (`Plages_horaires`, `Type`) sont également prises en compte dans cette opération de fusion.
# 

# In[66]:


from delta.tables import DeltaTable
from pyspark.sql import functions as F

# 3. MERGE dans la table Delta
deltaTable = DeltaTable.forName(spark, "cleansed_Silver.demandes_silver")

deltaTable.alias("silver") \
    .merge(
        df_De.alias("updates"),
        "silver.Numero = updates.Numero "
    ) \
    .whenMatchedUpdate(set={
        "Ferme": "updates.Ferme",
        "Numero": "updates.Numero",
        "Description_breve": "updates.Description_breve",
        "Affecte_a": "updates.Affecte_a",
        "Task_Duration": "updates.Task_Duration",
        "Cree_h": "updates.Cree_h",
        "Ferme_h": "updates.Ferme_h",
        "Plages_horaires": "updates.Plages_horaires",  # ajout ici
        "Type": "updates.Type"  # ajouter ici
    }) \
    .whenNotMatchedInsert(values={
        "Cree": "updates.Cree",
        "Ferme": "updates.Ferme",
        "Numero": "updates.Numero",
        "Description_breve": "updates.Description_breve",
        "Affecte_a": "updates.Affecte_a",
        "Task_Duration": "updates.Task_Duration",
        "Cree_h": "updates.Cree_h",
        "Ferme_h": "updates.Ferme_h",
        "Plages_horaires": "updates.Plages_horaires",  # ajout ici
        "Type": "updates.Type"  # ajouter ici
    }) \
    .execute()


# ### 📥 Lecture des données depuis la table Delta Silver
# 
# À cette étape, nous lisons les données stockées dans la table Delta `cleansed_Silver.demande_silv` afin de :
# 
# - ✅ Vérifier que les données ont bien été insérées ou mises à jour,
# - 🔍 Visualiser un échantillon (les 10 premières lignes) du résultat final,
# - 📊 S'assurer que la transformation depuis la couche Bronze vers la Silver s'est déroulée correctement.
# 
# Cette opération permet de **valider le pipeline de traitement** et de préparer les données pour la suite du processus, notamment vers la couche Gold ou pour des analyses en dashboard.
# 

# In[67]:


df = spark.read.table("cleansed_Silver.demandes_silver")
display(df.head(10))


# In[68]:


# Charger le DataFrame
df = spark.read.table("cleansed_Silver.demandes_silver")

# Filtrer les lignes où 'Type' est 'Mise en production' et compter le nombre de ces lignes
count_mise_Demande = df.filter(df["Type"] == "Demande").count()

# Afficher le résultat
print(f"Nombre de 'Demande' dans la colonne 'Type' : {count_mise_Demande}")


# In[69]:


print(spark.read.table("cleansed_Silver.demandes_silver").count())

