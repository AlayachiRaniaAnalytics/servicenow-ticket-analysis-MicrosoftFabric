#!/usr/bin/env python
# coding: utf-8

# ## Transform data for silver 
# 
# New notebook

# ## Documentation du Notebook : Transformation des données de Bronze à Silver

# ###  Contexte

# ###### Ce notebook s'inscrit dans une architecture Medallion mise en place dans Microsoft Fabric, et assure la transformation des données brutes (bronze) vers un format nettoyé et structuré (silver), en préparation pour leur exploitation ultérieure (gold / rapport Power BI).
# 
# 

# ### 📦 Importation des bibliothèques nécessaires
# 
# Dans cette section, nous importons les bibliothèques requises pour la suite du traitement :
# 
# - `pyspark.sql.types` : permet de définir les schémas (types de colonnes) pour structurer correctement les DataFrames.
# - `delta.tables` : nécessaire pour manipuler les tables Delta (mise à jour, suppression, merge, etc.), qui assurent la fiabilité et la traçabilité des données dans la couche Silver.
# 

# In[1]:


# Importation des types de données Spark SQL nécessaires pour définir ou manipuler les schémas de DataFrames
from pyspark.sql.types import *

# Importation des classes nécessaires pour manipuler des tables Delta (lecture, écriture, mise à jour)
from delta.tables import *


# ### 🧾 Définition du schéma des données en entrée
# 
# Afin d'assurer une lecture fiable et cohérente des données CSV, nous définissons manuellement le schéma attendu du fichier. Cela permet :
# 
# - d'éviter que Spark déduise automatiquement les types de colonnes, ce qui peut entraîner des erreurs ou des incohérences,
# - de garantir une structure stable et maîtrisée pour les étapes de traitement suivantes.
# 
# Chaque champ est typé explicitement (ex. : `TimestampType` pour les dates, `StringType` pour les chaînes de caractères).
# 

# In[2]:


#On définit le shéma du fichier csv pour éviter que Spark infére automatiquement le type de colonnes 

ticketSchema = StructType([
    StructField("Numero", StringType()),
    StructField("Ouvert", TimestampType()),
    StructField("Mis_a_jour", TimestampType()),
    StructField("Societe", StringType()),
    StructField("Priorite", StringType()),
    StructField("Groupe_affectation", StringType()),
    StructField("Etat", StringType()),
    StructField("Ouvert_par", StringType()),
    StructField("Categorie", StringType()),
    StructField("N_du_ticket_externe", StringType())
])


# ### 📥 Chargement des données brutes (zone Bronze)
# 
# Cette cellule permet de charger les données issues de la couche Bronze :
# 
# - Le fichier source est un CSV situé dans le répertoire `Files/bronze` du Lakehouse.
# - L’option `.option("header", "true")` indique que la première ligne du fichier contient les noms de colonnes.
# - Le schéma explicite défini précédemment (`ticketSchema`) est appliqué pour forcer le typage correct des colonnes dès la lecture.
# 
# Ce chargement constitue la première étape du traitement vers la couche Silver.
# 

# In[3]:


# Chargement des fichiers depuis le dossier bronze du lakehouse
df = spark.read.format("csv").option("header", "true").schema(ticketSchema).load("Files/bronze/test2.csv")


# ### 🧬 Vérification du schéma du DataFrame `df`
# 
# Après avoir défini manuellement le schéma `ticketSchema` pour garantir une lecture fiable des colonnes du fichier CSV, il est important de s'assurer que ce schéma a bien été appliqué lors du chargement des données.
# 
# La commande `df.printSchema()` permet d'afficher la structure du DataFrame, en précisant pour chaque colonne :
# - son nom,
# - son type de données (ex. : `StringType`, `TimestampType`, etc.),
# - et si elle accepte les valeurs nulles.
# 
# Cela constitue une étape essentielle pour valider que les données sont bien conformes aux attentes avant d'entamer les traitements analytiques.
# 

# In[4]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df.printSchema()


# In[5]:


# Affichage des 10 premières lignes
df.show(10, truncate=False)


# In[6]:


# Calcul du nombre total de records (lignes) dans le DataFrame `df`
total_records = df.count()

# Affichage du nombre de records
print(f"Nombre total de records : {total_records}")


# ### 📊 Vérification de la qualité des données
# 
# Dans cette section, nous vérifions deux aspects importants de la qualité des données dans notre DataFrame :
# 
# 1. **Comptage des valeurs nulles** : Nous comptons les valeurs nulles dans chaque colonne afin d'identifier les colonnes potentiellement incomplètes ou mal renseignées. Cela permet de prendre des mesures correctives, telles que l'imputation de données ou la suppression de colonnes problématiques.
# 
# 2. **Détection des doublons** : Nous vérifions si des doublons existent dans le DataFrame en comparant le nombre total de lignes avec le nombre de lignes distinctes. Cette étape permet de garantir que les données ne sont pas redondantes, ce qui est essentiel pour des analyses précises et fiables.
# 

# In[7]:


from pyspark.sql.functions import col, when, sum

# Compter les valeurs nulles par colonne
null_counts = df.select([ 
    (when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
])

# Effectuer l'agrégation pour obtenir le nombre de valeurs nulles
null_counts_agg = null_counts.agg(*[sum(col(c)).alias(c) for c in df.columns])

# Afficher le nombre de valeurs nulles par colonne
null_counts_agg.show()


# In[8]:


# Calcul du nombre total de lignes et du nombre de lignes distinctes
total_records = df.count()
distinct_records = df.distinct().count()

# Calcul et affichage du nombre de doublons
duplicates = total_records - distinct_records
print(f"Nombre de doublons : {duplicates}")


# ### 🗂️ Création de la table Delta pour la couche Silver
# 
# Dans cette étape, nous définissons la structure de la table `supervision_silver` au sein du schéma `Inetum_Data`.  
# La méthode `createIfNotExists` permet de créer la table **uniquement si elle n'existe pas déjà**, ce qui évite les erreurs en cas de réexécution du notebook.
# 
# #### 📌 Pourquoi utiliser une table Delta dans la couche Silver ?
# 
# L'utilisation du **format Delta** présente plusieurs avantages clés, notamment dans une architecture Medallion :
# 
# - ✅ **Gestion des versions** (time travel) : possibilité de revenir à une version antérieure des données.
# - ✅ **Mise à jour et suppression facilitées** (`MERGE`, `UPDATE`, `DELETE`) : très utile pour gérer des données évolutives.
# - ✅ **Fiabilité et intégrité des données** : transactions ACID garanties.
# - ✅ **Optimisation des performances** : via la gestion des fichiers et l’indexation (Delta cache, Z-Ordering…).
# - ✅ **Intégration native avec Spark & Fabric** : facilite l’exploitation en downstream (tables gold, dashboards…).
# 
# Cette table accueillera donc les données transformées, nettoyées et structurées issues de la couche Silver, prêtes à être utilisées pour l'analyse ou la visualisation.
# 

# In[9]:


# Création de la table Delta 'supervision_silver' dans le schéma 'Inetum_Data' si elle n'existe pas encore. 
# Cette table accueillera les données nettoyées et structurées issues de la couche Silver.   
DeltaTable.createIfNotExists(spark) \
     .tableName("Inetum_Data.supervision_silver") \
     .addColumn("Numero", StringType()) \
     .addColumn("Ouvert", TimestampType()) \
     .addColumn("Mis_a_jour", TimestampType()) \
     .addColumn("Societe", StringType()) \
     .addColumn("Priorite", StringType()) \
     .addColumn("Groupe_affectation", StringType()) \
     .addColumn("Etat", StringType()) \
     .addColumn("Ouvert_par", StringType()) \
     .addColumn("Categorie", StringType()) \
     .addColumn("N_du_ticket_externe", StringType()) \
     .execute() 


# ### 🔄 Synchronisation des données dans la table Delta (MERGE)
# 
# Dans cette étape, nous utilisons la commande `MERGE` pour synchroniser les données du DataFrame `df` avec la table Delta `supervision_silver`.  
# L'objectif est de **mettre à jour** les enregistrements existants et **insérer** les nouveaux.
# 
# #### Avantages du `MERGE` dans une table Delta :
# - ✅ Évite les doublons en mettant à jour les données existantes selon une clé de correspondance.
# - ✅ Ajoute automatiquement les nouvelles lignes non présentes dans la table cible.
# - ✅ Garantit l'intégrité des données grâce aux transactions ACID.
# - ✅ Recommandé dans les traitements **Silver** où les données peuvent être mises à jour régulièrement (ex : suivi de tickets).
# 
# Les conditions de correspondance (clause `ON`) sont basées sur des colonnes stables permettant d’identifier de manière unique un ticket (`Numero`, `Ouvert`, etc.).
# 

# In[10]:


from delta.tables import *

# Récupération de la table Delta existante dans le schéma 'Inetum_Data'
deltaTable = DeltaTable.forName(spark, "Inetum_Data.supervision_silver")

# DataFrame contenant les nouvelles données nettoyées ou mises à jour
dfUpdates = df  

# Synchronisation des données :
# - Si une correspondance est trouvée sur les colonnes clés, les colonnes restantes sont mises à jour.
# - Sinon, la ligne est insérée comme nouvelle.

deltaTable.alias("silver") \
  .merge(
    dfUpdates.alias("updates"),
    """
    silver.Numero = updates.Numero
    AND silver.Ouvert = updates.Ouvert
    AND silver.Societe = updates.Societe
    AND silver.Groupe_affectation = updates.Groupe_affectation
    """
  ) \
  .whenMatchedUpdate(set =
    {
      "Mis_a_jour": "updates.Mis_a_jour",
      "Priorite": "updates.Priorite",
      "Etat": "updates.Etat",
      "Ouvert_par": "updates.Ouvert_par",
      "Categorie": "updates.Categorie",
      "N_du_ticket_externe": "updates.N_du_ticket_externe",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "Numero": "updates.Numero",
      "Ouvert": "updates.Ouvert",
      "Mis_a_jour": "updates.Mis_a_jour",
      "Societe": "updates.Societe",
      "Priorite": "updates.Priorite",
      "Groupe_affectation": "updates.Groupe_affectation",
      "Etat": "updates.Etat",
      "Ouvert_par": "updates.Ouvert_par",
      "Categorie": "updates.Categorie",
      "N_du_ticket_externe": "updates.N_du_ticket_externe",
    }
  ) \
  .execute()


# In[11]:


# Chargement de la table 'Inetum_Data.supervision_silver' dans un DataFrame Spark
df = spark.read.table("Inetum_Data.supervision_silver")


# In[12]:


# Affichage des 10 premières lignes du DataFrame pour une inspection rapide des données
display(df.head(10))


# ### 🧾 Définition du schéma des données en entrée
# 
# Afin d'assurer une lecture fiable et cohérente des données CSV, nous définissons manuellement le schéma attendu du fichier. Cela permet :
# 
# - d'éviter que Spark déduise automatiquement les types de colonnes, ce qui peut entraîner des erreurs ou des incohérences,
# - de garantir une structure stable et maîtrisée pour les étapes de traitement suivantes.
# 
# Chaque champ est typé explicitement, comme ici :
# - `TimestampType` pour représenter des dates et heures (colonne `Ouvert`),
# - `StringType` pour représenter des chaînes de caractères (colonne `Plage_horaire`).
# 

# In[13]:


#On définit le shéma du fichier csv pour éviter que Spark infére automatiquement le type de colonnes 

ShiftSchema = StructType([
    StructField("Ouvert", TimestampType()),
    StructField("Plage_horaire", StringType()),
])


# In[14]:


# Chargement des fichiers depuis le dossier bronze du lakehouse
df_Shift = spark.read.format("csv").option("header", "true").schema(ShiftSchema).load("Files/bronze/dim_shift.csv")


# ### 🧬 Affichage du schéma du DataFrame `df_Shift`
# 
# La commande `printSchema()` permet d'afficher la structure du DataFrame `df_Shift`. Elle montre, pour chaque colonne :
# 
# - le nom,
# - le type de données (ex. : `TimestampType`, `StringType`, etc.),
# - et si la colonne peut contenir des valeurs nulles.
# 
# Cette visualisation est utile pour vérifier que les colonnes du DataFrame sont bien typées avant de les insérer ou de les comparer dans une table Delta.
# 

# In[15]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini

df_Shift.printSchema()


# In[16]:


# Affichage des 10 premières lignes
df_Shift.show(10, truncate=False)


# ### 🗂️ Création conditionnelle d'une table Delta
# 
# Cette instruction permet de créer la table Delta `Inetum_Data.Dim_Shift` uniquement si elle n'existe pas déjà dans le catalogue. Cette approche évite les erreurs dues à des tentatives de recréation de table, tout en assurant la disponibilité de la structure nécessaire au traitement.
# 
# L'utilisation de `DeltaTable.createIfNotExists()` présente plusieurs avantages :
# 
# - Prévention des erreurs lors de l'exécution répétée du notebook,
# - Garantie que la table est bien présente avant insertion ou transformation,
# - Définition explicite de la structure (nom des colonnes et types).
# 
# La table comporte :
# - une colonne `Ouvert` de type `TimestampType`,
# - une colonne `Plage_horaire` de type `StringType`.
# 

# In[17]:


# Création conditionnelle de la table Delta 'Dim_Shift' si elle n'existe pas déjà
    
DeltaTable.createIfNotExists(spark) \
     .tableName("Inetum_Data.Dim_Shift") \
     .addColumn("Ouvert", TimestampType()) \
     .addColumn("Plage_horaire", StringType()) \
     .execute() 


# ### ✅ Mise à jour incrémentielle de la table Delta avec `MERGE`
# 
# Cette opération permet d’effectuer une mise à jour incrémentielle de la table `Inetum_Data.Dim_Shift` via une commande `MERGE`. Elle combine les actions d’**UPDATE** (mise à jour des lignes existantes) et d’**INSERT** (insertion des nouvelles lignes), en fonction d’une condition de correspondance.
# 
# Avantages de cette approche :
# - Évite les doublons dans la table cible,
# - Gère automatiquement les mises à jour et les insertions en une seule commande,
# - Idéal pour les traitements de type *upsert* (update + insert) dans les pipelines Delta.
# 
# Le bloc ci-dessous :
# - recherche les correspondances entre la table `Dim_Shift` et le DataFrame `df_Shift` sur les colonnes `Ouvert` et `Plage_horaire`,
# - insère les lignes non présentes dans la table cible.
# 

# In[18]:


from delta.tables import *

# Référence à la table Delta existante
deltaTable = DeltaTable.forName(spark, "Inetum_Data.Dim_Shift")

# DataFrame contenant les nouvelles données à insérer ou à mettre à jour
dfUpdates = df_Shift  

# Exécution du MERGE : mise à jour si correspondance, sinon insertion
deltaTable.alias("silver") \
  .merge(
    dfUpdates.alias("updates"),
    """
    silver.Ouvert = updates.Ouvert
    AND silver.Plage_horaire = updates.Plage_horaire
    """
  ) \
  .whenMatchedUpdate(set =
    {
      # Aucune mise à jour à faire ici car les clés d'identification sont aussi les seules colonnes
      # Ce bloc est laissé vide intentionnellement
      
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "Ouvert": "updates.Ouvert",
      "Plage_horaire": "updates.Plage_horaire"
      
    }
  ) \
  .execute()


# ### 🧬 Affichage du schéma du DataFrame `df_Shift`
# 
# La commande `printSchema()` permet d'afficher la structure du DataFrame `df_Shift`. Elle montre, pour chaque colonne :
# 
# - le nom,
# - le type de données (ex. : `TimestampType`, `StringType`, etc.),
# - et si la colonne peut contenir des valeurs nulles.
# 
# Cette visualisation est utile pour vérifier que les colonnes du DataFrame sont bien typées avant de les insérer ou de les comparer dans une table Delta.
# 

# In[19]:


# Affiche la structure (schéma) du DataFrame pour vérifier que les colonnes et types ont bien été interprétés selon le schéma défini
df_Shift.printSchema()


# In[20]:


# Lecture de la table Delta 'Dim_Shift' depuis le catalogue et affichage des 10 premières lignes.
# Cela permet de vérifier visuellement le contenu de la dimension des shifts (créneaux horaires),
# notamment les valeurs présentes dans les colonnes 'Ouvert' et 'Plage_horaire'.
df_Shift = spark.read.table("Inetum_Data.Dim_Shift")
display(df_Shift.head(10))


# ## 📊 Vérification de la qualité des données
# Dans cette section, nous vérifions deux aspects importants de la qualité des données dans notre DataFrame :
# 
# 1. **Comptage des valeurs nulles** : Nous comptons les valeurs nulles dans chaque colonne afin d'identifier les colonnes potentiellement incomplètes ou mal renseignées. Cela permet de prendre des mesures correctives, telles que l'imputation de données ou la suppression de colonnes problématiques.
# 
# 2. **Détection des doublons** : Nous vérifions si des doublons existent dans le DataFrame en comparant le nombre total de lignes avec le nombre de lignes distinctes. Cette étape permet de garantir que les données ne sont pas redondantes, ce qui est essentiel pour des analyses précises et fiables.

# In[21]:


from pyspark.sql.functions import col, when, sum

# Compter les valeurs nulles par colonne dans df_Shift
null_counts = df_Shift.select([ 
    (when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df_Shift.columns
])

# Effectuer l'agrégation pour obtenir le nombre de valeurs nulles
null_counts_agg = null_counts.agg(*[sum(col(c)).alias(c) for c in df_Shift.columns])

# Afficher le nombre de valeurs nulles par colonne
null_counts_agg.show()


# ### 🔍 Détection des doublons dans `df_Shift` par `Ouvert` et `Plage_horaire`
# 
# Ce bloc de code permet de détecter les lignes du DataFrame `df_Shift` qui ont des valeurs dupliquées dans les colonnes `Ouvert` et `Plage_horaire`. Cela permet de vérifier s'il y a plusieurs tickets ouverts à la même heure et pour la même plage horaire.
# 
# #### Étapes du traitement :
# - `groupBy("Ouvert", "Plage_horaire")` : regroupe les lignes par les colonnes `Ouvert` et `Plage_horaire`, afin de trouver des doublons en fonction de ces deux champs.
# - `agg(count("*").alias("nb"))` : calcule le nombre d'occurrences pour chaque combinaison de `Ouvert` et `Plage_horaire`.
# - `filter("nb > 1")` : filtre pour ne conserver que les groupes où le nombre d'occurrences est supérieur à 1, c'est-à-dire les doublons.
# - `show(truncate=False)` : affiche les doublons trouvés sans tronquer les valeurs des colonnes.
# 
# Cela est utile pour vérifier que chaque plage horaire n’a pas plusieurs tickets associés à la même heure d’ouverture.
# 

# In[22]:


# Identification des doublons dans le DataFrame `df_Shift` en fonction des colonnes 'Ouvert' et 'Plage_horaire'.
from pyspark.sql.functions import count

df_Shift.groupBy("Ouvert", "Plage_horaire") \
  .agg(count("*").alias("nb")) \
  .filter("nb > 1") \
  .show(truncate=False)


# ### 🔢 Calcul du nombre de doublons dans `df_Shift`
# 
# Ce bloc de code calcule et affiche le nombre total de lignes, le nombre de lignes distinctes, ainsi que le nombre de doublons dans le DataFrame `df_Shift`.
# 
# #### Étapes du traitement :
# - `total = df_Shift.count()` : calcule le nombre total de lignes dans le DataFrame `df_Shift`.
# - `distincts = df_Shift.distinct().count()` : calcule le nombre de lignes distinctes en éliminant les doublons.
# - `total - distincts` : calcule le nombre de doublons en faisant la différence entre le total des lignes et le nombre de lignes distinctes.
# - `print()` : affiche les résultats sous forme de texte.
# 
# Cela permet de rapidement obtenir une vue d'ensemble des doublons dans le DataFrame.
# 

# In[23]:


from pyspark.sql.functions import countDistinct

# Calcul du nombre total de lignes dans le DataFrame df_Shift
total = df_Shift.count()

# Calcul du nombre de lignes distinctes (en éliminant les doublons)
distincts = df_Shift.distinct().count()

# Affichage du total, des lignes distinctes et des doublons
# Le nombre de doublons est calculé comme la différence entre le total et les lignes distinctes.
print(f"Total lignes : {total}, Lignes distinctes : {distincts}, Doublons : {total - distincts}")


# In[24]:


# Identification des doublons dans le DataFrame `df_Shift` en fonction des colonnes 'Ouvert' et 'Plage_horaire'.
df_duplicates = df_Shift.groupBy("Ouvert", "Plage_horaire") \
  .agg(count("*").alias("nb")) \
  .filter("nb > 1") # On filtre pour ne garder que les combinaisons de 'Ouvert' et 'Plage_horaire' ayant plus d'une occurrence.

# Jointure entre le DataFrame d'origine et le DataFrame des doublons pour extraire les lignes dupliquées.
df_Shift.join(df_duplicates, on=["Ouvert", "Plage_horaire"], how="inner").show(truncate=False) 
# Affichage des doublons trouvés, sans tronquer les valeurs des colonnes.


# ### 🧹 Suppression des doublons dans `df_Shift`
# 
# Le code suivant permet de supprimer toutes les lignes dupliquées dans le DataFrame `df_Shift`, en conservant uniquement les lignes uniques. Il utilise la méthode `dropDuplicates()`, qui supprime les lignes où toutes les colonnes sont identiques.
# 
# #### Explication :
# - `dropDuplicates()` : cette méthode supprime les doublons en vérifiant l'égalité sur l'ensemble des colonnes du DataFrame. Les lignes ayant exactement les mêmes valeurs dans toutes les colonnes seront considérées comme des doublons et supprimées.
# 
# Cela est utile pour nettoyer les données et éviter les biais ou les erreurs dans les analyses ou rapports ultérieurs.
# 

# In[25]:


# Suppression des doublons dans le DataFrame `df_Shift` en utilisant la méthode `dropDuplicates()`.
# Cette méthode élimine les lignes identiques (toutes les colonnes doivent correspondre pour être considérées comme des doublons),
# laissant uniquement les lignes uniques dans le DataFrame.
df_Shift_clean = df_Shift.dropDuplicates()


# ### 🔢 Vérification des doublons après nettoyage dans `df_Shift_clean`
# 
# Ce bloc de code permet de vérifier le nombre total de lignes, le nombre de lignes distinctes, et le nombre de doublons restants dans le DataFrame `df_Shift_clean`, après avoir éliminé les doublons à l'aide de `dropDuplicates()`.
# 
# #### Étapes du traitement :
# - `total = df_Shift_clean.count()` : calcule le nombre total de lignes dans le DataFrame `df_Shift_clean`.
# - `distincts = df_Shift_clean.distinct().count()` : calcule le nombre de lignes distinctes dans le DataFrame `df_Shift_clean`.
# - `total - distincts` : calcule le nombre de doublons restants après le nettoyage.
# - `print()` : affiche les résultats sous forme de texte, permettant de vérifier le succès du nettoyage des doublons.
# 
# Cela permet de confirmer que les doublons ont été correctement supprimés et de connaître le nombre exact de lignes uniques.
# 

# In[26]:


from pyspark.sql.functions import countDistinct

# Calcul du nombre total de lignes dans le DataFrame `df_Shift_clean`, après suppression des doublons.
total = df_Shift_clean.count()

# Calcul du nombre de lignes distinctes dans le DataFrame `df_Shift_clean`, en éliminant les doublons.
distincts = df_Shift_clean.distinct().count()

# Affichage du nombre total de lignes, du nombre de lignes distinctes, et du nombre de doublons.
# Le nombre de doublons est calculé en faisant la différence entre le total et les lignes distinctes.
print(f"Total lignes : {total}, Lignes distinctes : {distincts}, Doublons : {total - distincts}")


# ### 💾 Sauvegarde des données dans une table Delta du Lakehouse `Inetum_Data`
# 
# Ce bloc de code permet de sauvegarder le DataFrame `df_Shift_clean` dans une table Delta appelée `Dim_Shift`, située dans le **Lakehouse** `Inetum_Data`. Cette action remplace les données existantes dans la table par les données traitées et nettoyées.
# 
# #### Explication des options utilisées :
# - `format("delta")` : spécifie que la table doit être sauvegardée au format Delta, qui offre des avantages de gestion de version, de transactions et de performance.
# - `mode("overwrite")` : remplace la table Delta existante. Si la table n'existe pas encore, elle sera créée.
# - `option("overwriteSchema", "true")` : permet de mettre à jour le schéma de la table Delta pour qu'il corresponde à la structure des données actuelles.
# - `saveAsTable("Inetum_Data.Dim_Shift")` : sauvegarde les données dans la table Delta `Dim_Shift` du **Lakehouse** `Inetum_Data`.
# 
# En enregistrant les données dans une table Delta, les données sont versionnées et optimisées pour des performances accrues, tout en restant disponibles pour des analyses futures dans l'architecture du Lakehouse.
# 

# In[27]:


# Écriture du DataFrame nettoyé (df_Shift_clean) dans une table Delta 'Dim_Shift' située dans le lakehouse 'Inetum_Data'.
# Cette opération permet de sauvegarder les données traitées et nettoyées dans le format Delta, en remplaçant la table existante.
df_Shift_clean.write.format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("Inetum_Data.Dim_Shift")


# In[28]:


# Lecture de la table Delta 'Dim_Shift'.
df_result = spark.read.table("Inetum_Data.Dim_Shift")
# Affichage des 10 premières lignes du DataFrame pour inspection.
display(df_result.head(10))


# ### 📋 Définition du schéma des données CSV
# 
# Afin d'assurer une lecture correcte et cohérente des données CSV dans Spark, nous définissons manuellement le schéma attendu du fichier. Cela permet d'éviter que Spark infère automatiquement les types de données, ce qui peut conduire à des erreurs.
# 
# #### Schéma défini :
# - **"Numéro"** : type `StringType`, destiné à contenir des valeurs sous forme de chaîne de caractères.
# - **"Affecté à"** : type `StringType`, également pour des valeurs sous forme de chaîne de caractères.
# 
# Cette étape permet d'assurer que les données seront lues dans le format attendu, en évitant des erreurs d'interprétation.
# 

# In[29]:


#On définit le shéma du fichier csv pour éviter que Spark infére automatiquement le type de colonnes 

CollabSchema = StructType([
    StructField("Numéro", StringType()),
    StructField("Affecté à", StringType()),
])


# In[30]:


# Chargement des fichiers depuis le dossier bronze du lakehouse
df_Collab = spark.read.format("csv").option("header", "true").schema(CollabSchema).load("Files/bronze/dim_affecte.csv")


# In[31]:


# Affichage du schéma du DataFrame `df_Collab` afin de vérifier la structure des données.
df_Collab.printSchema()


# In[32]:


# Affichage des 10 premières lignes
df_Collab.show(10, truncate=False)


# ### 🛠️ Création de la table Delta `Dim_Collaborateur`
# 
# Ce bloc de code permet de créer la table Delta `Dim_Collaborateur` dans le **Lakehouse** `Inetum_Data`, en s'assurant que la table soit créée uniquement si elle n'existe pas déjà. Deux colonnes sont ajoutées : `Numero` et `Affecte`, toutes deux de type `String`.
# 
# #### Explication des étapes :
# 1. `createIfNotExists(spark)` : crée la table Delta si elle n'existe pas encore dans le lakehouse.
# 2. `.tableName("Inetum_Data.Dim_Collaborateur")` : définit le nom de la table.
# 3. `.addColumn("Numero", StringType())` : ajoute la colonne `Numero` avec un type de données `String`.
# 4. `.addColumn("Affecte", StringType())` : ajoute la colonne `Affecte` avec un type de données `String`.
# 5. `.execute()` : exécute la création de la table avec les colonnes spécifiées.
# 
# Cela garantit que la table est prête à être utilisée pour des opérations de lecture ou d'écriture dans le Lakehouse.
# 

# In[33]:


# Création d'une table Delta 'Dim_Collaborateur' dans le lakehouse 'Inetum_Data' si elle n'existe pas encore.
    
DeltaTable.createIfNotExists(spark) \
     .tableName("Inetum_Data.Dim_Collaborateur") \
     .addColumn("Numero", StringType()) \
     .addColumn("Affecte", StringType()) \
     .execute() 


# ### 🔄 Mise à jour ou insertion des données dans la table Delta `Dim_Collaborateur`
# 
# Ce bloc de code réalise un **MERGE** entre le DataFrame `df_Collab` (après renaming des colonnes) et la table Delta `Dim_Collaborateur` située dans le **Lakehouse** `Inetum_Data`. Le but est de mettre à jour les lignes existantes ou d'insérer de nouvelles lignes si aucune correspondance n'est trouvée.
# 
# #### Étapes du processus :
# 1. **Renommage des colonnes** : les colonnes du DataFrame `df_Collab` sont renommées pour correspondre aux noms des colonnes de la table Delta (ex. : "Numéro" devient "Numero").
# 2. **Récupération de la table Delta** : la table Delta `Dim_Collaborateur` est récupérée à l'aide de la méthode `forName()`.
# 3. **Mise à jour ou insertion avec MERGE** :
#    - Si une ligne correspondant à `Numero` et `Affecte` existe dans la table Delta, elle est mise à jour.
#    - Si aucune ligne correspondante n'est trouvée, une nouvelle ligne est insérée dans la table.
# 4. **Exécution de la commande MERGE** : les modifications sont appliquées avec la méthode `execute()`.
# 
# Cette opération permet de maintenir les données dans la table Delta à jour, tout en évitant les doublons et en garantissant une gestion efficace des données.
# 

# In[34]:


from pyspark.sql.functions import col
from delta.tables import *

# Renommer les colonnes du DataFrame pour correspondre à la table Delta
dfUpdates = df_Collab \
    .withColumnRenamed("Numéro", "Numero") \
    .withColumnRenamed("Affecté à", "Affecte")

# Récupérer la table Delta
deltaTable = DeltaTable.forName(spark, "Inetum_Data.Dim_Collaborateur")

# MERGE pour mettre à jour ou insérer les données
deltaTable.alias("silver") \
  .merge(
    dfUpdates.alias("updates"),
    """
    silver.Numero = updates.Numero
    AND silver.Affecte = updates.Affecte
    """
  ) \
  .whenNotMatchedInsert(values =
    {
      "Numero": "updates.Numero",
      "Affecte": "updates.Affecte"
    }
  ) \
  .execute()


# In[35]:


# Lecture de la table Delta 'dim_collaborateur'.
df_Shift = spark.read.table("Inetum_Data.dim_collaborateur")
# Affichage des 10 premières lignes du DataFrame pour inspection.
display(df_Shift.head(10))


# ## 📊 Vérification de la qualité des données
# Dans cette section, nous vérifions deux aspects importants de la qualité des données dans notre DataFrame :
# 
# 1. **Comptage des valeurs nulles** : Nous comptons les valeurs nulles dans chaque colonne afin d'identifier les colonnes potentiellement incomplètes ou mal renseignées. Cela permet de prendre des mesures correctives, telles que l'imputation de données ou la suppression de colonnes problématiques.
# 
# 2. **Détection des doublons** : Nous vérifions si des doublons existent dans le DataFrame en comparant le nombre total de lignes avec le nombre de lignes distinctes. Cette étape permet de garantir que les données ne sont pas redondantes, ce qui est essentiel pour des analyses précises et fiables.

# In[36]:


from pyspark.sql.functions import col, when, sum

# Compter les valeurs nulles par colonne dans df_Collab
null_counts = df_Collab.select([ 
    (when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df_Collab.columns
])

# Effectuer l'agrégation pour obtenir le nombre de valeurs nulles
null_counts_agg = null_counts.agg(*[sum(col(c)).alias(c) for c in df_Collab.columns])

# Afficher le nombre de valeurs nulles par colonne
null_counts_agg.show()


# ### 📊 Calcul du nombre de doublons et de lignes distinctes
# 
# Ce bloc de code permet de calculer et d'afficher le nombre total de lignes, de lignes distinctes et de doublons dans le DataFrame `df_Collab`.
# 
# 
# #### Interprétation :
# - **Total lignes** : Le nombre total de lignes dans le DataFrame est de 18 414.
# - **Lignes distinctes** : Le nombre de lignes distinctes après suppression des doublons est également de 18 414.
# - **Doublons** : Aucun doublon n'a été trouvé dans les données, ce qui signifie que toutes les lignes sont uniques.
# 

# In[37]:


from pyspark.sql.functions import countDistinct
# Identification des doublons dans le DataFrame `df_Collab`
total = df_Collab.count()
distincts = df_Collab.distinct().count()
print(f"Total lignes : {total}, Lignes distinctes : {distincts}, Doublons : {total - distincts}")

