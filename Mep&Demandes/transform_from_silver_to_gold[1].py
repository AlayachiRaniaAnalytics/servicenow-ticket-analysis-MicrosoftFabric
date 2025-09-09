#!/usr/bin/env python
# coding: utf-8

# ## transform from silver to gold
# 
# New notebook

# ### 📊 Transformation des données de la couche Silver à la couche Gold
# 
# #### Objectif :
# Ce processus consiste à effectuer la transformation des données depuis la **couche Silver** vers la **couche Gold** dans le Lakehouse. La couche Silver contient des données nettoyées et enrichies, tandis que la couche Gold représente les données finales prêtes pour l'analyse et la visualisation. 
# 
# Dans cette étape, nous mettons en place le **schéma en étoile** en créant les **tables de dimension** et la **table de fait** qui alimenteront nos rapports analytiques.

# ### 📦 Importation des bibliothèques nécessaires
# 
# Dans cette section, nous importons les bibliothèques requises pour la suite du traitement :
# 
# - `pyspark.sql.types` : permet de définir les schémas (types de colonnes) pour structurer correctement les DataFrames.
# 
# - `delta.tables` : nécessaire pour manipuler les tables Delta (mise à jour, suppression, merge, etc.), qui assurent la fiabilité et la traçabilité des données dans la couche Silver.
# 
# - `pyspark.sql.functions` : contient un ensemble de fonctions permettant la transformation et la manipulation de colonnes dans un DataFrame. 
# 
# - `pyspark.sql.SparkSession` : permet d’instancier une session Spark, qui constitue le point d’entrée principal pour la manipulation des données avec PySpark (lecture, écriture, transformation, etc.).
# 

# In[1]:


from pyspark.sql.functions import col, split, dayofmonth, month, year
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit


# # Les MEP

# #### Étapes de la transformation
# 
# 1. **Chargement des données depuis la couche Silver :**
#    Les données sont lues depuis la table `Inetum_Data.supervision_silver`, qui contient les informations de niveau intermédiaire, c'est-à-dire des données nettoyées mais non encore totalement agrégées ou enrichies.

# In[2]:


df = spark.read.table("cleansed_Silver.MEP_silver")


# In[3]:


display(df.head(10))


# ### 🛠️ Création de la table de dimension `Dim_Date_Ouverture`
# 
# Dans cette étape, nous créons une nouvelle table de dimension `Dim_Date_Ouverture` dans le lakehouse. Cette table sera utilisée pour stocker des informations sur les dates d'ouverture des tickets dans une structure dimensionnelle.
# 
# #### Processus de création :
# 
# 1. **Vérification de l'existence de la table** : Nous utilisons la méthode `createIfNotExists` pour garantir que la table est créée uniquement si elle n'existe pas déjà.
# 2. **Définition du schéma** : La table contient plusieurs colonnes pour détailler les informations relatives à la date et à l'heure d'ouverture des tickets :
#    - `Ouvert` : Le timestamp de l'ouverture du ticket.
#    - `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, `Seconde` : Ces colonnes permettent de décomposer la date d'ouverture pour une analyse plus précise.
# 3. **Exécution de la création** : La méthode `execute()` permet de finaliser la création de la table.
# 
# Cette structure nous permettra de faire des jointures avec d'autres tables, comme les faits relatifs aux tickets, en utilisant des clés dimensionnelles basées sur la date et l'heure d'ouverture.
# 

# In[4]:


# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Date_Ouverture") \
    .addColumn("Ouvert", DateType()) \
    .addColumn("Jour", IntegerType()) \
    .addColumn("Mois", IntegerType()) \
    .addColumn("Annee", IntegerType()) \
    .addColumn("Heure", IntegerType()) \
    .addColumn("Minute", IntegerType()) \
    .addColumn("Seconde", IntegerType()) \
    .execute()
    


# ### 📅 Création et transformation de la table `Dim_Date_Ouverture`
# 
# Dans cette étape, nous préparons les données pour créer une table de dimension `Dim_Date_Ouverture`. Cette table sera utilisée pour stocker les informations détaillées sur la date et l'heure d'ouverture des tickets, et nous la préparons à l'aide de transformations spécifiques sur la colonne `Ouvert` (timestamp).
# 
# #### Processus de transformation :
# 
# 1. **Suppression des doublons** : Nous utilisons la méthode `dropDuplicates` pour éliminer les doublons dans la colonne `Ouvert`. Cela garantit qu'il n'y ait qu'une seule entrée par date d'ouverture.
# 2. **Création de nouvelles colonnes** : Nous extrayons plusieurs éléments temporels de la colonne `Ouvert` :
#    - `Jour` : Le jour du mois de l'ouverture (`dayofmonth`).
#    - `Mois` : Le mois de l'ouverture (`month`).
#    - `Annee` : L'année de l'ouverture (`year`).
#    - `Heure`, `Minute`, `Seconde` : L'heure, la minute, et la seconde de l'ouverture, respectivement.
# 3. **Tri des résultats** : Le DataFrame est ensuite trié par la colonne `Ouvert` pour assurer une organisation chronologique des données.
# #### Aperçu des données :
# 
# Le code utilise la méthode `show(10)` pour afficher les 10 premières lignes du DataFrame `dfdimDate_Ouverture`, ce qui permet de prévisualiser les résultats et vérifier que les transformations ont été appliquées correctement.
# 
# Cette table de dimension `Dim_Date_Ouverture` sera utilisée dans le modèle de données en étoile, où elle sera liée à une table de faits.
# 

# In[5]:


# On part du principe que df contient les colonnes "Ouvert" et "Ovrt_h"
dfdimDate_Ouverture = df.dropDuplicates(["Ouvert"]).select(
    col("Ouvert"),
    dayofmonth("Ouvert").alias("Jour"),
    month("Ouvert").alias("Mois"),
    year("Ouvert").alias("Annee"),
    split(col("Ovrt_h"), ":")[0].cast("int").alias("Heure"),
    split(col("Ovrt_h"), ":")[1].cast("int").alias("Minute"),
    split(col("Ovrt_h"), ":")[2].cast("int").alias("Seconde")
).orderBy("Ouvert")

dfdimDate_Ouverture.show(10)


# ### 🔄 Mise à jour ou insertion dans la table Delta `dim_date_ouverture`
# 
# Dans cette étape, nous effectuons une opération de **merge** entre le DataFrame `dfdimDate_Ouverture` (contenant les nouvelles données) et la table Delta `dim_date_ouverture` (qui contient les données existantes). Cette opération est effectuée dans deux scénarios :
# 
# 1. **Mise à jour des lignes existantes** : Si une ligne dans la table `dim_date_ouverture` correspond à une ligne du DataFrame sur la base de la colonne `Ouvert`, les colonnes `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde` de la table Delta sont mises à jour avec les nouvelles valeurs du DataFrame.
#    
# 2. **Insertion de nouvelles lignes** : Si une ligne n'existe pas déjà dans la table `dim_date_ouverture` (basée sur la colonne `Ouvert`), de nouvelles lignes sont insérées dans la table avec les valeurs correspondantes du DataFrame.
# 
# L'opération `merge` est un moyen efficace de maintenir la table de dimension à jour avec de nouvelles données tout en évitant les duplications. Cela garantit que la table contient les informations les plus récentes et évite d'avoir à réécrire toutes les données.
# 
# #### Fonctionnement du code :
# 
# - La table Delta est référencée avec `DeltaTable.forPath()`.
# - Une opération `merge` est ensuite effectuée entre la table existante (`gold`) et les nouvelles données (`updates`).
# - Le match est effectué sur la colonne `Ouvert` (date d'ouverture).
# - En cas de correspondance (`whenMatchedUpdate`), les colonnes de la table `dim_date_ouverture` sont mises à jour.
# - En cas de non-correspondance (`whenNotMatchedInsert`), de nouvelles lignes sont insérées.
# 
# Cette opération garantit que la table de dimension reste synchronisée avec les données entrantes et est prête à être utilisée dans le modèle en étoile pour les jointures avec d'autres tables de faits.
# 

# In[6]:


# Référence à la table Delta existante
deltaTable = DeltaTable.forPath(spark,'Tables/dim_date_ouverture')

# Préparation des données à insérer
dfUpdates = dfdimDate_Ouverture

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.Ouvert = updates.Ouvert'  # Correspondance sur la colonne timestamp
    ) \
    .whenMatchedUpdate(set={
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .whenNotMatchedInsert(values={
        'Ouvert': 'updates.Ouvert',
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .execute()


# In[7]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.Dim_Date_Ouverture")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# ### 🕒 Transformation des données pour la table de dimension `Dim_Date_MAJ`
# 
# Dans cette étape, nous créons le DataFrame `dfdimDate_MAJ` en appliquant diverses transformations à la colonne `Mis_a_jour` de notre DataFrame source. Cette table de dimension contient des informations sur la date et l'heure de mise à jour des tickets.
# 
# #### Objectifs :
# 1. **Suppression des doublons** : Nous supprimons les doublons dans la colonne `Mis_a_jour` afin de ne conserver qu'une seule entrée pour chaque timestamp unique de mise à jour.
# 2. **Extraction des composantes temporelles** : 
#    - Nous extrayons le jour, le mois, l'année, l'heure, la minute et la seconde de la colonne `Mis_a_jour` pour faciliter les analyses temporelles.
# 3. **Sélection et transformation des colonnes** : 
#    - Nous ajoutons les colonnes `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde` à notre DataFrame à partir de la colonne `Mis_a_jour`.
# 4. **Tri des données** : Les données sont triées par la colonne `Mis_a_jour` pour faciliter l'organisation temporelle.
# 
# #### Affichage des premières lignes :
# Nous utilisons la méthode `show(10)` pour afficher les 10 premières lignes du DataFrame transformé et vérifier que les transformations ont été effectuées correctement.
# 
# #### Utilisation future :
# Cette table pourra être utilisée pour effectuer des jointures avec d'autres tables dans le cadre de l'analyse des mises à jour des tickets ou d'autres processus décisionnels dans notre modèle en étoile.
# 
# 

# In[8]:


# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Date_maj") \
    .addColumn("Mis_a_jour", DateType()) \
    .addColumn("Jour", IntegerType()) \
    .addColumn("Mois", IntegerType()) \
    .addColumn("Annee", IntegerType()) \
    .addColumn("Heure", IntegerType()) \
    .addColumn("Minute", IntegerType()) \
    .addColumn("Seconde", IntegerType()) \
    .execute()


# In[9]:


# On part du principe que df contient les colonnes "Ouvert" et "Ovrt_h"
dfdimDate_maj = df.dropDuplicates(["Mis_a_jour"]).select(
    col("Mis_a_jour"),
    dayofmonth("Mis_a_jour").alias("Jour"),
    month("Mis_a_jour").alias("Mois"),
    year("Mis_a_jour").alias("Annee"),
    split(col("Mis_h"), ":")[0].cast("int").alias("Heure"),
    split(col("Mis_h"), ":")[1].cast("int").alias("Minute"),
    split(col("Mis_h"), ":")[2].cast("int").alias("Seconde")
).orderBy("Mis_a_jour")

dfdimDate_maj.show(10)


# ### 🔄 Fusion des données pour la table de dimension `Dim_Date_MAJ`
# 
# Dans cette étape, nous utilisons la commande **MERGE** de Delta Lake pour mettre à jour ou insérer des données dans la table de dimension `Dim_Date_MAJ`.
# 
# #### Objectifs :
# 1. **Mise à jour de la table Delta** : 
#    - Nous mettons à jour les enregistrements existants dans la table `Dim_Date_MAJ` si les dates de mise à jour (`Mis_a_jour`) correspondent. Cela permet d'assurer que les valeurs des colonnes `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde` sont à jour.
#    
# 2. **Insertion de nouvelles données** :
#    - Si un enregistrement n'existe pas dans la table, il sera inséré avec les valeurs appropriées pour `Mis_a_jour`, `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde`.
# 
# #### Détails de la fusion (MERGE) :
# - **Condition de correspondance** : Nous avons basé la correspondance sur la colonne `Mis_a_jour` afin de vérifier les lignes existantes dans la table.
# - **Mise à jour des lignes existantes** : Si une ligne existe déjà avec la même valeur dans `Mis_a_jour`, les autres colonnes seront mises à jour avec les valeurs correspondantes des nouvelles données.
# - **Insertion des nouvelles lignes** : Si aucune ligne correspondante n'est trouvée, une nouvelle ligne est insérée dans la table avec les nouvelles valeurs.
# 
# #### Résultat :
# Cette opération assure que la table `Dim_Date_MAJ` reste à jour avec les dernières informations de mise à jour et peut être utilisée dans des jointures avec d'autres tables de faits pour l'analyse.
# 
# 

# In[10]:


# Référence à la table Delta existante
deltaTable = DeltaTable.forPath(spark,'Tables/dim_date_maj')

# Préparation des données à insérer
dfUpdates = dfdimDate_maj

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.Mis_a_jour = updates.Mis_a_jour'  # Correspondance sur la colonne timestamp
    ) \
    .whenMatchedUpdate(set={
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .whenNotMatchedInsert(values={
        'Mis_a_jour': 'updates.Mis_a_jour',
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .execute()


# In[11]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_date_maj")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# ### 🔨 Création de la table de dimension `Dim_Description`
# 
# Dans cette étape, nous créons la table de dimension **Dim_Description** dans notre **Lakehouse**. Cette table contiendra les informations liées aux mises en production (MEP).
# #### Objectifs :
# 1. **Création de la table** : 
#    - Si la table **Dim_Description** n'existe pas déjà, elle est créée automatiquement.
# 
# 2. **Structure de la table** :
#    - **ID_Description** : Identifiant unique de la description, de type **LongType**.
#    - **Description** : Détail ou libellé de la mise en production, de type **StringType**.
# 
# Cette table sera utilisée pour stocker les descriptions des MEP, facilitant ainsi l’analyse, la catégorisation, et l’enrichissement des données de faits lors de la construction du modèle décisionnel.
# 
# #### Détails techniques :
# - **`DeltaTable.createIfNotExists()`** : Cette fonction vérifie si la table existe déjà et la crée dans le cas contraire. Elle permet de définir la structure de la table avec précision et garantit sa cohérence au sein du Lakehouse.
# - **Colonnes** :
#    - `ID_Description` : Sert d’identifiant technique unique pour chaque enregistrement.
#    - `Description` : Contient le libellé explicatif de la MEP.
# 
# Une fois cette table créée, elle pourra être utilisée dans les processus de transformation et de modélisation, notamment pour faire le lien avec les données opérationnelles ou analytiques liées aux déploiements et évolutions.
# 

# In[12]:


# Création de la table de dimension Dim_Description
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_descriptions") \
    .addColumn("ID_description", LongType()) \
    .addColumn("Description_breve", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Description`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Description`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Description`**. 
# 
# Nous utilisons la méthode **`dropDuplicates()`** afin d’éliminer les enregistrements redondants et de s'assurer que chaque description de mise en production (MEP) est unique dans la table.
# 
# Cette étape est essentielle pour maintenir l'intégrité des données et éviter les anomalies lors des jointures ou des analyses ultérieures.
# 

# In[13]:


# Suppression des doublons sur la colonne 'Description_breve'
dfdimDescription= df.dropDuplicates(["Description_breve"]).select(col("Description_breve"))
# Affichage des premières lignes du DataFrame
dfdimDescription.show(10)


# ### 🔄 Identification et ajout de nouvelles descriptions à la table de dimension `Dim_Description`
# 
# Ce processus permet de traiter les descriptions existantes et d’ajouter les nouvelles descriptions de mises en production (MEP) qui ne figurent pas encore dans la table de dimension **`Dim_Description`**. Voici les principales étapes réalisées :
# 
# #### 🧩 Chargement de la table existante :
# Nous commençons par charger la table **`Dim_Description`** existante depuis le Delta Lake, afin de récupérer les descriptions déjà enregistrées.
# 
# #### 🔍 Récupération du dernier ID :
# Nous récupérons la valeur maximale de la colonne **`ID_Description`** dans la table existante, ce qui permettra d’attribuer de nouveaux identifiants uniques aux nouvelles descriptions à insérer.
# 
# #### 🧹 Extraction des descriptions uniques :
# À partir du DataFrame source (`df`), nous extrayons les valeurs distinctes de la colonne **`Description`**, en supprimant les doublons.
# 
# #### 🧠 Identification des nouvelles descriptions :
# Nous comparons les descriptions extraites avec celles déjà présentes dans la table **`Dim_Description`**, afin d’identifier uniquement celles qui n’ont pas encore été enregistrées.
# 
# #### 🆔 Attribution d’un identifiant unique :
# Les nouvelles descriptions identifiées se voient attribuer un
# 

# In[14]:


# Charger la table existante Dim_description
dfdimDescription_temp = spark.read.table("refined_gold.Dim_descriptions")

# Récupérer le dernier ID_description existant
MAXDescriptionID = dfdimDescription_temp.select(coalesce(max(col("ID_description")), lit(0)).alias("MAXDescriptionID")).first()[0]

# Extraire les descriptions uniques de df et les comparer avec la table existante
dfdimDescription_silver = df.dropDuplicates(["Description_breve"]).select(col("Description_breve"))

# Identifier les nouvelles descriptions qui n'existent pas encore dans Dim_description
dfdimDescription_gold = dfdimDescription_silver.join(dfdimDescription_temp, dfdimDescription_silver.Description_breve == dfdimDescription_temp.Description_breve, "left_anti")

# Ajouter un ID_description unique aux nouvelles descriptions
dfdimDescription_gold = dfdimDescription_gold.withColumn("ID_description", monotonically_increasing_id() + MAXDescriptionID + 1)

# Afficher les 10 premières lignes
dfdimDescription_gold.show(10)


# ### 🔄 Insertion des nouvelles descriptions dans la table `Dim_Description`
# 
# Cette étape permet d’ajouter les nouvelles descriptions à la table de dimension **`Dim_Description`** en utilisant une opération **MERGE** dans Delta Lake. Voici les étapes réalisées :
# 
# 1. **Référence à la table Delta existante** : Nous accédons à la table Delta existante **`Dim_Description`** via son chemin dans le système de fichiers (Delta Lake).
# 
# 2. **Préparation des données à insérer** : Un DataFrame nommé **`dfdimDescription_gold`** est préparé, contenant les nouvelles descriptions à insérer, accompagnées de leur identifiant unique **`ID_Description`**.
# 
# 3. **Opération MERGE** : Grâce à la méthode **`merge()`**, nous effectuons une opération de type *upsert* (mise à jour ou insertion). Si la description existe déjà dans la table, aucune modification n’est apportée. Si elle est absente, elle est insérée.
# 
# 4. **Insertion des nouvelles descriptions** : Les descriptions absentes de la table seront insérées avec les colonnes **`Description`** et **`ID_Description`**, garantissant ainsi l’unicité et la traçabilité des entrées.
# 

# In[15]:


#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Description
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_descriptions')

# Préparation des données à insérer   
dfUpdates = dfdimDescription_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Description_breve = updates.Description_breve'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Description_breve": "updates.Description_breve",
      "ID_description": "updates.ID_description"
    }
  ) \
  .execute()


# ### 🔨 Création de la table de dimension `Dim_Collaborateur`
# 
# Dans cette étape, nous créons la table de dimension **Dim_Collaborateur** dans notre **Lakehouse**. Cette table contiendra les informations relatives aux collaborateurs impliqués dans les différents processus (ex. : ouverture de tickets, exécution des MEP, etc.).
# 
# #### Objectifs :
# 1. **Création de la table** :  
#    - Si la table **Dim_Collaborateur** n’existe pas encore, elle est automatiquement créée.
# 
# 2. **Structure de la table** :
#    - **ID_Collaborateur** : Identifiant unique du collaborateur, de type **LongType**.
#    - **Ouvert par** : Nom complet ou identifiant du collaborateur, de type **StringType**.
# 
# Cette table facilitera l’analyse des actions menées par les collaborateurs, permettra une meilleure traçabilité des opérations et constituera une référence lors de la construction du modèle décisionnel.
# 
# #### Détails techniques :
# - **`DeltaTable.createIfNotExists()`** : Cette méthode permet de vérifier si la table existe déjà et la crée si besoin. Elle définit aussi la structure des colonnes et garantit leur intégrité dans l’environnement Delta Lake.
# - **Colonnes** :
#    - `ID_Collaborateur` : Sert d’identifiant technique unique pour chaque collaborateur.
#    - `Ouvert par` : Contient le nom du collaborateur ou son identifiant dans le système source.
# 
# Une fois cette table en place, elle pourra être intégrée dans les pipelines de transformation et utilisée dans les jointures avec d’autres entités (tickets, projets, tâches, etc.) pour enrichir les analyses décisionnelles.
# 

# In[16]:


# Création de la table de dimension Dim_Collaborateurs
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Collaborateur") \
    .addColumn("ID_Collaborateur", LongType()) \
    .addColumn("Ouvert_par", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Collaborateur`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Ouvert par`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Collaborateur`**. 
# 
# Nous utilisons la méthode **`dropDuplicates()`** afin d’éliminer les enregistrements redondants et de s'assurer que chaque collaborateur est unique dans la table.
# 
# Cette étape est essentielle pour maintenir l'intégrité des données et éviter les anomalies lors des jointures ou des analyses ultérieures.
# 

# In[17]:


# Suppression des doublons sur la colonne 'Ouvert_par'
dfdimCollaborateurs = df.dropDuplicates(["Ouvert_par"]).select(col("Ouvert_par"))
# Affichage des premières lignes du DataFrame
dfdimCollaborateurs.show(10)


# ### 🔄 Identification et ajout de nouveaux collaborateurs à la table de dimension `Dim_Collaborateur`
# 
# Ce processus permet de traiter les collaborateurs existants et d’ajouter ceux qui ne figurent pas encore dans la table de dimension **`Dim_Collaborateur`**. Voici les principales étapes réalisées :
# 
# #### 🧩 Chargement de la table existante :
# Nous commençons par charger la table **`Dim_Collaborateur`** existante depuis le Delta Lake, afin de récupérer les collaborateurs déjà enregistrés.
# 
# #### 🔍 Récupération du dernier ID :
# Nous récupérons la valeur maximale de la colonne **`ID_Collaborateur`** dans la table existante, ce qui permettra d’attribuer de nouveaux identifiants uniques aux nouveaux collaborateurs à insérer.
# 
# #### 🧹 Extraction des collaborateurs uniques :
# À partir du DataFrame source (`df`), nous extrayons les valeurs distinctes de la colonne **'Ouvert par'**, en supprimant les doublons.
# 
# #### 🧠 Identification des nouveaux collaborateurs :
# Nous comparons les collaborateurs extraits avec ceux déjà présents dans la table **`Dim_Collaborateur`**, afin d’identifier uniquement ceux qui n’ont pas encore été enregistrés.
# 
# #### 🆔 Attribution d’un identifiant unique :
# Les nouveaux collaborateurs identifiés se voient attribuer un **`ID_Collaborateur`** unique, en continuant la numérotation à partir du dernier identifiant existant.
# 
# #### 📊 Affichage des résultats :
# Enfin, nous affichons les **10 premières lignes** du DataFrame contenant les nouveaux collaborateurs prêts à être insérés dans la table de dimension.
# 

# In[18]:


# Charger la table existante Dim_Collaborateurs
dfdimCollaborateurs_temp = spark.read.table("refined_gold.Dim_Collaborateur")

# Récupérer le dernier ID_Collaborateur existant
MAXCollaborateurID = dfdimCollaborateurs_temp.select(coalesce(max(col("ID_Collaborateur")), lit(0)).alias("MAXCollaborateurID")).first()[0]

# Extraire les collaborateurs uniques de df et les comparer avec la table existante
dfdimCollaborateurs_silver = df.dropDuplicates(["Ouvert_par"]).select(col("Ouvert_par"))

# Identifier les nouveaux collaborateurs qui n'existent pas encore dans Dim_Collaborateurs
dfdimCollaborateurs_gold = dfdimCollaborateurs_silver.join(
    dfdimCollaborateurs_temp,
    dfdimCollaborateurs_silver["Ouvert_par"] == dfdimCollaborateurs_temp["Collaborateur"],
    "left_anti"
)

# Ajouter un ID_Collaborateur unique aux nouveaux collaborateurs
dfdimCollaborateurs_gold = dfdimCollaborateurs_gold.withColumn("ID_Collaborateur", monotonically_increasing_id() + MAXCollaborateurID + 1)

# Afficher les 10 premières lignes
dfdimCollaborateurs_gold.show(10)


# ### 🔄 Insertion des nouveaux collaborateurs dans la table `Dim_Collaborateur`
# 
# Cette étape permet d’ajouter les nouveaux collaborateurs à la table de dimension **`Dim_Collaborateur`** en utilisant une opération **MERGE** dans Delta Lake. Voici les étapes réalisées :
# 
# 1. **Référence à la table Delta existante** : Nous accédons à la table Delta existante **`Dim_Collaborateur`** via son chemin dans le système de fichiers (Delta Lake).
# 
# 2. **Préparation des données à insérer** : Un DataFrame nommé **`dfdimCollaborateur_gold`** est préparé, contenant les nouveaux collaborateurs à insérer, accompagnés de leur identifiant unique **`ID_Collaborateur`**.
# 
# 3. **Opération MERGE** : Grâce à la méthode **`merge()`**, nous effectuons une opération de type *upsert* (mise à jour ou insertion). Si le collaborateur existe déjà dans la table, aucune modification n’est apportée. Si elle est absente, elle est insérée.
# 
# 4. **Insertion des nouveaux collaborateurs** : Les collaborateurs absents de la table seront insérés avec les colonnes **`Ouvert par`** et **`ID_Collaborateur`**, garantissant ainsi l’unicité et la traçabilité des entrées.
# 

# In[19]:


#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Societe
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_collaborateur')

# Préparation des données à insérer   
dfUpdates = dfdimCollaborateurs_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Collaborateur = updates.Ouvert_par'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Collaborateur": "updates.Ouvert_par",
      "ID_Collaborateur": "updates.ID_Collaborateur"
    }
  ) \
  .execute()


# ### 🛠️ Renommage de colonne dans la table `Dim_Collaborateur`
# 
# Dans cette étape, nous effectuons une mise à jour structurelle de la table **Dim_Collaborateur** en renommant la colonne **`Ouvert_par`** en **`Collaborateur`** pour une meilleure cohérence sémantique dans notre modèle de données.
# 
# #### Étapes réalisées :
# 
# 1. **Lecture de la table Delta existante** :  
#    - Nous chargeons la table `Dim_Collaborateur` à partir de son emplacement dans le Delta Lake, afin d’effectuer les modifications nécessaires.
# 
# 2. **Renommage de la colonne** :  
#    - La colonne `Ouvert_par`, qui contient les informations sur les collaborateurs, est renommée en `Collaborateur` pour améliorer la lisibilité et l'uniformité des noms de colonnes à travers l’ensemble du modèle.
# 
# 3. **Écriture de la version mise à jour** :  
#    - La table modifiée est ensuite enregistrée en écrasant l’ancienne version à l’aide du mode **overwrite** avec l’option **overwriteSchema** activée. Cela permet de conserver l’intégrité du schéma tout en appliquant la modification.
# 
# Cette opération garantit que la structure de la table reste à jour avec les conventions de nommage adoptées, facilitant ainsi l'intégration avec d'autres tables du Lakehouse et l’analyse dans Power BI ou d'autres outils.
# 

# In[20]:


# Lire la table actuelle
df_table = spark.read.format("delta").load("Tables/dim_collaborateur")

# Renommer la colonne
df_table_renamed = df_table.withColumnRenamed("Ouvert_par", "Collaborateur")

# Écraser l’ancienne table avec la nouvelle version
df_table_renamed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/dim_collaborateur")


# In[21]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_collaborateur")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# ### 🔨 Création de la table de dimension `Dim_Plage_Horaires`
# 
# Dans cette étape, nous créons la table de dimension **Dim_Plage_Horaires** dans notre **Lakehouse**. Cette table contiendra les informations relatives aux plages horaires utilisées dans les différents processus (ex. : horaires d'ouverture de tickets, exécution de MEP, etc.).
# 
# #### Objectifs :
# 1. **Création de la table** :  
#    - Si la table **Dim_Plage_Horaires** n’existe pas encore, elle est automatiquement créée.
# 
# 2. **Structure de la table** :
#    - **ID_Plage_Horaires** : Identifiant unique de la plage horaire, de type **LongType**.
#    - **Plages_Horaires** : Libellé ou description de la tranche horaire, de type **StringType**.
# 
# Cette table facilitera l’analyse temporelle des activités, permettra une meilleure organisation des événements et constituera une référence solide pour la construction du modèle décisionnel.
# 
# #### Détails techniques :
# - **`DeltaTable.createIfNotExists()`** : Cette méthode permet de vérifier si la table existe déjà et la crée si besoin. Elle définit également la structure des colonnes et garantit leur intégrité dans l’environnement Delta Lake.
# - **Colonnes** :
#    - `ID_Plage_Horaire` : Sert d’identifiant technique unique pour chaque plage horaire.
#    - `Plage_Horaire` : Contient le descriptif de la tranche horaire ou la période d’activité.
# 
# Une fois cette table en place, elle pourra être intégrée dans les pipelines de transformation et utilisée dans les jointures avec d’autres entités (tickets, activités, incidents, etc.) pour enrichir les analyses temporelles.
# 

# In[22]:


# Création de la table de dimension Dim_Plage_Horaires
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Plage_Horaires") \
    .addColumn("ID_Plage_Horaires", LongType()) \
    .addColumn("Plages_horaires", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Plage_Horaires`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Plage_Horaire`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Plage_Horaires`**.
# 
# Nous utilisons la méthode **`dropDuplicates()`** afin d’éliminer les enregistrements redondants et de s'assurer que chaque plage horaire est unique dans la table.
# 
# Cette étape est essentielle pour maintenir l'intégrité des données et éviter les anomalies lors des jointures ou des analyses temporelles ultérieures.
# 

# In[23]:


# Suppression des doublons sur la colonne 'Ouvert_par'
dfdimPlageHoraire_silver = df.dropDuplicates(["Plages_horaires"]).select(col("Plages_horaires"))
# Affichage des premières lignes du DataFrame
dfdimPlageHoraire_silver.show(10)


# ### 🔄 Identification et ajout de nouvelles plages horaires à la table de dimension `Dim_Plage_Horaires`
# 
# Ce processus permet de traiter les plages horaires existantes et d’ajouter celles qui ne figurent pas encore dans la table de dimension **`Dim_Plage_Horaires`**. Voici les principales étapes réalisées :
# 
# #### 🧩 Chargement de la table existante :
# Nous commençons par charger la table **`Dim_Plage_Horaires`** existante depuis le Delta Lake, afin de récupérer les plages horaires déjà enregistrées.
# 
# #### 🔍 Récupération du dernier ID :
# Nous récupérons la valeur maximale de la colonne **`ID_Plage_Horaire`** dans la table existante, ce qui permettra d’attribuer de nouveaux identifiants uniques aux nouvelles plages horaires à insérer.
# 
# #### 🧹 Extraction des plages horaires uniques :
# À partir du DataFrame source (`df`), nous extrayons les valeurs distinctes de la colonne **`Plage_Horaire`**, en supprimant les doublons.
# 
# #### 🧠 Identification des nouvelles plages horaires :
# Nous comparons les plages horaires extraites avec celles déjà présentes dans la table **`Dim_Plage_Horaires`**, afin d’identifier uniquement celles qui n’ont pas encore été enregistrées.
# 
# #### 🆔 Attribution d’un identifiant unique :
# Les nouvelles plages horaires identifiées se voient attribuer un **`ID_Plage_Horaire`** unique, en continuant la numérotation à partir du dernier identifiant existant.
# 
# #### 📊 Affichage des résultats :
# Enfin, nous affichons les **10 premières lignes** du DataFrame contenant les nouvelles plages horaires prêtes à être insérées dans la table de dimension.
# 

# In[24]:


# Charger la table existante Dim_Collaborateurs
dfdimPlageHoraire_temp = spark.read.table("refined_gold.Dim_Plage_Horaires")

# Récupérer le dernier ID_Collaborateur existant
MAXPlageHoraireID = dfdimPlageHoraire_temp.select(coalesce(max(col("ID_Plage_Horaires")), lit(0)).alias("MAXPlageHoraireID")).first()[0]

# Extraire les collaborateurs uniques de df et les comparer avec la table existante
dfdimPlageHoraire_silver = df.dropDuplicates(["Plages_horaires"]).select(col("Plages_horaires"))

# Identifier les nouveaux collaborateurs qui n'existent pas encore dans Dim_Collaborateurs
dfdimPlageHoraire_gold = dfdimPlageHoraire_silver.join(
    dfdimPlageHoraire_temp,
    dfdimPlageHoraire_silver.Plages_horaires== dfdimPlageHoraire_temp.Plages_horaires,
    "left_anti"
)

# Ajouter un ID_Collaborateur unique aux nouveaux collaborateurs
dfdimPlageHoraire_gold = dfdimPlageHoraire_gold.withColumn("ID_Plage_Horaires", monotonically_increasing_id() + MAXPlageHoraireID + 1)

# Afficher les 10 premières lignes
dfdimPlageHoraire_gold.show(10)


# ### 🔄 Insertion des nouvelles plages horaires dans la table `Dim_Plage_Horaires`
# 
# Cette étape permet d’ajouter les nouvelles plages horaires à la table de dimension **`Dim_Plage_Horaires`** en utilisant une opération **MERGE** dans Delta Lake. Voici les étapes réalisées :
# 
# 1. **Référence à la table Delta existante** : Nous accédons à la table Delta existante **`Dim_Plage_Horaires`** via son chemin dans le système de fichiers (Delta Lake).
# 
# 2. **Préparation des données à insérer** : Un DataFrame nommé **`dfdimPlageHoraires_gold`** est préparé, contenant les nouvelles plages horaires à insérer, accompagnées de leur identifiant unique **`ID_Plage_Horaire`**.
# 
# 3. **Opération MERGE** : Grâce à la méthode **`merge()`**, nous effectuons une opération de type *upsert* (mise à jour ou insertion). Si la plage horaire existe déjà dans la table, aucune modification n’est apportée. Si elle est absente, elle est insérée.
# 
# 4. **Insertion des nouvelles plages horaires** : Les plages horaires absentes de la table seront insérées avec les colonnes **`Plage_Horaire`** et **`ID_Plage_Horaire`**, garantissant ainsi l’unicité et la traçabilité des entrées.
# 

# In[25]:


#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Description
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_plage_horaires')

# Préparation des données à insérer   
dfUpdates = dfdimPlageHoraire_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Plages_horaires = updates.Plages_horaires'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Plages_horaires": "updates.Plages_horaires",
      "ID_Plage_Horaires": "updates.ID_Plage_Horaires"
    }
  ) \
  .execute()


# In[26]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_plage_horaires")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# # Les Demandes

# #### Étapes de la transformation
# 
# 1. **Chargement des données depuis la couche Silver :**  
#    Les données sont lues depuis la table `cleansed_Silver.demandes_silver`, qui contient les informations nettoyées issues du traitement de la couche Bronze. Cette table constitue une base fiable pour effectuer des enrichissements et transformations supplémentaires dans les couches supérieures du Lakehouse.
# 

# In[27]:


dfd = spark.read.table("cleansed_Silver.demandes_silver")


# In[28]:


display(dfd.head(10))


# ### 🛠️ Création de la table de dimension `Dim_Date_Ouverture_d`
# 
# Dans cette étape, nous créons une nouvelle table de dimension `Dim_Date_Ouverture_d` dans le lakehouse. Cette table sera utilisée pour stocker des informations sur les dates d'ouverture des tickets dans une structure dimensionnelle.
# 
# #### Processus de création :
# 
# 1. **Vérification de l'existence de la table** : Nous utilisons la méthode `createIfNotExists` pour garantir que la table est créée uniquement si elle n'existe pas déjà.
# 2. **Définition du schéma** : La table contient plusieurs colonnes pour détailler les informations relatives à la date et à l'heure d'ouverture des tickets :
#    - `Ouvert` : Le timestamp de l'ouverture du ticket.
#    - `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, `Seconde` : Ces colonnes permettent de décomposer la date d'ouverture pour une analyse plus précise.
# 3. **Exécution de la création** : La méthode `execute()` permet de finaliser la création de la table.
# 
# Cette structure nous permettra de faire des jointures avec d'autres tables, comme les faits relatifs aux tickets, en utilisant des clés dimensionnelles basées sur la date et l'heure d'ouverture.
# 

# In[29]:


# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Date_Ouverture_d") \
    .addColumn("Cree", DateType()) \
    .addColumn("Jour", IntegerType()) \
    .addColumn("Mois", IntegerType()) \
    .addColumn("Annee", IntegerType()) \
    .addColumn("Heure", IntegerType()) \
    .addColumn("Minute", IntegerType()) \
    .addColumn("Seconde", IntegerType()) \
    .execute()


# ### 📅 Création et transformation de la table `Dim_Date_Ouverture_d`
# 
# Dans cette étape, nous préparons les données pour créer une table de dimension `Dim_Date_Ouverture_d`. Cette table sera utilisée pour stocker les informations détaillées sur la date et l'heure d'ouverture des tickets, et nous la préparons à l'aide de transformations spécifiques sur la colonne `Ouvert` (timestamp).
# 
# #### Processus de transformation :
# 
# 1. **Suppression des doublons** : Nous utilisons la méthode `dropDuplicates` pour éliminer les doublons dans la colonne `Ouvert`. Cela garantit qu'il n'y ait qu'une seule entrée par date d'ouverture.
# 2. **Création de nouvelles colonnes** : Nous extrayons plusieurs éléments temporels de la colonne `Ouvert` :
#    - `Jour` : Le jour du mois de l'ouverture (`dayofmonth`).
#    - `Mois` : Le mois de l'ouverture (`month`).
#    - `Annee` : L'année de l'ouverture (`year`).
#    - `Heure`, `Minute`, `Seconde` : L'heure, la minute, et la seconde de l'ouverture, respectivement.
# 3. **Tri des résultats** : Le DataFrame est ensuite trié par la colonne `Ouvert` pour assurer une organisation chronologique des données.
# #### Aperçu des données :
# 
# Le code utilise la méthode `show(10)` pour afficher les 10 premières lignes du DataFrame `dfdimDate_Ouverture`, ce qui permet de prévisualiser les résultats et vérifier que les transformations ont été appliquées correctement.
# 
# Cette table de dimension `Dim_Date_Ouverture_d` sera utilisée dans le modèle de données en étoile, où elle sera liée à une table de faits.
# 

# In[30]:


# On part du principe que df contient les colonnes "Ouvert" et "Ovrt_h"
dfdimDate_Ouverture_d = dfd.dropDuplicates(["Cree"]).select(
    col("Cree"),
    dayofmonth("Cree").alias("Jour"),
    month("Cree").alias("Mois"),
    year("Cree").alias("Annee"),
    split(col("Cree_h"), ":")[0].cast("int").alias("Heure"),
    split(col("Cree_h"), ":")[1].cast("int").alias("Minute"),
    split(col("Cree_h"), ":")[2].cast("int").alias("Seconde")
).orderBy("Cree")

dfdimDate_Ouverture_d.show(10)


# ### 🔄 Mise à jour ou insertion dans la table Delta `dim_date_ouverture_d`
# 
# Dans cette étape, nous effectuons une opération de **merge** entre le DataFrame `dfdimDate_Ouverture_d` (contenant les nouvelles données) et la table Delta `dim_date_ouverture_d` (qui contient les données existantes). Cette opération est effectuée dans deux scénarios :
# 
# 1. **Mise à jour des lignes existantes** : Si une ligne dans la table `dim_date_ouverture_d` correspond à une ligne du DataFrame sur la base de la colonne `Ouvert`, les colonnes `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde` de la table Delta sont mises à jour avec les nouvelles valeurs du DataFrame.
#    
# 2. **Insertion de nouvelles lignes** : Si une ligne n'existe pas déjà dans la table `dim_date_ouverture_d` (basée sur la colonne `Ouvert`), de nouvelles lignes sont insérées dans la table avec les valeurs correspondantes du DataFrame.
# 
# L'opération `merge` est un moyen efficace de maintenir la table de dimension à jour avec de nouvelles données tout en évitant les duplications. Cela garantit que la table contient les informations les plus récentes et évite d'avoir à réécrire toutes les données.
# 
# #### Fonctionnement du code :
# 
# - La table Delta est référencée avec `DeltaTable.forPath()`.
# - Une opération `merge` est ensuite effectuée entre la table existante (`gold`) et les nouvelles données (`updates`).
# - Le match est effectué sur la colonne `Ouvert` (date d'ouverture).
# - En cas de correspondance (`whenMatchedUpdate`), les colonnes de la table `dim_date_ouverture_d` sont mises à jour.
# - En cas de non-correspondance (`whenNotMatchedInsert`), de nouvelles lignes sont insérées.
# 
# Cette opération garantit que la table de dimension reste synchronisée avec les données entrantes et est prête à être utilisée dans le modèle en étoile pour les jointures avec d'autres tables de faits.
# 

# In[31]:


# Référence à la table Delta existante
deltaTable = DeltaTable.forPath(spark,'Tables/dim_date_ouverture_d')

# Préparation des données à insérer
dfUpdates = dfdimDate_Ouverture_d

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.Cree = updates.Cree'  # Correspondance sur la colonne timestamp
    ) \
    .whenMatchedUpdate(set={
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .whenNotMatchedInsert(values={
        'Cree': 'updates.Cree',
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .execute()


# ### 🕒 Transformation des données pour la table de dimension `Dim_Fermeture_D`
# 
# Dans cette étape, nous créons le DataFrame `dfdimDate_Fermeture_D` en appliquant diverses transformations à la colonne `Ferme` de notre DataFrame source. Cette table de dimension contient des informations sur la date et l'heure de fermeture des tickets.
# 
# #### Objectifs :
# 1. **Suppression des doublons** : Nous supprimons les doublons dans la colonne `Ferme` afin de ne conserver qu'une seule entrée pour chaque timestamp unique de fermeture.
# 2. **Extraction des composantes temporelles** : 
#    - Nous extrayons le jour, le mois, l'année, l'heure, la minute et la seconde de la colonne `Ferme` pour faciliter les analyses temporelles.
# 3. **Sélection et transformation des colonnes** : 
#    - Nous ajoutons les colonnes `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde` à notre DataFrame à partir de la colonne `Ferme`.
# 4. **Tri des données** : Les données sont triées par la colonne `Ferme` pour faciliter l'organisation temporelle.
# 
# #### Affichage des premières lignes :
# Nous utilisons la méthode `show(10)` pour afficher les 10 premières lignes du DataFrame transformé et vérifier que les transformations ont été effectuées correctement.
# 
# #### Utilisation future :
# Cette table pourra être utilisée pour effectuer des jointures avec d'autres tables dans le cadre de l'analyse des fermeture des tickets ou d'autres processus décisionnels dans notre modèle en étoile.
# 

# In[32]:


# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Fermeture_D") \
    .addColumn("Ferme", DateType()) \
    .addColumn("Jour", IntegerType()) \
    .addColumn("Mois", IntegerType()) \
    .addColumn("Annee", IntegerType()) \
    .addColumn("Heure", IntegerType()) \
    .addColumn("Minute", IntegerType()) \
    .addColumn("Seconde", IntegerType()) \
    .execute()


# ### 🔄 Fusion des données pour la table de dimension `Dim_Date_Fermeture`
# 
# Dans cette étape, nous utilisons la commande **MERGE** de Delta Lake pour mettre à jour ou insérer des données dans la table de dimension `Dim_Date_fermeture`.
# 
# #### Objectifs :
# 1. **Mise à jour de la table Delta** : 
#    - Nous mettons à jour les enregistrements existants dans la table `Dim_Date_Fermeture` si les dates de Fermeture (`ferme`) correspondent. Cela permet d'assurer que les valeurs des colonnes `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde` sont à jour.
#    
# 2. **Insertion de nouvelles données** :
#    - Si un enregistrement n'existe pas dans la table, il sera inséré avec les valeurs appropriées pour `Ferme`, `Jour`, `Mois`, `Annee`, `Heure`, `Minute`, et `Seconde`.
# 
# #### Détails de la fusion (MERGE) :
# - **Condition de correspondance** : Nous avons basé la correspondance sur la colonne `Ferme` afin de vérifier les lignes existantes dans la table.
# - **Mise à jour des lignes existantes** : Si une ligne existe déjà avec la même valeur dans `Mis_a_jour`, les autres colonnes seront mises à jour avec les valeurs correspondantes des nouvelles données.
# - **Insertion des nouvelles lignes** : Si aucune ligne correspondante n'est trouvée, une nouvelle ligne est insérée dans la table avec les nouvelles valeurs.
# 
# #### Résultat :
# Cette opération assure que la table `Dim_Date_MAJ` reste à jour avec les dernières informations de mise à jour et peut être utilisée dans des jointures avec d'autres tables de faits pour l'analyse.
# 

# In[33]:


# On part du principe que df contient les colonnes "Ouvert" et "Ovrt_h"
dfdimDate_Fermeture_d = dfd.dropDuplicates(["Ferme"]).select(
    col("Ferme"),
    dayofmonth("Ferme").alias("Jour"),
    month("Ferme").alias("Mois"),
    year("Ferme").alias("Annee"),
    split(col("Ferme_h"), ":")[0].cast("int").alias("Heure"),
    split(col("Ferme_h"), ":")[1].cast("int").alias("Minute"),
    split(col("Ferme_h"), ":")[2].cast("int").alias("Seconde")
).orderBy("Ferme")

dfdimDate_Fermeture_d.show(10)


# In[34]:


# Référence à la table Delta existante
deltaTable = DeltaTable.forPath(spark,'Tables/dim_fermeture_d')

# Préparation des données à insérer
dfUpdates = dfdimDate_Fermeture_d

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.Ferme = updates.Ferme'  # Correspondance sur la colonne timestamp
    ) \
    .whenMatchedUpdate(set={
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .whenNotMatchedInsert(values={
        'Ferme': 'updates.Ferme',
        'Jour': 'updates.Jour',
        'Mois': 'updates.Mois',
        'Annee': 'updates.Annee',
        'Heure': 'updates.Heure',
        'Minute': 'updates.Minute',
        'Seconde': 'updates.Seconde'
    }) \
    .execute()


# In[35]:


# Création de la table de dimension Dim_Description
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_descriptions_d") \
    .addColumn("ID_description", LongType()) \
    .addColumn("Description_breve", StringType()) \
    .execute()


# In[36]:


# Suppression des doublons sur la colonne 'Description_breve'
dfdimDescription= dfd.dropDuplicates(["Description_breve"]).select(col("Description_breve"))
# Affichage des premières lignes du DataFrame
dfdimDescription.show(10)


# In[37]:


# Charger la table existante Dim_description
dfdimDescription_temp = spark.read.table("refined_gold.Dim_descriptions_d")

# Récupérer le dernier ID_description existant
MAXDescriptionID = dfdimDescription_temp.select(coalesce(max(col("ID_description")), lit(0)).alias("MAXDescriptionID")).first()[0]

# Extraire les descriptions uniques de df et les comparer avec la table existante
dfdimDescription_silver = dfd.dropDuplicates(["Description_breve"]).select(col("Description_breve"))

# Identifier les nouvelles descriptions qui n'existent pas encore dans Dim_description
dfdimDescription_gold = dfdimDescription_silver.join(dfdimDescription_temp, dfdimDescription_silver.Description_breve == dfdimDescription_temp.Description_breve, "left_anti")

# Ajouter un ID_description unique aux nouvelles descriptions
dfdimDescription_gold = dfdimDescription_gold.withColumn("ID_description", monotonically_increasing_id() + MAXDescriptionID + 1)

# Afficher les 10 premières lignes
dfdimDescription_gold.show(10)


# In[38]:


#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Description
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_descriptions_d')

# Préparation des données à insérer   
dfUpdates = dfdimDescription_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Description_breve = updates.Description_breve'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Description_breve": "updates.Description_breve",
      "ID_description": "updates.ID_description"
    }
  ) \
  .execute()


# In[39]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_descriptions_d")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# In[40]:


# Création de la table de dimension Dim_Collaborateurs
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Collaborateur_d") \
    .addColumn("ID_Collaborateur", LongType()) \
    .addColumn("Affecte_a", StringType()) \
    .execute()


# In[41]:


# Suppression des doublons sur la colonne 'Ouvert_par'
dfdimCollaborateurs = dfd.dropDuplicates(["Affecte_a"]).select(col("Affecte_a"))
# Affichage des premières lignes du DataFrame
dfdimCollaborateurs.show(10)


# In[42]:


# Charger la table existante Dim_Collaborateurs
dfdimCollaborateurs_temp = spark.read.table("refined_gold.Dim_Collaborateur_d")

# Récupérer le dernier ID_Collaborateur existant
MAXCollaborateurID = dfdimCollaborateurs_temp.select(coalesce(max(col("ID_Collaborateur")), lit(0)).alias("MAXCollaborateurID")).first()[0]

# Extraire les collaborateurs uniques de df et les comparer avec la table existante
dfdimCollaborateurs_silver = dfd.dropDuplicates(["Affecte_a"]).select(col("Affecte_a"))

# Identifier les nouveaux collaborateurs qui n'existent pas encore dans Dim_Collaborateurs
dfdimCollaborateurs_gold = dfdimCollaborateurs_silver.join(
    dfdimCollaborateurs_temp,
    dfdimCollaborateurs_silver.Affecte_a == dfdimCollaborateurs_temp.Collaborateur,
    "left_anti"
)

# Ajouter un ID_Collaborateur unique aux nouveaux collaborateurs
dfdimCollaborateurs_gold = dfdimCollaborateurs_gold.withColumn("ID_Collaborateur", monotonically_increasing_id() + MAXCollaborateurID + 1)

# Afficher les 10 premières lignes
dfdimCollaborateurs_gold.show(10)


# In[43]:


#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Societe
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_collaborateur_d')

# Préparation des données à insérer   
dfUpdates = dfdimCollaborateurs_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Collaborateur = updates.Affecte_a'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Collaborateur": "updates.Affecte_a",
      "ID_Collaborateur": "updates.ID_Collaborateur"
    }
  ) \
  .execute()


# In[44]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_collaborateur_d")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# In[45]:


# Lire la table actuelle
df_table = spark.read.format("delta").load("Tables/dim_collaborateur_d")

# Renommer la colonne
df_table_renamed = df_table.withColumnRenamed("Affecte_a", "Collaborateur")

# Écraser l’ancienne table avec la nouvelle version
df_table_renamed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/dim_collaborateur_d")


# In[46]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_collaborateur_d")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# In[47]:


# Création de la table de dimension Dim_Plage_Horaires
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Plage_Horaires_D") \
    .addColumn("ID_Plage_Horaires", LongType()) \
    .addColumn("Plages_horaires", StringType()) \
    .execute()


# In[48]:


# Suppression des doublons sur la colonne 'Ouvert_par'
dfdimPlageHoraire = dfd.dropDuplicates(["Plages_horaires"]).select(col("Plages_horaires"))
# Affichage des premières lignes du DataFrame
dfdimPlageHoraire.show(10)


# In[49]:


# Charger la table existante Dim_Collaborateurs
dfdimPlageHoraire_temp = spark.read.table("refined_gold.Dim_Plage_Horaires_D")

# Récupérer le dernier ID_Collaborateur existant
MAXPlageHoraireID = dfdimPlageHoraire_temp.select(coalesce(max(col("ID_Plage_Horaires")), lit(0)).alias("MAXPlageHoraireID")).first()[0]

# Extraire les collaborateurs uniques de df et les comparer avec la table existante
dfdimPlageHoraire_silver = dfd.dropDuplicates(["Plages_horaires"]).select(col("Plages_horaires"))

# Identifier les nouveaux collaborateurs qui n'existent pas encore dans Dim_Collaborateurs
dfdimPlageHoraire_gold = dfdimPlageHoraire_silver.join(
    dfdimPlageHoraire_temp,
    dfdimPlageHoraire_silver.Plages_horaires== dfdimPlageHoraire_temp.Plages_horaires,
    "left_anti"
)

# Ajouter un ID_Collaborateur unique aux nouveaux collaborateurs
dfdimPlageHoraire_gold = dfdimPlageHoraire_gold.withColumn("ID_Plage_Horaires", monotonically_increasing_id() + MAXPlageHoraireID + 1)

# Afficher les 10 premières lignes
dfdimPlageHoraire_gold.show(10)


# In[50]:


#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Description
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_plage_horaires_d')

# Préparation des données à insérer   
dfUpdates = dfdimPlageHoraire_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Plages_horaires = updates.Plages_horaires'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Plages_horaires": "updates.Plages_horaires",
      "ID_Plage_Horaires": "updates.ID_Plage_Horaires"
    }
  ) \
  .execute()


# In[51]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("refined_gold.dim_plage_horaires_d")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# In[52]:


# Création de la table de dimension Dim_Type
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.Dim_Type") \
    .addColumn("ID_Type", LongType()) \
    .addColumn("Type", StringType()) \
    .execute()


# In[53]:


from pyspark.sql.functions import col

# Extraire les types distincts des deux sources
df_type_dfd = dfd.select(col("Type")).dropDuplicates()
df_type_df = df.select(col("Type")).dropDuplicates()

# Union des deux et suppression des doublons globaux
dfdimType = df_type_dfd.union(df_type_df).dropDuplicates()

dfdimType.show(10)


# In[54]:


from pyspark.sql.functions import col, coalesce, max, lit, monotonically_increasing_id

# Charger la table existante Dim_Type
dfdimType_temp = spark.read.table("refined_gold.Dim_Type")

# Récupérer le dernier ID_Type existant
MAXTypeID = dfdimType_temp.select(coalesce(max(col("ID_Type")), lit(0)).alias("MAXTypeID")).first()[0]

# Extraire les types uniques des deux tables
df_type_dfd = dfd.select(col("Type")).dropDuplicates()
df_type_df = df.select(col("Type")).dropDuplicates()

# Fusionner les deux DataFrames et supprimer les doublons globaux
dfdimType_silver = df_type_dfd.union(df_type_df).dropDuplicates()

# Identifier les nouveaux types qui n'existent pas encore dans Dim_Type
dfdimType_gold = dfdimType_silver.join(
    dfdimType_temp,
    dfdimType_silver.Type == dfdimType_temp.Type,
    "left_anti"
)

# Ajouter un ID_Type unique aux nouveaux types
dfdimType_gold = dfdimType_gold.withColumn("ID_Type", monotonically_increasing_id() + MAXTypeID + 1)

# Afficher les 10 premières lignes
dfdimType_gold.show(10)


# In[55]:


from delta.tables import *
#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Description
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_type')

# Préparation des données à insérer   
dfUpdates = dfdimType_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Type = updates.Type'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Type": "updates.Type",
      "ID_Type": "updates.ID_Type"
    }
  ) \
  .execute()


# In[56]:


# Charger la table Delta dans un DataFrame
df_loaded_t = spark.read.format("delta").table("refined_gold.dim_type")

# Afficher les 10 premières lignes de la table chargée
df_loaded_t.show(10)


# In[57]:


from pyspark.sql.types import *
from delta.tables import *
# Création de la table de fait factSupervision_gold    
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.factMep_gold") \
    .addColumn("Numero", StringType()) \
    .addColumn("Ouvert", DateType()) \
    .addColumn("Mis_a_jour", DateType()) \
    .addColumn("ID_description", LongType()) \
    .addColumn("Etat", StringType()) \
    .addColumn("ID_Collaborateur", LongType()) \
    .addColumn("Ovrt_h", StringType()) \
    .addColumn("Mis_h", StringType()) \
    .addColumn("ID_Plage_Horaires", LongType()) \
    .addColumn("ID_Type", LongType()) \
    .execute()


# In[58]:


from pyspark.sql.functions import col
from delta.tables import *

# Charger les tables de dimensions
dfdimDescriptions = spark.read.table("refined_gold.dim_descriptions")
dfdimCollaborateur = spark.read.table("refined_gold.dim_collaborateur")
dfdimPlageHoraires = spark.read.table("refined_gold.dim_plage_horaires")
dfdimType = spark.read.table("refined_gold.dim_type")

# Joindre les tables de dimensions avec les clés correspondantes
dffactMep_gold = df.alias("df1") \
    .join(dfdimDescriptions.alias("df2"), col("df1.Description_breve") == col("df2.Description_breve"), "left") \
    .join(dfdimCollaborateur.alias("df3"), col("df1.Ouvert_par") == col("df3.Collaborateur"), "left") \
    .join(dfdimPlageHoraires.alias("df4"), col("df1.Plages_horaires") == col("df4.Plages_horaires"), "left") \
    .join(dfdimType.alias("df5"), col("df1.Type") == col("df5.Type"), "left") \
    .select(
        col("df1.Numero"),
        col("df1.Ouvert"),
        col("df1.Mis_a_jour"),
        col("df1.Etat"),
        col("df1.Ovrt_h"),
        col("df1.Mis_h"),
        col("df2.ID_description"),
        col("df3.ID_Collaborateur"),
        col("df4.ID_Plage_Horaires"),
        col("df5.ID_Type")
    ) \
    .orderBy(col("df1.Ouvert"), col("df1.Numero"))

# Affichage des 10 premières lignes
display(dffactMep_gold.limit(10))


# In[59]:


from delta.tables import *

# Référence à la table Delta de faits (à adapter selon ton nom de table réel)
deltaTable = DeltaTable.forPath(spark, 'Tables/factmep_gold')

# Préparation des données à insérer
dfUpdates = dffactMep_gold

# MERGE dans la table Delta : mise à jour si le Numero existe, sinon insertion
deltaTable.alias('target') \
  .merge(
    dfUpdates.alias('updates'),
    'target.Numero = updates.Numero'
  ) \
  .whenMatchedUpdate(set =
    {
        "Ouvert": "updates.Ouvert",
        "Mis_a_jour": "updates.Mis_a_jour",
        "Etat": "updates.Etat",
        "Ovrt_h": "updates.Ovrt_h",
        "Mis_h": "updates.Mis_h",
        "ID_description": "updates.ID_description",
        "ID_Collaborateur": "updates.ID_Collaborateur",
        "ID_Plage_Horaires": "updates.ID_Plage_Horaires",
        "ID_Type": "updates.ID_Type"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
        "Numero": "updates.Numero",
        "Ouvert": "updates.Ouvert",
        "Mis_a_jour": "updates.Mis_a_jour",
        "Etat": "updates.Etat",
        "Ovrt_h": "updates.Ovrt_h",
        "Mis_h": "updates.Mis_h",
        "ID_description": "updates.ID_description",
        "ID_Collaborateur": "updates.ID_Collaborateur",
        "ID_Plage_Horaires": "updates.ID_Plage_Horaires",
        "ID_Type": "updates.ID_Type"
    }
  ) \
  .execute()


# In[60]:


display(spark.read.table("refined_gold.factmep_gold"))


# In[61]:


from pyspark.sql.types import *
from delta.tables import *
# Création de la table de fait factSupervision_gold    
DeltaTable.createIfNotExists(spark) \
    .tableName("refined_gold.factDemande_gold") \
    .addColumn("Cree", DateType()) \
    .addColumn("Ferme", DateType()) \
    .addColumn("Numero", StringType()) \
    .addColumn("ID_description", LongType()) \
    .addColumn("ID_Collaborateur", LongType()) \
    .addColumn("Task_Duration", LongType()) \
    .addColumn("Cree_h", StringType()) \
    .addColumn("Ferme_h", StringType()) \
    .addColumn("ID_Plage_Horaires", LongType()) \
    .addColumn("ID_Type", LongType()) \
    .execute()


# In[62]:


from pyspark.sql.functions import col
from delta.tables import *

# Charger les tables de dimensions
dfdimDescriptions = spark.read.table("refined_gold.dim_descriptions_d")
dfdimCollaborateur = spark.read.table("refined_gold.dim_collaborateur_d")
dfdimPlageHoraires = spark.read.table("refined_gold.dim_plage_horaires_d")
dfdimType = spark.read.table("refined_gold.dim_type")

# Joindre les tables de dimensions avec les clés correspondantes
dffactDemande_gold = dfd.alias("df1") \
    .join(dfdimDescriptions.alias("df2"), col("df1.Description_breve") == col("df2.Description_breve"), "left") \
    .join(dfdimCollaborateur.alias("df3"), col("df1.Affecte_a") == col("df3.Collaborateur"), "left") \
    .join(dfdimPlageHoraires.alias("df4"), col("df1.Plages_horaires") == col("df4.Plages_horaires"), "left") \
    .join(dfdimType.alias("df5"), col("df1.Type") == col("df5.Type"), "left") \
    .select(
        col("df1.Numero"),
        col("df1.Cree"),
        col("df1.Ferme"),
        col("df1.Task_Duration"),
        col("df1.Cree_h"),
        col("df1.Ferme_h"),
        col("df2.ID_description"),
        col("df3.ID_Collaborateur"),
        col("df4.ID_Plage_Horaires"),
        col("df5.ID_Type")
    ) \
    .orderBy(col("df1.Cree"), col("df1.Numero"))

# Affichage des 10 premières lignes
display(dffactDemande_gold.limit(10))


# In[63]:


from delta.tables import *

# Référence à la table Delta de faits (adaptée pour factDemande_gold)
deltaTable = DeltaTable.forName(spark, "refined_gold.factdemande_gold")

# Préparation des données à insérer
dfUpdates = dffactDemande_gold

# MERGE dans la table Delta : mise à jour si le Numero existe, sinon insertion
deltaTable.alias('target') \
  .merge(
    dfUpdates.alias('updates'),
    'target.Numero = updates.Numero'
  ) \
  .whenMatchedUpdate(set =
    {
        "Cree": "updates.Cree",
        "Ferme": "updates.Ferme",
        "Task_Duration": "updates.Task_Duration",
        "Cree_h": "updates.Cree_h",
        "Ferme_h": "updates.Ferme_h",
        "ID_description": "updates.ID_description",
        "ID_Collaborateur": "updates.ID_Collaborateur",
        "ID_Plage_Horaires": "updates.ID_Plage_Horaires",
        "ID_Type": "updates.ID_Type"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
        "Numero": "updates.Numero",
        "Cree": "updates.Cree",
        "Ferme": "updates.Ferme",
        "Task_Duration": "updates.Task_Duration",
        "Cree_h": "updates.Cree_h",
        "Ferme_h": "updates.Ferme_h",
        "ID_description": "updates.ID_description",
        "ID_Collaborateur": "updates.ID_Collaborateur",
        "ID_Plage_Horaires": "updates.ID_Plage_Horaires",
        "ID_Type": "updates.ID_Type"
    }
  ) \
  .execute()


# In[64]:


display(spark.read.table("refined_gold.factdemande_gold"))


# In[65]:


print(spark.read.table("refined_gold.factdemande_gold").count())

