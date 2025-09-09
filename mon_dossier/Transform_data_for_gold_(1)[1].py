#!/usr/bin/env python
# coding: utf-8

# ## Transform data for gold
# 
# New notebook

# ### 📊 Transformation des données de la couche Silver à la couche Gold
# 
# #### Objectif :
# Ce processus consiste à effectuer la transformation des données depuis la **couche Silver** vers la **couche Gold** dans le Lakehouse. La couche Silver contient des données nettoyées et enrichies, tandis que la couche Gold représente les données finales prêtes pour l'analyse et la visualisation. 
# 
# Dans cette étape, nous mettons en place le **schéma en étoile** en créant les **tables de dimension** et la **table de fait** qui alimenteront nos rapports analytiques.
# 
# #### Étapes de la transformation
# 
# 1. **Chargement des données depuis la couche Silver :**
#    Les données sont lues depuis la table `Inetum_Data.supervision_silver`, qui contient les informations de niveau intermédiaire, c'est-à-dire des données nettoyées mais non encore totalement agrégées ou enrichies.

# In[31]:


# Chargement des données depuis la couche Silver
df = spark.read.table("Inetum_Data.supervision_silver")


# In[32]:


# Chargement des données depuis la couche Silver
df = spark.read.table("Inetum_Data.supervision_silver")


# In[33]:


# Affiche les 10 premières lignes du DataFrame pour une inspection rapide
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

# In[34]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Date_Ouverture") \
    .addColumn("Ouvert", TimestampType()) \
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
# 
# #### Aperçu des données :
# 
# Le code utilise la méthode `show(10)` pour afficher les 10 premières lignes du DataFrame `dfdimDate_Ouverture`, ce qui permet de prévisualiser les résultats et vérifier que les transformations ont été appliquées correctement.
# 
# Cette table de dimension `Dim_Date_Ouverture` sera utilisée dans le modèle de données en étoile, où elle sera liée à une table de faits.
# 

# In[35]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *


# Créer le DataFrame pour Dim_Date_Ouverture avec les transformations
dfdimDate_Ouverture = df.dropDuplicates(["Ouvert"]).select(
    col("Ouvert"),
    dayofmonth("Ouvert").alias("Jour"),
    month("Ouvert").alias("Mois"),
    year("Ouvert").alias("Annee"),
    hour("Ouvert").alias("Heure"),
    minute("Ouvert").alias("Minute"),
    second("Ouvert").alias("Seconde")
).orderBy("Ouvert")

# Afficher les 10 premières lignes de la table chargée
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

# In[36]:


from delta.tables import *

# Référence à la table Delta existante 
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_date_ouverture')

# Préparation des données à insérer   
dfUpdates = dfdimDate_Ouverture

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.Ouvert = updates.Ouvert'  # Matching on the 'Ouvert' timestamp column
    ) \
    .whenMatchedUpdate(set =
        {
            
            'gold.Jour': 'updates.Jour',
            'gold.Mois': 'updates.Mois',
            'gold.Annee': 'updates.Annee',
            'gold.Heure': 'updates.Heure',
            'gold.Minute': 'updates.Minute',
            'gold.Seconde': 'updates.Seconde'
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "Ouvert": "updates.Ouvert",
            "Jour": "updates.Jour",
            "Mois": "updates.Mois",
            "Annee": "updates.Annee",
            "Heure": "updates.Heure",
            "Minute": "updates.Minute",
            "Seconde": "updates.Seconde"
        }
    ) \
    .execute()


# In[37]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("Inetum_Data.Dim_Date_Ouverture")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# ### 📅 Création de la table de dimension `Dim_Date_MAJ`
# 
# Dans cette étape, nous créons la table de dimension `Dim_Date_MAJ` dans le lakehouse en utilisant Delta Lake. Cette table sera utilisée pour stocker des informations sur la date et l'heure de la mise à jour des tickets, ce qui est essentiel pour les analyses temporelles dans le cadre du traitement des tickets ou d'autres processus décisionnels.
# 
# #### Structure de la table :
# La table `Dim_Date_MAJ` contient plusieurs colonnes, chacune représentant une unité de temps, afin de faciliter les agrégations par date ou par heure :
# - **Mis_a_jour** : La colonne principale de type `TimestampType()`, qui contient la date et l'heure de la mise à jour.
# - **Jour** : Le jour du mois extrait de la colonne `Mis_a_jour` (type `IntegerType()`).
# - **Mois** : Le mois extrait de la colonne `Mis_a_jour` (type `IntegerType()`).
# - **Annee** : L'année extraite de la colonne `Mis_a_jour` (type `IntegerType()`).
# - **Heure** : L'heure extraite de la colonne `Mis_a_jour` (type `IntegerType()`).
# - **Minute** : La minute extraite de la colonne `Mis_a_jour` (type `IntegerType()`).
# - **Seconde** : La seconde extraite de la colonne `Mis_a_jour` (type `IntegerType()`).
# 
# #### Fonctionnement du code :
# - La méthode `createIfNotExists()` est utilisée pour créer la table uniquement si elle n'existe pas déjà.
# - Les colonnes de la table sont définies à l'aide de la méthode `addColumn()`, où chaque colonne est spécifiée avec son nom et son type.
# - La table est ensuite exécutée avec `.execute()`, ce qui la crée dans le schéma spécifié (`Inetum_Data`).
# 
# Cette table pourra être utilisée pour effectuer des jointures avec d'autres tables dans le cadre de l'analyse des mises à jour des tickets ou d'autres processus décisionnels dans notre modèle en étoile.
# 

# In[38]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension 
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Date_MAJ") \
    .addColumn("Mis_a_jour", TimestampType()) \
    .addColumn("Jour", IntegerType()) \
    .addColumn("Mois", IntegerType()) \
    .addColumn("Annee", IntegerType()) \
    .addColumn("Heure", IntegerType()) \
    .addColumn("Minute", IntegerType()) \
    .addColumn("Seconde", IntegerType()) \
    .execute()


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

# In[39]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *


# Créer le DataFrame pour Dim_Date_Maj avec les transformations
dfdimDate_MAJ = df.dropDuplicates(["Mis_a_jour"]).select(
    col("Mis_a_jour"),
    dayofmonth("Mis_a_jour").alias("Jour"),
    month("Mis_a_jour").alias("Mois"),
    year("Mis_a_jour").alias("Annee"),
    hour("Mis_a_jour").alias("Heure"),
    minute("Mis_a_jour").alias("Minute"),
    second("Mis_a_jour").alias("Seconde")
).orderBy("Mis_a_jour")

# Afficher les 10 premières lignes de la table chargée
dfdimDate_MAJ.show(10)


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

# In[40]:


from delta.tables import *

# Référence à la table Delta existante 
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_date_maj')

# Préparation des données à insérer   
dfUpdates = dfdimDate_MAJ

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
    .merge(
        dfUpdates.alias('updates'),
        'gold.Mis_a_jour = updates.Mis_a_jour'  # Matching on the 'Ouvert' timestamp column
    ) \
    .whenMatchedUpdate(set =
        {
            
            'gold.Jour': 'updates.Jour',
            'gold.Mois': 'updates.Mois',
            'gold.Annee': 'updates.Annee',
            'gold.Heure': 'updates.Heure',
            'gold.Minute': 'updates.Minute',
            'gold.Seconde': 'updates.Seconde'
        }
    ) \
    .whenNotMatchedInsert(values =
        {
            "Mis_a_jour": "updates.Mis_a_jour",
            "Jour": "updates.Jour",
            "Mois": "updates.Mois",
            "Annee": "updates.Annee",
            "Heure": "updates.Heure",
            "Minute": "updates.Minute",
            "Seconde": "updates.Seconde"
        }
    ) \
    .execute()


# In[41]:


# Charger la table Delta dans un DataFrame
df_loaded = spark.read.format("delta").table("Inetum_Data.Dim_Date_MAJ")

# Afficher les 10 premières lignes de la table chargée
df_loaded.show(10)


# ### 🔨 Création de la table de dimension `Dim_Societe`
# 
# Dans cette étape, nous créons la table de dimension **Dim_Societe** dans notre **Lakehouse**. Cette table contiendra des informations sur les sociétés, telles que leur identifiant unique et le nom de la société.
# 
# #### Objectifs :
# 1. **Création de la table** : 
#    - Si la table **Dim_Societe** n'existe pas déjà, elle est créée automatiquement.
#    
# 2. **Structure de la table** :
#    - **ID_Societe** : Identifiant unique de la société, de type **LongType**.
#    - **Societe** : Nom de la société, de type **StringType**.
#    
# Cette table sera utilisée pour stocker des informations sur les sociétés, ce qui permettra de les lier à d'autres tables de faits lors des analyses décisionnelles et de l'exploration des données.
# 
# #### Détails techniques :
# - **`DeltaTable.createIfNotExists()`** : Cette fonction vérifie si la table existe déjà et la crée si elle ne le fait pas. Elle assure également que la structure de la table est définie avec les bonnes colonnes et types de données.
# - **Colonnes** :
#    - `ID_Societe` : Utilisé pour identifier de manière unique chaque société.
#    - `Societe` : Stocke le nom de la société.
# 
# Une fois cette table créée, elle pourra être utilisée dans des processus de transformation des données, notamment pour lier des informations de sociétés avec des données de tickets ou d'autres entités de notre modèle.
# 

# In[42]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Societe") \
    .addColumn("ID_Societe", LongType()) \
    .addColumn("Societe", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Societe`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Societe`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Societe`**. Nous utilisons la méthode **`dropDuplicates()`** pour supprimer les enregistrements redondants et nous nous assurons que chaque société est unique dans la table.

# In[43]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *
# Suppression des doublons sur la colonne 'Societe'
dfdimSociete= df.dropDuplicates(["Societe"]).select(col("Societe"))
# Affichage des premières lignes du DataFrame
dfdimSociete.show(10)


# ### 🔄 Identification et ajout de nouvelles sociétés à la table de dimension `Dim_Societe`
# 
# Ce code permet de traiter les sociétés existantes et d'ajouter les nouvelles sociétés qui ne figurent pas encore dans la table de dimension **`Dim_Societe`**. Voici les principales étapes effectuées :
# 
# 1. **Chargement de la table existante** : Nous commençons par charger la table **`Dim_Societe`** existante à partir de Delta Lake pour récupérer les sociétés déjà présentes.
# 
# 2. **Récupération du dernier ID** : Nous récupérons l'ID le plus élevé dans la table existante **`Dim_Societe`**, afin d'ajouter de nouveaux identifiants à la suite.
# 
# 3. **Extraction des sociétés uniques** : Nous extrayons les sociétés uniques à partir du DataFrame initial **`df`**, en supprimant les doublons sur la colonne **`Societe`**.
# 
# 4. **Identification des nouvelles sociétés** : Nous comparons ces sociétés extraites avec celles déjà présentes dans la table **`Dim_Societe`** pour identifier les sociétés qui n'existent pas encore.
# 
# 5. **Ajout d'un ID unique** : À ces nouvelles sociétés, nous attribuons un identifiant **`ID_Societe`** unique, en l'incrémentant par rapport au dernier identifiant existant.
# 
# 6. **Affichage des résultats** : Enfin, nous affichons les 10 premières lignes de la table mise à jour.
# 

# In[44]:


from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit

# Charger la table existante Dim_Societe
dfdimSociete_temp = spark.read.table("Inetum_Data.Dim_Societe")

# Récupérer le dernier ID_Societe existant
MAXSocieteID = dfdimSociete_temp.select(coalesce(max(col("ID_Societe")), lit(0)).alias("MAXSocieteID")).first()[0]

# Extraire les sociétés uniques de df et les comparer avec la table existante
dfdimSociete_silver = df.dropDuplicates(["Societe"]).select(col("Societe"))

# Identifier les nouvelles sociétés qui n'existent pas encore dans Dim_Societe
dfdimSociete_gold = dfdimSociete_silver.join(dfdimSociete_temp, dfdimSociete_silver.Societe == dfdimSociete_temp.Societe, "left_anti")

# Ajouter un ID_Societe unique aux nouvelles sociétés
dfdimSociete_gold = dfdimSociete_gold.withColumn("ID_Societe", monotonically_increasing_id() + MAXSocieteID + 1)

# Afficher les 10 premières lignes
dfdimSociete_gold.show(10)


# ### 🔄 Insertion des nouvelles sociétés dans la table `Dim_Societe`
# 
# Cette étape permet d'ajouter les nouvelles sociétés à la table de dimension **`Dim_Societe`** en utilisant une opération **MERGE** dans Delta Lake. Voici les étapes réalisées :
# 
# 1. **Référence à la table Delta existante** : Nous commençons par accéder à la table Delta existante **`Dim_Societe`** à partir de son chemin sur le système de fichiers.
# 
# 2. **Préparation des données à insérer** : Nous préparons un DataFrame **`dfdimSociete_gold`** contenant les nouvelles sociétés à ajouter, avec un identifiant unique **`ID_Societe`** pour chaque nouvelle société.
# 
# 3. **Opération MERGE** : Nous utilisons la méthode **`merge()`** pour effectuer une mise à jour ou une insertion selon que la société existe déjà dans la table ou non. Si une société existe déjà, elle ne sera pas modifiée (aucune mise à jour dans cette opération). Si la société n'existe pas, elle sera insérée dans la table avec son identifiant et son nom.
# 
# 4. **Insertion des nouvelles sociétés** : Les sociétés qui n'existent pas encore dans la table seront insérées avec les colonnes **`Societe`** et **`ID_Societe`**.
# 

# In[45]:


from delta.tables import *
#### Code de mise à jour et insertion :

# Référence à la table Delta existante Dim_Societe
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_societe')

# Préparation des données à insérer   
dfUpdates = dfdimSociete_gold

# Opération MERGE : mise à jour si existant, sinon insertion
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Societe = updates.Societe'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Societe": "updates.Societe",
      "ID_Societe": "updates.ID_Societe"
    }
  ) \
  .execute()


# In[46]:


spark.read.table("Inetum_Data.dim_societe").show()


# ### 🔄 Création de la table de dimension `Dim_Categorie`
# 
# Cette étape consiste à créer la table **`Dim_Categorie`**, une table de dimension utilisée pour stocker les informations relatives aux catégories. Voici les opérations effectuées :
# 
# 1. **Création de la table si elle n'existe pas déjà** : Nous utilisons la méthode **`createIfNotExists()`** pour créer la table **`Dim_Categorie`** dans le système Delta Lake si elle n'existe pas encore. Cette méthode garantit que la table sera créée uniquement si elle est absente.
# 
# 2. **Ajout des colonnes** :
#    - **`ID_Categorie`** : Un identifiant unique pour chaque catégorie, de type **Long**.
#    - **`Categorie`** : Le nom ou la description de la catégorie, de type **String**.
# 
# 3. **Exécution de la création de la table** : Enfin, la méthode **`execute()`** permet de créer la table avec les colonnes spécifiées dans le schéma.

# In[47]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Date_Categorie
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Categorie") \
    .addColumn("ID_Categorie", LongType()) \
    .addColumn("Categorie", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Categorie`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Categorie`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Categorie`**. Nous utilisons la méthode **`dropDuplicates()`** pour supprimer les enregistrements redondants et nous nous assurons que chaque société est unique dans la table.

# In[48]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *

# Suppression des doublons sur la colonne 'Categorie'
dfdimCategorie= df.dropDuplicates(["Categorie"]).select(col("Categorie"))
# Afficher les 10 premières lignes
dfdimCategorie.show(10)


# ### 🔄 Transformation et préparation des données pour la table de dimension `Dim_Categorie`
# 
# Dans cette étape, nous effectuons les transformations nécessaires pour préparer les données destinées à la table de dimension **`Dim_Categorie`**. Voici les principales opérations effectuées :
# 
# 1. **Chargement de la table existante** :
#    - Nous chargeons la table de dimension **`Dim_Categorie`** existante à partir de Delta Lake afin de pouvoir la comparer avec les nouvelles données.
# 
# 2. **Récupération du dernier `ID_Categorie`** :
#    - Nous récupérons le dernier identifiant **`ID_Categorie`** existant dans la table **`Dim_Categorie`**. Cela nous permet de générer de nouveaux identifiants uniques pour les nouvelles catégories.
# 
# 3. **Extraction des catégories uniques** :
#    - Nous utilisons **`dropDuplicates()`** pour extraire les catégories uniques de la DataFrame initiale **`df`**, puis nous sélectionnons uniquement la colonne **`Categorie`** pour la préparer à l'insertion dans la table de dimension.
# 
# 4. **Identification des nouvelles catégories** :
#    - Nous effectuons une jointure **`left_anti`** pour identifier les catégories présentes dans **`df`** mais pas encore dans la table **`Dim_Categorie`**. Cela permet de s'assurer que seules les nouvelles catégories sont insérées.
# 
# 5. **Attribution d'un ID unique** :
#    - Nous générons un nouvel **`ID_Categorie`** unique pour chaque nouvelle catégorie en utilisant **`monotonically_increasing_id()`** et en l'ajustant avec l'ID maximum existant pour éviter les conflits.
# 
# 6. **Affichage des données** :
#    - Enfin, nous affichons les 10 premières lignes du DataFrame pour vérifier les résultats avant l'insertion dans la table.
# 

# In[49]:


from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit

# Charger la table existante Dim_Categorie
dfdimCategorie_temp = spark.read.table("Inetum_Data.Dim_Categorie")

# Récupérer le dernier ID_Categorie existant
MAXCategorieID = dfdimCategorie_temp.select(coalesce(max(col("ID_Categorie")), lit(0)).alias("MAXCategorieID")).first()[0]

# Extraire les Categories uniques de df et les comparer avec la table existante
dfdimCategorie_silver = df.dropDuplicates(["Categorie"]).select(col("Categorie"))

# Identifier les nouvelles Categories qui n'existent pas encore dans Dim_Categorie
dfdimCategorie_gold = dfdimCategorie_silver.join(dfdimCategorie_temp, dfdimCategorie_silver.Categorie == dfdimCategorie_temp.Categorie, "left_anti")

# Ajouter un ID_Categorie unique aux nouvelles Categories
dfdimCategorie_gold = dfdimCategorie_gold.withColumn("ID_Categorie", monotonically_increasing_id() + MAXCategorieID + 1)

# Afficher les 10 premières lignes
dfdimCategorie_gold.show(10)


# ### 🔄 Insertion des nouvelles catégories dans la table de dimension `Dim_Categorie`
# 
# Dans cette étape, nous effectuons l'insertion des nouvelles catégories dans la table de dimension **`Dim_Categorie`** à l'aide de la commande **`MERGE`** de Delta Lake. Voici les opérations effectuées :
# 
# 1. **Chargement de la table Delta existante** :
#    - Nous chargeons la table **`Dim_Categorie`** existante à partir de Delta Lake pour pouvoir effectuer les mises à jour ou insertions nécessaires.
# 
# 2. **Fusion des données** :
#    - Nous utilisons la méthode **`merge`** pour comparer les données existantes dans **`Dim_Categorie`** avec les nouvelles données dans **`dfdimCategorie_gold`**. Cela nous permet d'effectuer les mises à jour ou les insertions de manière efficace.
# 
# 3. **Mise à jour des données existantes** :
#    - La clause **`whenMatchedUpdate`** est utilisée pour mettre à jour les lignes existantes lorsque la catégorie correspondante est trouvée. Cependant, dans ce cas particulier, aucune mise à jour spécifique n'est effectuée, car la structure de la table ne nécessite pas de modifications supplémentaires.
# 
# 4. **Insertion des nouvelles catégories** :
#    - La clause **`whenNotMatchedInsert`** est utilisée pour insérer les nouvelles catégories dans la table **`Dim_Categorie`**. Si une catégorie n'existe pas encore dans la table, elle sera ajoutée avec son identifiant unique (**`ID_Categorie`**).
# 

# In[50]:


from delta.tables import *
# Charger la table existante Dim_Categorie

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_categorie')

# DataFrame contenant les mises à jour
   
dfUpdates = dfdimCategorie_gold

# Exécution du MERGE pour insérer les nouvelles catégories
   
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Categorie = updates.Categorie'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Categorie": "updates.Categorie",
      "ID_Categorie": "updates.ID_Categorie"
    }
  ) \
  .execute()


# In[51]:


spark.read.table("Inetum_Data.dim_categorie").show()


# ### 🔄 Création de la table de dimension `Dim_Etat`
# 
# Dans cette étape, nous créons la table de dimension **`Dim_Etat`** dans Delta Lake. Cette table contiendra les informations relatives aux différents états, avec un identifiant unique pour chaque état. Voici les étapes principales :
# 
# 1. **Vérification de l'existence de la table** :
#    - Nous utilisons la méthode **`createIfNotExists`** de Delta Lake pour vérifier si la table **`Dim_Etat`** existe déjà. Si elle n'existe pas, elle est créée.
# 
# 2. **Ajout des colonnes** :
#    - Nous définissons deux colonnes principales pour cette table :
#      - **`ID_Etat`** : un identifiant unique pour chaque état, de type **`LongType`**.
#      - **`Etat`** : le nom de l'état, de type **`StringType`**.

# In[52]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Etat
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Etat") \
    .addColumn("ID_Etat", LongType()) \
    .addColumn("Etat", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Etat`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Etat`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Etat`**. Nous utilisons la méthode **`dropDuplicates()`** pour supprimer les enregistrements redondants et nous nous assurons que chaque société est unique dans la table.

# In[53]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *

# Suppression des doublons sur la colonne 'Etat'
dfdimEtat= df.dropDuplicates(["Etat"]).select(col("Etat"))
# Afficher les 10 premières lignes
dfdimEtat.show(10)


# ### 🔄 Traitement des données pour la dimension `Dim_Etat`
# 
# Dans cette section, nous générons les données de la table de dimension **`Dim_Etat`** à partir des données sources, tout en évitant les doublons et en attribuant un identifiant unique à chaque nouvel état. Voici les étapes principales :
# 
# 1. **Chargement de la table existante** :
#    - La table **`Dim_Etat`** est chargée depuis le catalogue Delta afin de vérifier les états déjà présents.
# 
# 2. **Récupération du dernier identifiant** :
#    - On extrait le dernier identifiant **`ID_Etat`** existant pour continuer la numérotation sans doublons.
# 
# 3. **Détection des nouveaux états** :
#    - On identifie les états présents dans le DataFrame source mais absents de la table actuelle grâce à une jointure en mode **left anti**.
# 
# 4. **Attribution des identifiants** :
#    - Pour chaque nouvel état, un identifiant unique est généré dynamiquement en s'appuyant sur la fonction **`monotonically_increasing_id()`**, en l'incrémentant à partir du dernier ID existant.
# 
# 5. **Aperçu des données** :
#    - On affiche les 10 premières lignes pour valider visuellement les transformations réalisées.
# 
# Ce processus assure une intégration continue et sans doublons des nouveaux états dans la table de dimension.
# 

# In[54]:


from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit

# Charger la table existante Dim_Etat
dfdimEtat_temp = spark.read.table("Inetum_Data.Dim_Etat")

# Récupérer le dernier ID_Etat existant
MAXEtatID = dfdimEtat_temp.select(coalesce(max(col("ID_Etat")), lit(0)).alias("MAXEtatID")).first()[0]

# Extraire les etats uniques de df et les comparer avec la table existante
dfdimEtat_silver = df.dropDuplicates(["Etat"]).select(col("Etat"))

# Identifier les nouvelles etats qui n'existent pas encore dans Dim_Etat
dfdimEtat_gold = dfdimEtat_silver.join(dfdimEtat_temp, dfdimEtat_silver.Etat == dfdimEtat_temp.Etat, "left_anti")

# Ajouter un ID_Etat unique aux nouvelles Etat
dfdimEtat_gold = dfdimEtat_gold.withColumn("ID_Etat", monotonically_increasing_id() + MAXEtatID + 1)

# Afficher les 10 premières lignes
dfdimEtat_gold.show(10)


# ### 🔄 Insertion des nouvelles valeurs dans la table de dimension `Dim_Etat`
# 
# Dans cette section, nous mettons à jour la table de dimension **`Dim_Etat`** avec les nouvelles valeurs identifiées précédemment.
# 
# - Nous utilisons la méthode **`merge()`** de Delta Lake pour insérer uniquement les nouveaux enregistrements dans la table Delta.
# - La condition de correspondance est basée sur la colonne **`Etat`**, afin de vérifier si une valeur existe déjà dans la table cible.
# - Les lignes **non présentes** dans la table (`whenNotMatchedInsert`) sont insérées avec les colonnes **`Etat`** et **`ID_Etat`**.
# - Aucun traitement n'est effectué pour les lignes correspondantes déjà existantes (`whenMatchedUpdate` vide), ce qui garantit que seules les nouvelles valeurs sont prises en compte.
# 
# Cette approche assure une alimentation incrémentale de la dimension **`Etat`**, tout en évitant les doublons.
# 

# In[55]:


from delta.tables import *
# Charger la table existante Dim_etat

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_etat')

# DataFrame contenant les mises à jour
  
dfUpdates = dfdimEtat_gold

# Exécution du MERGE pour insérer les nouvelles etats
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Etat = updates.Etat'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Etat": "updates.Etat",
      "ID_Etat": "updates.ID_Etat"
    }
  ) \
  .execute()


# In[56]:


spark.read.table("Inetum_Data.dim_etat").show()


# ### 🗂️ Création de la table de dimension `Dim_Priorite`
# 
# Dans cette section, nous créons la table de dimension **`Dim_Priorite`** à l'aide de la méthode **`createIfNotExists()`** de Delta Lake. Cette table contiendra les différentes valeurs de priorité présentes dans les tickets, ainsi qu'un identifiant unique associé à chacune d'elles.
# 
# Les colonnes définies sont :
# - `ID_Priorite` : identifiant unique de la priorité (type `Long`),
# - `Priorite` : libellé de la priorité (type `String`).
# 
# Cette table s'inscrit dans la construction du schéma en étoile et pourra être utilisée dans les analyses pour catégoriser les tickets selon leur niveau de priorité.
# 

# In[57]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Priorite") \
    .addColumn("ID_Priorite", LongType()) \
    .addColumn("Priorite", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Priorite`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Priorite`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Priorite`**. Nous utilisons la méthode **`dropDuplicates()`** pour supprimer les enregistrements redondants et nous nous assurons que chaque société est unique dans la table.

# In[58]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *
# Suppression des doublons sur la colonne 'Priorite'
dfdimPriorite= df.dropDuplicates(["Priorite"]).select(col("Priorite"))
# Afficher les 10 premières lignes
dfdimPriorite.show(10)


# In[59]:


dfdimPriorite.show(10)


# ### 🔄 Transformation des données pour la table `Dim_Priorite`
# 
# Dans cette section, nous mettons à jour la table de dimension **`Dim_Priorite`** en identifiant les nouvelles priorités issues des données sources. Le processus se déroule comme suit :
# 
# 1. Chargement de la table existante `Dim_Priorite`.
# 2. Récupération du dernier identifiant utilisé (`ID_Priorite`).
# 3. Extraction des priorités uniques depuis la source.
# 4. Identification des nouvelles priorités absentes de la table actuelle.
# 5. Attribution d’un identifiant unique à chaque nouvelle priorité à l’aide de `monotonically_increasing_id`.
# 
# Ce traitement garantit que seules les nouvelles valeurs sont ajoutées à la table de dimension, évitant ainsi les doublons tout en maintenant l’intégrité des identifiants.
# 

# In[60]:


from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit

# Charger la table existante Dim_Priorite
dfdimPriorite_temp = spark.read.table("Inetum_Data.Dim_Priorite")

# Récupérer le dernier ID_Priorite existant
MAXPrioriteID = dfdimPriorite_temp.select(coalesce(max(col("ID_Priorite")), lit(0)).alias("MAXPrioriteID")).first()[0]

# Extraire les priorites uniques de df et les comparer avec la table existante
dfdimPriorite_silver = df.dropDuplicates(["Priorite"]).select(col("Priorite"))

# Identifier les nouvelles priorites qui n'existent pas encore dans Dim_Priorite
dfdimPriorite_gold = dfdimPriorite_silver.join(dfdimPriorite_temp, dfdimPriorite_silver.Priorite == dfdimPriorite_temp.Priorite, "left_anti")

# Ajouter un ID_Priorite unique aux nouvelles priorites
dfdimPriorite_gold = dfdimPriorite_gold.withColumn("ID_Priorite", monotonically_increasing_id() + MAXPrioriteID + 1)

# Afficher les 10 premières lignes
dfdimPriorite_gold.show(10)


# ### 🧩 Mise à jour incrémentale de la table de dimension `Dim_Priorite`
# 
# Dans cette section, nous mettons à jour la table de dimension **`Dim_Priorite`** en utilisant l’instruction **`merge()`** de Delta Lake. L’objectif est d’insérer uniquement les nouvelles valeurs de priorité qui ne sont pas encore présentes dans la table existante :
# 
# - La condition de jointure se base sur la colonne `Priorite`.
# - Si une priorité existe déjà, aucune mise à jour n’est effectuée.
# - Si une priorité est absente, elle est insérée avec un identifiant unique (`ID_Priorite`).
# 
# Ce mécanisme garantit l’unicité des entrées dans la table de dimension.
# 

# In[61]:


from delta.tables import *

# Charger la table existante Dim_priorite

deltaTable = DeltaTable.forPath(spark, 'Tables/dim_priorite')

# DataFrame contenant les mises à jour
 
dfUpdates = dfdimPriorite_gold

# Exécution du MERGE pour insérer les nouvelles Priorite

deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Priorite = updates.Priorite'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Priorite": "updates.Priorite",
      "ID_Priorite": "updates.ID_Priorite"
    }
  ) \
  .execute()


# In[62]:


# Charger la table Delta dans un DataFrame
df_priorite=spark.read.table("Inetum_Data.dim_priorite")
# Afficher les 10 premières lignes
df_priorite.show()


# In[63]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Groupe_affectation") \
    .addColumn("ID_Groupe_affectation", LongType()) \
    .addColumn("Groupe_affectation", StringType()) \
    .execute()


# In[64]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *

dfdimGroupe_affectation= df.dropDuplicates(["Groupe_affectation"]).select(col("Groupe_affectation"))
dfdimGroupe_affectation.show(10)


# In[65]:


from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit

# Charger la table existante Dim_Societe
dfdimGroupe_affectation_temp = spark.read.table("Inetum_Data.Dim_Groupe_affectation")

# Récupérer le dernier ID_Societe existant
MAXGroupe_affectationID = dfdimGroupe_affectation_temp.select(coalesce(max(col("ID_Groupe_affectation")), lit(0)).alias("MAXGroupe_affectationID")).first()[0]

# Extraire les sociétés uniques de df et les comparer avec la table existante
dfdimGroupe_affectation_silver = df.dropDuplicates(["Groupe_affectation"]).select(col("Groupe_affectation"))

# Identifier les nouvelles sociétés qui n'existent pas encore dans Dim_Societe
dfdimGroupe_affectation_gold = dfdimGroupe_affectation_silver.join(dfdimGroupe_affectation_temp, dfdimGroupe_affectation_silver.Groupe_affectation == dfdimGroupe_affectation_temp.Groupe_affectation, "left_anti")

# Ajouter un ID_Societe unique aux nouvelles sociétés
dfdimGroupe_affectation_gold = dfdimGroupe_affectation_gold.withColumn("ID_Groupe_affectation", monotonically_increasing_id() + MAXGroupe_affectationID + 1)

# Afficher les 10 premières lignes
dfdimGroupe_affectation_gold.show(10)


# In[66]:


# Charger la table Delta existante
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_groupe_affectation')

# Effectuer le merge des données
dfUpdates = dfdimGroupe_affectation_gold

deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Groupe_affectation = updates.Groupe_affectation'
  ) \
  .whenMatchedUpdate(set =
    {
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "Groupe_affectation": "updates.Groupe_affectation",
      "ID_Groupe_affectation": "updates.ID_Groupe_affectation",
    }
  ) \
  .execute()


# In[67]:


df_check = spark.read.format("delta").table("Inetum_Data.dim_groupe_affectation")
df_check.show(10)


# ### 🗂️ Création de la table de dimension `Dim_Groupe_aff`
# 
# Dans cette section, nous créons la table de dimension **`Dim_Groupe_aff`** à l'aide de la méthode **`createIfNotExists()`** de Delta Lake. Cette table contiendra les différents groupes d'affectation et leur type, ainsi qu'un identifiant unique associé à chacun d'eux.
# 
# Les colonnes définies sont :
# - `ID_Groupe_affectation` : identifiant unique du groupe d'affectation (type `Long`),
# - `Groupe_affectation` : libellé du groupe d'affectation (type `String`),
# - `Type_Groupe` : type du groupe d'affectation (type `String`).
# 
# Cette table s'inscrit dans la construction du schéma en étoile et pourra être utilisée dans les analyses pour catégoriser les tickets en fonction de leur groupe d'affectation.
# 

# In[68]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Groupe_aff
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Groupe_aff") \
    .addColumn("ID_Groupe_affectation", LongType()) \
    .addColumn("Groupe_affectation", StringType()) \
    .addColumn("Type_Groupe", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Groupe_aff`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Groupe_affectation`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Groupe_aff`**. Nous utilisons la méthode **`dropDuplicates()`** pour supprimer les enregistrements redondants et nous nous assurons que chaque société est unique dans la table.

# In[69]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *
# Suppression des doublons sur la colonne 'Priorite'
dfdimGroupe_affectation= df.dropDuplicates(["Groupe_affectation"]).select(col("Groupe_affectation"))
# Afficher les 10 premières lignes de la table chargée
dfdimGroupe_affectation.show(10)


# ### 🧑‍💼 Définition et classification des groupes d'affectation
# 
# Dans cette section, nous procédons à la classification des groupes d'affectation en fonction de leur domaine d'activité : **Exploitation**, **Supervision**, ou **Autre**.
# 
# #### Étapes suivies :
# 1. **Définition des groupes** :
#    - Nous avons d'abord défini deux listes de groupes :
#      - **`groupes_exploitation`** : comprend tous les groupes relatifs à l'exploitation des systèmes et applications.
#      - **`groupes_supervision`** : comprend les groupes associés à la supervision des opérations.
# 
# 2. **Suppression des doublons** :
#    - Nous avons utilisé la méthode **`dropDuplicates()`** pour éliminer les doublons dans la colonne **`Groupe_affectation`**, ne conservant qu'une occurrence unique de chaque groupe.
# 
# 3. **Ajout de la colonne `Type_Groupe`** :
#    - Une nouvelle colonne **`Type_Groupe`** a été ajoutée à notre DataFrame, classant chaque groupe en fonction de sa présence dans les listes de groupes définies. La classification est effectuée avec la méthode **`when()`**, en attribuant la valeur :
#      - **`Exploitation`** pour les groupes présents dans **`groupes_exploitation`**,
#      - **`Supervision`** pour les groupes présents dans **`groupes_supervision`**,
#      - **`Autre`** pour les groupes qui ne figurent dans aucune des deux listes.
# 
# 

# In[70]:


from pyspark.sql.functions import col, when, lit

# Définition des groupes
groupes_exploitation = [
    "ASDF - Exploitation Applicative",
    "ASDF - Exploitation Téléphonie",
    "IO - Sécurité Opérationnelle",
    "ASDF - Système, Stockage, SGBD",
    "ASDF - Réseau et sécurité",
    "ASDF - Cloud",
    "IO - Support TOURS",
    "IO - Run PEGA/KOFAX ASP",
    "IO - Marine Support BackOffice",
    "IO - Data Factory",
    "IO - Distribution Legacy",
    "IO - Santé Prévoyance Collective",
    "IO - Santé Prévoyance individuelle",
    "MOI APRIL Marine",
    "MOI APRIL International Care France",
    "TMA - BULL",
    "DD - MarketPlace",
    "DD - Distribution Digitale",
    "IO - Etudes - IARD WAF",
    "ASDF - Environnement Utilisateurs",
    "IO - AICF_DEV"
]

groupes_supervision = [
    "ASDF - Supervision SAM",
    "SCIS TOOLING SUP/HYP",
    "SCIS M&C MA1",
    "SCIS M&C TL",
    "SCIS RUNOPS MA"
]

# Nettoyage des doublons et ajout de la colonne Type_Groupe
dfdimGroupe_affectation = df.dropDuplicates(["Groupe_affectation"]).select(col("Groupe_affectation"))

dfdimGroupe_affectation = dfdimGroupe_affectation.withColumn(
    "Type_Groupe",
    when(col("Groupe_affectation").isin(groupes_exploitation), lit("Exploitation"))
    .when(col("Groupe_affectation").isin(groupes_supervision), lit("Supervision"))
    .otherwise(lit("Autre"))
)

dfdimGroupe_affectation.show(10)


# ### 🔄 Identification et ajout des nouvelles lignes pour la table de dimension `Dim_Groupe_aff`
# 
# Dans cette étape, nous identifions les nouvelles lignes à insérer dans la table de dimension **`Dim_Groupe_aff`** en utilisant une jointure avec la table existante et en générant un nouvel identifiant unique pour chaque entrée.
# 
# #### Étapes suivies :
# 1. **Chargement de la table existante** :
#    - La table de dimension **`Dim_Groupe_aff`** est chargée à partir de Delta Lake dans un DataFrame temporaire (**`dfdimGroupe_affectation_temp`**).
# 
# 2. **Récupération du dernier identifiant existant** :
#    - Nous récupérons le dernier **`ID_Groupe_affectation`** en utilisant **`max()`** et **`coalesce()`** pour gérer les valeurs nulles. Cette valeur est utilisée pour générer de nouveaux identifiants uniques.
# 
# 3. **Identification des nouvelles lignes** :
#    - Une jointure de type **`left_anti`** est effectuée entre le DataFrame des groupes d'affectation (**`dfdimGroupe_affectation`**) et la table existante **`Dim_Groupe_aff`** afin d'identifier les nouvelles lignes (les groupes qui ne figurent pas encore dans la table).
# 
# 4. **Ajout du nouvel identifiant unique** :
#    - Un identifiant unique est généré pour chaque nouvelle ligne en utilisant la fonction **`monotonically_increasing_id()`** et en ajoutant le dernier **`ID_Groupe_affectation`** récupéré.
# 

# In[71]:


from pyspark.sql.functions import monotonically_increasing_id, coalesce, max

# Charger la table existante
dfdimGroupe_affectation_temp = spark.read.table("Inetum_Data.Dim_Groupe_aff")

# Récupérer le dernier ID
MAXGroupe_affectationID = dfdimGroupe_affectation_temp.select(
    coalesce(max(col("ID_Groupe_affectation")), lit(0)).alias("MAXGroupe_affectationID")
).first()[0]

# Identifier les nouvelles lignes
dfdimGroupe_affectation_gold = dfdimGroupe_affectation.join(
    dfdimGroupe_affectation_temp,
    "Groupe_affectation",
    "left_anti"
)

# Ajouter un nouvel ID
dfdimGroupe_affectation_gold = dfdimGroupe_affectation_gold.withColumn(
    "ID_Groupe_affectation",
    monotonically_increasing_id() + MAXGroupe_affectationID + 1
)

dfdimGroupe_affectation_gold.show(10)


# ### 🔄 Mise à jour et insertion des données dans la table `Dim_Groupe_aff`
# 
# Dans cette étape, nous effectuons un **MERGE** sur la table de dimension **`Dim_Groupe_aff`** pour mettre à jour les lignes existantes et insérer les nouvelles lignes identifiées dans l'étape précédente.
# 
# #### Étapes suivies :
# 1. **Chargement de la table Delta** :
#    - La table **`Dim_Groupe_aff`** est chargée depuis Delta Lake en utilisant la méthode **`forPath()`** de DeltaTable.
# 
# 2. **Exécution du MERGE** :
#    - Une jointure est effectuée entre les données existantes dans la table **`gold`** et les données mises à jour (**`updates`**) à partir de **`dfdimGroupe_affectation_gold`**. 
#    - Si une correspondance est trouvée sur la colonne **`Groupe_affectation`**, la colonne **`Type_Groupe`** est mise à jour avec la nouvelle valeur.
#    - Si aucune correspondance n'est trouvée (c'est-à-dire si le groupe d'affectation est nouveau), une nouvelle ligne est insérée avec les valeurs des colonnes **`Groupe_affectation`**, **`ID_Groupe_affectation`**, et **`Type_Groupe`**.
# 

# In[72]:


from delta.tables import *

# Charger la table Delta
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_groupe_aff')

# Effectuer le merge
deltaTable.alias('gold') \
  .merge(
    dfdimGroupe_affectation_gold.alias('updates'),
    'gold.Groupe_affectation = updates.Groupe_affectation'
  ) \
  .whenMatchedUpdate(set =
    {
        "Type_Groupe": "updates.Type_Groupe"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
        "Groupe_affectation": "updates.Groupe_affectation",
        "ID_Groupe_affectation": "updates.ID_Groupe_affectation",
        "Type_Groupe": "updates.Type_Groupe"
    }
  ) \
  .execute()


# In[73]:


# Lecture de la table Delta 'Dim_Groupe_aff' depuis la base de données et affichage de son contenu
df_groupe = spark.read.table("Inetum_Data.Dim_Groupe_aff").show()


# ### 🗂️ Création de la table de dimension `Dim_Ouvert_par`
# 
# Dans cette section, nous créons la table de dimension **`Dim_Ouvert_par`** à l'aide de la méthode **`createIfNotExists()`** de Delta Lake. Cette table contiendra les différentes valeurs de la colonne **`Ouvert_par`**, qui représente l'entité ou la personne ayant ouvert le ticket, ainsi qu'un identifiant unique associé à chacune d'elles.
# 
# Les colonnes définies sont :
# - `ID_Ouvert_par` : identifiant unique de l'entité ayant ouvert le ticket (type `Long`),
# - `Ouvert_par` : libellé de l'entité ayant ouvert le ticket (type `String`).
# 
# Cette table sera utilisée dans le cadre du traitement des tickets pour identifier l'entité responsable de l'ouverture de chaque ticket.
# 

# In[74]:


from pyspark.sql.types import *
from delta.tables import *

# Création de la table de dimension Dim_Date_Ouverture
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.Dim_Ouvert_par") \
    .addColumn("ID_Ouvert_par", LongType()) \
    .addColumn("Ouvert_par", StringType()) \
    .execute()


# ### 🔄 Suppression des doublons pour la table de dimension `Dim_Ouvert_par`
# 
# Dans cette étape, nous nous concentrons sur la suppression des doublons dans la colonne **`Ouvert_par`** de notre DataFrame. L'objectif est de garantir qu'il n'y ait pas de valeurs dupliquées avant d'utiliser ces données pour construire la table de dimension **`Dim_Ouvert_par`**. Nous utilisons la méthode **`dropDuplicates()`** pour supprimer les enregistrements redondants et nous nous assurons que chaque société est unique dans la table.

# In[75]:


from pyspark.sql.functions import col, dayofmonth, month, year, hour, minute, second
from pyspark.sql import SparkSession
from delta.tables import *
# Suppression des doublons sur la colonne 'Priorite'
dfdimOuvert_par= df.dropDuplicates(["Ouvert_par"]).select(col("Ouvert_par"))
# Afficher les 10 premières lignes de la table chargée
dfdimOuvert_par.show(10)


# ### 🛠️ Transformation des données et ajout de l'ID dans la table `Dim_Ouvert_par`
# 
# Dans cette section, nous préparons les données pour la table de dimension **`Dim_Ouvert_par`** en effectuant plusieurs étapes de transformation et de manipulation :
# 
# 1. **Chargement de la table existante** :
#    - Nous chargeons la table **`Dim_Ouvert_par`** existante à partir du DataFrame **`dfdimOuvert_par_temp`**.
# 
# 2. **Récupération du dernier identifiant** :
#    - Nous récupérons le dernier identifiant `ID_Ouvert_par` de la table existante, en prenant la valeur maximale ou en utilisant 0 si la table est vide.
# 
# 3. **Suppression des doublons** :
#    - Nous supprimons les doublons dans la colonne **`Ouvert_par`** et sélectionnons les sociétés uniques, ce qui permet de garder uniquement les entités distinctes.
# 
# 4. **Identification des nouvelles valeurs** :
#    - Nous identifions les entités **`Ouvert_par`** qui n'existent pas encore dans la table **`Dim_Ouvert_par`** en utilisant une jointure **`left_anti`**.
# 
# 5. **Ajout d'un identifiant unique** :
#    - Un nouvel identifiant unique **`ID_Ouvert_par`** est généré pour les nouvelles valeurs **`Ouvert_par`** à l'aide de la fonction **`monotonically_increasing_id()`**.
# 
# 6. **Affichage des premières lignes** :
#    - Nous affichons les 10 premières lignes du DataFrame résultant pour vérifier les données après la transformation.
# 
# Le résultat de cette étape est un DataFrame **`dfdimOuvert_par_gold`** contenant les nouvelles entités **`Ouvert_par`** avec des identifiants uniques, prêtes à être insérées ou mises à jour dans la table de dimension **`Dim_Ouvert_par`**.
# 

# In[76]:


from pyspark.sql.functions import monotonically_increasing_id, col, coalesce, max, lit

# Charger la table existante Dim_Ouvert_par
dfdimOuvert_par_temp = spark.read.table("Inetum_Data.Dim_Ouvert_par")

# Récupérer le dernier ID_Ouvert_par existant
MAXOuvert_parID = dfdimOuvert_par_temp.select(coalesce(max(col("ID_Ouvert_par")), lit(0)).alias("MAXOuvert_par")).first()[0]

# Extraire les Ouvert_par uniques de df et les comparer avec la table existante
dfdimOuvert_par_silver = df.dropDuplicates(["Ouvert_par"]).select(col("Ouvert_par"))

# Identifier les nouvelles Ouvert_par qui n'existent pas encore dans Dim_Ouvert_par
dfdimOuvert_par_gold = dfdimOuvert_par_silver.join(dfdimOuvert_par_temp, dfdimOuvert_par_silver.Ouvert_par == dfdimOuvert_par_temp.Ouvert_par, "left_anti")

# Ajouter un ID_Ouvert_par unique aux nouvelles sociétés
dfdimOuvert_par_gold = dfdimOuvert_par_gold.withColumn("ID_Ouvert_par", monotonically_increasing_id() + MAXOuvert_parID + 1)

# Afficher les 10 premières lignes
dfdimOuvert_par_gold.show(10)


# ### 🔄 Mise à jour et insertion des données dans la table `Dim_Ouvert_par`
# 
# Dans cette section, nous mettons à jour ou insérons les données dans la table **`Dim_Ouvert_par`** en utilisant la commande **`MERGE`** de Delta Lake pour gérer les nouvelles valeurs. Voici les étapes clés :
# 
# 1. **Chargement de la table Delta** :
#    - Nous chargeons la table Delta **`dim_ouvert_par`** en utilisant **`DeltaTable.forPath()`** pour manipuler les données dans la table existante.
# 
# 2. **Préparation des données à mettre à jour** :
#    - Nous préparons le DataFrame **`dfdimOuvert_par_gold`** (les nouvelles lignes à insérer) pour le **`MERGE`** avec la table existante **`dim_ouvert_par`**.
# 
# 3. **Merge - Mise à jour ou insertion** :
#    - **Quand une correspondance est trouvée** : Actuellement, aucun champ n'est mis à jour lors de la correspondance (la section `whenMatchedUpdate` est vide). Cela pourrait être utilisé pour mettre à jour des colonnes spécifiques si nécessaire.
#    - **Quand aucune correspondance n'est trouvée** : Si la valeur **`Ouvert_par`** n'existe pas encore dans la table, elle est insérée dans la table avec son **`ID_Ouvert_par`**.
# 
# Le résultat de cette opération est la mise à jour de la table **`Dim_Ouvert_par`** avec les nouvelles entités **`Ouvert_par`** qui ne sont pas encore présentes dans la table.
# 

# In[77]:


from delta.tables import *
# Charger la table Delta
deltaTable = DeltaTable.forPath(spark, 'Tables/dim_ouvert_par')
# DataFrame contenant les mises à jour    
dfUpdates = dfdimOuvert_par_gold
# Effectuer le merge    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Ouvert_par = updates.Ouvert_par'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "Ouvert_par": "updates.Ouvert_par",
      "ID_Ouvert_par": "updates.ID_Ouvert_par"
    }
  ) \
  .execute()


# In[78]:


# Lecture de la table Delta 'Dim_Groupe_aff' depuis la base de données et affichage de son contenu
df_Ouvert_par = spark.read.table("Inetum_Data.dim_ouvert_par").show()


# ### 🗂️ Création de la table de fait `factSupervision_gold`
# 
# Dans cette section, nous créons la table de fait **`factSupervision_gold`** à l'aide de la méthode **`createIfNotExists()`** de Delta Lake. Cette table de fait contient les informations relatives aux tickets supervisés, avec un identifiant pour chaque dimension associée à un ticket.
# 
# Les colonnes définies sont :
# 
# - `Numero` : le numéro du ticket (type `String`),
# - `N_du_ticket_externe` : le numéro externe du ticket (type `String`),
# - `Ouvert` : la date et l'heure d'ouverture du ticket (type `Timestamp`),
# - `Mis_a_jour` : la date et l'heure de la dernière mise à jour du ticket (type `Timestamp`),
# - `ID_Societe` : identifiant de la société associée au ticket (type `Long`),
# - `ID_Priorite` : identifiant de la priorité du ticket (type `Long`),
# - `ID_Groupe_affectation` : identifiant du groupe d'affectation (type `Long`),
# - `ID_Etat` : identifiant de l'état du ticket (type `Long`),
# - `ID_Ouvert_par` : identifiant de l'entité ayant ouvert le ticket (type `Long`),
# - `ID_Categorie` : identifiant de la catégorie du ticket (type `Long`).
# 
# Cette table représente une vue d'ensemble des tickets supervisés, avec des clés étrangères pointant vers les tables de dimensions correspondantes. Elle sera utilisée pour les analyses permettant de suivre l'état des tickets, leur priorité, leur catégorie, et d'autres attributs associés. Elle fait partie du processus de construction du schéma en étoile et est essentielle pour les rapports de supervision et les analyses opérationnelles.
# 

# In[79]:


from pyspark.sql.types import *
from delta.tables import *
# Création de la table de fait factSupervision_gold    
DeltaTable.createIfNotExists(spark) \
    .tableName("Inetum_Data.factSupervision_gold") \
    .addColumn("Numero", StringType()) \
    .addColumn("N_du_ticket_externe", StringType()) \
    .addColumn("Ouvert", TimestampType()) \
    .addColumn("Mis_a_jour", TimestampType()) \
    .addColumn("ID_Societe", LongType()) \
    .addColumn("ID_Priorite", LongType()) \
    .addColumn("ID_Groupe_affectation", LongType()) \
    .addColumn("ID_Etat", LongType()) \
    .addColumn("ID_Ouvert_par", LongType()) \
    .addColumn("ID_Categorie", LongType()) \
    .execute()


# ### 🗂️ Jointure des tables de dimensions dans `factSupervision_gold`
# 
# Dans cette section, nous effectuons la jointure des tables de dimensions avec les clés correspondantes pour créer une table de faits **`factSupervision_gold`**. Nous chargeons les tables de dimensions existantes, puis nous les joignons avec les données de faits (ici représentées par le DataFrame `df`). Chaque table de dimension est associée à une clé correspondante pour enrichir les informations sur chaque ticket.
# 
# Les étapes sont les suivantes :
# 1. Chargement des tables de dimensions depuis **`Inetum_Data`** : 
#    - `dim_societe`
#    - `dim_priorite`
#    - `dim_groupe_aff`
#    - `dim_etat`
#    - `dim_ouvert_par`
#    - `dim_categorie`
# 
# 2. Jointure des données de faits avec ces dimensions sur les clés correspondantes, telles que la société, la priorité, l'affectation, l'état, l'entité ayant ouvert le ticket, et la catégorie.
# 
# 3. Sélection des colonnes nécessaires dans la table de faits et organisation des résultats.
# 
# 4. Affichage des 10 premières lignes du DataFrame résultant.
# 

# In[80]:


from pyspark.sql.functions import col
from delta.tables import *

# Charger les tables de dimensions
dfdimSociete = spark.read.table("Inetum_Data.dim_societe")
dfdimPriorite = spark.read.table("Inetum_Data.dim_priorite")
dfdimGroupeAffectation = spark.read.table("Inetum_Data.dim_groupe_affectation")
dfdimEtat = spark.read.table("Inetum_Data.dim_etat")
dfdimOuvertPar = spark.read.table("Inetum_Data.dim_ouvert_par")
dfdimCategorie = spark.read.table("Inetum_Data.dim_categorie")

# Joindre les tables de dimensions avec les clés correspondantes
dffactSupervision_gold = df.alias("df1") \
    .join(dfdimSociete.alias("df2"), col("df1.Societe") == col("df2.Societe"), "left") \
    .join(dfdimPriorite.alias("df3"), col("df1.Priorite") == col("df3.Priorite"), "left") \
    .join(dfdimGroupeAffectation.alias("df4"), col("df1.Groupe_affectation") == col("df4.Groupe_affectation"), "left") \
    .join(dfdimEtat.alias("df5"), col("df1.Etat") == col("df5.Etat"), "left") \
    .join(dfdimOuvertPar.alias("df6"), col("df1.Ouvert_par") == col("df6.Ouvert_par"), "left") \
    .join(dfdimCategorie.alias("df7"), col("df1.Categorie") == col("df7.Categorie"), "left") \
    .select(
        col("df1.Numero"),
        col("df1.N_du_ticket_externe"),
        col("df1.Ouvert"),
        col("df1.Mis_a_jour"),
        col("df2.ID_Societe"),
        col("df3.ID_Priorite"),
        col("df4.ID_Groupe_affectation"),
        col("df5.ID_Etat"),
        col("df6.ID_Ouvert_par"),
        col("df7.ID_Categorie")
    ) \
    .orderBy(col("df1.Ouvert"), col("df1.Numero"))

# Affichage des 10 premières lignes
display(dffactSupervision_gold.limit(10))


# ### 🗂️ Mise à jour de la table de faits `factsupervision_gold` avec de nouvelles données
# 
# Dans cette section, nous effectuons la mise à jour de la table de faits **`factsupervision_gold`** en utilisant la méthode **`merge()`** de Delta Lake. Cette opération permet soit d'insérer de nouvelles lignes, soit de mettre à jour les lignes existantes dans la table de faits en fonction de la clé primaire `Numero` des tickets.
# 
# Les étapes sont les suivantes :
# 1. **Chargement de la table Delta** : Nous chargeons la table **`factsupervision_gold`** existante à partir du chemin spécifié.
# 2. **Définition des nouvelles données** : Nous définissons le DataFrame **`dfUpdates`**, qui contient les nouvelles données que nous souhaitons insérer ou mettre à jour dans la table de faits.
# 3. **Mise à jour ou insertion** :
#    - **Mise à jour** : Si une ligne avec un `Numero` correspondant existe déjà dans la table, les colonnes sont mises à jour avec les nouvelles valeurs.
#    - **Insertion** : Si aucune ligne avec le même `Numero` n'est trouvée, une nouvelle ligne est insérée dans la table.
# 4. **Exécution du `merge`** : L'opération **`merge()`** est effectuée, ce qui garantit que les données dans **`factsupervision_gold`** sont soit mises à jour, soit ajoutées selon les conditions spécifiées.
# 

# In[81]:


from delta.tables import *

# Charger la table Delta
deltaTable = DeltaTable.forPath(spark, 'Tables/factsupervision_gold')

# Définir les nouvelles données mises à jour
dfUpdates = dffactSupervision_gold

# Effectuer le merge pour insérer ou mettre à jour les données
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.Numero = updates.Numero'
  ) \
  .whenMatchedUpdate(set =
    {
      "N_du_ticket_externe": col("updates.N_du_ticket_externe"),
      "Ouvert": col("updates.Ouvert"),
      "Mis_a_jour": col("updates.Mis_a_jour"),
      "ID_Societe": col("updates.ID_Societe"),
      "ID_Priorite": col("updates.ID_Priorite"),
      "ID_Groupe_affectation": col("updates.ID_Groupe_affectation"),
      "ID_Etat": col("updates.ID_Etat"),
      "ID_Ouvert_par": col("updates.ID_Ouvert_par"),
      "ID_Categorie": col("updates.ID_Categorie")
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "Numero": col("updates.Numero"),
      "N_du_ticket_externe": col("updates.N_du_ticket_externe"),
      "Ouvert": col("updates.Ouvert"),
      "Mis_a_jour": col("updates.Mis_a_jour"),
      "ID_Societe": col("updates.ID_Societe"),
      "ID_Priorite": col("updates.ID_Priorite"),
      "ID_Groupe_affectation": col("updates.ID_Groupe_affectation"),
      "ID_Etat": col("updates.ID_Etat"),
      "ID_Ouvert_par": col("updates.ID_Ouvert_par"),
      "ID_Categorie": col("updates.ID_Categorie")
    }
  ) \
  .execute()


# ### 🗂️ Sélectionner et afficher les 1000 premières lignes de `factsupervision_gold`
# 
# Dans cette section, nous exécutons une requête SQL pour récupérer les 1000 premières lignes de la table **`factsupervision_gold`**. Ensuite, nous affichons les résultats sous forme de DataFrame.

# In[82]:


df = spark.sql("SELECT * FROM Inetum_Data.factsupervision_gold LIMIT 1000")
display(df)


# In[83]:


df = spark.sql("SELECT * FROM Inetum_Data.dim_etat LIMIT 1000")
display(df)


# In[ ]:




