# Supply Chain Intelligence Lakehouse

## Présentation du projet

Ce projet met en œuvre une plateforme Data Engineering orientée logistique et Supply Chain en utilisant Azure Databricks, Azure Data Lake Storage Gen2, Delta Lake et Apache Spark.

La plateforme permet d’ingérer des données logistiques de manière incrémentale, de les organiser selon une architecture Bronze / Silver / Gold, d’appliquer des contrôles de qualité, de reconstruire le cycle de vie des expéditions à partir d’événements, de produire des tables analytiques Gold et d’orchestrer l’ensemble du pipeline avec Databricks Workflows.

Le projet a été conçu comme un prototype Data Engineering réaliste, avec un fort accent sur :

- l’ingestion incrémentale ;
- l’idempotence ;
- Delta Lake ;
- la Data Quality ;
- l’analyse des plans d’exécution Spark ;
- l’orchestration ;
- l’observabilité.

---

## Architecture

```text
Logistics Data Sources
        ↓
Azure Data Lake Storage Gen2
Landing Zone
        ↓
Databricks Auto Loader
        ↓
Bronze Delta Layer
        ↓
PySpark / Spark SQL Transformations
        ↓
Silver Delta Layer
        ↓
Data Quality & Quarantine
        ↓
Delta MERGE / Incremental Processing
        ↓
Shipment Lifecycle Reconstruction
        ↓
Gold Delta Layer
        ↓
<<<<<<< Updated upstream
Databricks SQL
```

L’ensemble du pipeline est orchestré avec Databricks Workflows.

Une couche d’observabilité simple permet également de suivre les exécutions via :

```text
pipeline_runs
```

---

## Stack technique

- Python
- SQL
- Apache Spark
- PySpark
- Azure Databricks
- Delta Lake
- Azure Data Lake Storage Gen2
- Databricks Auto Loader
- Spark SQL
- Databricks SQL
- Databricks Workflows
- Git
- GitHub

---

## Modèle de données

La plateforme traite quatre jeux de données principaux.

### Orders

Principales colonnes :

```text
order_id
customer_id
order_time
destination_city
destination_country
total_amount
currency
order_status
updated_at
```

### Shipments

Principales colonnes :

```text
shipment_id
order_id
carrier_id
shipment_time
expected_delivery_time
actual_delivery_time
origin_city
destination_city
distance_km
shipment_status
updated_at
```

### Delivery Events

Principales colonnes :

```text
event_id
shipment_id
event_time
event_status
location_city
location_country
source_system
updated_at
```

Chaque shipment peut contenir plusieurs événements représentant son cycle de vie :

```text
CREATED
→ PACKED
→ SHIPPED
→ HUB_SCAN
→ OUT_FOR_DELIVERY
→ DELIVERED
```

### Carriers

Principales colonnes :

```text
carrier_id
carrier_name
carrier_type
country
service_level
active
updated_at
```

---

## Organisation du Data Lake

Le container ADLS Gen2 est structuré de la manière suivante :

```text
logistics/

├── landing/
├── bronze/
├── silver/
├── gold/
├── quality/
└── checkpoints/
```

---

## Couche Bronze

La couche Bronze contient les données proches de leur format source.

Principaux datasets :

```text
bronze/orders
bronze/shipments
bronze/delivery_events
bronze/carriers
```

Des métadonnées techniques sont ajoutées pendant l’ingestion :

```text
source_file
ingestion_timestamp
batch_id
```

Ces colonnes permettent notamment :

- la traçabilité ;
- l’auditabilité ;
- le suivi des batches ;
- le debugging ;
- le retraitement si nécessaire.

---

## Ingestion incrémentale avec Auto Loader

L’ingestion Bronze est réalisée avec Databricks Auto Loader.

Le pipeline utilise notamment :

```python
spark.readStream.format("cloudFiles")
```

avec :

```python
trigger(availableNow=True)
```

Cela permet d’utiliser la logique de Structured Streaming pour faire de l’ingestion incrémentale par micro-batch, sans maintenir un streaming continu.

Chaque source possède son propre checkpoint, par exemple :

```text
checkpoints/bronze_orders/
checkpoints/bronze_shipments/
checkpoints/bronze_delivery_events/
checkpoints/bronze_carriers/
```

Le checkpoint permet à Spark de mémoriser les fichiers déjà traités.

Ainsi, lorsqu’un notebook d’ingestion est relancé, les fichiers déjà consommés ne sont pas réingérés.

---

## Couche Silver

La couche Silver contient des données nettoyées, standardisées, dédupliquées et validées.

Principaux datasets :

```text
silver/orders
silver/shipments
silver/delivery_events
silver/carriers
silver/shipment_lifecycle
```

Les principaux traitements incluent :

- normalisation des schémas ;
- normalisation des timestamps ;
- harmonisation des statuts ;
- validation des types ;
- gestion des valeurs manquantes ;
- déduplication ;
- contrôles d’intégrité référentielle ;
- validation de règles métier ;
- enrichissements ;
- joins ;
- reconstruction du cycle de vie des expéditions.

---

## Déduplication

La déduplication est réalisée avec les Window Functions de Spark.

La logique utilisée est :

```text
partition by business key
order by updated_at DESC
then ingestion_timestamp DESC
```

Les clés métier utilisées sont :

```text
orders          → order_id
shipments       → shipment_id
delivery_events → event_id
carriers        → carrier_id
```

Cette logique permet de conserver la version la plus récente d’un enregistrement métier.

---

## Data Quality

Le projet met en place des règles de Data Quality pendant la construction de la couche Silver.

Exemples de règles :

```text
shipment_id IS NOT NULL
event_time IS NOT NULL
distance_km > 0
expected_delivery_time >= shipment_time
actual_delivery_time >= shipment_time
carrier_id doit exister
order_id doit exister
status doit appartenir à une liste autorisée
```

Les enregistrements invalides sont placés dans des datasets de quarantaine sous :

```text
quality/dq_invalid_records/
```

Les métriques de Data Quality incluent notamment :

```text
records_checked
valid_records
invalid_records
duplicate_records
dq_pass_rate
```

Exemples d’erreurs gérées :

```text
INVALID_TOTAL_AMOUNT
INVALID_ORDER_STATUS
INVALID_CURRENCY
ORDER_NOT_FOUND
CARRIER_NOT_FOUND
EXPECTED_DELIVERY_BEFORE_SHIPMENT
SHIPMENT_NOT_FOUND
INVALID_EVENT_STATUS
```

---

## Traitement incrémental et Delta MERGE

Le projet démontre également la gestion des nouvelles données et des corrections métier avec Delta Lake MERGE.

La logique est la suivante :

```text
business key existante + updated_at plus récent
→ UPDATE

nouvelle business key
→ INSERT
```

Cette logique a été testée sur :

```text
orders
shipments
delivery_events
```

Le MERGE utilise notamment :

```python
whenMatchedUpdateAll(
    condition="source.updated_at > target.updated_at"
)
```

et :

```python
whenNotMatchedInsertAll()
```

Cela permet de démontrer un comportement d’UPSERT idempotent.

---

## Checkpoint vs Déduplication vs MERGE

Ces trois mécanismes répondent à des problèmes différents :

```text
Checkpoint
→ mémorise les fichiers physiques déjà ingérés

Window Deduplication
→ sélectionne la bonne version d’un enregistrement métier

Delta MERGE
→ applique une logique INSERT / UPDATE dans une table cible
```

Ils sont donc complémentaires.

---

## Reconstruction du Shipment Lifecycle

La reconstruction du cycle de vie d’une expédition constitue une partie centrale du projet.

Les événements peuvent arriver dans un ordre différent de leur ordre métier réel.

Exemple :

```text
08:00 CREATED
15:00 SHIPPED
12:00 PACKED
18:00 DELIVERED
```

Même si `PACKED` arrive après `SHIPPED` dans le pipeline, l’événement s’est produit avant.

Le traitement se base donc sur :

```text
event_time
```

et non sur l’ordre d’ingestion.

Les milestones reconstruits incluent :

```text
created_at
packed_at
shipped_at
hub_scan_at
out_for_delivery_at
delivered_at
```

D’autres indicateurs sont également calculés :

```text
current_status
event_count
is_delivered
is_lifecycle_complete
delivery_duration_hours
delivery_delay_hours
```

Cette partie permet de démontrer la gestion de données événementielles et des événements late-arriving ou out-of-order.

---

## Couche Gold

La couche Gold contient les données préparées pour l’analyse.

### Delivery Performance

```text
gold/delivery_performance
```

Grain :

```text
une ligne par shipment
```

Indicateurs principaux :

```text
delivery duration
delivery delay
late / on-time status
delivery performance status
```

### Carrier Performance

```text
gold/carrier_performance
```

Grain :

```text
une ligne par carrier
```

KPIs :

```text
total shipments
delivered shipments
late shipments
on-time shipments
average delivery duration
average late delay
on-time delivery rate
```

### Daily Shipments

```text
gold/daily_shipments
```

Grain :

```text
une ligne par date d’expédition
```

KPIs :

```text
shipments per day
delivered shipments
late shipments
average delivery duration
on-time delivery rate
```

### Late Shipments

```text
gold/late_shipments
```

Cette table contient les expéditions livrées en retard avec les informations associées au carrier.

---

## Analyse avec Databricks SQL

Les datasets Gold sont explorés directement avec Databricks SQL.

Les analyses réalisées incluent notamment :

- les KPIs globaux de livraison ;
- la performance des carriers ;
- l’évolution quotidienne des shipments ;
- l’analyse des retards ;
- les retards par carrier.

Aucune application BI externe n’a été ajoutée au projet.

---

## Optimisation Spark

Le volume métier du prototype est volontairement limité.

Afin d’étudier réellement le comportement distribué de Spark sans prétendre que le volume du prototype nécessite Spark à grande échelle, un benchmark synthétique de :

```text
1 000 000 de shipments
```

a été créé.

Le benchmark reproduit un scénario classique :

```text
grosse table shipments
+
petite table de référence carriers
```

### Analyse des plans d’exécution

Les plans physiques Spark ont été analysés avec :

```python
df.explain(mode="formatted")
```

L’analyse a porté notamment sur :

```text
Exchange
Shuffle
SortMergeJoin
BroadcastHashJoin
HashAggregate
AdaptiveSparkPlan
```

### SortMergeJoin vs BroadcastHashJoin

Un `SortMergeJoin` a été forcé afin de créer une baseline, puis comparé à un `BroadcastHashJoin`.

Temps observés sur une exécution du benchmark :

```text
SortMergeJoin      ≈ 1.493 s
BroadcastHashJoin  ≈ 0.600 s
```

Le BroadcastHashJoin a été environ 59,82 % plus rapide sur cette exécution précise.

Ce chiffre n’est pas présenté comme un gain universel.

La conclusion principale est architecturale :

```text
SortMergeJoin
→ redistribution / tri des données

BroadcastHashJoin
→ diffusion de la petite dimension
→ évite de redistribuer la grosse table pour le join
```

Spark / Databricks a également été observé en train de sélectionner automatiquement un BroadcastHashJoin lorsque la dimension carriers était suffisamment petite.

---

## Repartition vs Coalesce

Le projet inclut également une expérimentation sur le partitionnement Spark.

### repartition

```python
df.repartition(16)
```

Permet notamment de :

- redistribuer les données ;
- augmenter ou diminuer le nombre de partitions ;
- contrôler le parallélisme ;
- répartir les données selon une clé.

Cette opération provoque généralement un shuffle.

### coalesce

```python
df.coalesce(4)
```

Utilisé principalement pour :

- réduire le nombre de partitions ;
- limiter le nombre de fichiers en sortie ;
- éviter une redistribution complète lorsque cela est possible.

Le projet utilise également :

```python
df.coalesce(1)
```

uniquement pour générer de très petits fichiers synthétiques.

Cette pratique ne serait pas adaptée à un gros dataset de production, car elle réduirait fortement le parallélisme.

---

## Orchestration avec Databricks Workflows

L’ensemble du pipeline est orchestré avec Databricks Workflows.

Le graphe principal suit la logique suivante :

```text
Bronze Orders ───────────────┐
Bronze Shipments ────────────┤
Bronze Delivery Events ──────┤
Bronze Carriers ─────────────┘
              ↓
        Validate Bronze
          ↙       ↘
 Silver Orders   Silver Carriers
          ↘       ↙
      Silver Shipments
              ↓
   Silver Delivery Events
              ↓
    Shipment Lifecycle
              ↓
 Gold Delivery Performance
      ↙          ↓          ↘
Carrier       Daily        Late
Performance  Shipments   Shipments
      ↘          ↓          ↙
       Pipeline Observability
```

Les tâches indépendantes peuvent s’exécuter en parallèle.

Les tâches downstream ne s’exécutent qu’après la réussite de leurs dépendances upstream.

---

## Stratégie de retry

Les tâches du Workflow sont configurées avec :

```text
maximum retries : 2
retry interval : 1 minute
```

Cela permet de gérer automatiquement certains échecs temporaires.

Si une tâche upstream échoue définitivement, les tâches dépendantes ne poursuivent pas normalement l’exécution.

---

## Scheduling

Un déclencheur planifié a été configuré dans Databricks Workflows afin de simuler une exécution de type production.

La planification sert ici à simuler un fonctionnement de production, et non à maintenir inutilement un workload permanent.

---

## Observabilité

Une couche d’observabilité légère est mise en place via :

```text
quality/pipeline_runs
```

Chaque exécution peut enregistrer :

```text
run_id
pipeline_name
start_time
end_time
rows_processed
rows_rejected
status
```

L’objectif est de démontrer le suivi d’exécution d’un pipeline sans construire une plateforme de monitoring complexe.

---

## Structure du repository

```text
.
├── notebooks/
│   ├── 01_bronze/
│   │   ├── 01_ingest_orders_bronze
│   │   ├── 02_ingest_shipments_bronze
│   │   ├── 03_ingest_delivery_events_bronze
│   │   ├── 04_ingest_carriers_bronze
│   │   └── 05_validate_bronze
│   │
│   ├── 02_silver/
│   │   ├── 01_build_silver_orders
│   │   ├── 02_build_silver_shipments
│   │   ├── 03_build_silver_delivery_events
│   │   ├── 04_build_silver_carriers
│   │   ├── 05_merge_silver_orders
│   │   ├── 06_merge_silver_shipments
│   │   ├── 07_merge_silver_delivery_events
│   │   └── 08_build_shipment_lifecycle
│   │
│   ├── 03_gold/
│   │   ├── 01_build_gold_delivery_performance
│   │   ├── 02_build_gold_carrier_performance
│   │   ├── 03_build_gold_daily_shipments
│   │   ├── 04_build_gold_late_shipments
│   │   └── 05_gold_sql_analysis
│   │
│   ├── 04_quality/
│   │   └── 01_pipeline_observability
│   │
│   └── 05_optimization/
│       └── 01_spark_join_optimization
│
├── src/
│   └── data_generator/
│       └── generate_source_data
│
├── tests/
├── docs/
├── README.md
└── .gitignore
```

---

## Ce qui a réellement été implémenté

Les éléments suivants ont été construits et exécutés :

- stockage ADLS Gen2 ;
- connexion avec Azure Databricks ;
- ingestion incrémentale avec Auto Loader ;
- architecture Bronze / Silver / Gold ;
- tables Delta ;
- règles de Data Quality ;
- datasets de quarantaine ;
- déduplication avec Window Functions ;
- validation d’intégrité référentielle ;
- ingestion incrémentale par fichiers ;
- expérimentation Delta MERGE / UPSERT ;
- reconstruction du shipment lifecycle ;
- tables analytiques Gold ;
- analyses Databricks SQL ;
- analyse des plans d’exécution Spark ;
- benchmark BroadcastHashJoin ;
- expérimentations `repartition` / `coalesce` ;
- Databricks Workflows ;
- dépendances entre tâches ;
- retries ;
- scheduling ;
- observabilité simple des exécutions.

---

## Ce qui a été simulé

Les éléments suivants ont été volontairement simulés :

- arrivée progressive de batches de fichiers ;
- corrections de données métier ;
- enregistrements invalides ;
- doublons ;
- événements late-arriving ou out-of-order ;
- volumétrie importante avec 1 million de lignes synthétiques ;
- exécution planifiée de type production.

---

## Ce qui serait différent en production

Dans un environnement d’entreprise réel, l’architecture pourrait être complétée par :

- compute Spark plus important ou autoscaling ;
- IAM et sécurité avancés ;
- gouvernance Unity Catalog plus poussée ;
- gestion professionnelle des secrets ;
- CI/CD ;
- Infrastructure as Code ;
- monitoring et alerting avancés ;
- SLA / SLO ;
- véritables sources externes ;
- ingestion événementielle ou streaming ;
- environnements séparés dev / staging / production.

Ces éléments n’ont pas été ajoutés artificiellement au prototype uniquement pour enrichir la stack.

---

## Concepts Data Engineering démontrés

Le projet permet de travailler concrètement les concepts suivants :

```text
Data Lakehouse Architecture
Medallion Architecture
Incremental Ingestion
Auto Loader
Checkpointing
Idempotence
Delta Lake
ACID Transactions
UPSERT / MERGE
Data Quality
Quarantine
Window Functions
Deduplication
Referential Integrity
Late-Arriving Data
Event Time
Shipment Lifecycle Reconstruction
Spark Partitions
Shuffle
Broadcast Join
Sort Merge Join
Repartition
Coalesce
Spark Execution Plans
Workflow Orchestration
Task Dependencies
Retries
Scheduling
Pipeline Observability
```

---

## Décisions d’architecture

Le projet privilégie :

```text
compréhension
+
qualité
+
architecture claire
```

plutôt que l’ajout de technologies inutiles.

Des outils tels que Power BI, Azure Data Factory, Event Hubs, Terraform ou un CI/CD complexe n’ont pas été intégrés car ils n’étaient pas nécessaires pour démontrer les principaux concepts Data Engineering du projet.

---

## Objectif du projet

L’objectif n’est pas uniquement de construire un pipeline fonctionnel, mais également de comprendre et de pouvoir expliquer les décisions techniques prises.

Le projet cherche notamment à répondre à des questions concrètes :

- Comment ingérer uniquement les nouveaux fichiers ?
- À quoi sert un checkpoint Spark ?
- Comment éviter les doublons métier ?
- Quand utiliser Delta MERGE ?
- Comment gérer les corrections de données ?
- Comment valider les foreign keys dans un pipeline ?
- Comment reconstruire un lifecycle si les événements arrivent dans le désordre ?
- Qu’est-ce qui provoque un shuffle Spark ?
- Quand utiliser un BroadcastHashJoin ?
- Quelle est la différence entre `repartition` et `coalesce` ?
- Comment les dépendances entre tâches protègent-elles un pipeline ?
- Comment suivre l’exécution d’un pipeline ?

---

## Statut du projet

Le pipeline Data Engineering principal est implémenté et validé de bout en bout.

Le projet couvre :

```text
Ingestion incrémentale
→ Bronze
→ Silver + Data Quality
→ Delta incremental processing
→ Lifecycle reconstruction
→ Gold
→ Spark optimization benchmark
→ Databricks Workflow
→ Observability
```
=======
Databricks SQL
>>>>>>> Stashed changes
