# Architecture — Supply Chain Intelligence Lakehouse

## Vue générale

Le projet repose sur une architecture Lakehouse organisée selon les couches Landing, Bronze, Silver et Gold.

```text
Logistics Data Sources
        ↓
ADLS Gen2 — Landing
        ↓
Databricks Auto Loader
        ↓
Bronze Delta
        ↓
PySpark Transformations
        ↓
Silver Delta
        ↓
Data Quality
        ↓
Shipment Lifecycle
        ↓
Gold Delta
        ↓
Databricks SQL
```

L’ensemble du pipeline est orchestré avec Databricks Workflows.

---

## Landing

La Landing Zone reçoit les fichiers sources simulant les systèmes logistiques.

Sources principales :

- orders ;
- shipments ;
- delivery_events ;
- carriers.

Les batches sont déposés progressivement afin de simuler une arrivée incrémentale des données.

---

## Bronze

La couche Bronze conserve les données proches du format source.

Des métadonnées techniques sont ajoutées :

- `source_file`
- `batch_id`
- `ingestion_timestamp`

L’ingestion est réalisée avec Databricks Auto Loader.

Chaque source possède son propre checkpoint afin d’éviter le retraitement des fichiers déjà ingérés.

---

## Silver

La couche Silver applique les transformations nécessaires pour obtenir des données fiables et exploitables :

- normalisation ;
- typage ;
- déduplication ;
- validation métier ;
- intégrité référentielle ;
- Data Quality ;
- enrichissements.

Les enregistrements invalides sont envoyés vers une zone de quarantaine.

La déduplication repose sur des Window Functions utilisant notamment :

```text
business_key
updated_at
ingestion_timestamp
```

---

## Traitement incrémental

Deux niveaux d’incrémentalité sont utilisés.

### Ingestion

Auto Loader et les checkpoints permettent de ne traiter que les nouveaux fichiers.

### Données métier

Delta MERGE permet de gérer :

```text
nouvelle business key
→ INSERT

business key existante avec updated_at plus récent
→ UPDATE
```

---

## Shipment Lifecycle

Les événements logistiques sont reconstruits selon leur `event_time`.

Le pipeline peut ainsi reconstruire :

```text
CREATED
→ PACKED
→ SHIPPED
→ HUB_SCAN
→ OUT_FOR_DELIVERY
→ DELIVERED
```

même lorsque certains événements arrivent dans le pipeline dans le désordre.

---

## Gold

La couche Gold expose quatre principaux Data Products :

```text
gold_delivery_performance
gold_carrier_performance
gold_daily_shipments
gold_late_shipments
```

Ils fournissent notamment :

- delivery duration ;
- delivery delay ;
- on-time delivery rate ;
- late shipments ;
- carrier performance ;
- shipments per day.

---

## Spark Optimization

Un benchmark synthétique de 1 000 000 de shipments a été utilisé pour étudier le comportement distribué de Spark.

Les principaux concepts analysés sont :

- partitions ;
- shuffle ;
- Exchange ;
- SortMergeJoin ;
- BroadcastHashJoin ;
- repartition ;
- coalesce ;
- execution plans.

Un BroadcastHashJoin a notamment été comparé à un SortMergeJoin pour un scénario grosse table shipments / petite dimension carriers.

---

## Orchestration

Databricks Workflows orchestre le pipeline.

```text
Bronze ingestion
        ↓
Bronze validation
        ↓
Silver processing
        ↓
Shipment lifecycle
        ↓
Gold processing
        ↓
Pipeline observability
```

Les tâches indépendantes peuvent être exécutées en parallèle.

Les tâches dépendantes ne sont exécutées qu’après la réussite de leurs tâches upstream.

Chaque tâche dispose également d’une stratégie de retry.

---

## Observabilité

Une table Delta :

```text
pipeline_runs
```

permet de suivre des informations telles que :

```text
run_id
pipeline_name
start_time
end_time
rows_processed
rows_rejected
status
```

---

## Prototype vs Production

Le projet implémente réellement :

- ADLS Gen2 ;
- Azure Databricks ;
- Auto Loader ;
- Delta Lake ;
- Bronze / Silver / Gold ;
- Data Quality ;
- MERGE ;
- Databricks Workflows ;
- observabilité simple.

Certains scénarios sont simulés :

- arrivée progressive des fichiers ;
- corrections ;
- doublons ;
- événements late-arriving ;
- volumétrie Spark élevée.

Une architecture de production pourrait également ajouter :

- CI/CD ;
- Infrastructure as Code ;
- monitoring avancé ;
- gouvernance Unity Catalog avancée ;
- sécurité et IAM renforcés ;
- environnements dev / staging / production.
