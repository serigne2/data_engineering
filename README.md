# Food Delivery Data Platform — Data Engineering & AI

Plateforme Data Engineering end-to-end construite autour d'un jeu de données de type Food Delivery (Zomato) : ingestion, stockage, transformation, modélisation analytique et enrichissement par l'intelligence artificielle.

## Présentation

Ce projet couvre l'ensemble de la chaîne Data Engineering, depuis l'ingestion de fichiers sources jusqu'à la mise à disposition de données analytiques dans Snowflake, avec une couche IA permettant d'enrichir les avis clients et d'interagir avec les données en langage naturel.

## Architecture

```
Fichiers CSV (restaurants, users, orders, products, reviews...)
        │
        ▼
      MinIO  (stockage objet, Data Lake compatible S3)
        │
        ▼
  Snowflake — RAW        (chargement brut + INGESTION_LOG)
        │
        ▼  dbt
  Snowflake — STAGING     (nettoyage, typage, déduplication)
        │
        ▼  dbt
  Snowflake — MARTS       (tables de faits/dimensions, indicateurs métier)
        │
        ▼
  Snowflake — AI          (avis enrichis par LLM, embeddings)
```

L'ensemble du pipeline (chargement RAW + exécution des modèles dbt) est orchestré quotidiennement par un DAG Apache Airflow.

## Stack technique

| Composant | Rôle |
|---|---|
| **Python** | Scripts d'ingestion, composants IA (enrichissement, RAG, text-to-SQL) |
| **MinIO** | Stockage objet local, compatible S3 |
| **Snowflake** | Data Warehouse (schémas RAW / STAGING / MARTS / AI) |
| **dbt** | Transformations SQL, tests de qualité, gestion des dépendances entre modèles |
| **Apache Airflow** | Orchestration du pipeline quotidien |
| **Docker** | Conteneurisation d'Airflow, MinIO et des services associés |
| **Streamlit** | Interface utilisateur pour le RAG et le Text-to-SQL |
| **LLM / embeddings** | Enrichissement des avis, recherche sémantique, génération de SQL |

## Modélisation des données (couche MARTS)

- `FCT_ORDERS` — table de faits des commandes
- `FACT_ORDER_ITEMS` — détail des lignes de commande
- `DIM_CUSTOMER`, `DIM_RESTAURANTS`, `DIM_FOOD`, `DIM_DATE` — dimensions
- `MART_DAILY_CITY_REVENUE` — GMV, nombre de commandes, taux d'annulation, panier moyen par ville
- `MART_RESTAURANT_PERFORMANCE` — performance des restaurants
- `MART_DELIVERY_SLA` — indicateurs de performance de livraison

## Qualité et idempotence

- Table `INGESTION_LOG` : suit les fichiers déjà traités (clé source, ETag, taille) pour éviter les rechargements inutiles.
- Tests dbt : unicité et valeurs NULL sur les clés principales.
- Traitement des doublons au niveau STAGING avant utilisation dans les modèles analytiques.

## Couche Intelligence Artificielle

### 1. Enrichissement automatique des avis clients (`enrich_reviews.py`)
Analyse chaque avis client via un LLM pour en extraire :
- le sentiment (positif / négatif / neutre) et un score associé,
- la thématique (livraison, qualité des produits, prix, service, emballage...),
- le problème principal identifié.

Résultats stockés dans `ZOMATO.AI.REVIEW_ENRICHED`.

### 2. RAG sur les avis clients (`rag_chat.py`)
Les avis sont transformés en embeddings pour permettre une recherche sémantique. Une question en langage naturel (ex. *« Quels sont les principaux problèmes concernant la livraison ? »*) déclenche une recherche des avis les plus pertinents, utilisés comme contexte pour générer une réponse.

### 3. Text-to-SQL (`text_to_sql.py`)
Traduit une question en langage naturel (ex. *« Quelles sont les 10 villes ayant le GMV le plus élevé ? »*) en requête SQL exécutée sur Snowflake. Une couche de validation limite l'exécution aux requêtes de lecture, sans modification ni suppression de données.

## Interface utilisateur

Les modules RAG et Text-to-SQL sont exposés via une application **Streamlit**, permettant à un utilisateur non technique de :
- poser des questions sur les avis clients et consulter les avis utilisés pour la réponse,
- poser des questions métier en langage naturel,
- visualiser les résultats des requêtes générées.

## Compétences démontrées

Conception d'architecture Data Engineering end-to-end · ingestion et chargement de données · stockage objet (MinIO) · modélisation dimensionnelle · transformations SQL avec dbt · tests de qualité des données · gestion de l'idempotence · orchestration Airflow · conteneurisation Docker · développement d'applications Streamlit · intégration d'API IA · embeddings et RAG · génération de SQL à partir du langage naturel.

---

*Projet réalisé à des fins de démonstration technique (Data Engineering & IA appliquée).*
