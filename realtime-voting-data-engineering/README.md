# Système de vote électoral en temps réel

> Plateforme de vote simulée illustrant un pipeline de traitement en flux continu : génération d'événements, transport via Kafka, agrégation en temps réel avec Spark Structured Streaming, et restitution des résultats via un dashboard Streamlit.

---

## 📋 Contexte et objectif

Ce projet met en place une **plateforme de vote simulée** dont l'objectif est de démontrer une architecture de **streaming de bout en bout** :

- Génération continue de profils d'électeurs et de votes
- Transport des événements via **Apache Kafka**
- Agrégation en temps réel avec **Spark Structured Streaming**
- Stockage des données de référence dans **PostgreSQL**
- Restitution des résultats en quasi temps réel via **Streamlit**

L'ensemble de la stack est conteneurisé avec **Docker Compose** pour un démarrage rapide et reproductible.

---

## 🏗️ Architecture


![Architecture du système](system_architecture.jpg)

---

## 🛠️ Stack technique

| Composant | Rôle |
|-----------|------|
| **Python 3.9+** | Langage principal |
| **Apache Kafka** | Bus d'événements distribué |
| **Zookeeper** | Coordination du cluster Kafka |
| **Spark Structured Streaming (PySpark)** | Traitement et agrégation en flux continu |
| **PostgreSQL** | Base de données de référence (voters, candidates, votes) |
| **Streamlit** | Dashboard de restitution temps réel |
| **Docker Compose** | Orchestration de l'infrastructure |

---

## ✨ Réalisations

### 1. Générateur de données Python
- Génération de **profils d'électeurs et de candidats** via API externe
- Gestion robuste des erreurs : **retries** et **timeouts** configurables
- Injection dans PostgreSQL et publication dans les topics Kafka

### 2. Schéma PostgreSQL avec contraintes d'intégrité
- Tables `voters`, `candidates`, `votes`
- Contrainte forte : **un vote par électeur** (unicité garantie côté base)

### 3. Producteur Kafka avec suivi de livraison
- Publication des messages avec **callback de confirmation**
- Suivi du taux de livraison et détection des échecs

### 4. Job Spark Structured Streaming
- **Parsing JSON avec schéma imbriqué**
- Gestion de l'événementiel tardif via **watermark**
- **Agrégation continue** :
  - Votes par candidat
  - Participation par région
- **Republication des résultats** dans des topics Kafka dédiés
- **Checkpointing** activé pour la tolérance aux pannes

### 5. Infrastructure conteneurisée
- Zookeeper, Kafka, PostgreSQL dans des conteneurs Docker
- **Healthchecks** configurés pour garantir un démarrage propre dans l'ordre

### 6. Dashboard Streamlit
- Consommation des données agrégées depuis Kafka et PostgreSQL
- Affichage en temps réel des résultats de vote

---

## 📁 Structure du projet



---

## 🚀 Démarrage rapide

### Prérequis
- **Python 3.9+** installé
- **Docker** et **Docker Compose** installés
- Un navigateur pour accéder au dashboard Streamlit

### 1. Cloner le dépôt

```bash
git clone https://github.com/<ton-user>/realtime-election-voting.git
cd realtime-election-voting
