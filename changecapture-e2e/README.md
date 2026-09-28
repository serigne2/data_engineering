# CDC en temps réel avec Debezium, Kafka et PostgreSQL

> Pipeline de Change Data Capture (CDC) capturant en temps réel les modifications d'une base transactionnelle PostgreSQL et les diffusant dans Kafka, sans impacter l'application source.

---

## 📋 Contexte et objectif

Ce projet met en place une architecture de **Change Data Capture (CDC)** permettant de :

- Capturer en temps réel les modifications d'une base transactionnelle (transactions financières simulées)
- Diffuser ces changements sous forme d'événements Kafka exploitables en aval
- Garantir **zéro impact** sur l'application source (pas de polling, pas de requêtes lourdes)

L'objectif est de fournir un socle CDC fonctionnel, observable et extensible, servant de base à des cas d'usage analytiques, d'audit ou de synchronisation temps réel.

---

