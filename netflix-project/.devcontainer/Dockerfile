FROM mcr.microsoft.com/devcontainers/python:3.11

# Dépendances système utiles (optionnel)
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends curl \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*
