# Environnement TP Data "VéloCity"

Cet environnement **Docker Compose** fournit tous les services techniques nécessaires pour réaliser le "TP - Cycle de Vie de la Donnée : De la Source au Dashboard".

Il simule l'infrastructure de l'entreprise **"VéloCity"** en lançant une base de données, un client SQL web, et un outil de Business Intelligence (BI).

---

## Composition de l'Environnement

Cet environnement est défini dans le fichier `docker-compose.yml` et se compose des services suivants :

### 1. PostgreSQL (Base de Données Principale)
La base de données "source" de VéloCity.

* **Service Docker :** `postgres`
* **Image :** `postgres:18`
* **Conteneur :** `epsi_postgres_container`
* **Port (sur votre machine) :** `5432`
* **Identifiants de connexion :**
    * **Hôte :** `localhost` (depuis votre machine) ou `postgres` (depuis un autre conteneur)
    * **Base de données :** `postgres`
    * **Utilisateur :** `postgres`
    * **Mot de passe :** `postgres`
* **Données :** Au démarrage, le conteneur exécute le script `init/backup.sql` pour créer le schéma `raw` et y insérer les **26 tables** du TP.

### 2. pgAdmin (Client SQL Web)
Un outil web pour explorer la base de données PostgreSQL, exécuter des requêtes SQL et voir les tables.

* **Service Docker :** `pgadmin`
* **Image :** `dpage/pgadmin4:8`
* **Conteneur :** `epsi_pgadmin_container`
* **URL (sur votre machine) :** `http://localhost:5050`
* **Identifiants de connexion (pour l'interface pgAdmin) :**
    * **Email :** `pgadmin@pgadmin.org`
    * **Mot de passe :** `pgadmin`
* **Note :** Le serveur de base de données est déjà pré-configuré (`epsi_server`).

### 3. Metabase (Outil de BI)
L'outil de Business Intelligence (BI) qui sera utilisé pour créer le dashboard final.

* **Service Docker :** `metabase`
* **Image :** `metabase/metabase:latest`
* **Conteneur :** `metabase_app`
* **URL (sur votre machine) :** `http://localhost:3000`
* **Base de données interne :** Metabase utilise sa propre base de données (`metabase_db`) pour stocker ses métadonnées (utilisateurs, dashboards, etc.).

---

##  Données Incluses

### Tables Brutes (`raw`)
Le schéma `raw` contient 26 tables.

### Catalogue de Données (Backup)
Le fichier `catalogue_donnees.yml` sert de documentation de secours pour le TP.

* Il liste les 26 tables réparties par **domaines métier** (Finance, Support, Logistique, etc.).
* Il documente les colonnes des tables principales.

---

## Démarrage Rapide

1.  Ouvrez un terminal à la racine de ce dossier (où se trouve le `docker-compose.yml`).
2.  Exécutez la commande suivante pour construire et démarrer tous les services en arrière-plan :

    ```bash
    docker-compose up -d
    ```
3.  Attendez quelques instants que les services démarrent.

---

## 💻 Accès aux Services

Une fois les conteneurs démarrés, suivez ces étapes pour vous connecter.

### 1. Accès à pgAdmin (Exploration SQL)

1.  Ouvrez votre navigateur et allez sur : `http://localhost:5050`.
2.  Connectez-vous à pgAdmin avec :
    * **Email :** `pgadmin@pgadmin.org`
    * **Mot de passe :** `pgadmin`
3.  Développez le serveur `epsi_server`. Il vous demandera le mot de passe de la *base de données* :
    * **Mot de passe :** `postgres`
4.  Vous pouvez maintenant naviguer dans `Bases de données` -> `postgres` -> `Schémas` -> `raw` pour voir les 26 tables.

### 2. Accès à Metabase (Dashboarding)

1.  Ouvrez votre navigateur et allez sur : `http://localhost:3000`.
2.  **Première connexion :** Suivez les étapes de configuration pour créer votre compte administrateur Metabase.
3.  **Ajouter la base de données (étape cruciale) :**
    * Lors de l'étape d'ajout de la base de données, sélectionnez **PostgreSQL**.
    * Remplissez les informations de connexion à la base de données principale (la base des données brutes, et non la base interne de Metabase) :
        * **Hôte :** `postgres` (Nom du service dans Docker Compose)
        * **Port :** `5432`
        * **Nom de la base de données :** `postgres`
        * **Nom d'utilisateur :** `postgres`
        * **Mot de passe :** `postgres`
    * Cliquez sur "Enregistrer".

Vous êtes maintenant prêt à commencer le TP.

---

## Arrêter l'environnement

Pour arrêter tous les conteneurs, exécutez :

```bash
docker-compose down
```


