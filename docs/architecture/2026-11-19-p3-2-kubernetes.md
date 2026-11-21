# P3-2 — Kubernetes : chart Helm officiel, KubernetesExecutor, Spark sur Kubernetes

- **Date :** 2026-11-19
- **Item de backlog :** P3-2 (`docs/audit/2026-11-05-baseline.md`, constat M6 : « cible : `SparkSubmitOperator` puis Spark on Kubernetes »)
- **Code :** `Dockerfile`, `.dockerignore`, `deploy/k8s/` (image, kind, values du chart Airflow, chart du projet), `scripts/k8s_secrets.py`, `Makefile` (section « Kubernetes »), `dags/local_read_file_dag.py` (`ReadFilesSparkSubmitOperator.execute` et `_prepare_k8s_cluster`, `_job_exit_code`, `k8s_cluster_application`, `k8s_task_instance_label`), `pyproject.toml` / `uv.lock` (`apache-airflow-providers-cncf-kubernetes`)
- **Détails d'exploitation :** `CLAUDE.md`, section « Kubernetes (kind + Helm, P3-2) »
- **Modèle suivi :** les notes P2-5 et P3-3 : tout est optionnel, le pipeline local ne change pas, versions et empreintes épinglées et vérifiées.
- **Revues (2026-11-20) :** les correctifs des revues code (C-1, C-2...) et sécurité (SEC-1, SEC-2...) sont décrits à leur place ; validation réelle refaite le 2026-11-21 (§8).

## 1. Le problème de départ

Le pipeline tourne sur un seul poste : `airflow standalone`, Spark en `local[*]`, le lake et l'entrepôt DuckDB sur le disque local. Rien ne dit qu'il tourne ailleurs, et rien ne permet de le vérifier.

- **Airflow** n'a été lancé qu'avec SQLite et le LocalExecutor de `standalone`. Les DAGs supposent la racine du projet, un `.env`, des chemins locaux.
- **Spark** n'a jamais tourné qu'en `local[*]`. Le passage au mode cluster était documenté comme une simple affaire de Connection, sans avoir été essayé. Et P2-6 notait déjà qu'en mode cluster le code de sortie du job n'arrive pas jusqu'à Airflow, donc que les échecs déterministes y seraient relancés pour rien.
- **dbt** écrit un fichier DuckDB : avec un pod par tâche, il faut un stockage partagé.

## 2. La solution

```
kind (1 nœud, Kubernetes 1.37.0)          namespace airflow-project-one
┌──────────────────────────────────────────────────────────────────────────────┐
│ release `project` (deploy/k8s/charts/project)                                │
│   Postgres 17 (metadata `airflow` + `file_list`)   PVC project-shared          │
│   ConfigMap spark-conf (spark-defaults.conf,       PVC airflow-logs            │
│     gabarits de pods driver/exécuteur)             toolbox (copie des sources) │
│   RBAC : Roles spark-driver, spark-submit, airflow-scheduler                   │
│                                                                              │
│ release `airflow` (chart officiel 1.22.0, deploy/k8s/values-airflow.yaml)     │
│   api-server  scheduler  dag-processor  triggerer   (image du projet)         │
│        │ KubernetesExecutor : un pod par tâche                               │
│        ▼                                                                     │
│   pod worker spark_read_files ── spark-submit --master k8s://... --deploy-mode cluster
│                                        │                                     │
│                                        ▼                                     │
│                              pod driver (read_files_job.py) ──▶ pod(s) exécuteur(s)
│                                 lake sur /opt/airflow/shared (PVC partagé)    │
│   pods worker de transform_dag (Cosmos, dbt + DuckDB sur le même PVC)         │
└──────────────────────────────────────────────────────────────────────────────┘
        ▲ kubectl port-forward --address 127.0.0.1 (make k8s-ui)
```

- **Une seule image** (`Dockerfile`) pour tous les pods : Airflow et ses workers, et le driver et les exécuteurs Spark.
- **Deux releases Helm** : le chart officiel d'Airflow, paramétré par un fichier de values versionné, et un petit chart du projet pour tout ce que le chart officiel ne fournit pas.
- **Spark** : `spark-submit` natif en `k8s://`, lancé par le `ReadFilesSparkSubmitOperator` existant. **Seule la Connection `spark_default` change** : master `k8s://https://kubernetes.default.svc:443`, `deploy-mode` cluster, `namespace`. Tout le reste (image, compte de service, volumes, environnement du driver) est dans le `spark-defaults.conf` de l'environnement.
- **Rien ne change en local.** `make airflow`, `make check`, `make spark-job` ne touchent à aucune de ces pièces (test à blanc `make -n`), et le mode local/client du DAG garde son comportement exact (tests P2-6 inchangés, plus un test dédié).

## 3. Les briques et leurs raisons

| Brique | Version épinglée | Vérification |
|---|---|---|
| Image de base `apache/airflow:slim-3.3.2-python3.12` | digest d'index `sha256:a54e7bd6…3ff3` | `docker buildx imagetools inspect`, 2026-11-19 |
| JRE `eclipse-temurin:17.0.21_9-jre-noble` | digest `sha256:853d06b5…3889` | idem. C'est la base des images officielles Spark 4.3.0 (`apache/spark-docker`). |
| Entrypoint Kubernetes de Spark | fichier `4.3.0/scala2.13-java17-ubuntu/entrypoint.sh` du commit `f933b448` d'`apache/spark-docker`, SHA-256 `ec72936b…ecf3` | copié tel quel (`deploy/k8s/image/spark-entrypoint.sh`), empreinte vérifiée par un test |
| Jar OpenLineage | 1.55.0, SHA-256 du Makefile | vérifié pendant le build (`sha256sum -c`), HTTPS seulement |
| Chart Airflow | 1.22.0, SHA-256 `1f7d1dfe…a5b6` | égal au `digest` de `https://airflow.apache.org/index.yaml` ; le `.sha512` publié correspond ; la signature `.asc` est valide (clé de Jarek Potiuk, `5E5F D9EF 639B 70A8 0D7B 9730 B311 40BB B84D C54D`, du fichier `KEYS` d'Airflow) |
| kind | v0.33.0, SHA-256 `aee61515…8e0d` | égal au `.sha256sum` de la release GitHub |
| Image de nœud kind | `kindest/node:v1.37.0@sha256:a1ed56cf…80ae5` | digest donné par les notes de release de kind v0.33.0 |
| kubectl | v1.37.2, SHA-256 `c0448752…f8f4` | égal au `.sha256` de `dl.k8s.io`. Signature cosign non vérifiée (cosign absent). |
| Helm | v4.3.2, SHA-256 de l'archive `d1c0278d…3983` (et du binaire extrait `876242ea…4931`) | égal au `.sha256sum` de `get.helm.sh` et aux notes de release ; la signature `.asc` de l'archive est valide (clé `208D D36E D5BB 3745 A167 43A4 C7C6 FBB5 B91C 1155`, fichier `KEYS` de Helm) |
| Postgres `postgres:17.12` | digest d'index `sha256:b60e9004…e45c` | `docker buildx imagetools inspect` |
| kubeconform (tests) | `ghcr.io/yannh/kubeconform:v0.8.0@sha256:faffaf43…f64e` | idem |
| Provider `apache-airflow-providers-cncf-kubernetes` | `>=11.0,<12` (11.0.0 dans le lock) | `uv add --group airflow`. Requis : le KubernetesExecutor en fait partie, et le hook Spark refuse un master `k8s://` sans lui (`kube_client`). `pip-audit` du lock : aucune vulnérabilité connue. |

Aucune clé de signature n'est certifiée par un chemin de confiance local : la signature dit que l'artefact vient du détenteur de la clé publiée par le projet, pas plus.

### Faits vérifiés avant ou pendant la réalisation

- **Chart Airflow.** 1.22.0 est la dernière version publiée (2026-06-01). Son Airflow par défaut est 3.2.2 ; `airflowVersion: "3.3.2"` et l'image du projet le font tourner en 3.3.2 (vérifié : déploiement, migrations, les cinq DAGs chargés, run complet). Exigences : Kubernetes 1.30.13+, Helm 3.19+. Son sous-chart Postgres utilise `bitnamilegacy/postgresql:16.1.0-debian-11-r15`, une image qui n'est plus maintenue : écarté.
- **Helm 4.** Helm 3 ne reçoit plus que des correctifs de sécurité, jusqu'au 10 février 2027 ([annonce](https://helm.sh/blog/helm-v3-end-of-life)). Helm 4.3.2 installe le chart 1.22.0 sans difficulté.
- **cgroup v1.** Docker sous WSL2 (noyau 5.15) tourne en cgroup v1, que le kubelet refuse par défaut depuis Kubernetes 1.35. La configuration kind pose `failCgroupV1: false` (sans effet sur un hôte en cgroup v2). Sans cela, `kind create cluster` échoue au démarrage du plan de contrôle.
- **Spark 4.3.0 crée une NetworkPolicy.** Depuis Spark 4.2 ([SPARK-55653](https://github.com/apache/spark/commit/1e1b3635cb)), le client de soumission applique une `NetworkPolicy` qui ne laisse joindre les exécuteurs que par les pods de l'application. La documentation 4.3.0 n'en parle pas ; trouvé par un 403 au premier run réel.

## 4. L'image

`Dockerfile` multi-étapes, `.dockerignore` en liste blanche.

1. **`openlineage-jar`** : télécharge le jar épinglé par le Makefile (`make image` passe version et SHA-256 ; pas de valeur par défaut dans le Dockerfile) et vérifie son empreinte.
2. **`transform`** : le venv dbt (`uv sync --frozen` de `transform/uv.lock`) à son chemin final, puis `dbt parse` avec les `LAKE_ROOT` / `DBT_DUCKDB_PATH` du déploiement. Le manifest est donc dans l'image, plus récent que les modèles (pas d'erreur « manifest périmé »).
3. **`jre`** : seulement pour copier `/opt/java/openjdk`.
4. **Image finale** : la base Airflow, `tini` (paquet Debian, exigé par l'entrypoint de Spark), la JRE, puis :
   - le venv de l'image (`/home/airflow/.local`) **synchronisé exactement sur `uv.lock`** (`uv sync --frozen --no-dev --no-install-project`) : runtime + groupe `airflow`, sans le groupe `dev` ; les paquets de la base absents du lock sont retirés ;
   - le code (`dags/`, `pipeline/`, `utils/`, `storage/`, `config/`), `transform/` et son venv, le jar, `deploy/airflow/pools.json` ;
   - `PYTHONPATH=/opt/airflow` joue le rôle de l'installation éditable (le backend de build `hatchling` n'est pas dans le lock) et garde `config/` et `transform/` à côté du code qui les résout ;
   - `SPARK_HOME` : la distribution Spark de la roue PySpark (elle contient `spark-submit` et `spark-kubernetes_2.13-4.3.0.jar`) ;
   - `data -> shared/data` : `config/extractor_config.yaml` liste `./data/`, relatif au répertoire de travail `/opt/airflow` ;
   - utilisateur `50000` (celui de l'image officielle), jamais root.
- **Entrypoint.** Spark lance ses pods avec l'entrypoint de l'image et les arguments `driver ...` ou `executor ...`. `deploy/k8s/image/entrypoint.sh` les envoie à l'entrypoint de Spark, tout le reste à celui de l'image Airflow, inchangé (ce que le chart attend).
- **Jamais dans l'image** (vérifié sur le vrai contexte de build par `tests/integration/test_p3_2_image_context.py`) : `.env`, `airflow.cfg`, `airflow.db`, `lake/`, `logs/`, `.venv`, `data/`, `lineage/`, `transform/.venv`, `transform/target`, l'entrepôt DuckDB, `tests/`, `.git/`.
- **Taille.** 4,15 Go, dont la roue PySpark (485 Mo), la JRE, le venv dbt et les paquets de la base remplacés (ils restent dans la couche d'origine). Construction : 3 min 45 s à froid ; `make kind-load` : 1 min.

## 5. Les deux releases Helm

### Chart officiel (`deploy/k8s/values-airflow.yaml`)

- `executor: KubernetesExecutor` ; les DAGs sont dans l'image (ni git-sync, ni volume de DAGs).
- Pas de sous-chart Postgres, ni PgBouncer, ni Redis, ni StatsD. `data.metadataSecretName: airflow-metadata`.
- Clé Fernet, clé secrète de l'API et secret JWT dans des Secrets générés une fois : sinon le chart en génère de nouvelles à chaque `helm upgrade`.
- `migrateDatabaseJob.useHelmHooks: false` : avec `helm --wait` et le hook `post-install`, Helm attend les pods, qui attendent les migrations, que le hook ne lancerait qu'après. Le Job étant immuable, `make k8s-install` supprime le Job terminé de l'installation précédente avant chaque mise à jour (`airflow db migrate` est idempotent).
- Gestionnaire d'authentification : le Simple Auth Manager, comme `make airflow` (le chart met FAB par défaut). Utilisateur `admin`. Son mot de passe est généré une fois par `scripts/k8s_secrets.py` (Secret `airflow-ui-admin`), jamais par l'api-server, qui l'écrirait dans son log et en changerait à chaque redémarrage. Le Secret dérivé `airflow-ui-passwords` contient le fichier `passwords.json` du Simple Auth Manager ; un init container de l'api-server le copie dans un `emptyDir` (`simple_auth_manager_passwords_file`), car l'api-server ouvre ce fichier en écriture et un volume de Secret est en lecture seule. `make k8s-ui-password` lit le Secret.
- Connections `spark_default` et `file_list_db` en variables `AIRFLOW_CONN_*`, par `secretKeyRef` vers le Secret `airflow-connections`, **dans les pods worker seulement** (`workers.kubernetes.env`) : le scheduler, l'api-server, le dag-processor et le triggerer n'en ont pas besoin et ne les reçoivent pas (testé sur le rendu).
- `allowPodLaunching: false` et `allowJobLaunching: false` : le Role `pod-launcher` du chart n'est pas créé (il donnait `pods/exec` et les pods au scheduler **et** aux workers). Le chart du projet donne au scheduler ce dont le KubernetesExecutor a besoin (§7).
- `LAKE_ROOT=/opt/airflow/shared/lake`, `DBT_DUCKDB_PATH=/opt/airflow/shared/warehouse.duckdb` pour tous les pods ; les pods worker montent le PVC partagé et la ConfigMap `spark-conf` (`SPARK_CONF_DIR=/opt/spark-conf`), avec le compte de service `airflow-worker`.
- Logs des tâches sur le PVC `airflow-logs` : l'interface les lit après la suppression du pod worker. Les pods worker en échec sont gardés (`delete_worker_pods_on_failure = False`).
- Triggerer activé : les capteurs Cosmos de `transform_dag` sont différables.
- Pods non root, `seccompProfile: RuntimeDefault`, `allowPrivilegeEscalation: false`, toutes les capacités retirées : chaque pod passe le niveau `restricted` (§7, Pod Security Admission).

### Chart du projet (`deploy/k8s/charts/project`)

- **Postgres 17 (dev seulement).** Une base `airflow` (rôle `airflow`) et une base `file_list` (rôle `file_list`, propriétaire de la table). Le DDL vient de `utils/SQL/script_tables.sql` par `--set-file` (une seule source). Les mots de passe passent à `psql` comme variables (`:'nom'`), jamais interpolés. Comme le volume compose, un PVC existant garde les mots de passe de son premier démarrage. Uid 999 (l'utilisateur `postgres` de l'image), sans privilège.
- **Labels stables (C-2).** Le sélecteur et les `volumeClaimTemplates` du StatefulSet sont immuables : ils ne portent que des labels qui ne changent jamais (`project.stableLabels`, sans `helm.sh/chart`). Avec la version du chart dans ces labels, toute montée de version du chart serait refusée par l'API (champ immuable). Test sur le rendu, et montée de version réelle au §8.
- **Volumes.** `project-shared` (sources, lake, entrepôt) et `airflow-logs`, en `ReadWriteOnce` : suffisant sur un nœud unique (tous les pods y tournent). `helm.sh/resource-policy: keep` : `make k8s-uninstall` garde les données, `make kind-down` les supprime.
- **Spark.** ConfigMap `spark-conf` : `spark-defaults.conf` et les gabarits de pods du driver et des exécuteurs (non root, sans privilège ; pas de jeton de compte de service pour les exécuteurs).
- **RBAC et politique d'admission** (§7).
- **Toolbox.** Un petit pod qui monte le volume partagé, pour y copier les sources (`make k8s-load-data`) et lire le lake. Sans jeton, sans privilège.

### Secrets (`scripts/k8s_secrets.py`)

Aucun secret dans un fichier de values, dans git, sur une ligne de commande ou dans la sortie du script (testé). Les valeurs sont générées par le module `secrets`, gardées en mémoire et envoyées à `kubectl` sur son entrée standard.

- **Générés** (`project-postgres`, `file-list-db`, `airflow-fernet-key`, `airflow-api-secret-key`, `airflow-jwt-secret`, `airflow-ui-admin`) : créés une fois puis gardés. Un Secret existant auquel il manque une clé attendue est une erreur, jamais réécrit (le script s'arrête et dit de le supprimer ; pour un mot de passe Postgres, il faut aussi un nouveau volume).
- **Dérivés** (`airflow-metadata`, `airflow-connections`, `airflow-ui-passwords`) : recalculés à chaque `make k8s-install` et appliqués côté serveur (`apply --server-side`) : un `apply` côté client recopierait les valeurs en clair dans l'annotation `last-applied-configuration`.
- **Erreurs.** `kubectl` absent ou non exécutable, contexte refusé, espace de noms absent : un message `error: ...` sans trace Python et sans valeur, code 1. Seul le message `secrets "<nom>" not found` veut dire « Secret absent » ; tout autre `NotFound` est une erreur.

## 6. Spark sur Kubernetes et le code de sortie

### Ce qui change dans le DAG

Rien dans la définition du DAG. `ReadFilesSparkSubmitOperator.execute` gagne quelques comportements, actifs seulement en mode cluster Kubernetes : un master qui **commence** par `k8s://` et `deploy-mode` cluster, tous deux lus dans la Connection (`_is_k8s_cluster_mode`). Pas le `_is_kubernetes` du provider, qui répond vrai pour tout master contenant « k8s » (`spark://k8s-master:7077` compris). Avant la soumission (`_prepare_k8s_cluster`) :

1. **`local://`.** Un chemin sans schéma est, pour `spark-submit` en mode cluster, un fichier du poste qui soumet, à téléverser (`spark.kubernetes.file.upload.path`). Le job est dans l'image du driver, au même chemin : il est passé en `local:///opt/airflow/pipeline/jobs/read_files_job.py`.
2. **Lignage Spark retiré.** La conf OpenLineage de P2-5 est enlevée, avec un avertissement : le jar du listener serait un fichier du worker à téléverser, et le transport `file` écrirait dans le pod driver. La soumission ne dépend donc jamais du lignage (voir §11).
3. **Un seul driver par instance de tâche (C-1)**, ci-dessous.

Après la soumission : **le code de sortie du driver** (`_job_exit_code`, ci-dessous).

### Ce que fait réellement spark-submit 4.3.0 (vérifié sur kind)

Run `read_file_dag` déclenché à la main avec un `run_id` inexistant (le job sort en 3) :

```
container status:
  container name: spark-kubernetes-driver
  container state: terminated
  exit code: 3
  termination reason: Error
LoggingPodStatusWatcherImpl: Application status for spark-4f3fac10... (phase: Failed)
...
AirflowFailException: read_files_job exit code 3 (no file_list row for the run id): deterministic, not retried.
  Cannot execute: spark-submit --master k8s://https://kubernetes.default.svc:443 ... --deploy-mode cluster
  local:///opt/airflow/pipeline/jobs/read_files_job.py --run-id=p3-2-no-such-run.
  Error code is: 0. Kubernetes spark exit code is: 3
airflow_failure {"dag_id": "read_file_dag", ..., "spark_exit_code": 3, "spark_exit_code_meaning": "no file_list row for the run id", ...}
```

- **`spark-submit` sort en 0** alors que le driver est sorti en 3. Le code de retour du processus ne dit donc rien du job : c'est ce que P2-6 craignait.
- **Le code du driver est dans la sortie.** Avec `spark.kubernetes.submission.waitAppCompletion=true` (le défaut de Spark, écrit en clair dans `spark-defaults.conf`), le watcher de `spark-submit` suit le pod driver et imprime l'état final de son conteneur, `exit code: N` compris. Le hook du provider (6.3.4) l'extrait dans `_spark_exit_code` et fait échouer la tâche quand il n'est pas 0.
- **Conséquence.** En mode cluster Kubernetes, l'opérateur lit `_spark_exit_code` au lieu du code de retour de `spark-submit`. Les codes 2, 3, 5 et 6 ne sont pas relancés, comme en local ; 1, 4 et le reste gardent leur retry. Le callback de P3-3 rapporte ce code (`spark_exit_code`).
- **Soumission refusée.** Un 403 (observé : la NetworkPolicy sans droit) fait sortir `spark-submit` en 1, sans ligne `exit code:` : code inconnu, la tâche est relancée (côté sûr), avec un avertissement.
- **Tâche tuée.** Pod worker supprimé pendant un run : `on_kill` du hook a supprimé le pod driver (`Deleted driver pod arrow-spark-7cad85a1f5334f50-driver`, vérifié), puis la tâche est passée en `up_for_retry`. Le chien de garde du job (code 4) reste le mécanisme du mode local.
- **Mode client sur Kubernetes** (`deploy-mode` client) : le driver est `spark-submit` lui-même, son code de retour est celui du job, comme en local (test).
- **Driver disparu sans code** (pod driver supprimé, watcher coupé) : `spark-submit` sort en 0 sans ligne `exit code:`, le hook échoue (`Kubernetes spark exit code is: None`), le code est inconnu, la tâche est relancée. L'opérateur supprime alors le pod driver que le hook a identifié (`_kubernetes_driver_pod`) : le retry ne tourne jamais à côté de lui. Une erreur à cette suppression est journalisée, jamais levée.

### Un seul driver par instance de tâche (C-1)

En mode cluster, le driver vit dans son propre pod. Si le pod worker meurt (OOM, nœud perdu, pod supprimé hors d'Airflow), `on_kill` ne passe pas, le driver continue d'écrire dans le lake, et le retry en soumettait un second : deux drivers sur les mêmes partitions et les mêmes lignes de `file_list`.

- Chaque driver porte le label `airflow-project-one/task-instance` (`K8S_TI_LABEL`) : 40 caractères hexadécimaux du SHA-256 de (dag, run, tâche, index de map), identique à chaque tentative, valide quel que soit le `run_id` (texte libre pour un run manuel). Il passe par `spark.kubernetes.driver.label.*`.
- Avant de soumettre, l'opérateur liste les pods `spark-role=driver` qui portent ce label et supprime ceux en `Pending` ou `Running`, puis attend qu'ils aient disparu (120 s au plus). Un driver terminé est gardé (ses logs).
- Si la liste ou la suppression échoue (droit manquant, API injoignable, pod toujours là), rien n'est soumis : la tentative échoue et sera relancée. Mieux vaut un retry de plus qu'un second driver.
- Droits : `list` et `delete` sur les pods, déjà dans le Role `spark-submit`. Le mode local/client ne pose ni label ni liste (test).

`tests/unit/dags/test_p3_2_spark_k8s.py` rejoue ces cas avec le vrai hook et un faux `spark-submit` qui imprime les lignes réelles.

### Environnement du driver

`spark-defaults.conf` (ConfigMap `spark-conf`) :

- `ENV=prod` : `utils/spark_session.py` garde alors le master `k8s://` au lieu de forcer `local[*]` (et `spark.driver.host=127.0.0.1`, qui empêcherait les exécuteurs de joindre le driver). Effet de bord connu : il ajoute `spark.jars.packages` (spark-excel) dans le builder de la session, sans effet sur une JVM déjà démarrée par `spark-submit` (aucun téléchargement observé) ; la lecture xlsx n'est donc pas disponible sur Kubernetes.
- `LAKE_ROOT`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER` par `spark.kubernetes.driverEnv.*` ; `DB_PASSWORD` par `spark.kubernetes.driver.secretKeyRef.DB_PASSWORD=file-list-db:password` : le kubelet le résout, il n'apparaît dans aucune propriété Spark (que `spark-submit` journalise).
- Le volume partagé au même chemin dans le driver et les exécuteurs ; image, compte de service, gabarits de pods, ressources (1 cœur demandé à 500m, 1 Go, un exécuteur).

## 7. RBAC minimal

Trois `Role` d'espace de noms, sans joker, sans ressource `secrets`.

| Role | Lié à | Droits |
|---|---|---|
| `spark-driver` | SA `spark` (pods driver) | `pods` : create, get, list, watch, delete, deletecollection |
| `spark-submit` | SA `airflow-worker` (pods worker, qui lancent `spark-submit`) | `pods` : create, get, list, watch, delete ; `configmaps`, `services` : create, patch, delete ; `networking.k8s.io/networkpolicies` : create, patch, delete |
| `airflow-scheduler` | SA `airflow-scheduler` (KubernetesExecutor) | `pods` : create, get, list, watch, patch, delete ; `events` : list, watch |

- `spark-submit` crée le pod driver, puis ses ConfigMaps, son Service et la NetworkPolicy par application côté serveur (create + patch) ; il les supprime si la soumission échoue, et `on_kill` supprime le pod driver.
- Trois nettoyages de Spark demanderaient plus au driver ; ils sont coupés dans `spark-defaults.conf` :
  - `spark.kubernetes.driver.service.deleteOnTermination=false` : sinon le driver supprime les Services par label (`deletecollection` sur `services`). Le Service appartient au pod driver (ownerReference) et part avec lui.
  - `spark.kubernetes.driver.ownPersistentVolumeClaim=false` : sinon le driver liste et supprime les PVC par label. Un `deletecollection` sur les PVC pourrait atteindre le volume de Postgres ; il n'y a pas de PVC à la demande ici.
  - `spark.kubernetes.executor.disableConfigMap=true` : sinon le driver crée et supprime une ConfigMap pour les exécuteurs, qui n'ont besoin d'aucun fichier du `SPARK_CONF_DIR` du driver.
- **Vérification réelle.** Sans `networkpolicies`, 403 et échec de la soumission. Avec les droits d'origine, le driver journalisait deux 403 non bloquants à l'arrêt (suppression des Services et des PVC par label) : d'où les trois réglages. Avec les Roles finaux : run réussi, **0** ligne `forbidden` dans le log du driver et dans celui de la tâche. `kubectl auth can-i list secrets --as system:serviceaccount:airflow-project-one:spark` : `no`.
- **Le Role `pod-launcher` du chart est coupé (SEC-2).** Il donnait `pods` (create, delete, patch...) **et `pods/exec`** au scheduler et aux workers, pour le KubernetesPodOperator, que les DAGs n'utilisent pas. `allowPodLaunching: false` le retire ; le Role `airflow-scheduler` du chart du projet donne au scheduler ce que le KubernetesExecutor fait des pods worker (les lancer, les suivre, les étiqueter, les supprimer), sans `exec`. Les workers n'ont plus que le Role `spark-submit`. L'api-server garde le Role `pod-log-reader` du chart (lecture des logs d'une tâche en cours). `kubectl exec` reste possible pour l'utilisateur du cluster (`make k8s-install` importe les pools ainsi), pas pour un pod.
- **`pods:create` donne beaucoup.** Un pod créé peut monter n'importe quel Secret de l'espace de noms. Une `ValidatingAdmissionPolicy` (GA depuis Kubernetes 1.30, `admission-policy.yaml`) limite les pods créés par les comptes `spark` et `airflow-worker` : compte de service `spark`, image du projet seulement (conteneurs init et éphémères compris), aucun volume de Secret, aucun Secret lu par `env`/`envFrom` sauf le mot de passe de `file_list` (`secretKeyRef` du driver). Elle est liée à l'espace de noms du projet seulement.
- **Pod Security Admission (SEC-1).** `make k8s-install` étiquette l'espace de noms `pod-security.kubernetes.io/{enforce,warn,audit}=restricted`, version `v1.37` (celle du nœud kind, `PSA_LEVEL` / `PSA_VERSION`). Tous les pods y satisfont : Postgres (uid 999), toolbox, pods Airflow et leurs init containers (dont `ui-passwords`), gabarit des pods worker, gabarits du driver et des exécuteurs Spark. Un test vérifie le rendu des deux charts contre les règles du niveau `restricted` ; le run réel (§8) n'a eu aucun refus.

## 8. Validation réelle (2026-11-19)

Cluster kind d'un nœud, `make k8s-tools kind-up image kind-load k8s-install`, puis `make k8s-load-data DATA=data/orders_sample.csv`, les trois DAGs réactivés, `list_files_dag` déclenché :

| DAG | Run | État | Tâches |
|---|---|---|---|
| `list_files_dag` | `manual__p3_2_k8s_e2e` | success | |
| `read_file_dag` | `asset_triggered__…` (Asset `file_list`) | success | `resolve_source_run_ids`, `spark_read_files` [0], `publish_lake_staging` : success ; `fail_if_spark_failed` : skipped |
| `transform_dag` | `asset_triggered__…` (Asset `lake_staging`) | success | producteur watcher, `stg_*` run/test, mart run/test, `lineage_lake_staging`, `publish_dbt_marts` : success |

Log du driver (pod `arrow-spark-…-driver`, un exécuteur `readfilesjob-…-exec-1` enregistré) :

```
INFO utils.functions_utils - Retrieved 1 files from file_list for run_id=manual__p3_2_k8s_e2e
INFO __main__ - Writing to lake root file:///opt/airflow/shared/lake
INFO pipeline.file_reader.base_reader - data/orders_sample.csv: source 'orders' (format csv)
INFO storage.parquet_lake_writer - 5 rows written to file:///opt/airflow/shared/lake/raw/orders (lake_source_file='orders_sample.csv')
INFO storage.parquet_lake_writer - 3 rows written to file:///opt/airflow/shared/lake/quality/orders (lake_source_file='orders_sample.csv')
INFO storage.parquet_lake_writer - 5 rows written to file:///opt/airflow/shared/lake/staging/orders (lake_source_file='orders_sample.csv')
INFO utils.functions_utils - Updated file_list id=1 to status=processed
INFO __main__ - 1/1 files processed successfully.
```

`file_list` : une ligne `processed`. Entrepôt DuckDB (lecture seule, depuis la toolbox) : `stg_orders__current` contient les 5 commandes, `lake_run_id = manual__p3_2_k8s_e2e`. Interface : `make k8s-ui` écoute sur `127.0.0.1:8081` ; `/api/v2/monitor/health` répond `healthy` pour la base, le scheduler, le triggerer et le DAG processor.

### Revalidation après les revues (2026-11-21)

Cluster recréé (`make kind-down kind-up`), nouvelle image (`make image kind-load k8s-install IMAGE_TAG=p3-2-v3`), espace de noms en `restricted`, pod-launcher du chart coupé, politique d'admission active.

- **Installation.** Aucun avertissement PodSecurity dans la sortie de `kubectl` / `helm`. `kubectl label --dry-run=server ... enforce=restricted` sur l'espace de noms peuplé : aucun pod en infraction. Roles présents : `airflow-scheduler`, `spark-driver`, `spark-submit` et le `pod-log-reader` du chart, pas de `pod-launcher`.
- **RBAC effectif** (`kubectl auth can-i ... --subresource=exec --as system:serviceaccount:...`) : `pods/exec` refusé à `airflow-worker`, `airflow-scheduler`, `spark` et `airflow-api-server` ; `list secrets` refusé aux quatre. `pods/log` : l'api-server seulement.
- **Run de bout en bout** (`manual__p3_2_k8s_e2e_v3`) : `list_files_dag`, puis `read_file_dag` (Asset `file_list` ; driver et exécuteur en pods, driver sorti en 0 en 17 s) et `transform_dag` (Asset `lake_staging` ; neuf tâches) : **success**. `stg_orders__current` contient 5 lignes de ce run. Le driver porte le label `airflow-project-one/task-instance`.
- **Aucun 403 ni refus.** 0 ligne `forbidden` dans les 48 fichiers de log de tâches, dans les logs des 5 drivers et dans celui du scheduler. Aucun événement de refus d'admission.
- **Admission, en réel** (`--dry-run=server`). Un pod créé comme `airflow-worker` avec un volume de Secret : refusé (`ValidatingAdmissionPolicy 'airflow-project-one-spark-pods' ... must not mount a Secret`) ; le même sans le volume : accepté. Un pod sans `runAsNonRoot`, même créé par l'administrateur : refusé (`violates PodSecurity "restricted:v1.37"`).
- **Mot de passe de l'UI.** `POST /auth/token` avec le mot de passe du Secret `airflow-ui-admin` : 201 ; avec un autre : 401. Le mot de passe n'apparaît dans le log d'aucun conteneur de l'api-server (seul l'avertissement générique du Simple Auth Manager mentionne le mot « password »). `/api/v2/monitor/health` : tout `healthy`.
- **C-1, driver vivant** (run `manual__p3_2_c1c`). Tous les processus du conteneur driver figés par `SIGSTOP` (sauf `tini`, le PID 1), puis le superviseur de la tâche tué par `SIGKILL` dans le pod worker : pas d'`on_kill`, comme un OOM. Airflow passe la tâche en `up_for_retry`, le driver reste `Running`. Cinq minutes plus tard (`retry_delay`), la tentative 2 journalise `Deleting driver pod arrow-spark-5430a7a1fd4ddb38-driver (Running) of an earlier try of this task instance before submitting a new one`, attend sa disparition (entre 20 et 35 s : délai de grâce de 30 s, processus figés), puis soumet un seul nouveau driver, qui sort en 0 : run **success**, une ligne `processed` dans `file_list`.
- **C-1, driver terminé** (run `manual__p3_2_c1b`, même scénario mais le driver a fini avant le retry) : il est gardé (`Completed`), la tentative 2 soumet son driver et réussit.
- **C-2, montée de version du chart** : `make k8s-install PROJECT_CHART=<copie en version 0.2.1-c2check>` passe (révision 2 des deux releases), le StatefulSet Postgres porte le nouveau label `helm.sh/chart` et ses `volumeClaimTemplates` les seuls labels stables ; Postgres redémarre sur son volume (la ligne `processed` est toujours là). Les six Secrets générés sont `kept`, les trois dérivés réappliqués.
- Cluster supprimé à la fin (`make kind-down`).

## 9. Alternatives écartées

- **Spark Operator (kubeflow/spark-operator)** : une CRD, un contrôleur et un webhook à installer et à épingler, un autre opérateur Airflow (`SparkKubernetesOperator`) et un autre format de soumission. La politique de retry de P2-6, le callback de P3-3 et le lignage de P2-5 reposent sur `SparkSubmitOperator` ; `spark-submit` natif garde tout cela, avec la seule Connection pour différence. Décision de l'utilisateur.
- **Deux images (Airflow et `apache/spark:4.3.0`)** : le pod worker a de toute façon besoin d'une JVM et de `spark-submit`, et le driver du même code Python que les DAGs ; deux images, c'est deux versions de Python et de dépendances à garder alignées. L'image unique contient la distribution Spark de la roue PySpark, déjà dans `uv.lock`.
- **Sous-chart Postgres du chart Airflow** : image `bitnamilegacy` non maintenue.
- **`track_driver_via_k8s_api=True` du provider** (libère la JVM de `spark-submit` et interroge l'API) : il ne lit que la phase du pod, pas son code de sortie, et la politique de retry de P2-6 en a besoin.
- **Volume `hostPath` monté par kind (`extraMounts`)** : les pods tournent sous l'uid 50000, le répertoire de l'hôte appartient à l'utilisateur : il faudrait l'ouvrir en écriture à tous. Le provisionneur `local-path` de kind crée des volumes utilisables tels quels.
- **`kubectl create secret --from-literal`** : la valeur passe par la ligne de commande (`ps`).

## 10. Mode d'emploi

```bash
make k8s-tools                         # kind, kubectl, helm dans ~/.local/bin (épinglés, SHA-256 vérifiés)
make kind-up                           # cluster d'un nœud, API sur 127.0.0.1
make image kind-load                   # image du projet (IMAGE_TAG=dev par défaut), chargée dans le nœud
make k8s-install                       # Secrets, chart du projet, chart Airflow, pools
make k8s-load-data DATA=data/orders_sample.csv
make k8s-ui                            # http://127.0.0.1:8081, utilisateur admin ; make k8s-ui-password
make k8s-spark-clean                   # supprime les pods driver terminés
make k8s-uninstall                     # retire les deux releases (garde Secrets et volumes)
make kind-down                         # supprime le cluster et tout ce qu'il contient
```

- **Nouvelle image.** Changez de tag (`make image kind-load k8s-install IMAGE_TAG=dev2`) : `helm upgrade` remplace alors les pods. Avec le même tag, les pods déjà lancés gardent l'ancienne image.
- **DAGs.** Réactivez `read_file_dag` et `transform_dag` avant le premier listing (un événement d'Asset émis pendant que le DAG est en pause ne crée pas de run). Avec `kubectl exec deploy/airflow-scheduler -c scheduler -- airflow dags unpause <dag>`, ou dans l'interface.
- **Contexte kubectl.** Toutes les cibles nomment le contexte `kind-airflow-project-one` : elles n'agissent jamais sur un autre cluster de `~/.kube/config`. `kind create cluster` en fait cependant le contexte courant.

## 11. Limites et hors périmètre

- **Un seul nœud.** Les volumes sont `ReadWriteOnce`. Sur plusieurs nœuds, il faut un `ReadWriteMany` (NFS, EFS, Filestore...) ou un lake objet (S3, GCS) ; DuckDB reste à un seul écrivain.
- **Dev seulement.** Postgres sans sauvegarde ni réplication, Simple Auth Manager, pas d'Ingress ni de TLS.
- **Mis au backlog après les revues (non fait) :**
  - **NetworkPolicies** pour Airflow, Postgres et la toolbox (aujourd'hui seuls les exécuteurs Spark en ont une, posée par Spark), et **`spark.authenticate`** entre driver et exécuteurs (secret partagé par application ; son passage par un Secret Kubernetes reste à concevoir avec la politique d'admission) ;
  - **suivi des digests d'images** (Dockerfile, chart du projet, values) **par Dependabot**, et **`trivy image`** de l'image du projet en CI.
- **Lignage Spark sur Kubernetes : coupé.** Aucun transport OpenLineage n'est configuré dans les values, et l'opérateur retire de toute façon la conf du listener en mode cluster, avec un avertissement (§6). L'activer demanderait `spark.jars` en `local://` (le chemin du jar serait sinon à téléverser) et un transport HTTP (le transport `file` écrit dans le pod). Le jar est dans l'image, vérifié, pour cette suite.
- **Métriques (P3-3) : non branchées.** Le chart sait déployer un collecteur OTel ; non configuré ici.
- **xlsx sur Kubernetes** : non disponible (voir §6, `ENV=prod`).
- **Pods driver terminés** : ils restent, avec leur Service, leurs ConfigMaps et leur NetworkPolicy, jusqu'à `make k8s-spark-clean` (comportement documenté de Spark).
- **Nom de l'application Spark** : `arrow-spark`, le défaut du provider (le DAG ne fixe pas `name`).
- **Dependabot** ne suit ni le Dockerfile, ni le chart, ni kind / kubectl / Helm, ni les images du chart du projet : l'écosystème `docker` n'est pas déclaré (le test P3-4 l'interdit aujourd'hui, pour une autre raison). Au backlog (ci-dessus), avec la suite de P3-4.
- **kubectl** : signature cosign non vérifiée (somme SHA-256 seulement).
- **Redémarrage de l'hôte pendant `make kind-load`** (observé) : le conteneur du nœud kind redémarre avec une copie corrompue de l'image (`module 'pydantic' has no attribute 'VERSION'` dans le pod, alors que la même image est saine sous `docker run`), et un nouveau `kind load` ne la remplace pas. Symptôme : le Job de migration et les `wait-for-airflow-migrations` en `CrashLoopBackOff`. Remède : `make kind-down kind-up kind-load k8s-install`.
- **Taille de l'image** (4,15 Go) : une image Spark séparée et allégée, ou la suppression des paquets de la base avant la synchronisation, la réduiraient.
- **CI** : les tests `helm` / kubeconform / contexte Docker sont ignorés sans les outils (`REQUIRE_K8S=1` en fait des échecs) ; la CI ne construit pas l'image et n'installe pas kind.
