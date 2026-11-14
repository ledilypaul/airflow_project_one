# P2-6 — Suites de la phase 2 : retries, contrôles IMDB, schéma orders, OpenLineage 1.56

- **Date :** 2026-11-14
- **Item de backlog :** P2-6 (`docs/audit/2026-11-05-baseline.md`)
- **Code :** `pipeline/jobs/read_files_job.py` (`EXIT_*`, `final_exit_code`), `pipeline/quality/checks.py` (`QualityChecksFailed.deterministic`), `dags/local_read_file_dag.py` (`ReadFilesSparkSubmitOperator`, `NO_RETRY_EXIT_CODES`), `config/sources/orders.yaml`, `transform/models/staging/`
- **Détails d'exploitation :** `CLAUDE.md` (codes de sortie, « Quality checks », « dbt transformation layer ») et `transform/README.md`

P2-6 regroupe quatre suites laissées ouvertes par P2-3, P2-4 et P2-5. Les deux premières touchent au comportement du pipeline, la troisième au schéma d'une source, la quatrième n'a rien changé.

## 1. Pas de retry pour un échec déterministe

### Le problème

Un contrôle qualité bloquant donnait exit 1, comme n'importe quel fichier en échec. `spark_read_files` (`retries=1`, `retry_delay=5 min`) relançait donc l'application Spark pour obtenir exactement le même résultat : mêmes données, mêmes contrôles, mêmes partitions réécrites. Les sorties 2, 3 et 5, déjà déterministes, étaient relancées de la même façon.

### La solution

- **Job : un code dédié, 6 (`EXIT_QUALITY_FAILED`).** Il est rendu quand au moins un fichier est `failed` et que **chaque** fichier en échec a échoué uniquement sur des contrôles bloquants qui ont trouvé des lignes en échec (statut `fail`, sévérité `error`), résultats enregistrés et partition staging supprimée. C'est `QualityChecksFailed.deterministic`.
- **Priorité** (`final_exit_code`) : 0 si rien n'a échoué, sinon 6 si tous les échecs sont de ce type, sinon 1. Il suffit d'un échec d'une autre nature pour garder 1, donc le retry.
- **Restent en 1**, parce qu'une nouvelle tentative peut changer l'issue :
  - erreur de lecture ou d'écriture ;
  - moteur qualité qui lève (`quality checks could not run`) ;
  - contrôle bloquant qui n'a pas pu s'exécuter (statut `error`, par exemple une erreur Spark) ;
  - résultats non enregistrés ou partition staging précédente non supprimée (notes de `QualityChecksFailed`) : un retry peut les réparer ;
  - deux fichiers de même nom.
- **DAG : `ReadFilesSparkSubmitOperator`**, une sous-classe de `SparkSubmitOperator` qui ne redéfinit que `execute`. Quand le hook lève son `AirflowException` et que spark-submit est sorti avec 2, 3, 5 ou 6 (`NO_RETRY_EXIT_CODES`), elle lève une `AirflowFailException`, dont le message nomme le code et garde celui du hook. 1, 4, un signal ou une erreur avant le lancement gardent le retry.
- **Inchangés :** `.partial().expand()` sur `application_args`, pool `spark`, champs templatés, `pre_execute=prepare_spark_lineage` (P2-5) et `on_kill`, tous hérités.
- **Aval inchangé :** `publish_lake_staging` (`all_done`) émet `lake_staging` dès qu'un fichier est `processed`, et `fail_if_spark_failed` (`one_failed`) garde le run en échec. Le test e2e le vérifie sur exit 6 avec un fichier `processed` et un `failed`.

### Faits vérifiés avant de coder

- **Le hook** (`apache-airflow-providers-apache-spark` 6.3.4, code installé `hooks/spark_submit.py`) garde le `subprocess.Popen` de spark-submit dans `_submit_sp`, puis lève `AirflowException("Cannot execute: <commande masquée>. Error code is: N.<fin du log>")` quand le code de retour est non nul. Il n'existe aucun accesseur public. `_spark_exit_code` n'est renseigné qu'en Kubernetes (code du pod driver lu dans le log). Un test lance le vrai hook avec un faux `spark-submit` pour épingler `_submit_sp.returncode`.
- **La propagation du code** : en mode local/client, `spark-submit` sort avec le code du driver Python. Vérifié avec PySpark 4.3.0 : `sys.exit(6)` donne `$? = 6`, et `sys.exit(1)` donne 1.
- **Le Task SDK** (`airflow/sdk/execution_time/task_runner.py`, Airflow 3.3.2) traite `AirflowFailException` à part, avec le commentaire « If AirflowFailException is raised, task should not retry », et la met en `FAILED` sans consulter `retries`.

### Alternatives écartées

- **Analyser le message du hook** (`Error code is: N`) : un format de texte est plus fragile qu'un attribut épinglé par un test.
- **`retries=0`** : on perdrait le retry utile pour 1 et 4.
- **Un callback de retry** : il ne peut pas annuler une tentative déjà planifiée.

### Tests

`tests/unit/jobs/test_p2_6_exit_codes.py` couvre la décision et `main()` avec un vrai Spark : 6 avec un autre fichier `processed`, 6 quand tous échouent, et 1 dans chaque cas mixte. `tests/unit/dags/test_p2_6_no_retry_exit_codes.py` couvre l'opérateur, avec un faux hook et avec le vrai hook. `tests/integration/test_p1_4_read_file_dag_e2e.py` couvre l'exécution réelle par `dag.test()`, `retries=1` conservé : une tentative sur exit 6, deux sur exit 1.

## 2. Contrôles IMDB : sévérité inchangée, faute de fichier

`config/sources/imdb.yaml` garde `not_null(id)` et `unique(id)` en `warn`, et n'ajoute ni `multiLine` ni `escape`.

- **Aucun fichier IMDB réel n'a été trouvé.** `source_directories` (`./data/`) ne contient que `orders_sample.csv`. Recherches faites :
  - `*imdb*`, `*tmdb*`, `*movie*` sous le répertoire utilisateur WSL ;
  - les mêmes motifs sous le profil Windows (hors `AppData`, `node_modules`, `.venv`, `.git`) ;
  - les chemins des anciennes configs (`../../Spark/data/`, `../../file_reader/`, `/data/imdb/`).

  Seule une ancienne copie du projet a été trouvée, sans données IMDB.
- **Le risque `multiLine` est réel, et la procédure le mesure.** Sur un CSV synthétique aux 42 colonnes d'`imdb.yaml` (3 films, dont un `overview` entre guillemets avec un retour à la ligne), voici les résultats :

  | Lecture | Lignes | `id` null | `not_null(id)` |
  |---|---|---|---|
  | config telle quelle | 4 | 1 (25 %) | `warn`, 1 ligne |
  | `multiLine: true` | 3 | 0 | `pass` |
  | `multiLine: true` + `escape: '"'` | 3 | 0 | `pass` |

  Sans `multiLine`, la seconde moitié du champ devient une ligne fantôme à `id` null. Ce n'est pas une preuve sur la vraie donnée : la décision attend le vrai fichier.
- **Liste blanche.** `multiLine` (booléen typé) et `escape` sont déjà dans `pipeline/config/reader_options.py`, avec leur source (doc Spark 4.3.0, CSV) : rien à ajouter.

### Procédure de vérification (à lancer sur le vrai fichier)

Le script lit le fichier avec la vraie config, comme le job : il nettoie les lignes pour le staging, puis lance les contrôles. Il le fait avec et sans `multiLine` / `escape`, n'écrit rien, et n'importe ni le job ni `utils.db_connection`, qui charge `.env` à l'import.

```bash
cd <racine du projet>
uv run python - /chemin/vers/le_fichier_imdb.csv <<'EOF'
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.file_reader.base_reader import BaseFileReader
from pipeline.processor.base_processor import BaseProcessor
from pipeline.quality import run_quality_checks

path = sys.argv[1]
spark = (
    SparkSession.builder.master("local[*]").appName("p2_6_check")
    .config("spark.sql.ansi.enabled", "false").config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
reader = BaseFileReader(spark)
config = reader.get_config(path)
print(f"config: {config.source.name}, options={config.reader.options}")
key = next((c.columns for c in config.quality.checks if c.type == "unique"), None)
variants = {
    "config as is": {},
    "multiLine": {"multiLine": True},
    "multiLine+escape": {"multiLine": True, "escape": '"'},
}
for label, extra in variants.items():
    options = {**config.reader.options, **extra}
    variant = config.model_copy(
        update={"reader": config.reader.model_copy(update={"options": options})}
    )
    df = reader.read_file(path, config=variant)
    staged = BaseProcessor(df).remove_empty_rows().remove_duplicates().df.cache()
    rows = staged.count()
    nulls = staged.filter(F.col(key[0]).isNull()).count()
    dups = staged.groupBy(*key).count().filter("count > 1").count()
    print(f"[{label}] read={df.count()} staged={rows} null {key[0]}={nulls} "
          f"({nulls / max(rows, 1):.2%}) duplicated keys={dups}")
    for r in run_quality_checks(staged, config.quality.checks):
        print(f"    {r.name:20} {r.severity:5} {r.status:5} failed_rows={r.failed_rows}")
    staged.unpersist()
spark.stop()
EOF
```

**Décision à prendre avec sa sortie :**

1. Si `multiLine` réduit le nombre de lignes ou de `id` null, ajouter `multiLine: true` à `reader.options`. Ajouter aussi `escape: '"'` si cette variante change encore le résultat : c'est le cas de guillemets doublés, que Spark échappe par défaut avec `\`.
2. Si les deux contrôles passent avec cette config, passer `not_null(id)` et `unique(id)` en `error`, et mettre à jour `test_p2_3_repo_imdb_checks_id_not_null_and_unique_as_warnings`.
3. Si un contrôle échoue encore, le laisser en `warn` et analyser les lignes en cause.

`make spark-job` sur un vrai run laisse en plus les résultats dans `<LAKE_ROOT>/quality/movies/`.

## 3. Schéma `orders`, puis contrat dbt

- **Le schéma vient de `data/orders_sample.csv`** : en-tête `order_id,customer,amount,order_date`, 5 lignes, UTF-8 (« Chloé »), montants à deux décimales, dates ISO. `columns_type` le déclare dans cet ordre : `int`, `string`, `double`, `date`.
  - `amount` est en `double`, faute de `decimal` dans `pipeline/config/types.py`. En `float`, 210.99 deviendrait 210.99000549.
  - `inferSchema` est retiré : Spark l'ignore dès qu'un schéma est donné.
- **`enforceSchema: false`.** Le schéma est positionnel. Une autre disposition de fichiers `orders` existe sur le poste (15 colonnes : `order_id,customer_id,order_date,status,...`), hors de `source_directories`, mais `orders_part_001.csv` correspondrait au motif `orders_*.csv`. Sans cette option, Spark l'aurait lue avec des colonnes décalées ou nulles. Avec elle, l'en-tête est comparé aux noms déclarés, et le fichier est `failed` avec un message explicite (test sur les deux cas).
- **Contrôles, tous en `warn`.** `not_null(order_id)` et `unique(order_id)` s'ajoutent à `row_count min 1` : un identifiant de commande est la seule règle évidente. Rien n'est supposé sur les montants, qui peuvent être négatifs pour un avoir. Ils passeront en `error` après revue de `quality/orders` sur de vrais fichiers.
- **dbt.**
  - La source `lake_staging.orders` déclare les quatre colonnes typées puis les colonnes techniques, si bien que la relation vide typée est complète.
  - `stg_orders__current` a un contrat appliqué, comme `stg_movies__current`.
  - Les tests d'alignement YAML↔source couvrent orders. Un nouveau test vérifie que chaque table `lake_staging` a un `columns_type`. Un test d'intégration vérifie qu'un ancien instantané en `BIGINT` casse le build.
- **Lecture réelle du sample avec la config** (procédure ci-dessus, sur `data/orders_sample.csv`) :

  ```
  config: orders, options={'header': True, 'delimiter': ',', 'encoding': 'UTF-8', 'enforceSchema': False}
  [config as is] read=5 staged=5 null order_id=0 (0.00%) duplicated keys=0
      not_null(order_id)   warn  pass  failed_rows=0
      unique(order_id)     warn  pass  failed_rows=0
      row_count            warn  pass  failed_rows=None
  ```

  Les variantes `multiLine` et `multiLine+escape` donnent la même chose : le sample n'a aucun champ entre guillemets.

  Le test synthétique de lecture (`-k synthetic`) passe avec la nouvelle config, ainsi qu'un test qui relit le contenu exact du sample avec ses types.
- **À savoir.** Un instantané écrit avant P2-6 dont un type diffère casse le contrat. `union_by_name` lit tous les instantanés : il faut donc supprimer ces partitions, relire le fichier ne suffit pas.

### Lake existant

Les partitions `staging/orders` écrites avant P2-6 (types inférés) élargissent les colonnes via `union_by_name` et peuvent faire échouer le contrat de `stg_orders__current`. Sur un lake déjà rempli, supprimer `<LAKE_ROOT>/staging/orders` (et `raw/orders`) puis relancer un listing avant `dbt build`. Le modèle liste ses colonnes explicitement, car le contrat associe par position.

## 4. OpenLineage 1.56 : pas encore publié

Rien n'a changé : le jar, le Makefile, `uv.lock` et les tests restent en 1.55.0. Vérifié le 2026-11-14 :

- **Maven Central.** `maven-metadata.xml` d'`io.openlineage:openlineage-spark_2.13` liste les versions jusqu'à 1.55.0 (`lastUpdated` 20261029154327), et `.../1.56.0/openlineage-spark_2.13-1.56.0.jar` répond 404.
- **GitHub.** La dernière release d'OpenLineage est 1.55.0, publiée le 2026-10-30.
- **PyPI.** `openlineage-python` 1.55.0 et `apache-airflow-providers-openlineage` 2.21.0 sont les dernières versions.

Sources :

- https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.13/maven-metadata.xml
- https://github.com/OpenLineage/OpenLineage/releases
- https://pypi.org/project/openlineage-python/
- https://pypi.org/project/apache-airflow-providers-openlineage/

**Quand 1.56.0 sortira :**

1. Changer `OPENLINEAGE_SPARK_VERSION` et `OPENLINEAGE_SPARK_SHA256` dans le Makefile. L'empreinte doit être égale au `.sha256` publié, et la signature `.asc` vérifiée avec la clé de la 1.55.0 (`B4AD C6AE E76F 4EED 97FA 78A1 C6C9 B79C 9583 2D71`), dans un trousseau GPG jetable.
2. Lancer `make lineage-jar`, puis `REQUIRE_LINEAGE=1 make check`.
3. Relire les règles de `FilesystemDatasetUtils` reprises dans `dags/local_transform_dag.py`.

## 5. Points ouverts

- **Pas de retry en mode cluster.** En YARN ou Kubernetes cluster, le code de retour de spark-submit n'est pas celui du driver : ces runs restent relancés, ce qui est le côté sûr. Le code du pod (`_spark_exit_code`) pourrait être lu en Kubernetes, mais ce n'est pas vérifié ici.
- **Statut `error` en sévérité `error`.** Un contrôle bloquant qui n'a pas pu s'exécuter reste en exit 1. Une colonne absente est pourtant déterministe, mais une erreur Spark ne l'est pas, et le moteur ne les distingue pas.
- **Fichiers de même nom (à corriger).** Ils sont déterministes mais sortent en 1, donc le run est relancé pour rien (relu par `/code-review`, 2026-11-14). À faire : leur donner le code 6 ou un code dédié dans `final_exit_code`, l'ajouter à `NO_RETRY_EXIT_CODES`, avec un test.
- **IMDB.** Il faut lancer la procédure du §2 sur le vrai fichier pour décider de `multiLine` et des sévérités.
- **Pas de `decimal`.** `pipeline/config/types.py` n'a pas de type décimal : les montants sont en `double`.
- **Chargement de `.env` à l'import.** `utils/db_connection.py` appelle `load_dotenv()` à l'import, si bien que tout import du job (tests compris) charge `.env` s'il existe. Hors périmètre.
