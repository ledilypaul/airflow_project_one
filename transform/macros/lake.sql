{#
  Accès à la zone staging du lake (P2-4). Voir README.md.

  - check_lake_environment() : hook on-run-start (dbt_project.yml). Avant tout
    modèle : refuse un LAKE_ROOT invalide (lake_root()) ou un DBT_DUCKDB_PATH
    relatif, et exige au moins un fichier Parquet dans la zone staging.
  - lake_staging(source_name, table_name) : la relation à lire dans un modèle,
    à la place de source(). Si la table n'a encore aucun fichier Parquet (aucun
    fichier de cette source lu), renvoie une relation vide typée par les
    colonnes déclarées dans la source, au lieu de l'erreur de DuckDB sur un
    glob vide.
#}

{% macro lake_root() %}
  {#- LAKE_ROOT validé, sans « / » final. Mêmes règles que
      storage.parquet_lake_writer.resolve_lake_root (le job Spark), restreintes
      à ce que DuckDB sait lire :
      - chemin absolu, ou `file:` suivi d'un chemin absolu (`file:/x`,
        `file:///x`, `file://localhost/x`) ;
      - URI distante d'un schéma lu par DuckDB : s3, gs, gcs, r2 (extension
        httpfs), az, azure, abfss (extension azure), avec un hôte. Ces
        extensions ne sont jamais chargées implicitement (profiles.yml) :
        `extensions:` doit les déclarer ;
      - tout autre schéma (hdfs, dbfs, abfs...) est refusé par son nom : DuckDB
        n'y trouverait aucun fichier, sans erreur ;
      - un chemin relatif dépendrait du répertoire courant (Cosmos lance dbt
        dans une copie temporaire du projet) ; `~` n'est pas développé ici (le
        DAG et le Makefile le développent) ;
      - `'` casserait le SQL ; `=` serait lu comme une partition hive et
        `* ? [ {` comme des jokers du glob.
      Pas de défaut ici : l'appelant (Makefile, DAG) le fournit. -#}
  {%- set root = env_var('LAKE_ROOT', '') | trim -%}
  {%- if not root -%}
    {{ exceptions.raise_compiler_error(
        "LAKE_ROOT n'est pas défini : `make dbt-build` et transform_dag le fournissent "
        ~ "(défaut <racine du projet>/lake, comme le job Spark)."
    ) }}
  {%- endif -%}
  {%- for char in ["'", "=", "*", "?", "[", "{"] -%}
    {%- if char in root -%}
      {{ exceptions.raise_compiler_error(
          "LAKE_ROOT=" ~ root ~ " contient le caractère '" ~ char ~ "' : refusé (apostrophe "
          ~ "dans le SQL, '=' lu comme une partition, '* ? [ {' comme des jokers du glob)."
      ) }}
    {%- endif -%}
  {%- endfor -%}
  {%- if root.startswith('~') -%}
    {{ exceptions.raise_compiler_error(
        "LAKE_ROOT=" ~ root ~ " : `~` n'est pas développé par dbt. Donner un chemin absolu "
        ~ "(transform_dag et `make dbt-build` développent `~` eux-mêmes)."
    ) }}
  {%- endif -%}
  {%- set head = root.split(':', 1) -%}
  {%- set scheme = head[0] | lower if head | length == 2 and head[0] | length > 1 else '' -%}
  {%- set remote_schemes = ['s3', 'gs', 'gcs', 'r2', 'az', 'azure', 'abfss'] -%}
  {%- if not scheme -%}
    {%- if not root.startswith('/') -%}
      {{ exceptions.raise_compiler_error(
          "LAKE_ROOT=" ~ root ~ " est un chemin relatif : il dépendrait du répertoire courant "
          ~ "de dbt. Donner un chemin absolu ou une URI (file:///..., s3://bucket/...)."
      ) }}
    {%- endif -%}
  {%- elif scheme == 'file' -%}
    {%- if not head[1].startswith('/') -%}
      {{ exceptions.raise_compiler_error(
          "LAKE_ROOT=" ~ root ~ " est un chemin relatif : il dépendrait du répertoire courant "
          ~ "de dbt. Donner un chemin absolu ou une URI (file:///..., s3://bucket/...)."
      ) }}
    {%- endif -%}
  {%- elif scheme in remote_schemes -%}
    {%- if not head[1].startswith('//') or head[1][2:].split('/')[0] == '' -%}
      {{ exceptions.raise_compiler_error(
          "LAKE_ROOT=" ~ root ~ " : une URI " ~ scheme ~ ":// doit nommer son bucket ou son "
          ~ "conteneur (" ~ scheme ~ "://<nom>/...)."
      ) }}
    {%- endif -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error(
        "LAKE_ROOT=" ~ root ~ " : le schéma '" ~ scheme ~ "' n'est pas lu par DuckDB "
        ~ "(il n'y trouverait aucun fichier, sans erreur). Schémas acceptés : chemin absolu, "
        ~ "file:, " ~ remote_schemes | join(', ') ~ "."
    ) }}
  {%- endif -%}
  {{ return(root.rstrip('/')) }}
{% endmacro %}


{% macro check_lake_environment() %}
  {#- Hook on-run-start : avant tout modèle, donc l'entrepôt n'est jamais
      touché quand il échoue. -#}
  {%- if execute -%}
    {%- set root = lake_root() -%}
    {%- set path = target.path | string -%}
    {%- if path != ':memory:' and not path.startswith('/') and not path.startswith('md:') -%}
      {{ exceptions.raise_compiler_error(
          "DBT_DUCKDB_PATH=" ~ path ~ " est un chemin relatif : l'entrepôt serait créé dans "
          ~ "le répertoire courant de dbt (une copie temporaire sous Cosmos). Donner un chemin absolu."
      ) }}
    {%- endif -%}
    {#- transform_dag n'est déclenché qu'après une écriture staging : un lake
        sans aucun fichier staging est un LAKE_ROOT faux (faute de frappe, autre
        machine), pas un état à publier vide. Une table seule sans fichier
        reste légitime (relation vide typée de lake_staging()). -#}
    {%- set pattern = root ~ '/staging/*/lake_run_id=*/lake_source_file=*/*.parquet' -%}
    {%- set found = run_query("select count(*) from glob('" ~ pattern ~ "')").columns[0].values()[0] -%}
    {%- if found == 0 -%}
      {{ exceptions.raise_compiler_error(
          "aucun fichier Parquet de staging sous LAKE_ROOT=" ~ root ~ " (" ~ pattern ~ ") : "
          ~ "LAKE_ROOT faux ? transform_dag ne tourne qu'après une écriture de la zone staging."
      ) }}
    {%- endif -%}
  {%- endif -%}
  select 1
{% endmacro %}


{% macro lake_staging(source_name, table_name) %}
  {%- set relation = source(source_name, table_name) -%}
  {#- À l'analyse (dbt parse, manifest de Cosmos), seule la dépendance compte. -#}
  {%- if not execute -%}
    {{ return(relation) }}
  {%- endif -%}

  {%- set root = lake_root() -%}
  {#- Le glob est lu dans la source elle-même (seule définition du chemin). -#}
  {%- set location = relation | string -%}
  {%- set prefix = "read_parquet('" -%}
  {%- if not location.startswith(prefix) -%}
    {{ exceptions.raise_compiler_error(
        "lake_staging(" ~ source_name ~ ", " ~ table_name ~ ") : external_location inattendu "
        ~ "(attendu read_parquet('<glob>', ...)) : " ~ location
    ) }}
  {%- endif -%}
  {%- set pattern = location[prefix | length:].split("'")[0] -%}
  {%- if not pattern.startswith(root ~ '/') -%}
    {{ exceptions.raise_compiler_error(
        "lake_staging(" ~ source_name ~ ", " ~ table_name ~ ") : le glob " ~ pattern
        ~ " n'est pas sous LAKE_ROOT=" ~ root
    ) }}
  {%- endif -%}

  {%- set found = run_query("select count(*) from glob('" ~ pattern ~ "')").columns[0].values()[0] -%}
  {%- if found > 0 -%}
    {{ return(relation) }}
  {%- endif -%}

  {{ log(
      "lake_staging: aucun fichier Parquet pour " ~ source_name ~ "." ~ table_name
      ~ " (" ~ pattern ~ ") : relation vide typée par les colonnes déclarées.",
      info=true
  ) }}
  {{ return(lake_empty_relation(source_name, table_name)) }}
{% endmacro %}


{% macro lake_empty_relation(source_name, table_name) %}
  {#- Relation vide, typée par les colonnes (avec data_type) déclarées pour
      la source : les modèles en aval compilent et s'exécutent, leurs tests
      passent sur 0 ligne. -#}
  {%- set node = graph.sources.values()
      | selectattr('source_name', 'equalto', source_name)
      | selectattr('name', 'equalto', table_name)
      | first -%}
  {%- set columns = node.columns.values() | list -%}
  {%- set untyped = columns | rejectattr('data_type') | map(attribute='name') | list -%}
  {%- if not columns or untyped -%}
    {{ exceptions.raise_compiler_error(
        "lake_staging(" ~ source_name ~ ", " ~ table_name ~ ") : chaque colonne de la source "
        ~ "doit déclarer son data_type pour la relation vide (sans type : "
        ~ (untyped | join(', ') if untyped else 'aucune colonne déclarée') ~ ")"
    ) }}
  {%- endif -%}
  (select
  {%- for column in columns %}
    cast(null as {{ column.data_type }}) as {{ adapter.quote(column.name) }}{{ "," if not loop.last }}
  {%- endfor %}
  where false)
{% endmacro %}
