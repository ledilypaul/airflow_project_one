CREATE TABLE IF NOT EXISTS file_list (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(255) NOT NULL,
    last_modified TIMESTAMP,
    status VARCHAR(255),
    dag_run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingested_at DATE DEFAULT CURRENT_DATE,
    error_message TEXT,
    source_name VARCHAR(100) NOT NULL DEFAULT ''
);

-- Idempotent pour les bases déjà créées avant l'ajout des colonnes. Tout ce script
-- peut être rejoué sur une base existante (`psql -f utils/SQL/script_tables.sql`) :
-- les lignes d'avant P5-3 gardent `source_name = ''` (aucune source).
ALTER TABLE file_list ADD COLUMN IF NOT EXISTS error_message TEXT;
ALTER TABLE file_list ADD COLUMN IF NOT EXISTS source_name VARCHAR(100) NOT NULL DEFAULT '';

-- Unicité sur (file_path, dag_run_id, source_name), pas sur file_path seul : le
-- même fichier source est légitimement re-listé à chaque exécution du DAG
-- `ingest__<source>` (un nouveau dag_run_id à chaque run), donc plusieurs
-- lignes avec le même file_path mais des dag_run_id différents sont normales et
-- voulues (historique par run). `source_name` en fait partie (P5-3) : deux DAGs
-- de sources différentes peuvent avoir le même dag_run_id (un `scheduled__...`
-- à la même minute) et deux sources peuvent réclamer le même fichier. Cette
-- contrainte empêche en revanche le doublon quand la tâche d'insertion est
-- retentée pour le même run (cf. ON CONFLICT (file_path, dag_run_id, source_name)
-- DO NOTHING côté application).
--
-- ATTENTION (opérationnel) : sur une base déjà peuplée contenant des
-- doublons (file_path, dag_run_id, source_name), cette création d'index UNIQUE
-- échouera tant que ces doublons n'ont pas été dédupliqués manuellement au
-- préalable (avant P5-3 l'index (file_path, dag_run_id) l'interdisait déjà).
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_path_source ON file_list (file_path, dag_run_id, source_name);

-- Remplacé par idx_file_path_source (créé plus haut, avant ce retrait).
DROP INDEX IF EXISTS idx_file_path;
