CREATE TABLE IF NOT EXISTS file_list (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(255) NOT NULL,
    last_modified TIMESTAMP,
    status VARCHAR(255),
    dag_run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingested_at DATE DEFAULT CURRENT_DATE,
    error_message TEXT
);

-- Unicité sur (file_path, dag_run_id), pas sur file_path seul : le même
-- fichier source est légitimement re-listé à chaque exécution de
-- list_files_dag (un nouveau dag_run_id à chaque run), donc plusieurs
-- lignes avec le même file_path mais des dag_run_id différents sont
-- normales et voulues (historique par run). Cette contrainte empêche en
-- revanche le doublon quand la tâche d'insertion est retentée pour le même
-- run (cf. ON CONFLICT (file_path, dag_run_id) DO NOTHING côté application).
--
-- ATTENTION (opérationnel) : sur une base déjà peuplée contenant des
-- doublons (file_path, dag_run_id), cette création d'index UNIQUE échouera
-- tant que ces doublons n'ont pas été dédupliqués manuellement au préalable.
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_path ON file_list (file_path, dag_run_id);

-- Idempotent pour les bases déjà créées avant l'ajout de la colonne.
ALTER TABLE file_list ADD COLUMN IF NOT EXISTS error_message TEXT;
