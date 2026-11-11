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

-- Assurer l’unicité d’un fichier par version de son chemin
CREATE INDEX IF NOT EXISTS idx_file_path ON file_list (file_path);

-- Idempotent pour les bases déjà créées avant l'ajout de la colonne.
ALTER TABLE file_list ADD COLUMN IF NOT EXISTS error_message TEXT;
