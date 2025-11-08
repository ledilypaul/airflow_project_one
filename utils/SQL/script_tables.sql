CREATE TABLE file_list (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(255) NOT NULL,
    last_modified TIMESTAMP,
    status VARCHAR(255),
    dag_run_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ingested_at DATE DEFAULT CURRENT_DATE
);

-- Assurer l’unicité d’un fichier par version de son chemin
CREATE INDEX idx_file_path ON file_list (file_path);
