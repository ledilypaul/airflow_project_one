import pytest
from unittest.mock import patch, MagicMock
from utils.db_connection import db_connection
from utils.functions_utils import insert_into_file_list  # Remplace "your_module" par le vrai module

def test_insert_into_file_list_success():
    # Mock de la connexion et du curseur
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # La connexion retourne un curseur
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Mock de la fonction db_connection
    with patch("utils.db_connection.db_connection", return_value=mock_conn):
        # Données d'entrée
        data = ("test_file.csv", "/path/to/test_file.csv", "2024-01-31")

        # Appel de la fonction à tester
        insert_into_file_list(data)

        # Vérifier que cursor.execute() a bien été appelé avec la bonne requête et les bons paramètres
        mock_cursor.execute.assert_called_once_with(
            """
                INSERT INTO file_list (file_name, file_path, last_modified)
                VALUES (%s, %s, %s)
            """.strip(), data
        )

def test_insert_into_file_list_db_error():
    # Mock de la connexion et du curseur
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # Simuler une erreur SQL sur cursor.execute()
    mock_cursor.execute.side_effect = Exception("DB error")

    # Mock de la fonction db_connection
    with patch("utils.db_connection.db_connection", return_value=mock_conn):
        data = ("test_file.csv", "/path/to/test_file.csv", "2024-01-31 12:00:00")

        # Vérifier que l'exception est bien levée
        with pytest.raises(Exception, match="Error during insertion attempt = DB error"):
            insert_into_file_list(data)
