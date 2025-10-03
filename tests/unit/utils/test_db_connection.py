import pytest
from unittest.mock import patch, MagicMock
import psycopg2
from utils.db_connection import db_connection, close_connection  # Remplace "your_module" par le vrai module

def test_db_connection_success():
    """Test si la connexion à la BDD est réussie."""
    mock_conn = MagicMock()

    with patch("psycopg2.connect", return_value=mock_conn) as mock_connect:
        conn = db_connection()
        
        # Vérifie que psycopg2.connect() a bien été appelé
        mock_connect.assert_called_once_with(
            host="localhost",
            database="apo_database",
            user="postgres",
            password="postgres"
        )

        # Vérifie que la fonction retourne bien une connexion
        assert conn == mock_conn

def test_db_connection_failure():
    """Test si une exception est levée en cas d'échec de connexion."""
    with patch("psycopg2.connect", side_effect=psycopg2.OperationalError("Connection failed")):
        with pytest.raises(Exception, match="Error during connection attempt = Connection failed"):
            db_connection()

def test_close_connection():
    """Test si la connexion est bien fermée."""
    mock_conn = MagicMock()

    close_connection(mock_conn)

    # Vérifie que conn.close() a bien été appelé
    mock_conn.close.assert_called_once()
