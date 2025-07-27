"""Sample data fixtures for testing."""

from typing import Dict, List, Any
import json
import tempfile
from pathlib import Path


class SampleDataFixtures:
    """Provides sample data for testing purposes."""

    @staticmethod
    def simple_csv_data() -> str:
        """Simple CSV data for basic testing."""
        return """id,name,age,city
1,Alice,25,New York
2,Bob,30,Los Angeles
3,Charlie,35,Chicago
4,Diana,28,Houston
5,Eve,32,Phoenix"""

    @staticmethod
    def complex_csv_data() -> str:
        """Complex CSV data with various data types."""
        return """id,name,age,salary,hire_date,is_active,department,skills
1,Alice Johnson,25,75000.50,2023-01-15,true,Engineering,"Python,SQL,Docker"
2,Bob Smith,30,85000.75,2022-03-22,true,Data Science,"R,Python,Statistics"
3,Charlie Brown,35,95000.00,2021-07-10,false,Engineering,"Java,Kubernetes,AWS"
4,Diana Prince,28,80000.25,2023-05-01,true,Product,"Design,Analytics,SQL"
5,Eve Wilson,32,90000.00,2020-11-30,true,Engineering,"Python,React,MongoDB" """

    @staticmethod
    def json_data() -> List[Dict[str, Any]]:
        """Sample JSON data structure."""
        return [
            {
                "id": 1,
                "user": {
                    "name": "Alice",
                    "email": "alice@example.com",
                    "profile": {
                        "age": 25,
                        "location": "New York"
                    }
                },
                "orders": [
                    {"id": 101, "amount": 25.99, "status": "completed"},
                    {"id": 102, "amount": 45.50, "status": "pending"}
                ]
            },
            {
                "id": 2,
                "user": {
                    "name": "Bob",
                    "email": "bob@example.com",
                    "profile": {
                        "age": 30,
                        "location": "Los Angeles"
                    }
                },
                "orders": [
                    {"id": 103, "amount": 15.75, "status": "completed"}
                ]
            }
        ]

    @staticmethod
    def api_response_data() -> Dict[str, Any]:
        """Sample API response structure."""
        return {
            "status": "success",
            "data": {
                "users": [
                    {
                        "id": 1,
                        "username": "alice_dev",
                        "email": "alice@example.com",
                        "created_at": "2023-01-15T10:30:00Z",
                        "metadata": {
                            "role": "engineer",
                            "department": "backend",
                            "skills": ["python", "sql", "docker"]
                        }
                    },
                    {
                        "id": 2,
                        "username": "bob_analyst",
                        "email": "bob@example.com",
                        "created_at": "2023-02-20T14:45:00Z",
                        "metadata": {
                            "role": "analyst",
                            "department": "data",
                            "skills": ["r", "python", "statistics"]
                        }
                    }
                ]
            },
            "pagination": {
                "page": 1,
                "per_page": 10,
                "total": 2,
                "total_pages": 1
            }
        }

    @staticmethod
    def database_schema() -> Dict[str, Any]:
        """Sample database schema for testing."""
        return {
            "tables": {
                "users": {
                    "columns": [
                        {"name": "id", "type": "INTEGER", "primary_key": True},
                        {"name": "username", "type": "VARCHAR(50)", "nullable": False},
                        {"name": "email", "type": "VARCHAR(100)", "nullable": False},
                        {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
                    ],
                    "indexes": ["email"]
                },
                "orders": {
                    "columns": [
                        {"name": "id", "type": "INTEGER", "primary_key": True},
                        {"name": "user_id", "type": "INTEGER", "foreign_key": "users.id"},
                        {"name": "amount", "type": "DECIMAL(10,2)", "nullable": False},
                        {"name": "status", "type": "VARCHAR(20)", "nullable": False},
                        {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
                    ],
                    "indexes": ["user_id", "status"]
                }
            }
        }

    @classmethod
    def create_temp_csv_file(cls, content: str = None) -> Path:
        """Create a temporary CSV file with sample data."""
        if content is None:
            content = cls.simple_csv_data()
        
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', 
            suffix='.csv', 
            delete=False
        )
        temp_file.write(content)
        temp_file.close()
        
        return Path(temp_file.name)

    @classmethod
    def create_temp_json_file(cls, data: Any = None) -> Path:
        """Create a temporary JSON file with sample data."""
        if data is None:
            data = cls.json_data()
        
        temp_file = tempfile.NamedTemporaryFile(
            mode='w', 
            suffix='.json', 
            delete=False
        )
        json.dump(data, temp_file, indent=2)
        temp_file.close()
        
        return Path(temp_file.name)

    @staticmethod
    def cleanup_temp_file(file_path: Path) -> None:
        """Clean up temporary file."""
        if file_path.exists():
            file_path.unlink()