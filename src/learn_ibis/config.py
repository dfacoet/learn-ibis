"""
Database configuration using Pydantic settings.
"""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # Database connection parameters
    db_name: str = Field(default="ibis", alias="DB_NAME")
    db_user: str = Field(default="username", alias="DB_USER")
    db_password: str = Field(default="password", alias="DB_PASSWORD")
    db_host: str = Field(default="localhost", alias="DB_HOST")
    db_port: int = Field(default=5433, alias="DB_PORT")

    # Optional direct connection string
    db_connection_string: Optional[str] = Field(
        default=None, alias="DB_CONNECTION_STRING"
    )

    @property
    def connection_string(self) -> str:
        """Get the database connection string."""
        if self.db_connection_string:
            return self.db_connection_string

        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def ibis_connection_string(self) -> str:
        """Get the connection string formatted for ibis."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

    def get_connection_params(self) -> dict:
        """Get connection parameters as a dictionary."""
        return {
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
        }

    def get_ibis_connection_params(self) -> dict:
        """Get connection parameters specifically for ibis (more reliable than connection string)."""
        return {
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
        }


# Global settings instance
db_settings = DatabaseSettings()


def get_db_settings() -> DatabaseSettings:
    """Get the database settings instance."""
    return db_settings
