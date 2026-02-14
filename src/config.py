# ==============================================
# Configuration Management
# ==============================================
#
# PURPOSE:
#   Load and validate all configuration from environment
#   variables / .env file. Provides typed config objects
#   to all other modules.
#
# CLASSES:
# --------
# - MySQLConfig (dataclass)
#     host: str          (default "localhost")
#     port: int          (default 3306)
#     user: str          (default "root")
#     password: str      (default "root")
#     database: str      (default "adaptive_db")
#
# - MongoConfig (dataclass)
#     host: str          (default "localhost")
#     port: int          (default 27017)
#     user: str | None   (default None)
#     password: str | None (default None)
#     database: str      (default "adaptive_db")
#
# - BufferConfig (dataclass)
#     buffer_size: int           (default 50)
#     buffer_timeout_seconds: float (default 5.0)
#
# - AppConfig (dataclass)
#     mysql: MySQLConfig
#     mongo: MongoConfig
#     buffer: BufferConfig
#     data_stream_url: str       (default "http://127.0.0.1:8000/GET/record")
#     metadata_dir: str          (default "metadata/")
#
# FUNCTION:
# ---------
# - get_config() -> AppConfig
#     Load .env using python-dotenv, construct AppConfig.
#     Returns the same singleton on repeated calls.
#
# USAGE:
# ------
#   from src.config import get_config
#   config = get_config()
#   print(config.mysql.host)
#   print(config.buffer.buffer_size)
#
# ==============================================

import os
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class MySQLConfig:
    """MySQL database configuration."""
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = "root"
    database: str = "adaptive_db"


@dataclass
class MongoConfig:
    """MongoDB database configuration."""
    host: str = "localhost"
    port: int = 27017
    user: Optional[str] = None
    password: Optional[str] = None
    database: str = "adaptive_db"


@dataclass
class BufferConfig:
    """Buffer configuration for record batching."""
    buffer_size: int = 50
    buffer_timeout_seconds: float = 5.0


@dataclass
class AppConfig:
    """Main application configuration."""
    mysql: MySQLConfig
    mongo: MongoConfig
    buffer: BufferConfig
    data_stream_url: str = "http://127.0.0.1:8000/GET/record"
    metadata_dir: str = "metadata/"


# Singleton instance
_config_instance: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """
    Load configuration from environment variables / .env file.
    Returns the same singleton instance on repeated calls.
    
    Returns:
        AppConfig: Application configuration
    """
    global _config_instance
    
    if _config_instance is not None:
        return _config_instance
    
    # Load .env file from project root
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    
    # Build MySQL configuration
    mysql_config = MySQLConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DATABASE", "adaptive_db")
    )
    
    # Build MongoDB configuration
    mongo_config = MongoConfig(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        user=os.getenv("MONGO_USER") or None,
        password=os.getenv("MONGO_PASSWORD") or None,
        database=os.getenv("MONGO_DATABASE", "adaptive_db")
    )
    
    # Build Buffer configuration
    buffer_config = BufferConfig(
        buffer_size=int(os.getenv("BUFFER_SIZE", "50")),
        buffer_timeout_seconds=float(os.getenv("BUFFER_TIMEOUT_SECONDS", "5.0"))
    )
    
    # Build main application configuration
    _config_instance = AppConfig(
        mysql=mysql_config,
        mongo=mongo_config,
        buffer=buffer_config,
        data_stream_url=os.getenv("DATA_STREAM_URL", "http://127.0.0.1:8000/GET/record"),
        metadata_dir=os.getenv("METADATA_DIR", "metadata/")
    )
    
    return _config_instance
