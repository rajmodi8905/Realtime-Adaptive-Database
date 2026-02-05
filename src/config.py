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

pass
