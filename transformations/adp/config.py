import os
import yaml
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv


def load_config(config_file: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from config file with environment variable substitution."""
    # Load environment variables from .env file
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    
    config_path = Path(__file__).parent.parent / config_file
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    # Substitute environment variables in config values
    config = _substitute_env_vars(config)
    return config


def _substitute_env_vars(obj):
    """Recursively substitute environment variables in config object."""
    if isinstance(obj, dict):
        return {key: _substitute_env_vars(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_substitute_env_vars(item) for item in obj]
    elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
        env_var = obj[2:-1]  # Remove ${ and }
        env_value = os.getenv(env_var)
        if env_value is None:
            # For testing, return a placeholder if env var not found
            return f"MISSING_{env_var}"
        return env_value
    else:
        return obj


def get_database_config(config_file: str = "config.yaml") -> Dict[str, Any]:
    """Get database configuration."""
    config = load_config(config_file)
    return config.get('database', {})


def get_logging_config(config_file: str = "config.yaml") -> Dict[str, Any]:
    """Get logging configuration."""
    config = load_config(config_file)
    return config.get('logging', {})


def get_adp_config(config_file: str = "config.yaml") -> Dict[str, Any]:
    """Get ADP-specific configuration."""
    config = load_config(config_file)
    return config.get('adp', {})


def get_database_url(config_file: str = "config.yaml") -> str:
    """Construct database URL from configuration."""
    db_config = get_database_config(config_file)
    
    engine = db_config.get('engine', 'postgresql')
    
    if engine == 'duckdb':
        return f"duckdb:///{db_config['database']}"
    elif engine == 'postgresql':
        return (
            f"postgresql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            f"?sslmode={db_config.get('sslmode', 'prefer')}"
        )
    else:
        raise ValueError(f"Unsupported database engine: {engine}")