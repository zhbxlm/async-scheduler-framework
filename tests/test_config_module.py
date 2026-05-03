from async_scheduler.config import ConfigContext, Environment, detect_environment, get_config


def test_detect_environment_from_explicit_env(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    assert detect_environment() == Environment.PRODUCTION


def test_get_config_reads_runtime_settings(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "local")
    monkeypatch.setenv("DATABASE_URL", "mysql+asyncmy://u:p@db:3306/test")
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/0")
    cfg = get_config()
    assert cfg.environment == Environment.LOCAL
    assert cfg.database.is_mysql is True
    assert cfg.backends.redis_url == "redis://cache:6379/0"


def test_config_context_temporarily_overrides_env(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    with ConfigContext(LOG_LEVEL="DEBUG") as cfg:
        assert cfg.logging.level == "DEBUG"
    cfg2 = get_config()
    assert cfg2.logging.level != "DEBUG"
