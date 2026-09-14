import os

import pytest
from hypothesis import settings

settings.register_profile("dev", deadline=None)
settings.register_profile("ci", max_examples=500, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "scale: loads real data volume; runs only in the full gate")
