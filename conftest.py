import pytest


@pytest.fixture(params=["duckdb", "postgres"])
def backend(request):
    """Parametrized backend fixture."""
    return request.param
