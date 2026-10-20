"""HTTP layer of hats-query: the FastAPI application, its routes and templates."""

from .app import create_app

__all__ = ["create_app"]
