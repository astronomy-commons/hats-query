"""FastAPI application factory."""

from fastapi import FastAPI

from .routes import doc


def create_app() -> FastAPI:
    """Create the hats-query FastAPI application.

    Each service lives in its own module under `hats_query.server.routes`
    and exposes an `APIRouter`, which is included here.

    Returns
    -------
    FastAPI
        The configured application.
    """
    app = FastAPI(title="hats-query")
    app.include_router(doc.router)
    return app
