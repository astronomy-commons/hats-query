"""FastAPI application factory."""

from fastapi import FastAPI

from .config import Config
from .routes import doc


def create_app(config: Config | None = None) -> FastAPI:
    """Create the hats-query FastAPI application.

    Each service lives in its own module under `hats_query.server.routes`
    and exposes an `APIRouter`, which is included here.

    Parameters
    ----------
    config : Config, optional
        The server configuration, `Config.from_env` by default.

    Returns
    -------
    FastAPI
        The configured application.
    """
    if config is None:
        config = Config.from_env()
    app = FastAPI(title="hats-query", debug=config.server.developer_mode)
    app.state.config = config
    app.include_router(doc.router)
    return app
