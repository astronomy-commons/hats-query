"""FastAPI application factory."""

from datetime import UTC, datetime

from fastapi import FastAPI
from pyvo.dal.tap import IVOA_DATETIME_FORMAT

from hats_query._extension import build_engine
from hats_query.server.config import Config
from hats_query.server.routes import doc, tap


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
    _set_state(app, config)
    _include_routers(app)
    return app


def _set_state(app: FastAPI, config: Config) -> None:
    """Store the application-wide values the routes read from `app.state`."""
    app.state.config = config
    app.state.engine = build_engine(config.mounts, config.tap.tables)
    app.state.up_since = datetime.now(UTC).strftime(IVOA_DATETIME_FORMAT)


def _include_routers(app: FastAPI) -> None:
    """Include the `APIRouter` of each route module."""
    app.include_router(doc.router)
    app.include_router(tap.router)
