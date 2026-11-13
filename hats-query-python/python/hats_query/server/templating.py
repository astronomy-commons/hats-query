"""Jinja2 templates shared by all routes."""

from fastapi.templating import Jinja2Templates
from jinja2 import Environment, PackageLoader, select_autoescape

templates = Jinja2Templates(
    env=Environment(
        loader=PackageLoader("hats_query.server", "templates"),
        autoescape=select_autoescape(),
    )
)
