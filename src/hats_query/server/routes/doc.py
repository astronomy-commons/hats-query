"""Human-readable documentation page."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ..templating import templates

# TODO: take it from the TAP router once it exists
TAP_PATH = "/tap"

router = APIRouter()


@router.get("/doc", response_class=HTMLResponse)
async def doc(request: Request) -> HTMLResponse:
    """Describe where the services live."""
    return templates.TemplateResponse(request, "doc.html", {"tap_path": TAP_PATH})
