"""Human-readable documentation page."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from hats_query.server.routes.tap import TAP_PATH
from hats_query.server.templating import templates

router = APIRouter()


@router.get("/doc", response_class=HTMLResponse)
async def doc(request: Request) -> HTMLResponse:
    """Describe where the services live."""
    return templates.TemplateResponse(request, "doc.html", {"tap_path": TAP_PATH})
