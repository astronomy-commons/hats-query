"""TAP service, see https://www.ivoa.net/Documents/TAP/20190927/index.html"""

from fastapi import APIRouter, Request
from fastapi.responses import Response

from hats_query.server.templating import templates

TAP_PATH = "/tap"

router = APIRouter(prefix=TAP_PATH)


@router.get("/availability", response_class=Response)
async def availability(request: Request) -> Response:
    """VOSI availability, see https://www.ivoa.net/Documents/VOSI/20170524/REC-VOSI-1.1.html#tth_sEc3.2"""
    return templates.TemplateResponse(
        request, "availability.xml", {"up_since": request.app.state.up_since}, media_type="text/xml"
    )
