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


@router.get("/capabilities", response_class=Response)
async def capabilities(request: Request) -> Response:
    """VOSI capabilities, see https://www.ivoa.net/Documents/VOSI/20170524/REC-VOSI-1.1.html#tth_sEc3.1"""
    capabilities_url = str(request.url_for("capabilities"))
    return templates.TemplateResponse(
        request,
        "capabilities.xml",
        {
            # TAP base URL is the parent of its VOSI endpoints
            "tap_url": capabilities_url.removesuffix("/capabilities"),
            "capabilities_url": capabilities_url,
            "availability_url": request.url_for("availability"),
            "examples_url": request.url_for("examples"),
        },
        media_type="text/xml",
    )


@router.get("/examples", response_class=Response)
async def examples(request: Request) -> Response:
    """DALI examples, see https://www.ivoa.net/documents/DALI/20231215/REC-DALI-1.1.html#tth_sEc2.3"""
    return templates.TemplateResponse(
        request, "examples.html", {"examples": request.app.state.config.tap.examples}, media_type="text/html"
    )
