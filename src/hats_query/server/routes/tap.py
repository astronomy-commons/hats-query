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
            "tables_url": request.url_for("tables"),
            "examples_url": request.url_for("examples"),
        },
        media_type="text/xml",
    )


@router.get("/tables", response_class=Response)
async def tables(request: Request) -> Response:
    """VOSI tables, see https://www.ivoa.net/Documents/VOSI/20170524/REC-VOSI-1.1.html#tth_sEc3.3"""
    engine = request.app.state.engine
    schemas = await _query_rows(
        engine, "SELECT schema_name, description FROM TAP_SCHEMA.schemas ORDER BY schema_index"
    )
    for schema in schemas:
        schema["tables"] = []
    tables_by_schema = {schema["schema_name"]: schema["tables"] for schema in schemas}
    columns_by_table = {}
    for table in await _query_rows(
        engine, "SELECT schema_name, table_name, description FROM TAP_SCHEMA.tables ORDER BY table_index"
    ):
        table["columns"] = []
        columns_by_table[table["table_name"]] = table["columns"]
        tables_by_schema[table["schema_name"]].append(table)
    for column in await _query_rows(
        engine,
        "SELECT table_name, column_name, description, unit, ucd, utype, datatype, arraysize, xtype, "
        "indexed, principal, std FROM TAP_SCHEMA.columns ORDER BY column_index",
    ):
        columns_by_table[column["table_name"]].append(column)
    return templates.TemplateResponse(request, "tables.xml", {"schemas": schemas}, media_type="text/xml")


async def _query_rows(engine, adql: str) -> list[dict]:
    """Run an ADQL query and return its rows as dicts."""
    return [row async for batch in engine.query(adql) for row in batch.to_pylist()]


@router.get("/examples", response_class=Response)
async def examples(request: Request) -> Response:
    """DALI examples, see https://www.ivoa.net/documents/DALI/20231215/REC-DALI-1.1.html#tth_sEc2.3"""
    return templates.TemplateResponse(
        request, "examples.html", {"examples": request.app.state.config.tap.examples}, media_type="text/html"
    )
