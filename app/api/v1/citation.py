"""Citation endpoints for academic use."""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/citation")


class CitationResponse(BaseModel):
    """Citation response with BibTeX format."""

    format: str
    citation: str
    apa: str
    chicago: str


BIBTEX_CITATION = """@misc{rpa_landuse_2020,
  title = {USDA Forest Service 2020 RPA Assessment: Land Use Projections},
  author = {{USDA Forest Service}},
  year = {2020},
  howpublished = {\\url{https://www.fs.usda.gov/research/rpa}},
  note = {Accessed via RPA Land Use Analytics. County-level land use transition projections for 2020-2070 across 20 climate scenarios.}
}"""

APA_CITATION = (
    "USDA Forest Service. (2020). 2020 RPA Assessment: Land Use Projections. "
    "Retrieved from https://www.fs.usda.gov/research/rpa"
)

CHICAGO_CITATION = (
    "USDA Forest Service. \"2020 RPA Assessment: Land Use Projections.\" 2020. "
    "https://www.fs.usda.gov/research/rpa."
)


@router.get("/bibtex", response_model=CitationResponse)
async def get_citation():
    """
    Get citation information for the RPA Land Use dataset.

    Returns BibTeX format as the primary citation format,
    along with APA and Chicago style alternatives.
    """
    return CitationResponse(
        format="bibtex",
        citation=BIBTEX_CITATION,
        apa=APA_CITATION,
        chicago=CHICAGO_CITATION,
    )
