from datetime import datetime
from pydantic import BaseModel, Field

class LoginIn(BaseModel):
    username: str
    password: str

class SurveyPointIn(BaseModel):
    point_code: str = Field(min_length=3, max_length=60)
    manhole_id: int
    x: float
    y: float
    elevation_mm: float
    settlement_mm: float = Field(ge=0, le=200)
    coordinate_reference: str
    measured_at: datetime

class BatchIn(BaseModel):
    batch_code: str
    measured_at: datetime
    instrument: str
    coordinate_reference: str
    notes: str = ""

class NoticeIn(BaseModel):
    road_name: str
    contractor: str
    permit_no: str
    starts_at: datetime
    ends_at: datetime
    lane_closure_required: bool = False

class ResurveyIn(BaseModel):
    measured_at: datetime
    settlement_after_mm: float = Field(ge=0, le=200)
    notes: str = ""

class ResurveyReadingIn(BaseModel):
    manhole_id: int
    settlement_mm: float = Field(ge=0, le=200)
    measured_at: datetime
    notes: str = Field(default="", max_length=500)

class ResurveyReadingsIn(BaseModel):
    readings: list[ResurveyReadingIn] = Field(min_length=1)

class ScatterDecisionIn(BaseModel):
    manhole_id: int
    decision: str = Field(pattern=r"^(accepted|residual)$")
    note: str = Field(default="", max_length=500)

class ScatterDecisionsIn(BaseModel):
    decisions: list[ScatterDecisionIn] = Field(min_length=1)

class CloseoutIn(BaseModel):
    analysis_id: int | None = None
    note: str = Field(default="", max_length=500)
    idempotency_key: str | None = Field(default=None, max_length=80)
