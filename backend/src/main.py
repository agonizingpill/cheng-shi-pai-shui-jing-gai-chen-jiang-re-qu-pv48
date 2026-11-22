import os, json, logging
from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from redis import Redis

from .operations_evidence import router as operations_evidence
from .database import Base, SessionLocal, engine, get_db
from .models import AnalysisJob, AuditLog, CloseoutReceipt, ManholeAsset, RepairZone, Resurvey, ResurveyReading, ScatterDecision, SurveyBatch, SurveyPoint, User, WorkNotice
from .schemas import BatchIn, CloseoutIn, LoginIn, NoticeIn, ResurveyIn, ResurveyReadingsIn, ScatterDecisionsIn, SurveyPointIn
from .security import create_token, current_user, require_roles, verify_password
from .services import residual as residual_service
from .services.audit import record
from .services.cluster import build_input
from .services.geometry import bounds, overlaps
from .middlewares.audit import RequestAuditMiddleware
from .middlewares.rate_limit import SimpleRateLimitMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manhole-settlement")
app = FastAPI(title="城市排水井盖沉降热区", version="1.0.0")
app.include_router(operations_evidence)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])
app.add_middleware(RequestAuditMiddleware)
app.add_middleware(SimpleRateLimitMiddleware)
redis_client = Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

@app.on_event("startup")
def startup():
    Base.metadata.create_all(engine)
    if os.getenv("SEED_DEMO", "true").lower() == "true":
        try:
            from scripts.seed_demo import seed
            seed()
        except Exception:
            logger.exception("seed_demo failed during startup")

@app.get("/health")
def health():
    return {"status": "ok", "service": "manhole-settlement-api"}

def iso(value):
    return value.isoformat() if value else None

def user_view(user):
    return {"id": user.id, "username": user.username, "display_name": user.display_name, "role": user.role}

def asset_view(a):
    return {"id": a.id, "asset_code": a.asset_code, "street_name": a.street_name, "x": a.x, "y": a.y, "coordinate_reference": a.coordinate_reference, "cover_type": a.cover_type, "depth_mm": a.depth_mm, "status": a.status, "last_inspected_at": iso(a.last_inspected_at)}

def point_view(p):
    return {"id": p.id, "point_code": p.point_code, "batch_id": p.batch_id, "manhole_id": p.manhole_id, "asset_code": p.manhole.asset_code if p.manhole else "", "street_name": p.manhole.street_name if p.manhole else "", "x": p.x, "y": p.y, "elevation_mm": p.elevation_mm, "settlement_mm": p.settlement_mm, "coordinate_reference": p.coordinate_reference, "measured_at": iso(p.measured_at), "quality_status": p.quality_status, "cluster_label": p.cluster_label, "local_density": p.local_density}

def zone_view(z, db):
    notice = db.get(WorkNotice, z.conflict_notice_id) if z.conflict_notice_id else None
    point_rows = db.execute(select(SurveyPoint).where(SurveyPoint.id.in_(z.point_ids or []))).scalars().all() if z.point_ids else []
    return {"id": z.id, "zone_code": z.zone_code, "batch_id": z.batch_id, "cluster_label": z.cluster_label, "name": z.name, "status": z.status, "point_ids": z.point_ids or [], "point_codes": [p.point_code for p in point_rows], "center_x": z.center_x, "center_y": z.center_y, "average_settlement_mm": z.average_settlement_mm, "max_settlement_mm": z.max_settlement_mm, "local_density": z.local_density, "boundary": z.boundary, "original_cluster_evidence": z.original_cluster_evidence, "split_from_id": z.split_from_id, "conflict_notice_id": z.conflict_notice_id, "split_reason": z.split_reason, "conflict_road": notice.road_name if notice else None, "residual": residual_service.state_for(db, z)}

def notice_view(n):
    return {"id": n.id, "notice_code": n.notice_code, "zone_id": n.zone_id, "road_name": n.road_name, "contractor": n.contractor, "permit_no": n.permit_no, "starts_at": iso(n.starts_at), "ends_at": iso(n.ends_at), "boundary": n.boundary, "status": n.status, "lane_closure_required": n.lane_closure_required, "conflict_note": n.conflict_note}

def resurvey_view(r):
    return {"id": r.id, "record_code": r.record_code, "zone_id": r.zone_id, "measured_at": iso(r.measured_at), "settlement_before_mm": r.settlement_before_mm, "settlement_after_mm": r.settlement_after_mm, "delta_mm": r.delta_mm, "pass_fail": r.pass_fail, "status": r.status, "verified_by": r.verified_by, "notes": r.notes}

@app.post("/api/auth/login")
def login(payload: LoginIn, db: Session = Depends(get_db)):
    user = db.execute(select(User).where(User.username == payload.username)).scalar_one_or_none()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="账号或密码错误")
    return {"access_token": create_token(user), "token_type": "bearer", "user": user_view(user)}

@app.get("/api/me")
def me(user: User = Depends(current_user)):
    return user_view(user)

@app.get("/api/dashboard")
def dashboard(db: Session = Depends(get_db), user: User = Depends(current_user)):
    batches = db.execute(select(SurveyBatch).order_by(SurveyBatch.measured_at.desc())).scalars().all()
    assets = db.execute(select(ManholeAsset).order_by(ManholeAsset.asset_code)).scalars().all()
    points = db.execute(select(SurveyPoint).order_by(SurveyPoint.id)).scalars().all()
    zones = db.execute(select(RepairZone).order_by(RepairZone.id)).scalars().all()
    notices = db.execute(select(WorkNotice).order_by(WorkNotice.id)).scalars().all()
    resurveys = db.execute(select(Resurvey).order_by(Resurvey.id.desc())).scalars().all()
    jobs = db.execute(select(AnalysisJob).order_by(AnalysisJob.id.desc())).scalars().all()
    return {"user": user_view(user), "stats": {"assets": len(assets), "points": len(points), "zones": len(zones), "open_zones": sum(z.status not in ["closed", "split"] for z in zones), "notices": len(notices), "resurveys": len(resurveys)}, "batches": [{"id": b.id, "batch_code": b.batch_code, "measured_at": iso(b.measured_at), "instrument": b.instrument, "coordinate_reference": b.coordinate_reference, "status": b.status, "notes": b.notes, "point_count": len(b.points)} for b in batches], "assets": [asset_view(a) for a in assets], "points": [point_view(p) for p in points], "zones": [zone_view(z, db) for z in zones], "notices": [notice_view(n) for n in notices], "resurveys": [resurvey_view(r) for r in resurveys], "jobs": [{"id": j.id, "job_code": j.job_code, "batch_id": j.batch_id, "status": j.status, "result": j.result, "error": j.error, "finished_at": iso(j.finished_at)} for j in jobs]}

@app.get("/api/survey-batches/{batch_id}")
def batch_detail(batch_id: int, db: Session = Depends(get_db), user: User = Depends(current_user)):
    batch = db.get(SurveyBatch, batch_id)
    if not batch:
        raise HTTPException(404, "测量批次不存在")
    return {"id": batch.id, "batch_code": batch.batch_code, "measured_at": iso(batch.measured_at), "instrument": batch.instrument, "coordinate_reference": batch.coordinate_reference, "status": batch.status, "notes": batch.notes, "points": [point_view(p) for p in batch.points]}

@app.post("/api/survey-batches")
def create_batch(payload: BatchIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    if db.execute(select(SurveyBatch).where(SurveyBatch.batch_code == payload.batch_code)).scalar_one_or_none():
        raise HTTPException(409, "批次编号已存在")
    batch = SurveyBatch(batch_code=payload.batch_code, measured_at=payload.measured_at, instrument=payload.instrument, coordinate_reference=payload.coordinate_reference, notes=payload.notes, status="open")
    db.add(batch); record(db, user.id, "create_batch", "SurveyBatch", payload.batch_code, {"coordinate_reference": payload.coordinate_reference}); db.commit(); db.refresh(batch)
    return {"id": batch.id, "batch_code": batch.batch_code, "status": batch.status}

@app.post("/api/survey-batches/{batch_id}/points")
def add_point(batch_id: int, payload: SurveyPointIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    batch = db.get(SurveyBatch, batch_id)
    asset = db.get(ManholeAsset, payload.manhole_id)
    if not batch or not asset:
        raise HTTPException(404, "批次或井盖资产不存在")
    if batch.status in ["clustered", "queued"]:
        raise HTTPException(409, "批次已进入聚类流程，不能追加测量点")
    if db.execute(select(SurveyPoint).where(SurveyPoint.point_code == payload.point_code)).scalar_one_or_none():
        raise HTTPException(409, "测量点编号已存在")
    point = SurveyPoint(batch_id=batch_id, **payload.model_dump())
    db.add(point); record(db, user.id, "add_survey_point", "SurveyPoint", payload.point_code, {"batch_code": batch.batch_code}); db.commit(); db.refresh(point)
    return point_view(point)

@app.post("/api/survey-points/{point_id}/correct-datum")
def correct_datum(point_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    point = db.get(SurveyPoint, point_id)
    if not point:
        raise HTTPException(404, "测量点不存在")
    batch = db.get(SurveyBatch, point.batch_id)
    old = point.coordinate_reference; point.coordinate_reference = batch.coordinate_reference; point.quality_status = "datum_corrected"
    if batch.status == "blocked": batch.status = "ready"
    record(db, user.id, "correct_coordinate_reference", "SurveyPoint", point.point_code, {"from": old, "to": point.coordinate_reference}); db.commit()
    return {"point_code": point.point_code, "coordinate_reference": point.coordinate_reference, "batch_status": batch.status}

@app.post("/api/survey-batches/{batch_id}/cluster")
def enqueue_cluster(batch_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    batch = db.get(SurveyBatch, batch_id)
    if not batch:
        raise HTTPException(404, "测量批次不存在")
    points = build_input(db, batch)
    refs = {batch.coordinate_reference, *(p["coordinate_reference"] for p in points)}
    if len(refs) != 1:
        batch.status = "blocked"
        record(db, user.id, "block_cluster_datum_conflict", "SurveyBatch", batch.batch_code, {"references": sorted(refs)})
        db.commit()
        conflicts = [p["point_code"] for p in points if p["coordinate_reference"] != batch.coordinate_reference]
        raise HTTPException(status_code=409, detail={"code": "DATUM_CONFLICT", "message": "测量点存在坐标基准冲突，禁止聚类", "point_codes": conflicts, "batch_status": batch.status})
    job = AnalysisJob(job_code=f"JOB-{batch.batch_code}-{int(datetime.now().timestamp())}", batch_id=batch.id, status="queued", input_summary={"points": points, "params": {"eps_m": 26.0, "min_samples": 3}})
    batch.status = "queued"; db.add(job); record(db, user.id, "enqueue_spatial_cluster", "AnalysisJob", job.job_code, {"batch_code": batch.batch_code, "point_count": len(points)}); db.commit(); db.refresh(job)
    redis_client.rpush("manhole_settlement_jobs", str(job.id))
    return {"job_id": job.id, "job_code": job.job_code, "status": job.status}

@app.get("/api/analysis-jobs/{job_id}")
def job_detail(job_id: int, db: Session = Depends(get_db), user: User = Depends(current_user)):
    job = db.get(AnalysisJob, job_id)
    if not job: raise HTTPException(404, "分析任务不存在")
    return {"id": job.id, "job_code": job.job_code, "status": job.status, "result": job.result, "error": job.error, "finished_at": iso(job.finished_at)}

@app.get("/api/zones")
def zones(db: Session = Depends(get_db), user: User = Depends(current_user)):
    return [zone_view(z, db) for z in db.execute(select(RepairZone).order_by(RepairZone.id)).scalars().all()]

@app.post("/api/zones/{zone_id}/split")
def split_zone(zone_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status != "needs_split": raise HTTPException(409, "当前片区没有待拆分的道路边界冲突")
    points = db.execute(select(SurveyPoint).where(SurveyPoint.id.in_(zone.point_ids))).scalars().all()
    notice = db.get(WorkNotice, zone.conflict_notice_id)
    ordered = sorted(points, key=lambda p: (p.x, p.y))
    split_x = (notice.boundary["min_x"] + notice.boundary["max_x"]) / 2 if notice else ordered[len(ordered)//2].x
    left = [p for p in ordered if p.x <= split_x]
    right = [p for p in ordered if p.x > split_x]
    if not left or not right:
        half = max(1, len(ordered)//2); left, right = ordered[:half], ordered[half:]
    zone.status = "split"; zone.split_reason = f"依据 {notice.road_name if notice else '道路边界'} 在 x={round(split_x, 2)} 拆分，保留原始聚类证据"
    children = []
    for idx, members in enumerate([left, right], 1):
        b = bounds([{ "x": p.x, "y": p.y } for p in members])
        child = RepairZone(zone_code=f"{zone.zone_code}-S{idx}", batch_id=zone.batch_id, cluster_label=zone.cluster_label, name=f"{zone.name} / 边界片 {idx}", status="ready_for_notice", point_ids=[p.id for p in members], center_x=round(sum(p.x for p in members)/len(members),2), center_y=round(sum(p.y for p in members)/len(members),2), average_settlement_mm=round(sum(p.settlement_mm for p in members)/len(members),2), max_settlement_mm=max(p.settlement_mm for p in members), local_density=zone.local_density, boundary=b, original_cluster_evidence={"split_from": zone.zone_code, "split_axis": "x", "split_x": split_x, "source_evidence": zone.original_cluster_evidence}, split_from_id=zone.id)
        db.add(child); children.append(child)
    record(db, user.id, "split_zone_by_road_boundary", "RepairZone", zone.zone_code, {"children": [f"{zone.zone_code}-S1", f"{zone.zone_code}-S2"], "split_x": split_x})
    db.commit()
    return {"source_zone": zone.zone_code, "children": [c.zone_code for c in children], "status": zone.status, "split_reason": zone.split_reason}

@app.post("/api/zones/{zone_id}/work-notices")
def create_notice(zone_id: int, payload: NoticeIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status not in ["proposed", "ready_for_notice"]: raise HTTPException(409, "当前片区不能发起施工联动")
    notice = WorkNotice(notice_code=f"WN-{zone.zone_code}-{int(datetime.now().timestamp())}", zone_id=zone.id, road_name=payload.road_name, contractor=payload.contractor, permit_no=payload.permit_no, starts_at=payload.starts_at, ends_at=payload.ends_at, boundary=zone.boundary, status="submitted", lane_closure_required=payload.lane_closure_required, created_by=user.id)
    db.add(notice); zone.status = "notice_sent"; record(db, user.id, "submit_work_notice", "WorkNotice", notice.notice_code, {"zone_code": zone.zone_code}); db.commit(); db.refresh(notice)
    return notice_view(notice)

@app.post("/api/work-notices/{notice_id}/acknowledge")
def acknowledge_notice(notice_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    notice = db.get(WorkNotice, notice_id)
    if not notice: raise HTTPException(404, "施工联动单不存在")
    if notice.status != "submitted": raise HTTPException(409, "施工联动单不在待确认状态")
    notice.status = "active"; record(db, user.id, "acknowledge_work_notice", "WorkNotice", notice.notice_code, {"status": "active"}); db.commit()
    return notice_view(notice)

@app.post("/api/zones/{zone_id}/start-repair")
def start_repair(zone_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    notice = db.execute(select(WorkNotice).where(WorkNotice.zone_id == zone.id, WorkNotice.status == "active")).scalar_one_or_none()
    if not notice: raise HTTPException(409, "施工联动单未激活，不能开始维修")
    zone.status = "repairing"; record(db, user.id, "start_repair", "RepairZone", zone.zone_code, {"notice_code": notice.notice_code}); db.commit()
    return zone_view(zone, db)

@app.post("/api/zones/{zone_id}/resurvey")
def submit_resurvey(zone_id: int, payload: ResurveyIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status != "repairing": raise HTTPException(409, "片区未进入维修中，不能提交复测")
    after = round(payload.settlement_after_mm, 2); before = zone.average_settlement_mm; delta = round(before - after, 2); pass_fail = "pass" if after <= 8.0 else "fail"
    record_code = f"RS-{zone.zone_code}-{int(datetime.now().timestamp())}"
    resurvey = Resurvey(record_code=record_code, zone_id=zone.id, measured_at=payload.measured_at, settlement_before_mm=before, settlement_after_mm=after, delta_mm=delta, pass_fail=pass_fail, status="submitted", notes=payload.notes)
    db.add(resurvey); zone.status = "awaiting_resurvey"; record(db, user.id, "submit_resurvey", "Resurvey", record_code, {"zone_code": zone.zone_code, "pass_fail": pass_fail}); db.commit(); db.refresh(resurvey)
    return resurvey_view(resurvey)

@app.post("/api/resurveys/{resurvey_id}/verify")
def verify_resurvey(resurvey_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    resurvey = db.get(Resurvey, resurvey_id)
    if not resurvey: raise HTTPException(404, "复测记录不存在")
    if resurvey.status != "submitted": raise HTTPException(409, "复测记录不在待核验状态")
    zone = db.get(RepairZone, resurvey.zone_id)
    state = residual_service.state_for(db, zone)
    if state["readings"]:
        raise HTTPException(409, {"code": "PER_COVER_RESURVEY_REQUIRED", "message": "该片区已按井盖复测，请在复测页完成残余热区判定后由主管签收销项", "analysis": state["analysis"]})
    resurvey.verified_by = user.id
    if resurvey.pass_fail == "pass":
        resurvey.status = "verified"; zone.status = "closed"
        notice = db.execute(select(WorkNotice).where(WorkNotice.zone_id == zone.id)).scalar_one_or_none()
        if notice: notice.status = "completed"
    else:
        resurvey.status = "rejected"; zone.status = "repairing"
    record(db, user.id, "verify_resurvey", "Resurvey", resurvey.record_code, {"result": resurvey.pass_fail, "zone_status": zone.status}); db.commit()
    return {"resurvey": resurvey_view(resurvey), "zone": zone_view(zone, db)}

@app.post("/api/zones/{zone_id}/readings")
def record_readings(zone_id: int, payload: ResurveyReadingsIn, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    """Record (or re-record) the per-cover reading after a repair.

    Missing covers can be filled in later; a changed reading invalidates any
    earlier residual judgement until the analysis is re-run.
    """
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status not in ["repairing", "awaiting_resurvey"]: raise HTTPException(409, "片区未进入维修复测阶段，不能录入复测值")
    members = {p.manhole_id: p for p in residual_service.zone_member_points(db, zone)}
    unknown = [item.manhole_id for item in payload.readings if item.manhole_id not in members]
    if unknown:
        raise HTTPException(422, {"code": "MANHOLE_NOT_IN_ZONE", "message": "复测井盖不属于该片区", "manhole_ids": unknown})
    seen = set()
    for item in payload.readings:
        if item.manhole_id in seen:
            raise HTTPException(422, {"code": "DUPLICATE_READING", "message": "同一井盖在本批次重复提交", "manhole_ids": [item.manhole_id]})
        seen.add(item.manhole_id)
    existing = {r.manhole_id: r for r in residual_service.readings_for_zone(db, zone.id)}
    changed = []
    for item in payload.readings:
        point = members[item.manhole_id]
        reading = existing.get(item.manhole_id)
        value = round(item.settlement_mm, 2)
        if reading:
            if reading.settlement_mm != value or residual_service.utc_stamp(reading.measured_at) != residual_service.utc_stamp(item.measured_at):
                reading.settlement_mm = value
                reading.measured_at = item.measured_at
                reading.notes = item.notes or reading.notes
                reading.recorded_by = user.id
                changed.append(point.manhole.asset_code if point.manhole else str(item.manhole_id))
        else:
            db.add(ResurveyReading(zone_id=zone.id, manhole_id=item.manhole_id, survey_point_id=point.id, settlement_mm=value, x=point.x, y=point.y, measured_at=item.measured_at, notes=item.notes, recorded_by=user.id))
    if zone.status == "repairing":
        zone.status = "awaiting_resurvey"
    db.flush()
    state = residual_service.state_for(db, zone)
    record(db, user.id, "record_resurvey_readings", "RepairZone", zone.zone_code, {"recorded": len(payload.readings), "changed": changed, "complete": state["complete"]})
    db.commit()
    state = residual_service.state_for(db, zone)
    return {"zone": zone_view(zone, db), "state": state, "changed": changed}


@app.get("/api/zones/{zone_id}/residual")
def zone_residual(zone_id: int, db: Session = Depends(get_db), user: User = Depends(current_user)):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    return residual_service.state_for(db, zone)


@app.post("/api/zones/{zone_id}/residual-analysis")
def run_residual_analysis(zone_id: int, db: Session = Depends(get_db), user: User = Depends(require_roles("operator", "supervisor"))):
    """Re-run DBSCAN at the original scale over still over-threshold readings."""
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    if zone.status not in ["repairing", "awaiting_resurvey"]: raise HTTPException(409, "片区未进入维修复测阶段，不能重新分析")
    analysis, missing = residual_service.run_analysis(db, zone, user.id)
    if missing:
        raise HTTPException(status_code=409, detail={"code": "RESURVEY_INCOMPLETE", "message": "复测未覆盖全部井盖，先补齐再分析", "missing": missing})
    analysis.finished_at = datetime.now(timezone.utc)
    record(db, user.id, "run_residual_analysis", "ResidualAnalysis", analysis.analysis_code, {"zone_code": zone.zone_code, "residual_count": analysis.residual_count, "scatter_count": analysis.scatter_count, "fingerprint": analysis.input_fingerprint})
    db.commit(); db.refresh(analysis)
    return residual_service.analysis_view(db, analysis, zone)


@app.post("/api/zones/{zone_id}/scatter-decisions")
def decide_scatter(zone_id: int, payload: ScatterDecisionsIn, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    """Supervisor reads the over-threshold scatter points one by one."""
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    analysis = residual_service.latest_analysis(db, zone.id)
    if not analysis: raise HTTPException(409, "请先运行残余热区分析")
    view = residual_service.analysis_view(db, analysis, zone)
    if view["stale"]:
        raise HTTPException(409, {"code": "ANALYSIS_STALE", "message": "复测读数已变更，旧判定失效，请重新运行残余热区分析"})
    allowed = {point["manhole_id"] for point in view["scatter_points"]}
    invalid = [item.manhole_id for item in payload.decisions if item.manhole_id not in allowed]
    if invalid:
        raise HTTPException(422, {"code": "NOT_A_SCATTER_POINT", "message": "该井盖不在超标散点列表中", "manhole_ids": invalid})
    existing = residual_service.decisions_for(db, analysis.id)
    for item in payload.decisions:
        row = existing.get(item.manhole_id)
        if row:
            row.decision = item.decision; row.note = item.note; row.decided_by = user.id; row.decided_at = datetime.utcnow()
        else:
            db.add(ScatterDecision(zone_id=zone.id, analysis_id=analysis.id, manhole_id=item.manhole_id, decision=item.decision, note=item.note, decided_by=user.id))
    record(db, user.id, "decide_scatter_points", "ResidualAnalysis", analysis.analysis_code, {"zone_code": zone.zone_code, "decisions": [{"manhole_id": item.manhole_id, "decision": item.decision} for item in payload.decisions]})
    db.commit()
    return residual_service.analysis_view(db, analysis, zone)


@app.post("/api/zones/{zone_id}/closeout")
def sign_closeout(zone_id: int, payload: CloseoutIn, db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor"))):
    """Idempotent supervisor sign-off; a replay returns the original receipt."""
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    existing_receipt = residual_service.receipt_for(db, zone.id)
    if existing_receipt:
        return residual_service.receipt_view(existing_receipt, replayed=True)
    if zone.status == "split":
        raise HTTPException(409, {"code": "ZONE_SPLIT", "message": "片区已拆分，请对边界片区签收"})
    analysis = residual_service.latest_analysis(db, zone.id)
    if not analysis:
        raise HTTPException(409, {"code": "ANALYSIS_REQUIRED", "message": "请先运行残余热区分析并完成散点判读"})
    if payload.analysis_id and payload.analysis_id != analysis.id:
        raise HTTPException(409, {"code": "ANALYSIS_SUPERSEDED", "message": "指定的分析结果已被更新的判定取代，请刷新"})
    view = residual_service.analysis_view(db, analysis, zone)
    if view["stale"]:
        raise HTTPException(409, {"code": "ANALYSIS_STALE", "message": "复测读数已变更，旧判定失效，请重新运行残余热区分析后再签收", "analysis_id": analysis.id})
    if not view["closeout"]["allowed"]:
        raise HTTPException(409, {"code": "RESIDUAL_HOTSPOT", "message": "；".join(view["closeout"]["blockers"]), "blockers": view["closeout"]["blockers"], "residual_zones": view["result"]["residual_zones"], "analysis_id": analysis.id})
    receipt = CloseoutReceipt(
        receipt_code=f"CR-{zone.zone_code}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}",
        zone_id=zone.id,
        analysis_id=analysis.id,
        input_fingerprint=analysis.input_fingerprint,
        decision="closed",
        conclusion={
            "zone_code": zone.zone_code,
            "threshold_mm": view["result"]["threshold_mm"],
            "reading_count": view["result"]["reading_count"],
            "average_before_mm": view["result"].get("average_before_mm"),
            "average_after_mm": view["result"].get("average_after_mm"),
            "residual_count": analysis.residual_count,
            "scatter_count": analysis.scatter_count,
            "eps_m": view["result"]["eps_m"],
            "min_samples": view["result"]["min_samples"],
            "analysis_code": analysis.analysis_code,
        },
        signer_id=user.id,
        signer=user.username,
        note=payload.note,
        idempotency_key=payload.idempotency_key,
    )
    db.add(receipt); zone.status = "closed"
    record(db, user.id, "sign_closeout_receipt", "RepairZone", zone.zone_code, {"receipt_code": receipt.receipt_code, "analysis_code": analysis.analysis_code, "fingerprint": analysis.input_fingerprint, "idempotency_key": payload.idempotency_key})
    try:
        db.commit()
    except IntegrityError:
        # A concurrent retry already wrote the receipt: return that original conclusion.
        db.rollback()
        original = residual_service.receipt_for(db, zone.id)
        if not original:
            raise
        return residual_service.receipt_view(original, replayed=True)
    db.refresh(receipt)
    return residual_service.receipt_view(receipt)


@app.get("/api/zones/{zone_id}/closeout-receipt")
def get_closeout_receipt(zone_id: int, db: Session = Depends(get_db), user: User = Depends(current_user)):
    zone = db.get(RepairZone, zone_id)
    if not zone: raise HTTPException(404, "维修片区不存在")
    receipt = residual_service.receipt_for(db, zone.id)
    if not receipt: raise HTTPException(404, "该片区尚无签收回执")
    return residual_service.receipt_view(receipt)


@app.get("/api/audit-logs")
def audit_logs(db: Session = Depends(get_db), user: User = Depends(require_roles("supervisor", "viewer"))):
    rows = db.execute(select(AuditLog).order_by(AuditLog.id.desc()).limit(80)).scalars().all()
    return [{"id": r.id, "action": r.action, "entity_type": r.entity_type, "entity_id": r.entity_id, "detail": r.detail, "created_at": iso(r.created_at)} for r in rows]
