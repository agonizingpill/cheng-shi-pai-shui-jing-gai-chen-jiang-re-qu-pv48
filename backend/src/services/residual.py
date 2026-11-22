"""Per-cover resurvey, residual hotspot detection and closeout gating.

The zone average is only a summary.  After a repair we collect a reading for
every cover in the zone, then re-run the *original* clustering scale over the
still over-threshold covers.  Clusters are residual hot zones; the remaining
over-threshold points are scatter that a supervisor has to read one by one.
"""

import hashlib
import json
from datetime import datetime, timezone
from math import isfinite

import numpy as np
from sklearn.cluster import DBSCAN
from sqlalchemy import select

from ..models import (
    AnalysisJob,
    CloseoutReceipt,
    RepairZone,
    ResidualAnalysis,
    ResurveyReading,
    ScatterDecision,
    SurveyPoint,
)
from .geometry import bounds

SETTLEMENT_THRESHOLD_MM = 8.0
DEFAULT_EPS_M = 26.0
DEFAULT_MIN_SAMPLES = 3


def iso(value):
    return value.isoformat() if value else None


def utc_stamp(value):
    """Normalise a datetime so comparisons survive DB round-trips (naive vs aware)."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def original_scale(db, zone):
    """Reuse the clustering scale of the batch that produced this zone."""
    job = db.execute(
        select(AnalysisJob)
        .where(AnalysisJob.batch_id == zone.batch_id, AnalysisJob.status == "succeeded")
        .order_by(AnalysisJob.id.desc())
    ).scalars().first()
    if job:
        params = (job.input_summary or {}).get("params") or (job.result or {})
        try:
            eps = float(params.get("eps_m", DEFAULT_EPS_M))
            min_samples = int(params.get("min_samples", DEFAULT_MIN_SAMPLES))
            if isfinite(eps) and eps > 0 and min_samples > 0:
                return {"eps_m": eps, "min_samples": min_samples, "source": f"批次任务 {job.job_code}"}
        except (TypeError, ValueError):
            pass
    return {"eps_m": DEFAULT_EPS_M, "min_samples": DEFAULT_MIN_SAMPLES, "source": "算法默认参数"}


def zone_member_points(db, zone):
    if not zone.point_ids:
        return []
    return db.execute(
        select(SurveyPoint).where(SurveyPoint.id.in_(zone.point_ids)).order_by(SurveyPoint.point_code)
    ).scalars().all()


def reading_fingerprint(readings):
    """Stable hash of the values a judgement was based on (ids + readings)."""
    snapshot = [
        [r.manhole_id, round(float(r.settlement_mm), 2), utc_stamp(r.measured_at), round(float(r.x), 4), round(float(r.y), 4)]
        for r in sorted(readings, key=lambda row: row.manhole_id)
    ]
    payload = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def readings_for_zone(db, zone_id):
    return db.execute(select(ResurveyReading).where(ResurveyReading.zone_id == zone_id)).scalars().all()


def missing_members(db, zone, readings, points=None):
    recorded = {r.manhole_id for r in readings}
    missing = []
    for point in points if points is not None else zone_member_points(db, zone):
        if point.manhole_id in recorded:
            continue
        missing.append({
            "manhole_id": point.manhole_id,
            "asset_code": point.manhole.asset_code if point.manhole else "",
            "street_name": point.manhole.street_name if point.manhole else "",
            "point_code": point.point_code,
            "settlement_before_mm": point.settlement_mm,
            "x": point.x,
            "y": point.y,
        })
    return missing


def reading_view(reading, survey_point=None):
    return {
        "id": reading.id,
        "zone_id": reading.zone_id,
        "manhole_id": reading.manhole_id,
        "asset_code": reading.manhole.asset_code if reading.manhole else "",
        "street_name": reading.manhole.street_name if reading.manhole else "",
        "survey_point_id": reading.survey_point_id,
        "point_code": survey_point.point_code if survey_point else None,
        "settlement_before_mm": survey_point.settlement_mm if survey_point else None,
        "settlement_mm": reading.settlement_mm,
        "x": reading.x,
        "y": reading.y,
        "measured_at": iso(reading.measured_at),
        "notes": reading.notes,
        "updated_at": iso(reading.updated_at),
    }


def run_analysis(db, zone, actor_id):
    """Cluster the over-threshold readings at the original DBSCAN scale."""
    points = zone_member_points(db, zone)
    readings = readings_for_zone(db, zone.id)
    missing = missing_members(db, zone, readings, points)
    if missing:
        return None, missing
    scale = original_scale(db, zone)
    threshold = SETTLEMENT_THRESHOLD_MM
    hot = [r for r in readings if round(float(r.settlement_mm), 2) > threshold]
    labels = np.array([], dtype=int)
    if hot:
        matrix = np.array([[r.x, r.y] for r in hot], dtype=float)
        labels = DBSCAN(eps=scale["eps_m"], min_samples=scale["min_samples"]).fit_predict(matrix)
    residuals, scatter = [], []
    for label in sorted({int(value) for value in labels}):
        members = [r for r, current in zip(hot, labels) if int(current) == label]
        if label < 0:
            for reading in members:
                scatter.append(_point_snapshot(reading))
            continue
        b = bounds([{"x": r.x, "y": r.y} for r in members])
        residual = {
            "cluster_label": label,
            "member_count": len(members),
            "center_x": round(sum(r.x for r in members) / len(members), 2),
            "center_y": round(sum(r.y for r in members) / len(members), 2),
            "average_settlement_mm": round(sum(r.settlement_mm for r in members) / len(members), 2),
            "max_settlement_mm": round(max(r.settlement_mm for r in members), 2),
            "boundary": b,
            "members": [_point_snapshot(r) for r in sorted(members, key=lambda row: row.manhole_id)],
        }
        residuals.append(residual)
    fingerprint = reading_fingerprint(readings)
    point_by_id = {point.id: point for point in points}
    before = [point_by_id[r.survey_point_id].settlement_mm for r in readings if r.survey_point_id in point_by_id]
    average_before = round(sum(before) / len(before), 2) if before else None
    average_after = round(sum(float(r.settlement_mm) for r in readings) / len(readings), 2) if readings else None
    result = {
        "algorithm": "DBSCAN over-threshold residual hotspot",
        "threshold_mm": threshold,
        "eps_m": scale["eps_m"],
        "min_samples": scale["min_samples"],
        "scale_source": scale["source"],
        "reading_count": len(readings),
        "over_threshold_count": len(hot),
        "average_before_mm": average_before,
        "average_after_mm": average_after,
        "average_meets_threshold": average_after is not None and average_after <= threshold,
        "residual_zones": residuals,
        "scatter_points": sorted(scatter, key=lambda row: row["manhole_id"]),
        "readings": [_point_snapshot(r) for r in sorted(readings, key=lambda row: row.manhole_id)],
        "formula": "settlement_mm > threshold -> DBSCAN(original eps/min_samples) -> residual zones",
    }
    analysis = ResidualAnalysis(
        analysis_code=f"RA-{zone.zone_code}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}",
        zone_id=zone.id,
        status="succeeded",
        input_fingerprint=fingerprint,
        params={"eps_m": scale["eps_m"], "min_samples": scale["min_samples"], "threshold_mm": threshold},
        result=result,
        reading_count=len(readings),
        residual_count=len(residuals),
        scatter_count=len(scatter),
        created_by=actor_id,
    )
    db.add(analysis)
    db.flush()
    # A fresh judgement supersedes older ones for this zone.
    for previous in db.execute(
        select(ResidualAnalysis).where(
            ResidualAnalysis.zone_id == zone.id,
            ResidualAnalysis.id != analysis.id,
            ResidualAnalysis.status == "succeeded",
        )
    ).scalars().all():
        previous.status = "superseded"
    return analysis, []


def _point_snapshot(reading):
    return {
        "manhole_id": reading.manhole_id,
        "asset_code": reading.manhole.asset_code if reading.manhole else "",
        "survey_point_id": reading.survey_point_id,
        "settlement_mm": reading.settlement_mm,
        "x": reading.x,
        "y": reading.y,
        "measured_at": iso(reading.measured_at),
    }


def latest_analysis(db, zone_id):
    return db.execute(
        select(ResidualAnalysis)
        .where(ResidualAnalysis.zone_id == zone_id)
        .order_by(ResidualAnalysis.id.desc())
    ).scalars().first()


def decisions_for(db, analysis_id):
    rows = db.execute(select(ScatterDecision).where(ScatterDecision.analysis_id == analysis_id)).scalars().all()
    return {row.manhole_id: row for row in rows}


def analysis_view(db, analysis, zone=None):
    zone = zone or db.get(RepairZone, analysis.zone_id)
    result = analysis.result or {}
    decisions = decisions_for(db, analysis.id)
    current = reading_fingerprint(readings_for_zone(db, zone.id))
    stale = analysis.status == "stale" or current != analysis.input_fingerprint
    scatter = []
    pending = 0
    rejected = 0
    for point in result.get("scatter_points", []):
        decision = decisions.get(point["manhole_id"])
        item = dict(point)
        item["decision"] = decision.decision if decision else None
        item["decision_note"] = decision.note if decision else None
        item["decided_by"] = decision.decided_by if decision else None
        item["decided_at"] = iso(decision.decided_at) if decision else None
        if not decision:
            pending += 1
        elif decision.decision == "residual":
            rejected += 1
        scatter.append(item)
    blockers = []
    if stale:
        blockers.append("复测读数已变更，旧判定失效，请重新运行残余热区分析")
    if analysis.residual_count:
        blockers.append(f"发现 {analysis.residual_count} 处残余热区，暂缓销项")
    if pending:
        blockers.append(f"{pending} 个超标散点待主管判读")
    if rejected:
        blockers.append(f"主管判定 {rejected} 个超标散点属于残余，暂缓销项")
    return {
        "id": analysis.id,
        "analysis_code": analysis.analysis_code,
        "zone_id": analysis.zone_id,
        "status": "stale" if stale else analysis.status,
        "input_fingerprint": analysis.input_fingerprint,
        "current_fingerprint": current,
        "params": analysis.params or {},
        "result": {
            "algorithm": result.get("algorithm"),
            "threshold_mm": result.get("threshold_mm", SETTLEMENT_THRESHOLD_MM),
            "eps_m": result.get("eps_m"),
            "min_samples": result.get("min_samples"),
            "scale_source": result.get("scale_source"),
            "reading_count": result.get("reading_count", analysis.reading_count),
            "over_threshold_count": result.get("over_threshold_count", 0),
            "average_before_mm": result.get("average_before_mm"),
            "average_after_mm": result.get("average_after_mm"),
            "average_meets_threshold": result.get("average_meets_threshold"),
            "residual_zones": result.get("residual_zones", []),
            "readings": result.get("readings", []),
            "formula": result.get("formula"),
        },
        "residual_count": analysis.residual_count,
        "scatter_count": analysis.scatter_count,
        "scatter_points": scatter,
        "pending_scatter_count": pending,
        "rejected_scatter_count": rejected,
        "stale": stale,
        "closeout": {"allowed": not blockers, "blockers": blockers},
        "created_at": iso(analysis.created_at),
        "finished_at": iso(analysis.finished_at),
    }


def receipt_view(receipt, replayed=False):
    return {
        "id": receipt.id,
        "receipt_code": receipt.receipt_code,
        "zone_id": receipt.zone_id,
        "analysis_id": receipt.analysis_id,
        "input_fingerprint": receipt.input_fingerprint,
        "decision": receipt.decision,
        "signer": receipt.signer,
        "note": receipt.note,
        "idempotency_key": receipt.idempotency_key,
        "conclusion": receipt.conclusion or {},
        "signed_at": iso(receipt.signed_at),
        "replayed": replayed,
    }


def receipt_for(db, zone_id):
    return db.execute(select(CloseoutReceipt).where(CloseoutReceipt.zone_id == zone_id)).scalars().first()


def state_for(db, zone):
    points = zone_member_points(db, zone)
    point_by_id = {point.id: point for point in points}
    readings = sorted(readings_for_zone(db, zone.id), key=lambda row: row.manhole_id)
    missing = missing_members(db, zone, readings, points)
    analysis = latest_analysis(db, zone.id)
    receipt = receipt_for(db, zone.id)
    analysis_payload = analysis_view(db, analysis, zone) if analysis else None
    if receipt and analysis_payload:
        analysis_payload["closeout"] = {"allowed": False, "blockers": [f"片区已签收销项（{receipt.receipt_code}）"]}
    return {
        "zone_id": zone.id,
        "zone_code": zone.zone_code,
        "status": zone.status,
        "required_count": len(readings) + len(missing),
        "recorded_count": len(readings),
        "complete": not missing,
        "threshold_mm": SETTLEMENT_THRESHOLD_MM,
        "missing": missing,
        "readings": [reading_view(reading, point_by_id.get(reading.survey_point_id)) for reading in readings],
        "analysis": analysis_payload,
        "receipt": receipt_view(receipt) if receipt else None,
    }
