"""Residual-hotspot and closeout reconciliation rules for this project."""

RESIDUAL_VIEWS = {
    'resurvey-residual-hotspot': (
        '复测残余热区与签收',
        '按井盖复测覆盖情况、原尺度残余聚类和超标散点判读，核对每个片区能否签收销项。',
        ['片区', '状态', '复测覆盖', '均值前 毫米', '均值后 毫米', '残余热区', '超标散点', '待判读散点', '签收回执', '销项条件'],
        """
SELECT z.zone_code,z.status,
       (SELECT COUNT(*) FROM resurvey_readings rr WHERE rr.zone_id=z.id)::text||'/'||json_array_length(z.point_ids)::text AS coverage,
       ra.result->>'average_before_mm',ra.result->>'average_after_mm',
       COALESCE(ra.residual_count,0),COALESCE(ra.scatter_count,0),
       (SELECT COUNT(*) FROM json_array_elements(COALESCE(ra.result->'scatter_points','[]'::json)) p
         WHERE NOT EXISTS (SELECT 1 FROM scatter_decisions sd WHERE sd.analysis_id=ra.id AND sd.manhole_id=(p->>'manhole_id')::int)) AS pending,
       cr.receipt_code,
       CASE WHEN cr.id IS NOT NULL THEN '已签收 '||cr.receipt_code
            WHEN ra.id IS NULL THEN '未运行残余分析'
            WHEN COALESCE(ra.residual_count,0)>0 THEN '存在残余热区，暂缓销项'
            WHEN (SELECT COUNT(*) FROM json_array_elements(COALESCE(ra.result->'scatter_points','[]'::json)) p
                   WHERE NOT EXISTS (SELECT 1 FROM scatter_decisions sd WHERE sd.analysis_id=ra.id AND sd.manhole_id=(p->>'manhole_id')::int))>0
                 THEN '超标散点待主管判读'
            WHEN (SELECT COUNT(*) FROM scatter_decisions sd WHERE sd.analysis_id=ra.id AND sd.decision='residual')>0 THEN '主管判为残余，暂缓销项'
            WHEN (SELECT COUNT(*) FROM resurvey_readings rr WHERE rr.zone_id=z.id)<json_array_length(z.point_ids)
                 THEN '复测缺测待补齐'
            ELSE '可签收销项' END
FROM repair_zones z
LEFT JOIN LATERAL (SELECT * FROM residual_analyses a WHERE a.zone_id=z.id AND a.status='succeeded' ORDER BY a.id DESC LIMIT 1) ra ON TRUE
LEFT JOIN closeout_receipts cr ON cr.zone_id=z.id
ORDER BY z.id
""",
    ),
}
