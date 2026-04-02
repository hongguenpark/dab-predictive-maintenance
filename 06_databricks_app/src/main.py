"""
예측 정비 (Predictive Maintenance) - 장비 상태 모니터링 대시보드
Databricks App으로 배포되는 FastAPI 웹 애플리케이션
"""

import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

app = FastAPI(title="예측 정비 대시보드")
w = WorkspaceClient()


def get_connection():
    return dbsql.connect(
        server_hostname=w.config.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        credentials_provider=lambda: w.config.authenticate,
    )


@app.get("/", response_class=HTMLResponse)
def dashboard():
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 장비 상태 조회
        cursor.execute("""
            SELECT e.equipment_id, e.equipment_name, e.criticality, e.operating_hours,
                   ROUND(AVG(s.vibration_rms), 2) as avg_vibration,
                   ROUND(AVG(s.temperature_c), 1) as avg_temp,
                   ebay_anomaly_detection_catalog.predictive_maintenance.assess_vibration_status(
                       AVG(s.vibration_rms), AVG(s.temperature_c)) as status
            FROM ebay_anomaly_detection_catalog.predictive_maintenance.equipment_master e
            JOIN ebay_anomaly_detection_catalog.predictive_maintenance.sensor_readings s ON e.equipment_id = s.equipment_id
            GROUP BY ALL ORDER BY e.equipment_id
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        status_colors = {"정상": "#22c55e", "주의": "#eab308", "위험": "#f97316", "긴급": "#ef4444"}

        table_rows = ""
        for r in rows:
            color = status_colors.get(r[6], "#6b7280")
            table_rows += f"""<tr>
                <td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td>
                <td>{r[3]:,}h</td><td>{r[4]}</td><td>{r[5]}°C</td>
                <td><span style="background:{color};color:white;padding:2px 12px;border-radius:12px">{r[6]}</span></td>
            </tr>"""

        return f"""<!DOCTYPE html>
        <html><head><meta charset="utf-8"><title>예측 정비 대시보드</title>
        <style>
            body {{ font-family: sans-serif; margin: 40px; background: #f8fafc; }}
            h1 {{ color: #1e293b; }} table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
            th {{ background: #1e40af; color: white; }}
            tr:hover {{ background: #f1f5f9; }}
        </style></head>
        <body><h1>🏭 예측 정비 - 장비 상태 모니터링</h1>
        <table><tr><th>장비ID</th><th>장비명</th><th>중요도</th><th>운전시간</th>
        <th>평균진동</th><th>평균온도</th><th>상태</th></tr>{table_rows}</table>
        </body></html>"""
    except Exception as e:
        return f"<h1>오류</h1><pre>{e}</pre>"


@app.get("/api/equipment")
def get_equipment():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM ebay_anomaly_detection_catalog.predictive_maintenance.equipment_master")
    cols = [d[0] for d in cursor.description]
    rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows
