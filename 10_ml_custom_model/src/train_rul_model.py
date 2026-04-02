# Databricks notebook source
# MAGIC %md
# MAGIC # 예측 정비 - 잔여 수명(RUL) 예측 모델 학습
# MAGIC
# MAGIC 센서 데이터를 기반으로 장비의 잔여 유효 수명(Remaining Useful Life)을 예측하는
# MAGIC LightGBM 모델을 학습하고 Unity Catalog에 등록합니다.

# COMMAND ----------

# MAGIC %pip install lightgbm
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
import lightgbm as lgb

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/hongguen.park@databricks.com/predictive_maintenance_rul")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 학습 데이터 준비

# COMMAND ----------

# 센서 데이터 집계 (장비별 특성)
df = spark.sql("""
    SELECT
        s.equipment_id,
        e.operating_hours,
        e.criticality,
        AVG(s.vibration_rms) as avg_vibration,
        MAX(s.vibration_rms) as max_vibration,
        STDDEV(s.vibration_rms) as std_vibration,
        AVG(s.temperature_c) as avg_temperature,
        MAX(s.temperature_c) as max_temperature,
        AVG(s.pressure_bar) as avg_pressure,
        AVG(s.current_amp) as avg_current,
        COUNT(*) as reading_count,
        -- 시뮬레이션: 실제 RUL (타겟 변수)
        GREATEST(0, 10000 - e.operating_hours - AVG(s.vibration_rms) * 50 - GREATEST(AVG(s.temperature_c) - 50, 0) * 30 + (RAND() * 500 - 250)) as rul_hours
    FROM hg_demos.predictive_maintenance.sensor_readings s
    JOIN hg_demos.predictive_maintenance.equipment_master e ON s.equipment_id = e.equipment_id
    GROUP BY s.equipment_id, e.operating_hours, e.criticality
""")

pdf = df.toPandas()

# 범주형 변수 인코딩
criticality_map = {"핵심": 3, "중요": 2, "보조": 1}
pdf["criticality_encoded"] = pdf["criticality"].map(criticality_map).fillna(0)

feature_cols = [
    "operating_hours", "avg_vibration", "max_vibration", "std_vibration",
    "avg_temperature", "max_temperature", "avg_pressure", "avg_current",
    "reading_count", "criticality_encoded"
]

X = pdf[feature_cols]
y = pdf["rul_hours"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 모델 학습 및 로깅

# COMMAND ----------

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

with mlflow.start_run(run_name="rul_lightgbm_v1") as run:
    # 하이퍼파라미터
    params = {
        "n_estimators": 100,
        "max_depth": 6,
        "learning_rate": 0.1,
        "num_leaves": 31,
        "objective": "regression",
    }
    mlflow.log_params(params)

    model = lgb.LGBMRegressor(**params)
    model.fit(X_train, y_train)

    # 예측 및 평가
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    mlflow.log_metrics({"mae": mae, "r2_score": r2})

    # Feature importance
    importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
    mlflow.log_dict(importance, "feature_importance.json")

    # 모델 등록
    mlflow.sklearn.log_model(
        model,
        artifact_path="rul_model",
        registered_model_name="hg_demos.predictive_maintenance.rul_prediction_model",
        input_example=X_train.head(1),
    )

    print(f"✅ 모델 학습 완료 - MAE: {mae:.1f}시간, R²: {r2:.3f}")
    print(f"   Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 모델 검증

# COMMAND ----------

results = pd.DataFrame({"actual": y_test.values, "predicted": y_pred})
results["error"] = results["actual"] - results["predicted"]
display(spark.createDataFrame(results))
