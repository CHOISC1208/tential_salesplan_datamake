# util/bq_io.py
from google.cloud import bigquery
import pandas as pd
from pathlib import Path

bq = bigquery.Client(project="tential-data-prd")

def _infer_bq_param(name: str, value):
    """型推論付き BigQuery パラメータ生成"""
    if value is None:
        return None
    # YYYY-MM-DD → DATE
    if isinstance(value, str) and len(value) == 10 and value[4] == '-' and value[7] == '-':
        return bigquery.ScalarQueryParameter(name, "DATE", value)
    # 数値 → FLOAT64
    if isinstance(value, (int, float)):
        return bigquery.ScalarQueryParameter(name, "FLOAT64", float(value))
    # その他 → STRING
    return bigquery.ScalarQueryParameter(name, "STRING", str(value))

def run_sql_file_to_df(path: Path, params: dict | None = None) -> pd.DataFrame:
    """SQLファイルを実行して DataFrame で返す"""
    query = path.read_text(encoding="utf-8")
    job_config = bigquery.QueryJobConfig()
    if params:
        qps = [qp for k, v in params.items() if (qp := _infer_bq_param(k, v)) is not None]
        job_config.query_parameters = qps
    job = bq.query(query, job_config=job_config)
    df = job.result().to_dataframe(create_bqstorage_client=True)
    return df
