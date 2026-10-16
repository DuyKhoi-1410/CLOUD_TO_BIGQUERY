# -*- coding: utf-8 -*-
"""
main.py
=======
HTTP server cho Google Cloud Run.
Nhận trigger từ Cloud Scheduler và chạy NLP pipeline + BigQuery loader.
"""

import logging
import os

from flask import Flask, jsonify

from pipeline import run as run_pipeline
from bq_loader import load_to_bigquery

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Đọc cấu hình từ biến môi trường (có thể override qua Cloud Run env vars)
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "bd-raw-data-bronze")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bigdataproject-297")
DATASET_ID = os.environ.get("BQ_DATASET_ID", "bd_silver_dataset")
LOCATION = os.environ.get("BQ_LOCATION", "asia-southeast1")


@app.route("/", methods=["GET"])
def health_check():
    """Health check endpoint để Cloud Run biết service đang sống."""
    return jsonify({"status": "ok", "service": "nlp-pipeline"}), 200


@app.route("/run", methods=["POST"])
def trigger_pipeline():
    """
    Endpoint chính được Cloud Scheduler gọi theo lịch.
    Chạy toàn bộ NLP pipeline rồi đẩy kết quả lên BigQuery.
    """
    logger.info("=== Bắt đầu chạy Pipeline NLP theo lịch ===")

    try:
        # Bước 1: Chạy Beam pipeline – đọc từ GCS, xử lý NLP, ghi ra /tmp/
        logger.info("Bước 1/2: Chạy Apache Beam NLP pipeline...")
        pipeline_result = run_pipeline(bucket_name=BUCKET_NAME)
        logger.info("Pipeline hoàn thành: %s", pipeline_result)

        # Bước 2: Đẩy kết quả JSONL lên BigQuery
        logger.info("Bước 2/2: Đẩy dữ liệu lên BigQuery...")
        bq_result = load_to_bigquery(
            all_comments_path=pipeline_result["all_comments_path"],
            person_sentiment_path=pipeline_result["person_sentiment_path"],
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            location=LOCATION,
        )
        logger.info("BigQuery load hoàn thành: %s", bq_result)

        return jsonify({
            "status": "success",
            "pipeline": pipeline_result,
            "bigquery": bq_result,
        }), 200

    except Exception as e:
        logger.exception("Pipeline thất bại: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    # Chỉ dùng khi chạy local. Cloud Run sẽ dùng Gunicorn thông qua Dockerfile.
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
