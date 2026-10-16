# -*- coding: utf-8 -*-
"""
bq_loader.py
============
Đẩy kết quả JSONL từ pipeline lên Google BigQuery.
Sử dụng BigQuery Load Job (WRITE_APPEND) – không ghi đè dữ liệu cũ.
"""

import logging
from pathlib import Path

from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = "bigdataproject-297"
DATASET_ID = "bd_silver_dataset"
LOCATION = "asia-southeast1"


def load_to_bigquery(
    all_comments_path: str = "/tmp/all_comments_sentiment.jsonl",
    person_sentiment_path: str = "/tmp/person_sentiment.jsonl",
    project_id: str = PROJECT_ID,
    dataset_id: str = DATASET_ID,
    location: str = LOCATION,
) -> dict:
    """
    Nạp các file JSONL kết quả từ pipeline lên BigQuery.

    Args:
        all_comments_path: Đường dẫn tới file all_comments_sentiment.jsonl
        person_sentiment_path: Đường dẫn tới file person_sentiment.jsonl
        project_id: GCP Project ID
        dataset_id: BigQuery Dataset ID
        location: Vị trí dataset BigQuery

    Returns:
        dict chứa số dòng của từng bảng sau khi nạp.
    """
    client = bigquery.Client(project=project_id)
    full_dataset_id = f"{project_id}.{dataset_id}"

    # Tạo dataset nếu chưa có
    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)
    logger.info("Dataset đã sẵn sàng: %s", full_dataset_id)

    # Khớp file kết quả với tên bảng đích trên BigQuery
    files_to_tables = {
        all_comments_path: "all_comments_sentiment",
        person_sentiment_path: "person_sentiment",
    }

    results = {}

    for file_path_str, table_name in files_to_tables.items():
        file_path = Path(file_path_str)

        if not file_path.exists():
            raise FileNotFoundError(
                f"Không tìm thấy file: {file_path}. "
                "Hãy chạy pipeline trước!"
            )

        table_id = f"{full_dataset_id}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            # Không ghi đè, chỉ append thêm dữ liệu mới
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        with open(file_path, "rb") as f:
            job = client.load_table_from_file(
                f, table_id, job_config=job_config, location=location
            )

        job.result()  # Chờ job hoàn tất
        table = client.get_table(table_id)
        logger.info(
            "Đã nạp thành công vào bảng: %s (Tổng số dòng hiện tại: %d)",
            table_id,
            table.num_rows,
        )
        results[table_name] = table.num_rows

    logger.info("Hoàn tất đẩy dữ liệu lên BigQuery tầng Silver!")
    return results
