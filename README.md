# NLP Pipeline - YouTube Comments Analysis

Pipeline tu dong xu ly NLP binh luan YouTube (lam sach text, NER, chuan hoa ten nghe si, phan tich cam xuc PhoBERT) va day ket qua len Google BigQuery.

Duoc trigger theo lich qua **Cloud Scheduler → Cloud Run**.

---

## Cau truc project

```
CLOUD TO BQ/
├── main.py           # Flask HTTP server - entry point Cloud Run
├── pipeline.py       # Apache Beam NLP pipeline (logic chinh)
├── bq_loader.py      # Day ket qua JSONL len BigQuery
├── requirements.txt  # Python dependencies
├── Dockerfile        # Container image
├── .dockerignore
└── .gitignore
```

---

## Yeu cau

- Google Cloud SDK (gcloud) da cai va dang nhap
- GCP Project: bigdataproject-297
- Service Account duoc cap quyen:
  - roles/storage.objectViewer (doc GCS)
  - roles/bigquery.dataEditor (ghi BigQuery)
  - roles/bigquery.jobUser

---

## Buoc 1: Day len GitHub

```bash
git init
git add main.py pipeline.py bq_loader.py requirements.txt Dockerfile .gitignore .dockerignore README.md
git commit -m "feat: NLP pipeline for Cloud Run deployment"
git remote add origin https://github.com/<your-username>/<repo-name>.git
git branch -M main
git push -u origin main
```

> TUYET DOI khong commit file JSON key cua Service Account len GitHub!

---

## Buoc 2: Build & Push Docker image

```bash
gcloud config set project bigdataproject-297

gcloud services enable run.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com cloudscheduler.googleapis.com

gcloud artifacts repositories create nlp-pipeline \
  --repository-format=docker \
  --location=asia-southeast1

gcloud builds submit \
  --tag asia-southeast1-docker.pkg.dev/bigdataproject-297/nlp-pipeline/nlp-pipeline:latest \
  .
```

---

## Buoc 3: Deploy len Cloud Run

```bash
gcloud run deploy nlp-pipeline \
  --image asia-southeast1-docker.pkg.dev/bigdataproject-297/nlp-pipeline/nlp-pipeline:latest \
  --region asia-southeast1 \
  --platform managed \
  --no-allow-unauthenticated \
  --service-account <YOUR_SERVICE_ACCOUNT_EMAIL> \
  --memory 8Gi \
  --cpu 4 \
  --timeout 3600 \
  --set-env-vars "GCS_BUCKET_NAME=bd-raw-data-bronze,GCP_PROJECT_ID=bigdataproject-297,BQ_DATASET_ID=bd_silver_dataset,BQ_LOCATION=asia-southeast1"
```

> Memory: NLP models (NER + PhoBERT) can it nhat 6-8GB RAM.

---

## Buoc 4: Tao Cloud Scheduler

```bash
SERVICE_URL=$(gcloud run services describe nlp-pipeline \
  --region asia-southeast1 \
  --format 'value(status.url)')

# Chay hang ngay luc 2:00 sang gio Viet Nam (UTC+7 = 19:00 UTC)
gcloud scheduler jobs create http nlp-pipeline-daily \
  --location asia-southeast1 \
  --schedule "0 19 * * *" \
  --uri "${SERVICE_URL}/run" \
  --http-method POST \
  --oidc-service-account-email <YOUR_SERVICE_ACCOUNT_EMAIL> \
  --oidc-token-audience "${SERVICE_URL}"
```

---

## Bien moi truong (Environment Variables)

| Ten bien          | Gia tri mac dinh       | Mo ta                     |
|-------------------|------------------------|---------------------------|
| GCS_BUCKET_NAME   | bd-raw-data-bronze     | GCS bucket chua raw data  |
| GCP_PROJECT_ID    | bigdataproject-297     | GCP Project ID            |
| BQ_DATASET_ID     | bd_silver_dataset      | BigQuery Dataset dich     |
| BQ_LOCATION       | asia-southeast1        | Vung BigQuery             |

---

## Endpoint API

| Endpoint | Method | Mo ta |
|----------|--------|-------|
| /        | GET    | Health check |
| /run     | POST   | Trigger pipeline (Cloud Scheduler goi) |

---

## Luong hoat dong

```
Cloud Scheduler (cron)
        |
        v POST /run
   Cloud Run (main.py)
        |
        +- pipeline.py
        |       |- Tu tim run moi nhat tren GCS
        |       |- Doc video_snapshots + comments (JSONL)
        |       |- Join video info vao comment
        |       |- CleanTextFn
        |       |- AddProgramNameFn
        |       |- Deduplicate by comment_id
        |       |- NormalizeEntityFn
        |       |- SentimentAnalysisFn (PhoBERT)
        |       |- ExtractPersonNERFn (NlpHUST)
        |       |- ExpandPersonRowsFn
        |       +- Ghi /tmp/*.jsonl
        |
        +- bq_loader.py
                +- Load JSONL -> BigQuery (WRITE_APPEND)
                        |- all_comments_sentiment
                        +- person_sentiment
```
