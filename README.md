# Mini data pipeline: CSV → ETL (Python/pandas) → PostgreSQL → FastAPI API

![CI](https://github.com/paripariume/mini-etl-fastapi-postgres/actions/workflows/ci.yml/badge.svg)

## 🚀 What
This repository provides a mini data pipeline demonstration:
- CSV input data → data transformation using Python/pandas  
- Loading into PostgreSQL database  
- Serving analytics via FastAPI  
- Containerised using Docker and docker-compose  
- Includes pytest tests and CI integration for reproducibility

## 📦 Tech Stack
- Python 3.12  
- FastAPI  
- SQLAlchemy 2.x  
- pandas  
- PostgreSQL 16  
- Docker & Docker Compose  
- pytest  
- GitHub Actions (CI)

## 🧩 Endpoints
| Endpoint                  | Description                  |
|---------------------------|------------------------------|
| `/health`                 | Service health check         |
| `/orders/summary`         | Summary of orders by customer|
| `/orders/daily?start=&end=` | Daily aggregates by date     |
| `/metrics`                | Internal load metrics (last run, count) |

## 🛠️ Usage
```bash
git clone https://github.com/paripariume/mini-etl-fastapi-postgres.git
cd mini-etl-fastapi-postgres
docker compose up -d --build
docker compose exec api python -m src.etl.load
curl http://localhost:8000/orders/summary
