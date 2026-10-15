# Employee Attendance & Analytics API

## Run locally

Use Python 3.11 or newer and a reachable MongoDB instance (Atlas or local MongoDB).

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Set `MONGO_URI` and `MONGO_DB` in the environment or in a local `.env` file. Real environment variables take precedence. Do not commit `.env` or credentials.

Start from the repository root:

```bash
uvicorn app.main:app --port 8000 --reload
```

Open `http://localhost:8000/docs` for the interactive API documentation. Index creation is attempted at startup and retried through `/health` if MongoDB was unavailable at startup.

## Tests and verification

Run the focused business-rule tests after installing requirements:

```bash
python -m pytest -q
```

The API was smoke-tested against the configured MongoDB Atlas database: `/health`, employee listing, monthly employee analytics, department summary, late leaderboard, department trend, and explain plans were exercised. The explain plan checked for department trend used an index (`IXSCAN`) and showed no collection scan on the current small test dataset. This is not a 100,000-record load test; concurrency and full-scale performance should be validated separately if time permits.

**Safety:** Do not commit `.env`, credentials, or `.venv`. Any sample seeding script that deletes and replaces documents in `employees` and `attendance_logs` must only be run against a disposable database.
