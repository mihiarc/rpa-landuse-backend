# RPA Land Use Analytics Backend

FastAPI backend for the RPA Land Use Analytics platform. Provides REST API and SSE streaming for natural language queries about USDA Forest Service RPA Assessment land use data.

## Features

- **Chat API** - Natural language queries with SSE streaming responses
- **Analytics API** - Pre-computed analytics and visualizations
- **Explorer API** - SQL query execution with schema browsing
- **Extraction API** - Bulk data export with templates and filters
- **Security** - SQL injection prevention and query validation

## Tech Stack

- **Framework**: FastAPI
- **Agent**: LangChain + LangGraph with GPT-4o-mini
- **Database**: DuckDB (star schema with 5.4M records)
- **Validation**: Pydantic v2

## Prerequisites

- Python 3.11+
- uv package manager
- The `landuse` package from [rpa-landuse](https://github.com/mihiarc/rpa-landuse)
- OpenAI API key

## Installation

```bash
# Install dependencies
uv pip install -e .

# Set environment variables
export OPENAI_API_KEY=your_key
export PYTHONPATH=/path/to/rpa-landuse/src
export LANDUSE_DATABASE__PATH=/path/to/landuse_analytics.duckdb
```

## Running

```bash
# Development server
uvicorn app.main:app --reload --port 8000

# Production
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

### Chat
- `POST /api/v1/chat/stream` - Stream chat response (SSE)
- `GET /api/v1/chat/history` - Get session history
- `DELETE /api/v1/chat/history` - Clear session

### Analytics
- `GET /api/v1/analytics/overview` - Land use overview stats
- `GET /api/v1/analytics/urbanization-sources` - Urbanization source data
- `GET /api/v1/analytics/scenario-comparison` - Climate scenario comparison
- `GET /api/v1/analytics/forest-transitions` - Forest transition analysis
- `GET /api/v1/analytics/agricultural-impact` - Agricultural impact data

### Explorer
- `GET /api/v1/explorer/schema` - Database schema
- `GET /api/v1/explorer/templates` - Query templates
- `POST /api/v1/explorer/query` - Execute SQL query

### Extraction
- `GET /api/v1/extraction/templates` - Export templates
- `GET /api/v1/extraction/filters` - Available filters
- `POST /api/v1/extraction/preview` - Preview export data
- `POST /api/v1/extraction/export` - Export data (CSV/JSON/Parquet)

### Health
- `GET /api/v1/health` - Health check

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Project Structure

```
backend/
├── app/
│   ├── main.py           # FastAPI application
│   ├── config.py         # Configuration
│   ├── dependencies.py   # Dependency injection
│   ├── routers/          # API route handlers
│   │   ├── chat.py
│   │   ├── analytics.py
│   │   ├── explorer.py
│   │   └── extraction.py
│   └── services/         # Business logic
│       ├── agent_service.py
│       └── database_service.py
├── tests/                # Test suite
├── pyproject.toml        # Dependencies
└── Dockerfile           # Container deployment
```

## Frontend

This backend is designed to work with [rpa-landuse-frontend](https://github.com/mihiarc/rpa-landuse-frontend).

## License

MIT
