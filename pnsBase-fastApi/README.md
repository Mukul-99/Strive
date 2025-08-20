# PNS Specification Analysis API

A FastAPI-based background job processing system for analyzing buyer specifications using PNS (Product Name Specification) data and CSV sources. This API automates the specification extraction and triangulation process that was originally implemented as an interactive Streamlit application.

## 🏗️ Architecture Overview

The system implements an asynchronous workflow that processes multiple data sources to extract and triangulate product specifications:

- **FastAPI**: RESTful API framework with async support
- **Redis**: Job queue and state management
- **LangGraph**: Workflow orchestration for complex processing pipelines
- **LangChain + OpenAI**: LLM integration for text analysis
- **BigQuery**: Data source for CSV datasets
- **Background Tasks**: Non-blocking job processing

## 📋 Features

### Core Functionality
- **Asynchronous Job Processing**: Non-blocking background analysis
- **Multi-source Data Integration**: PNS API + BigQuery CSV sources
- **AI-Powered Text Analysis**: LLM-based specification extraction
- **Cross-source Validation**: Triangulation for result confidence
- **Real-time Status Tracking**: Progress monitoring via Redis

### API Endpoints
- `POST /api/v1/analyze` - Create analysis job
- `GET /api/v1/jobs/{job_id}/status` - Check job progress  
- `GET /api/v1/jobs/{job_id}/results` - Retrieve completed results
- `DELETE /api/v1/jobs/{job_id}` - Cleanup job data
- `GET /api/v1/health` - System health check

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Redis server
- OpenAI API access
- Google Cloud BigQuery access (for production)

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd pnsBase-fastApi
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment Configuration**
   Create a `.env` file with the following variables:
   ```env
   # OpenAI Configuration
   OPENAI_API_KEY=your_openai_api_key
   OPENAI_MODEL=gpt-4.1-mini
   OPENAI_BASE_URL=https://api.openai.com/v1
   
   # Redis Configuration
   REDIS_HOST=localhost
   REDIS_PORT=6379
   REDIS_PASSWORD=your_redis_password  # Optional
   REDIS_DB=0
   
   # BigQuery Configuration (Production)
   BIGQUERY_PROJECT_ID=your-project-id
   BIGQUERY_DATASET=your_dataset
   BIGQUERY_TABLE=your_table
   BIGQUERY_CREDENTIALS_PATH=/path/to/service-account.json
   
   # External PNS API
   PNS_API_BASE_URL=https://extract-product-936671953004.asia-south1.run.app
   PNS_API_ENDPOINT=/process-mcat-from-gcs
   PNS_API_TIMEOUT=60
   
   # CORS Configuration (Production)
   ALLOWED_ORIGINS=https://your-frontend-domain.com,https://another-domain.com
   ```

4. **Start the server**
   ```bash
   # Development
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   
   # Production
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
   ```

### Testing the API

1. **Health Check**
   ```bash
   curl http://localhost:8000/api/v1/health
   ```

2. **Create Analysis Job**
   ```bash
   curl -X POST "http://localhost:8000/api/v1/analyze" \
        -H "Content-Type: application/json" \
        -d '{"mcat_id": "6472"}'
   ```

3. **Check Job Status**
   ```bash
   curl http://localhost:8000/api/v1/jobs/{job_id}/status
   ```

4. **Get Results**
   ```bash
   curl http://localhost:8000/api/v1/jobs/{job_id}/results
   ```

## 🔄 Data Processing Workflow

### 1. Job Creation
- Validates MCAT ID format (alphanumeric, 1-20 characters)
- Generates unique UUID for job tracking
- Creates job record in Redis
- Starts background processing

### 2. Data Fetching Phase
- **PNS Data**: Fetches from external API using MCAT ID
- **CSV Data**: Retrieves from BigQuery (4 sources):
  - Search Keywords (internal search queries)
  - LMS Chats (learning management conversations)
  - Rejection Comments (BLNI feedback)
  - WhatsApp Specs (custom specifications)

### 3. Processing Phase
- **PNS Processing**: Extracts specifications from JSON data
- **CSV Processing**: Uses LLM agents to extract specs from each source
- **Chunking**: Handles large datasets efficiently
- **Parallel Processing**: Multiple agents work simultaneously

### 4. Triangulation Phase
- **Cross-validation**: Compares specifications across all sources
- **Scoring**: Assigns confidence scores based on source agreement
- **Ranking**: Orders specifications by validation score
- **Final Results**: Structured output with individual and triangulated results

## 📊 API Response Structure

### Job Status Response
```json
{
  "job_id": "uuid",
  "status": "processing|pns_fetching|csv_fetching|analyzing|completed|failed",
  "progress": 65,
  "current_step": "Processing triangulation",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:32:30Z",
  "error": null
}
```

### Job Results Response
```json
{
  "job_id": "uuid",
  "status": "completed",
  "mcat_id": "6472",
  "individual_results": {
    "search_keywords": [...],
    "lms_chats": [...],
    "rejection_comments": [...],
    "custom_spec": [...],
    "pns_individual": [...]
  },
  "final_validation": [
    {
      "rank": 1,
      "score": 4,
      "pns": "Motor Power",
      "options": "100 kg/hr, 200 kg/hr, 50 kg/hr",
      "search_keywords": "Yes",
      "whatsapp_specs": "Yes",
      "rejection_comments": "Yes",
      "lms_chats": "Yes"
    }
  ],
  "processing_summary": {
    "total_sources": 5,
    "successful_extractions": 5,
    "pns_specs_found": 15,
    "final_triangulated_specs": 10,
    "processing_time": 45.2
  }
}
```

## 🛠️ Development

### Project Structure
```
pnsBase-fastApi/
├── app/
│   ├── main.py              # FastAPI application entry point
│   ├── core/                # Configuration and infrastructure
│   │   ├── config.py        # Environment settings
│   │   └── redis_client.py  # Redis connection and job management
│   ├── api/v1/              # API endpoints
│   │   ├── analyze.py       # Analysis endpoints
│   │   └── health.py        # Health check
│   ├── models/              # Pydantic data models
│   │   └── job.py          # Job-related models
│   ├── services/            # Business logic
│   │   ├── job_processor.py # Background job orchestration
│   │   ├── data_fetcher.py  # External API and BigQuery clients
│   │   ├── workflow.py      # LangGraph workflow management
│   │   ├── extraction_agent.py # LLM-based text processing
│   │   └── triangulation_agent.py # Cross-source validation
│   └── utils/               # Utilities and helpers
│       └── state.py         # State management for workflows
├── requirements.txt         # Python dependencies
├── test_api.py             # API integration tests
├── test_workflow.py        # Workflow unit tests
└── sample2/                # Sample data for testing
```

### Running Tests
```bash
# API Integration Tests
python test_api.py

# Workflow Tests (with sample data)
python test_workflow.py
```

### Code Quality
The codebase includes:
- **Type Hints**: Full type annotation support
- **Input Validation**: Pydantic models with field validation
- **Error Handling**: Comprehensive exception management
- **Logging**: Structured logging throughout
- **Documentation**: Inline documentation and examples

## 🔧 Configuration

### Environment Variables

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `OPENAI_API_KEY` | Yes | OpenAI API key for LLM processing | - |
| `OPENAI_MODEL` | No | OpenAI model to use | `gpt-4.1-mini` |
| `REDIS_HOST` | No | Redis server hostname | `localhost` |
| `REDIS_PORT` | No | Redis server port | `6379` |
| `REDIS_PASSWORD` | No | Redis password | - |
| `BIGQUERY_PROJECT_ID` | Yes* | Google Cloud project ID | - |
| `BIGQUERY_DATASET` | Yes* | BigQuery dataset name | - |
| `BIGQUERY_TABLE` | Yes* | BigQuery table name | - |
| `PNS_API_BASE_URL` | No | External PNS API URL | (default provided) |
| `ALLOWED_ORIGINS` | No | CORS allowed origins (comma-separated) | localhost only |

*Required for production with real BigQuery integration

### Job Configuration
- **Max Concurrent Jobs**: 10 (configurable via `MAX_CONCURRENT_JOBS`)
- **Job Cleanup Delay**: 5 minutes (configurable via `JOB_CLEANUP_DELAY_MINUTES`)
- **API Timeout**: 60 seconds for external calls
- **Progress Validation**: 0-100% range enforced

## 🚀 Deployment

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ ./app/
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Production Considerations
- **Environment Variables**: Use secure secret management
- **Redis**: Use managed Redis service (AWS ElastiCache, Google Memorystore)
- **BigQuery**: Ensure proper IAM roles and service account setup
- **Monitoring**: Add APM tools (Datadog, New Relic)
- **Scaling**: Use multiple worker processes or container orchestration
- **Security**: Configure CORS, add rate limiting, use HTTPS

## 🔍 Monitoring and Debugging

### Health Checks
The `/api/v1/health` endpoint provides:
- Redis connectivity status
- Service version information
- Basic system health

### Logging
Structured logging is available at multiple levels:
- **INFO**: Normal operation flow
- **WARNING**: Recoverable issues
- **ERROR**: Processing failures
- **DEBUG**: Detailed execution traces (development)

### Common Issues
1. **Redis Connection**: Check Redis server status and credentials
2. **OpenAI API**: Verify API key and rate limits
3. **BigQuery Access**: Ensure service account permissions
4. **Job Stuck**: Check background task processing and Redis status

## 📚 API Documentation

Interactive API documentation is available at:
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add type hints to all functions
- Include docstrings for public methods
- Write tests for new features
- Update documentation as needed

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section above
- Review the API documentation at `/docs`

---

**Note**: This API is the automated version of the original Streamlit application. It maintains the same core processing logic while providing a scalable, production-ready interface for specification analysis.
