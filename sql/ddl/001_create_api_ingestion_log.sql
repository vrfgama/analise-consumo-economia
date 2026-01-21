CREATE TABLE IF NOT EXISTS logs.api_ingestion_log (

    log_id BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,

    api_name VARCHAR(100) NOT NULL,
    api_url TEXT NOT NULL,
    http_method VARCHAR(10) NOT NULL,
    request_params JSONB,

    request_start_time TIMESTAMP,
    request_end_time TIMESTAMP,
    duration_ms INTEGER,

    status VARCHAR(20) NOT NULL,
    http_status_code INTEGER,
    records_count INTEGER,
    response_size_kb NUMERIC(10,2),
    is_empty_response BOOLEAN,

    file_name VARCHAR(255),
    file_path TEXT,
    file_format VARCHAR(20),

    error_message TEXT,
    error_type VARCHAR(100),
    retry_count INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);