-- Database initialization script for Agent-Orchestrated-ETL
-- =============================================================================

-- Create additional databases for testing and Airflow
CREATE DATABASE IF NOT EXISTS test_agent_etl;
CREATE DATABASE IF NOT EXISTS airflow;

-- Create users with appropriate permissions
CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'etl_password';
CREATE USER IF NOT EXISTS 'readonly_user'@'%' IDENTIFIED BY 'readonly_password';

-- Grant permissions
GRANT ALL PRIVILEGES ON agent_etl.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON test_agent_etl.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON airflow.* TO 'etl_user'@'%';

GRANT SELECT ON agent_etl.* TO 'readonly_user'@'%';
GRANT SELECT ON test_agent_etl.* TO 'readonly_user'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Create initial schema for agent_etl database
USE agent_etl;

-- Pipeline execution tracking table
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) UNIQUE NOT NULL,
    status ENUM('pending', 'running', 'completed', 'failed', 'cancelled') NOT NULL DEFAULT 'pending',
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    duration_seconds INT NULL,
    records_processed BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    error_message TEXT NULL,
    configuration JSON NULL,
    metadata JSON NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_execution_id (execution_id),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
);

-- Agent activity tracking table
CREATE TABLE IF NOT EXISTS agent_activities (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    agent_id VARCHAR(255) NOT NULL,
    agent_type ENUM('orchestrator', 'etl', 'monitor') NOT NULL,
    activity_type VARCHAR(100) NOT NULL,
    status ENUM('started', 'completed', 'failed') NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    duration_ms INT NULL,
    input_data JSON NULL,
    output_data JSON NULL,
    error_message TEXT NULL,
    metadata JSON NULL,
    
    INDEX idx_agent_id (agent_id),
    INDEX idx_agent_type (agent_type),
    INDEX idx_activity_type (activity_type),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
);

-- Data source metadata table
CREATE TABLE IF NOT EXISTS data_sources (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_id VARCHAR(255) UNIQUE NOT NULL,
    source_type ENUM('s3', 'postgres', 'api', 'file') NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    connection_config JSON NOT NULL,
    schema_info JSON NULL,
    last_accessed TIMESTAMP NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_source_id (source_id),
    INDEX idx_source_type (source_type),
    INDEX idx_is_active (is_active)
);

-- Pipeline configurations table
CREATE TABLE IF NOT EXISTS pipeline_configs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pipeline_id VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT NULL,
    source_config JSON NOT NULL,
    transformation_config JSON NULL,
    destination_config JSON NOT NULL,
    schedule_config JSON NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_by VARCHAR(255) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_is_active (is_active),
    INDEX idx_created_by (created_by)
);

-- System metrics table
CREATE TABLE IF NOT EXISTS system_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DECIMAL(20,6) NOT NULL,
    metric_unit VARCHAR(50) NULL,
    tags JSON NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_metric_name (metric_name),
    INDEX idx_timestamp (timestamp)
);

-- Agent decisions table for tracking AI decision-making
CREATE TABLE IF NOT EXISTS agent_decisions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    agent_id VARCHAR(255) NOT NULL,
    decision_id VARCHAR(255) UNIQUE NOT NULL,
    decision_type VARCHAR(100) NOT NULL,
    input_context JSON NOT NULL,
    decision_output JSON NOT NULL,
    confidence_score DECIMAL(5,4) NULL,
    execution_time_ms INT NOT NULL,
    token_usage INT NULL,
    model_used VARCHAR(100) NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_agent_id (agent_id),
    INDEX idx_decision_type (decision_type),
    INDEX idx_timestamp (timestamp)
);

-- Audit log table for security and compliance
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    user_id VARCHAR(255) NULL,
    agent_id VARCHAR(255) NULL,
    resource_type VARCHAR(100) NULL,
    resource_id VARCHAR(255) NULL,
    action VARCHAR(100) NOT NULL,
    result ENUM('success', 'failure', 'partial') NOT NULL,
    details JSON NULL,
    ip_address INET6 NULL,
    user_agent TEXT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_event_type (event_type),
    INDEX idx_user_id (user_id),
    INDEX idx_agent_id (agent_id),
    INDEX idx_action (action),
    INDEX idx_timestamp (timestamp)
);

-- Insert initial configuration data
INSERT INTO data_sources (source_id, source_type, name, description, connection_config) VALUES
('local_postgres', 'postgres', 'Local PostgreSQL', 'Local PostgreSQL database for development', 
 JSON_OBJECT('host', 'localhost', 'port', 5432, 'database', 'agent_etl')),
('local_s3', 's3', 'Local MinIO S3', 'Local S3-compatible storage for development',
 JSON_OBJECT('endpoint', 'http://localhost:9000', 'bucket', 'dev-bucket')),
('sample_api', 'api', 'Sample REST API', 'Sample API endpoint for testing',
 JSON_OBJECT('base_url', 'https://jsonplaceholder.typicode.com', 'timeout', 30))
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- Insert sample pipeline configuration
INSERT INTO pipeline_configs (pipeline_id, name, description, source_config, destination_config) VALUES
('sample_pipeline', 'Sample ETL Pipeline', 'A sample pipeline for demonstration',
 JSON_OBJECT('type', 'api', 'source_id', 'sample_api', 'endpoint', '/posts'),
 JSON_OBJECT('type', 'postgres', 'source_id', 'local_postgres', 'table', 'processed_posts'))
ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP;

-- Create views for common queries
CREATE OR REPLACE VIEW recent_pipeline_executions AS
SELECT 
    pe.pipeline_id,
    pc.name as pipeline_name,
    pe.execution_id,
    pe.status,
    pe.start_time,
    pe.end_time,
    pe.duration_seconds,
    pe.records_processed,
    pe.records_failed
FROM pipeline_executions pe
LEFT JOIN pipeline_configs pc ON pe.pipeline_id = pc.pipeline_id
WHERE pe.start_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
ORDER BY pe.start_time DESC;

CREATE OR REPLACE VIEW agent_performance_summary AS
SELECT 
    agent_id,
    agent_type,
    COUNT(*) as total_activities,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_activities,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_activities,
    AVG(duration_ms) as avg_duration_ms,
    MIN(start_time) as first_activity,
    MAX(start_time) as last_activity
FROM agent_activities
WHERE start_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
GROUP BY agent_id, agent_type;

-- Create stored procedures for common operations
DELIMITER //

CREATE PROCEDURE RecordPipelineExecution(
    IN p_pipeline_id VARCHAR(255),
    IN p_execution_id VARCHAR(255),
    IN p_configuration JSON
)
BEGIN
    INSERT INTO pipeline_executions (pipeline_id, execution_id, configuration)
    VALUES (p_pipeline_id, p_execution_id, p_configuration);
END //

CREATE PROCEDURE UpdatePipelineExecution(
    IN p_execution_id VARCHAR(255),
    IN p_status ENUM('pending', 'running', 'completed', 'failed', 'cancelled'),
    IN p_records_processed BIGINT,
    IN p_records_failed BIGINT,
    IN p_error_message TEXT
)
BEGIN
    UPDATE pipeline_executions 
    SET 
        status = p_status,
        end_time = CASE WHEN p_status IN ('completed', 'failed', 'cancelled') THEN NOW() ELSE end_time END,
        duration_seconds = CASE WHEN p_status IN ('completed', 'failed', 'cancelled') THEN TIMESTAMPDIFF(SECOND, start_time, NOW()) ELSE duration_seconds END,
        records_processed = COALESCE(p_records_processed, records_processed),
        records_failed = COALESCE(p_records_failed, records_failed),
        error_message = p_error_message,
        updated_at = NOW()
    WHERE execution_id = p_execution_id;
END //

CREATE PROCEDURE RecordAgentActivity(
    IN p_agent_id VARCHAR(255),
    IN p_agent_type ENUM('orchestrator', 'etl', 'monitor'),
    IN p_activity_type VARCHAR(100),
    IN p_input_data JSON
)
BEGIN
    INSERT INTO agent_activities (agent_id, agent_type, activity_type, status, input_data)
    VALUES (p_agent_id, p_agent_type, p_activity_type, 'started', p_input_data);
    
    SELECT LAST_INSERT_ID() as activity_id;
END //

CREATE PROCEDURE CompleteAgentActivity(
    IN p_activity_id BIGINT,
    IN p_status ENUM('completed', 'failed'),
    IN p_output_data JSON,
    IN p_error_message TEXT
)
BEGIN
    UPDATE agent_activities 
    SET 
        status = p_status,
        end_time = NOW(),
        duration_ms = TIMESTAMPDIFF(MICROSECOND, start_time, NOW()) / 1000,
        output_data = p_output_data,
        error_message = p_error_message
    WHERE id = p_activity_id;
END //

DELIMITER ;

-- Create triggers for audit logging
DELIMITER //

CREATE TRIGGER pipeline_config_audit_insert 
AFTER INSERT ON pipeline_configs
FOR EACH ROW
BEGIN
    INSERT INTO audit_logs (event_type, resource_type, resource_id, action, result, details)
    VALUES ('pipeline_config', 'pipeline', NEW.pipeline_id, 'create', 'success', 
            JSON_OBJECT('name', NEW.name, 'created_by', NEW.created_by));
END //

CREATE TRIGGER pipeline_config_audit_update
AFTER UPDATE ON pipeline_configs
FOR EACH ROW
BEGIN
    INSERT INTO audit_logs (event_type, resource_type, resource_id, action, result, details)
    VALUES ('pipeline_config', 'pipeline', NEW.pipeline_id, 'update', 'success',
            JSON_OBJECT('old_name', OLD.name, 'new_name', NEW.name));
END //

DELIMITER ;

-- Grant execute permissions on stored procedures
GRANT EXECUTE ON PROCEDURE agent_etl.RecordPipelineExecution TO 'etl_user'@'%';
GRANT EXECUTE ON PROCEDURE agent_etl.UpdatePipelineExecution TO 'etl_user'@'%';
GRANT EXECUTE ON PROCEDURE agent_etl.RecordAgentActivity TO 'etl_user'@'%';
GRANT EXECUTE ON PROCEDURE agent_etl.CompleteAgentActivity TO 'etl_user'@'%';

-- Final status message
SELECT 'Database initialization completed successfully' as status;