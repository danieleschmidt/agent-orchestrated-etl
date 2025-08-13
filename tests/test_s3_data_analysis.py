"""Tests for S3 data source analysis."""

from unittest.mock import Mock, patch

import pytest

from src.agent_orchestrated_etl.data_source_analysis import (
    BOTO3_AVAILABLE,
    S3AnalysisResult,
    S3DataAnalyzer,
    analyze_source,
)
from src.agent_orchestrated_etl.validation import ValidationError


class TestS3AnalysisResult:
    """Test S3AnalysisResult class."""

    def test_init_empty_result(self):
        """Test S3AnalysisResult initialization."""
        result = S3AnalysisResult()

        assert result.bucket_name == ""
        assert result.prefix == ""
        assert result.total_files == 0
        assert result.total_size_bytes == 0
        assert result.file_formats == {}
        assert result.schema_info == {}
        assert result.data_quality == {}
        assert result.sample_files == []
        assert result.partitions == []
        assert result.errors == []

    def test_to_metadata_empty(self):
        """Test converting empty result to metadata."""
        result = S3AnalysisResult()
        metadata = result.to_metadata()

        assert metadata["tables"] == []
        assert metadata["fields"] == ["file_key", "file_size", "last_modified", "content"]
        assert "analysis_metadata" in metadata
        assert metadata["analysis_metadata"]["total_files"] == 0

    def test_to_metadata_with_data(self):
        """Test converting populated result to metadata."""
        result = S3AnalysisResult()
        result.bucket_name = "test-bucket"
        result.prefix = "data/"
        result.total_files = 5
        result.total_size_bytes = 1024 * 1024  # 1MB
        result.file_formats = {"csv": 3, "json": 2}
        result.schema_info = {
            "csv": {"columns": ["id", "name", "email"], "row_count": 100},
            "json": {"columns": ["user_id", "data"], "row_count": 50}
        }
        result.partitions = ["year", "month"]

        metadata = result.to_metadata()

        assert "csv_files_partitioned" in metadata["tables"]
        assert "json_files_partitioned" in metadata["tables"]
        assert "id" in metadata["fields"]
        assert "name" in metadata["fields"]
        assert "user_id" in metadata["fields"]
        assert metadata["analysis_metadata"]["bucket_name"] == "test-bucket"
        assert metadata["analysis_metadata"]["total_size_mb"] == 1.0
        assert metadata["analysis_metadata"]["partitions"] == ["year", "month"]


class TestS3DataAnalyzer:
    """Test S3DataAnalyzer class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = S3DataAnalyzer(region_name="us-west-2", max_sample_files=5)

    def test_init(self):
        """Test S3DataAnalyzer initialization."""
        assert self.analyzer.region_name == "us-west-2"
        assert self.analyzer.max_sample_files == 5
        assert self.analyzer._s3_client is None

    def test_parse_s3_uri_valid(self):
        """Test parsing valid S3 URIs."""
        # Bucket only
        bucket, prefix = self.analyzer._parse_s3_uri("s3://my-bucket")
        assert bucket == "my-bucket"
        assert prefix == ""

        # Bucket with prefix
        bucket, prefix = self.analyzer._parse_s3_uri("s3://my-bucket/data/files")
        assert bucket == "my-bucket"
        assert prefix == "data/files"

        # Bucket with trailing slash
        bucket, prefix = self.analyzer._parse_s3_uri("s3://my-bucket/data/")
        assert bucket == "my-bucket"
        assert prefix == "data/"

    def test_parse_s3_uri_invalid(self):
        """Test parsing invalid S3 URIs."""
        # Missing s3:// prefix
        with pytest.raises(ValidationError, match="must start with 's3://'"):
            self.analyzer._parse_s3_uri("my-bucket/data")

        # Missing bucket name
        with pytest.raises(ValidationError, match="must specify a bucket name"):
            self.analyzer._parse_s3_uri("s3://")

        # Invalid bucket name
        with pytest.raises(ValidationError, match="Invalid S3 bucket name"):
            self.analyzer._parse_s3_uri("s3://My_Bucket")

    def test_detect_file_format(self):
        """Test file format detection."""
        assert self.analyzer._detect_file_format("data.csv") == "csv"
        assert self.analyzer._detect_file_format("data.json") == "json"
        assert self.analyzer._detect_file_format("data.parquet") == "parquet"
        assert self.analyzer._detect_file_format("data.txt") == "text"
        assert self.analyzer._detect_file_format("data.unknown") == "unknown"

        # Case insensitive
        assert self.analyzer._detect_file_format("DATA.CSV") == "csv"
        assert self.analyzer._detect_file_format("data.JSON") == "json"

        # Complex paths
        assert self.analyzer._detect_file_format("path/to/data.csv") == "csv"
        assert self.analyzer._detect_file_format("s3://bucket/data.jsonl") == "json"

    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.data_source_analysis.boto3.client')
    def test_analyze_s3_source_success(self, mock_boto_client):
        """Test successful S3 source analysis."""
        # Mock S3 client and paginator
        mock_client = Mock()
        mock_boto_client.return_value = mock_client

        mock_paginator = Mock()
        mock_client.get_paginator.return_value = mock_paginator

        # Mock paginated response
        mock_page_iterator = [
            {
                'Contents': [
                    {'Key': 'data/file1.csv', 'Size': 1024},
                    {'Key': 'data/file2.json', 'Size': 2048},
                    {'Key': 'data/subdir/', 'Size': 0},  # Directory - should be skipped
                ]
            }
        ]
        mock_paginator.paginate.return_value = mock_page_iterator

        # Mock file content analysis
        mock_client.get_object.return_value = {
            'Body': Mock(read=lambda: b'id,name,email\n1,John,john@example.com\n2,Jane,jane@example.com')
        }

        # Analyze S3 source
        result = self.analyzer.analyze_s3_source("s3://test-bucket/data/")

        # Verify results
        assert result.bucket_name == "test-bucket"
        assert result.prefix == "data/"
        assert result.total_files == 2
        assert result.total_size_bytes == 3072
        assert result.file_formats == {"csv": 1, "json": 1}
        assert len(result.sample_files) == 2
        assert result.errors == []

    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.data_source_analysis.boto3.client')
    def test_analyze_s3_source_no_such_bucket(self, mock_boto_client):
        """Test S3 analysis with non-existent bucket."""
        from botocore.exceptions import ClientError

        mock_client = Mock()
        mock_boto_client.return_value = mock_client

        # Mock NoSuchBucket error
        error_response = {'Error': {'Code': 'NoSuchBucket'}}
        mock_client.get_paginator.side_effect = ClientError(error_response, 'ListObjectsV2')

        # Test error handling
        with pytest.raises(ValidationError, match="does not exist"):
            self.analyzer.analyze_s3_source("s3://nonexistent-bucket")

    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.data_source_analysis.boto3.client')
    def test_analyze_s3_source_access_denied(self, mock_boto_client):
        """Test S3 analysis with access denied."""
        from botocore.exceptions import ClientError

        mock_client = Mock()
        mock_boto_client.return_value = mock_client

        # Mock AccessDenied error
        error_response = {'Error': {'Code': 'AccessDenied'}}
        mock_client.get_paginator.side_effect = ClientError(error_response, 'ListObjectsV2')

        # Test error handling
        with pytest.raises(ValidationError, match="Access denied"):
            self.analyzer.analyze_s3_source("s3://restricted-bucket")

    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.data_source_analysis.boto3.client')
    def test_analyze_s3_source_no_credentials(self, mock_boto_client):
        """Test S3 analysis with no AWS credentials."""
        from botocore.exceptions import NoCredentialsError

        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_paginator.side_effect = NoCredentialsError()

        # Test error handling
        with pytest.raises(ValidationError, match="AWS credentials not configured"):
            self.analyzer.analyze_s3_source("s3://test-bucket")

    def test_analyze_csv_content(self):
        """Test CSV content analysis."""
        csv_content = b'id,name,email,age\n1,John,john@example.com,25\n2,Jane,jane@example.com,30'

        schema_info, quality_info = self.analyzer._analyze_csv_content(csv_content)

        assert schema_info['format'] == 'csv'
        assert schema_info['columns'] == ['id', 'name', 'email', 'age']
        assert schema_info['row_count'] == 2

        assert quality_info['rows_analyzed'] == 2
        assert 'id' in quality_info['data_types']
        assert 'email' in quality_info['potential_pii']

    def test_analyze_json_content_jsonl(self):
        """Test JSON Lines content analysis."""
        json_content = b'{"id": 1, "name": "John", "email": "john@example.com"}\n{"id": 2, "name": "Jane", "email": "jane@example.com"}'

        schema_info, quality_info = self.analyzer._analyze_json_content(json_content)

        assert schema_info['format'] == 'json'
        assert set(schema_info['columns']) == {'id', 'name', 'email'}
        assert schema_info['row_count'] == 2

        assert quality_info['rows_analyzed'] == 2
        assert 'email' in quality_info['potential_pii']

    def test_analyze_json_content_array(self):
        """Test JSON array content analysis."""
        json_content = b'[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]'

        schema_info, quality_info = self.analyzer._analyze_json_content(json_content)

        assert schema_info['format'] == 'json'
        assert set(schema_info['columns']) == {'id', 'name'}
        assert schema_info['row_count'] == 2

    def test_infer_column_type(self):
        """Test column type inference."""
        # Numeric data
        assert self.analyzer._infer_column_type(['1', '2', '3', '4', '5']) == 'numeric'
        assert self.analyzer._infer_column_type(['1.5', '2.7', '3.14']) == 'numeric'

        # Date data
        assert self.analyzer._infer_column_type(['2023-01-01', '2023-01-02', '2023-01-03']) == 'date'
        assert self.analyzer._infer_column_type(['01/01/2023', '01/02/2023']) == 'date'

        # String data
        assert self.analyzer._infer_column_type(['John', 'Jane', 'Bob']) == 'string'

        # Mixed data (should be string)
        assert self.analyzer._infer_column_type(['1', 'John', '2023-01-01']) == 'string'

        # Null data
        assert self.analyzer._infer_column_type(['NULL', 'null', '', 'N/A']) == 'null'

        # Empty data
        assert self.analyzer._infer_column_type([]) == 'unknown'

    def test_check_potential_pii(self):
        """Test PII detection."""
        # Email column
        assert self.analyzer._check_potential_pii('email', ['john@example.com', 'jane@example.com'])
        assert self.analyzer._check_potential_pii('user_email', ['test@test.com'])

        # Phone column
        assert self.analyzer._check_potential_pii('phone', ['+1-555-123-4567', '555-987-6543'])
        assert self.analyzer._check_potential_pii('telephone', ['(555) 123-4567'])

        # Name column
        assert self.analyzer._check_potential_pii('firstname', ['John', 'Jane'])
        assert self.analyzer._check_potential_pii('last_name', ['Smith', 'Doe'])

        # Non-PII column
        assert not self.analyzer._check_potential_pii('id', ['1', '2', '3'])
        assert not self.analyzer._check_potential_pii('amount', ['100', '200', '300'])

    def test_detect_partitions(self):
        """Test partition detection."""
        result = S3AnalysisResult()
        result.prefix = "data/"
        result.sample_files = [
            "data/year=2023/month=01/day=01/file.csv",
            "data/year=2023/month=01/day=02/file.csv",
            "data/department=sales/region=west/file.json",
            "data/2023/01/15/log.txt"
        ]

        self.analyzer._detect_partitions(result)

        assert "year" in result.partitions
        assert "month" in result.partitions
        assert "day" in result.partitions
        assert "department" in result.partitions
        assert "region" in result.partitions
        assert "year/month/day" in result.partitions


class TestAnalyzeSourceWithS3:
    """Test analyze_source function with S3 integration."""

    @patch('src.agent_orchestrated_etl.data_source_analysis.S3DataAnalyzer')
    def test_analyze_source_s3_with_uri(self, mock_analyzer_class):
        """Test analyze_source with S3 URI."""
        # Mock analyzer instance
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        # Mock analysis result
        mock_result = S3AnalysisResult()
        mock_result.bucket_name = "test-bucket"
        mock_result.file_formats = {"csv": 5}
        mock_result.schema_info = {"csv": {"columns": ["id", "name"], "row_count": 100}}
        mock_analyzer.analyze_s3_source.return_value = mock_result

        # Test the function
        metadata = analyze_source("s3", "s3://test-bucket/data/")

        # Verify analyzer was called
        mock_analyzer_class.assert_called_once()
        mock_analyzer.analyze_s3_source.assert_called_once_with("s3://test-bucket/data/")

        # Verify metadata structure
        assert "tables" in metadata
        assert "fields" in metadata
        assert "analysis_metadata" in metadata
        assert metadata["analysis_metadata"]["bucket_name"] == "test-bucket"

    @patch('src.agent_orchestrated_etl.data_source_analysis.S3DataAnalyzer')
    def test_analyze_source_s3_analysis_failure(self, mock_analyzer_class):
        """Test analyze_source with S3 analysis failure."""
        # Mock analyzer to raise exception
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer
        mock_analyzer.analyze_s3_source.side_effect = Exception("Analysis failed")

        # Test the function
        metadata = analyze_source("s3", "s3://test-bucket/data/")

        # Verify fallback metadata
        assert metadata["tables"] == ["objects"]
        assert metadata["fields"] == ["key", "size", "last_modified"]
        assert "analysis_metadata" in metadata
        assert "error" in metadata["analysis_metadata"]
        assert metadata["analysis_metadata"]["fallback_used"] is True

    def test_analyze_source_s3_without_uri(self):
        """Test analyze_source S3 without URI (backward compatibility)."""
        metadata = analyze_source("s3")

        assert metadata["tables"] == ["objects"]
        assert metadata["fields"] == ["key", "size", "last_modified"]
        assert "analysis_metadata" in metadata
        assert "note" in metadata["analysis_metadata"]
        assert "Placeholder metadata" in metadata["analysis_metadata"]["note"]

    def test_analyze_source_s3_invalid_uri(self):
        """Test analyze_source with invalid S3 URI."""
        metadata = analyze_source("s3", "invalid-uri")

        # Should use placeholder since URI doesn't start with s3://
        assert metadata["tables"] == ["objects"]
        assert metadata["fields"] == ["key", "size", "last_modified"]
        assert "note" in metadata["analysis_metadata"]


class TestFileFormatDetection:
    """Test file format detection patterns."""

    def test_all_format_patterns(self):
        """Test all defined file format patterns."""
        test_cases = {
            'csv': ['data.csv', 'DATA.CSV', 'file.tsv', 'path/to/data.csv'],
            'json': ['data.json', 'data.jsonl', 'data.ndjson', 'api.JSON'],
            'parquet': ['data.parquet', 'data.pq', 'DATA.PARQUET'],
            'xml': ['config.xml', 'DATA.XML'],
            'text': ['log.txt', 'readme.TXT', 'data.log'],
            'avro': ['data.avro', 'DATA.AVRO'],
            'orc': ['data.orc', 'DATA.ORC']
        }

        analyzer = S3DataAnalyzer()

        for expected_format, filenames in test_cases.items():
            for filename in filenames:
                detected_format = analyzer._detect_file_format(filename)
                assert detected_format == expected_format, f"Expected {expected_format} for {filename}, got {detected_format}"

    def test_unknown_format(self):
        """Test detection of unknown file formats."""
        analyzer = S3DataAnalyzer()

        unknown_files = [
            'data.xyz',
            'file.unknown',
            'data',  # No extension
            'data.backup'
        ]

        for filename in unknown_files:
            assert analyzer._detect_file_format(filename) == 'unknown'


@pytest.mark.integration
class TestS3AnalysisIntegration:
    """Integration tests for S3 analysis (requires real AWS setup)."""

    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    def test_boto3_not_available_error(self):
        """Test error when boto3 is not available."""
        with patch('src.agent_orchestrated_etl.data_source_analysis.BOTO3_AVAILABLE', False):
            with pytest.raises(ValidationError, match="requires boto3"):
                analyzer = S3DataAnalyzer()
                analyzer.analyze_s3_source("s3://test-bucket")

    def test_cache_stats_empty(self):
        """Test cache statistics on empty analyzer."""
        analyzer = S3DataAnalyzer()
        # No cache stats method implemented yet - this is a placeholder test
        assert analyzer._s3_client is None
