"""Tests for ETL Agent file extraction functionality."""

import pytest
import tempfile
import json
import csv
import os
import pandas as pd
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

from agent_orchestrated_etl.agents.etl_agent import ETLAgent
from agent_orchestrated_etl.agents.base_agent import AgentTask


class TestETLFileExtraction:
    """Test file extraction methods in ETL Agent."""
    
    @pytest.fixture
    def etl_agent(self):
        """Create ETL agent for testing."""
        return ETLAgent(specialization="general")
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def sample_csv_data(self):
        """Sample CSV data for testing."""
        return [
            ['id', 'name', 'age', 'city'],
            ['1', 'John Doe', '30', 'New York'],
            ['2', 'Jane Smith', '25', 'Los Angeles'],
            ['3', 'Bob Johnson', '35', 'Chicago']
        ]
    
    @pytest.fixture
    def sample_json_data(self):
        """Sample JSON data for testing."""
        return [
            {'id': 1, 'name': 'John Doe', 'age': 30, 'city': 'New York'},
            {'id': 2, 'name': 'Jane Smith', 'age': 25, 'city': 'Los Angeles'},
            {'id': 3, 'name': 'Bob Johnson', 'age': 35, 'city': 'Chicago'}
        ]
    
    def create_test_csv(self, temp_dir, data):
        """Create test CSV file."""
        csv_path = os.path.join(temp_dir, 'test_data.csv')
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(data)
        return csv_path
    
    def create_test_json(self, temp_dir, data):
        """Create test JSON file."""
        json_path = os.path.join(temp_dir, 'test_data.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        return json_path
    
    def create_test_parquet(self, temp_dir, data):
        """Create test Parquet file."""
        parquet_path = os.path.join(temp_dir, 'test_data.parquet')
        df = pd.DataFrame(data[1:], columns=data[0])  # Skip header row for DataFrame
        df.to_parquet(parquet_path, index=False)
        return parquet_path
    
    @pytest.mark.asyncio
    async def test_extract_csv_file_success(self, etl_agent, temp_dir, sample_csv_data):
        """Test successful CSV file extraction."""
        await etl_agent.start()
        
        # Create test CSV file
        csv_path = self.create_test_csv(temp_dir, sample_csv_data)
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': csv_path,
            'encoding': 'utf-8'
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        # Verify results
        assert result['status'] == 'completed'
        assert result['extraction_method'] == 'file'
        assert result['format'] == 'csv'
        assert result['record_count'] == 3  # 3 data rows (excluding header)
        assert result['columns'] == ['id', 'name', 'age', 'city']
        assert len(result['sample_data']) > 0
        assert result['file_size'] > 0
        assert result['extraction_time'] > 0
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_json_file_success(self, etl_agent, temp_dir, sample_json_data):
        """Test successful JSON file extraction."""
        await etl_agent.start()
        
        # Create test JSON file
        json_path = self.create_test_json(temp_dir, sample_json_data)
        
        source_config = {
            'type': 'file',
            'format': 'json',
            'path': json_path,
            'encoding': 'utf-8'
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        # Verify results
        assert result['status'] == 'completed'
        assert result['extraction_method'] == 'file'
        assert result['format'] == 'json'
        assert result['record_count'] == 3
        assert set(result['columns']) == {'id', 'name', 'age', 'city'}
        assert len(result['sample_data']) > 0
        assert result['file_size'] > 0
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_parquet_file_success(self, etl_agent, temp_dir, sample_csv_data):
        """Test successful Parquet file extraction."""
        await etl_agent.start()
        
        # Create test Parquet file
        parquet_path = self.create_test_parquet(temp_dir, sample_csv_data)
        
        source_config = {
            'type': 'file',
            'format': 'parquet',
            'path': parquet_path
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        # Verify results
        assert result['status'] == 'completed'
        assert result['extraction_method'] == 'file'
        assert result['format'] == 'parquet'
        assert result['record_count'] == 3
        assert set(result['columns']) == {'id', 'name', 'age', 'city'}
        assert len(result['sample_data']) > 0
        assert result['file_size'] > 0
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_file_not_found(self, etl_agent):
        """Test extraction with non-existent file."""
        await etl_agent.start()
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': '/non/existent/file.csv'
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        # Should handle error gracefully
        assert result['status'] == 'error'
        assert 'error_message' in result
        assert 'File not found' in result['error_message'] or 'No such file' in result['error_message']
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_unsupported_format(self, etl_agent, temp_dir):
        """Test extraction with unsupported file format."""
        await etl_agent.start()
        
        # Create a file with unsupported format
        unsupported_path = os.path.join(temp_dir, 'test.xyz')
        with open(unsupported_path, 'w') as f:
            f.write("unsupported format content")
        
        source_config = {
            'type': 'file',
            'format': 'xyz',
            'path': unsupported_path
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'error'
        assert 'error_message' in result
        assert 'Unsupported file format' in result['error_message']
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_malformed_json(self, etl_agent, temp_dir):
        """Test extraction with malformed JSON file."""
        await etl_agent.start()
        
        # Create malformed JSON file
        json_path = os.path.join(temp_dir, 'bad.json')
        with open(json_path, 'w') as f:
            f.write('{"invalid": json content}')
        
        source_config = {
            'type': 'file',
            'format': 'json',
            'path': json_path
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'error'
        assert 'error_message' in result
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_empty_file(self, etl_agent, temp_dir):
        """Test extraction with empty file."""
        await etl_agent.start()
        
        # Create empty CSV file
        empty_path = os.path.join(temp_dir, 'empty.csv')
        with open(empty_path, 'w') as f:
            pass
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': empty_path
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'completed'
        assert result['record_count'] == 0
        assert result['columns'] == []
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_with_encoding_options(self, etl_agent, temp_dir):
        """Test extraction with different encoding options."""
        await etl_agent.start()
        
        # Create CSV with UTF-8 encoding
        csv_path = os.path.join(temp_dir, 'utf8.csv')
        data = [['id', 'name'], ['1', 'José']]
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(data)
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': csv_path,
            'encoding': 'utf-8'
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'completed'
        assert result['record_count'] == 1
        assert 'José' in str(result['sample_data'])
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_with_sample_size_limit(self, etl_agent, temp_dir, sample_csv_data):
        """Test extraction with sample size limitation."""
        await etl_agent.start()
        
        # Create larger dataset
        large_data = [sample_csv_data[0]]  # Header
        for i in range(100):
            large_data.append([str(i), f'User{i}', str(20+i), f'City{i}'])
        
        csv_path = self.create_test_csv(temp_dir, large_data)
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': csv_path,
            'sample_size': 5
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'completed'
        assert result['record_count'] == 100
        assert len(result['sample_data']) <= 5  # Should limit sample size
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_csv_with_delimiter_options(self, etl_agent, temp_dir):
        """Test CSV extraction with different delimiter options."""
        await etl_agent.start()
        
        # Create CSV with semicolon delimiter
        csv_path = os.path.join(temp_dir, 'semicolon.csv')
        with open(csv_path, 'w', newline='') as f:
            f.write('id;name;age\n1;John;30\n2;Jane;25\n')
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': csv_path,
            'delimiter': ';',
            'encoding': 'utf-8'
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'completed'
        assert result['record_count'] == 2
        assert result['columns'] == ['id', 'name', 'age']
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_extract_json_array_vs_lines(self, etl_agent, temp_dir, sample_json_data):
        """Test extraction of JSON array vs JSON lines format."""
        await etl_agent.start()
        
        # Test JSON array format (already tested above)
        json_array_path = self.create_test_json(temp_dir, sample_json_data)
        
        # Test JSON lines format
        json_lines_path = os.path.join(temp_dir, 'test_lines.jsonl')
        with open(json_lines_path, 'w') as f:
            for record in sample_json_data:
                f.write(json.dumps(record) + '\n')
        
        # Test JSON array
        source_config_array = {
            'type': 'file',
            'format': 'json',
            'path': json_array_path
        }
        result_array = await etl_agent._extract_from_file(source_config_array)
        
        # Test JSON lines
        source_config_lines = {
            'type': 'file',
            'format': 'jsonl',
            'path': json_lines_path
        }
        result_lines = await etl_agent._extract_from_file(source_config_lines)
        
        # Both should succeed and have same record count
        assert result_array['status'] == 'completed'
        assert result_lines['status'] == 'completed'
        assert result_array['record_count'] == result_lines['record_count'] == 3
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_data_type_inference(self, etl_agent, temp_dir):
        """Test that data types are properly inferred from files."""
        await etl_agent.start()
        
        # Create CSV with mixed data types
        data = [
            ['id', 'name', 'age', 'salary', 'is_active', 'hire_date'],
            ['1', 'John', '30', '50000.50', 'true', '2023-01-15'],
            ['2', 'Jane', '25', '60000.00', 'false', '2023-02-20']
        ]
        csv_path = self.create_test_csv(temp_dir, data)
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': csv_path,
            'infer_types': True
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        assert result['status'] == 'completed'
        assert 'schema' in result
        schema = result['schema']
        
        # Should detect different data types
        assert 'id' in schema
        assert 'name' in schema
        assert 'age' in schema
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_performance_metrics_collection(self, etl_agent, temp_dir, sample_csv_data):
        """Test that performance metrics are collected during extraction."""
        await etl_agent.start()
        
        csv_path = self.create_test_csv(temp_dir, sample_csv_data)
        
        source_config = {
            'type': 'file',
            'format': 'csv',
            'path': csv_path
        }
        
        result = await etl_agent._extract_from_file(source_config)
        
        # Verify performance metrics are included
        assert result['status'] == 'completed'
        assert 'extraction_time' in result
        assert 'file_size' in result
        assert 'memory_usage' in result
        assert isinstance(result['extraction_time'], (int, float))
        assert result['extraction_time'] > 0
        assert result['file_size'] >= 0
        
        await etl_agent.stop()