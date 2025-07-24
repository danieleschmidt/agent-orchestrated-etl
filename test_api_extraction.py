#!/usr/bin/env python3
"""Simple test for API extraction functionality."""

import asyncio
import json
from unittest.mock import Mock, patch
from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent
from src.agent_orchestrated_etl.agents.base_agent import AgentConfig


async def test_rest_api_extraction():
    """Test REST API extraction with mock response."""
    
    # Mock response data
    mock_response_data = [
        {"id": 1, "name": "Test User 1", "email": "test1@example.com"},
        {"id": 2, "name": "Test User 2", "email": "test2@example.com"},
        {"id": 3, "name": "Test User 3", "email": "test3@example.com"}
    ]
    
    # Create ETL agent
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    # Mock aiohttp session
    with patch('aiohttp.ClientSession') as mock_session:
        # Setup mock response
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text.return_value = json.dumps(mock_response_data)
        mock_response.headers = {}
        
        # Setup mock session context manager
        mock_session_instance = Mock()
        mock_session_instance.request.return_value.__aenter__.return_value = mock_response
        mock_session.return_value.__aenter__.return_value = mock_session_instance
        mock_session.return_value.__aexit__.return_value = None
        
        # Test configuration
        source_config = {
            'url': 'https://api.example.com/users',
            'api_type': 'rest',
            'method': 'GET',
            'headers': {'Accept': 'application/json'},
            'authentication': {
                'type': 'bearer',
                'token': 'test-token-123'
            },
            'rate_limit': {'requests_per_second': 5},
            'max_records': 1000
        }
        
        # Execute API extraction
        result = await etl_agent._extract_from_api(source_config)
        
        # Verify results
        print("API Extraction Result:")
        print(f"Status: {result['status']}")
        print(f"Extraction Method: {result['extraction_method']}")
        print(f"API Type: {result['api_type']}")
        print(f"Record Count: {result['record_count']}")
        print(f"Sample Data: {result['sample_data']}")
        print(f"Columns: {result['columns']}")
        
        assert result['status'] == 'completed'
        assert result['extraction_method'] == 'api'
        assert result['api_type'] == 'rest'
        assert result['record_count'] == 3
        assert len(result['data']) == 3
        assert result['columns'] == ['id', 'name', 'email']
        
        print("✅ REST API extraction test passed!")


async def test_graphql_api_extraction():
    """Test GraphQL API extraction."""
    
    # Mock GraphQL response
    mock_graphql_response = {
        "data": {
            "users": [
                {"id": "1", "name": "Alice", "role": "admin"},
                {"id": "2", "name": "Bob", "role": "user"}
            ]
        }
    }
    
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    with patch('aiohttp.ClientSession') as mock_session:
        mock_response = Mock()
        mock_response.status = 200
        mock_response.text.return_value = json.dumps(mock_graphql_response)
        mock_response.headers = {}
        
        mock_session_instance = Mock()
        mock_session_instance.request.return_value.__aenter__.return_value = mock_response
        mock_session.return_value.__aenter__.return_value = mock_session_instance
        mock_session.return_value.__aexit__.return_value = None
        
        source_config = {
            'url': 'https://api.example.com/graphql',
            'api_type': 'graphql',
            'data': {
                'query': '{ users { id name role } }',
                'variables': {}
            },
            'authentication': {
                'type': 'api_key',
                'key': 'api-key-123',
                'header': 'X-API-Key'
            }
        }
        
        result = await etl_agent._extract_from_api(source_config)
        
        print("\nGraphQL API Extraction Result:")
        print(f"Status: {result['status']}")
        print(f"Record Count: {result['record_count']}")
        print(f"Data: {result['data']}")
        
        assert result['status'] == 'completed'
        assert result['record_count'] == 2
        assert len(result['data']) == 2
        
        print("✅ GraphQL API extraction test passed!")


async def test_api_error_handling():
    """Test API error handling."""
    
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    # Test missing URL
    source_config = {
        'api_type': 'rest'
        # Missing 'url'
    }
    
    result = await etl_agent._extract_from_api(source_config)
    
    print("\nError Handling Test:")
    print(f"Status: {result['status']}")
    print(f"Error Message: {result['error_message']}")
    
    assert result['status'] == 'error'
    assert 'Missing' in result['error_message']
    
    print("✅ API error handling test passed!")


async def main():
    """Run all tests."""
    print("Testing API Extraction Implementation...")
    
    await test_rest_api_extraction()
    await test_graphql_api_extraction()
    await test_api_error_handling()
    
    print("\n🎉 All API extraction tests passed!")


if __name__ == '__main__':
    asyncio.run(main())