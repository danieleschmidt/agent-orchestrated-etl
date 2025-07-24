#!/usr/bin/env python3
"""Simple API extraction test without complex mocking."""

import asyncio
from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent
from src.agent_orchestrated_etl.agents.base_agent import AgentConfig


async def test_error_cases():
    """Test API extraction error handling."""
    
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    # Test 1: Missing URL
    print("Test 1: Missing URL")
    source_config = {'api_type': 'rest'}
    result = await etl_agent._extract_from_api(source_config)
    print(f"Status: {result['status']}")
    print(f"Error: {result['error_message']}")
    assert result['status'] == 'error'
    assert 'Missing' in result['error_message']
    print("✅ Missing URL test passed\n")
    
    # Test 2: Unsupported API type
    print("Test 2: Unsupported API type")
    source_config = {
        'url': 'https://example.com/api',
        'api_type': 'unsupported_type'
    }
    result = await etl_agent._extract_from_api(source_config)
    print(f"Status: {result['status']}")
    print(f"Error: {result['error_message']}")
    assert result['status'] == 'error'
    assert 'Unsupported API type' in result['error_message']
    print("✅ Unsupported API type test passed\n")
    
    # Test 3: GraphQL without query
    print("Test 3: GraphQL without query")
    source_config = {
        'url': 'https://example.com/graphql',
        'api_type': 'graphql',
        'data': {}  # Missing query
    }
    result = await etl_agent._extract_from_api(source_config)
    print(f"Status: {result['status']}")
    print(f"Error: {result['error_message']}")
    assert result['status'] == 'error'
    assert 'query is required' in result['error_message']
    print("✅ GraphQL missing query test passed\n")
    
    # Test 4: SOAP without data
    print("Test 4: SOAP without data")
    source_config = {
        'url': 'https://example.com/soap',
        'api_type': 'soap'
        # Missing data
    }
    result = await etl_agent._extract_from_api(source_config)
    print(f"Status: {result['status']}")
    print(f"Error: {result['error_message']}")
    assert result['status'] == 'error'
    assert 'SOAP envelope/body is required' in result['error_message']
    print("✅ SOAP missing data test passed\n")


async def test_authentication_headers():
    """Test authentication header generation."""
    
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    # Test Bearer token
    print("Testing Bearer token authentication...")
    auth_config = {'type': 'bearer', 'token': 'test-token-123'}
    headers = await etl_agent._setup_authentication(auth_config)
    print(f"Headers: {headers}")
    assert headers.get('Authorization') == 'Bearer test-token-123'
    print("✅ Bearer token test passed\n")
    
    # Test API key
    print("Testing API key authentication...")
    auth_config = {'type': 'api_key', 'key': 'api-key-456', 'header': 'X-Custom-Key'}
    headers = await etl_agent._setup_authentication(auth_config)
    print(f"Headers: {headers}")
    assert headers.get('X-Custom-Key') == 'api-key-456'
    print("✅ API key test passed\n")
    
    # Test Basic auth
    print("Testing Basic authentication...")
    auth_config = {'type': 'basic', 'username': 'user', 'password': 'pass'}
    headers = await etl_agent._setup_authentication(auth_config)
    print(f"Headers: {headers}")
    assert 'Authorization' in headers
    assert headers['Authorization'].startswith('Basic ')
    print("✅ Basic auth test passed\n")


def test_rate_limiter():
    """Test rate limiter configuration."""
    
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    # Test rate limiter creation
    rate_config = {'requests_per_second': 5}
    rate_limiter = etl_agent._create_rate_limiter(rate_config)
    
    print(f"Rate limiter: {rate_limiter}")
    assert rate_limiter['min_interval'] == 0.2  # 1/5 = 0.2 seconds
    print("✅ Rate limiter test passed\n")


def test_pagination_url_extraction():
    """Test pagination URL extraction logic."""
    
    config = AgentConfig()
    etl_agent = ETLAgent(config=config)
    
    # Test cursor pagination
    print("Testing cursor pagination...")
    response_data = {'next_cursor': 'abc123'}
    pagination_config = {'type': 'cursor', 'cursor_key': 'next_cursor', 'cursor_param': 'cursor'}
    current_url = 'https://api.example.com/data'
    
    next_url = etl_agent._get_next_page_url(response_data, pagination_config, current_url)
    print(f"Next URL: {next_url}")
    assert next_url == 'https://api.example.com/data?cursor=abc123'
    print("✅ Cursor pagination test passed\n")
    
    # Test no pagination
    print("Testing no pagination...")
    next_url = etl_agent._get_next_page_url({}, {}, current_url)
    print(f"Next URL: {next_url}")
    assert next_url is None
    print("✅ No pagination test passed\n")


async def main():
    """Run all tests."""
    print("Testing API Extraction Implementation Components...\n")
    
    await test_error_cases()
    await test_authentication_headers()
    test_rate_limiter()
    test_pagination_url_extraction()
    
    print("🎉 All API extraction component tests passed!")


if __name__ == '__main__':
    asyncio.run(main())