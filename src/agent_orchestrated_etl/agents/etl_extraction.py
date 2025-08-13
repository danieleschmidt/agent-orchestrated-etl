"""Data extraction functionality for ETL operations."""

from __future__ import annotations

import asyncio
import csv
import json
import os
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional

import aiohttp


class DataExtractor:
    """Handles data extraction operations from various sources."""

    def __init__(self):
        """Initialize the data extractor."""
        pass

    async def extract_from_database(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from database sources."""
        import time

        from sqlalchemy import create_engine, text

        start_time = time.time()

        try:
            # Extract configuration
            connection_string = source_config.get('connection_string')
            query = source_config.get('query')
            parameters = source_config.get('parameters', {})
            batch_size = source_config.get('batch_size')
            timeout = source_config.get('timeout', 30)
            pool_size = source_config.get('pool_size', 5)
            max_overflow = source_config.get('max_overflow', 10)

            if not connection_string or not query:
                return {
                    "extraction_method": "database",
                    "status": "error",
                    "error_message": "Missing connection_string or query in source_config",
                    "extraction_time": time.time() - start_time
                }

            # Create engine with connection pooling
            engine_kwargs = {
                'pool_size': pool_size,
                'max_overflow': max_overflow,
                'pool_timeout': timeout,
                'pool_recycle': 3600  # Recycle connections after 1 hour
            }

            # Handle SQLite special case (doesn't support connection pooling)
            if connection_string.startswith('sqlite://'):
                engine_kwargs = {'poolclass': None}

            engine = create_engine(connection_string, **engine_kwargs)

            # Execute the query with parameters
            with engine.connect() as conn:
                if parameters:
                    result = conn.execute(text(query), parameters)
                else:
                    result = conn.execute(text(query))

                # Fetch data
                columns = list(result.keys())
                rows = result.fetchall()

                # Convert to list of dictionaries
                data = [dict(zip(columns, row)) for row in rows]
                record_count = len(data)

                # Handle batch processing information
                batch_info = {}
                if batch_size:
                    total_batches = (record_count + batch_size - 1) // batch_size
                    batch_info = {
                        'batch_size': batch_size,
                        'total_batches': total_batches,
                        'batch_processing_enabled': True
                    }

                extraction_time = time.time() - start_time

                result_dict = {
                    "extraction_method": "database",
                    "source_config": source_config,
                    "status": "completed",
                    "data": data,
                    "record_count": record_count,
                    "extraction_time": extraction_time,
                    "connection_info": {
                        "pool_configured": pool_size > 0,
                        "connection_successful": True
                    }
                }

                if batch_info:
                    result_dict["batch_info"] = batch_info

                return result_dict

        except Exception as e:
            extraction_time = time.time() - start_time
            error_message = str(e)

            # Determine if it's a connection error
            if any(keyword in error_message.lower() for keyword in ['connection', 'connect', 'host', 'port', 'database lock', 'unable to open database']):
                error_type = "connection"
            elif 'timeout' in error_message.lower():
                return {
                    "extraction_method": "database",
                    "status": "timeout",
                    "error_message": f"Query timeout after {timeout} seconds: {error_message}",
                    "extraction_time": extraction_time
                }
            else:
                error_type = "query"

            return {
                "extraction_method": "database",
                "source_config": source_config,
                "status": "error",
                "error_type": error_type,
                "error_message": error_message,
                "extraction_time": extraction_time
            }

    async def extract_from_file(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from file sources."""
        start_time = time.time()

        try:
            file_path = source_config.get('file_path')
            file_format = source_config.get('format', 'auto')

            if not file_path:
                return {
                    "extraction_method": "file",
                    "status": "error",
                    "error_message": "Missing file_path in source_config",
                    "extraction_time": time.time() - start_time
                }

            if not os.path.exists(file_path):
                return {
                    "extraction_method": "file",
                    "status": "error",
                    "error_message": f"File not found: {file_path}",
                    "extraction_time": time.time() - start_time
                }

            # Auto-detect format if not specified
            if file_format == 'auto':
                _, ext = os.path.splitext(file_path)
                format_mapping = {
                    '.csv': 'csv',
                    '.json': 'json',
                    '.jsonl': 'jsonl',
                    '.parquet': 'parquet',
                    '.xml': 'xml'
                }
                file_format = format_mapping.get(ext.lower(), 'csv')

            # Initialize result
            result = {
                "extraction_method": "file",
                "source_config": source_config,
                "file_path": file_path,
                "detected_format": file_format,
                "status": "completed",
                "data": [],
                "columns": [],
                "record_count": 0,
                "file_size": os.path.getsize(file_path),
                "extraction_time": 0
            }

            # Extract based on format
            if file_format == 'csv':
                await self._extract_csv_data(file_path, source_config, result)
            elif file_format == 'json':
                await self._extract_json_data(file_path, source_config, result)
            elif file_format == 'jsonl':
                await self._extract_jsonl_data(file_path, source_config, result)
            elif file_format == 'parquet':
                await self._extract_parquet_data(file_path, source_config, result)
            else:
                result["status"] = "error"
                result["error_message"] = f"Unsupported file format: {file_format}"

            result["extraction_time"] = time.time() - start_time
            return result

        except Exception as e:
            return {
                "extraction_method": "file",
                "source_config": source_config,
                "status": "error",
                "error_message": f"File extraction error: {str(e)}",
                "extraction_time": time.time() - start_time
            }

    async def _extract_csv_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from CSV file."""
        delimiter = source_config.get('delimiter', ',')
        quotechar = source_config.get('quotechar', '"')
        has_header = source_config.get('has_header', True)
        encoding = source_config.get('encoding', 'utf-8')
        max_rows = source_config.get('max_rows')

        with open(file_path, encoding=encoding) as file:
            reader = csv.reader(file, delimiter=delimiter, quotechar=quotechar)

            rows = []
            columns = []

            for i, row in enumerate(reader):
                if i == 0 and has_header:
                    columns = row
                    result["columns"] = columns
                else:
                    if has_header:
                        row_dict = dict(zip(columns, row))
                    else:
                        if not columns:
                            columns = [f"column_{j}" for j in range(len(row))]
                            result["columns"] = columns
                        row_dict = dict(zip(columns, row))

                    rows.append(row_dict)

                    if max_rows and len(rows) >= max_rows:
                        break

            result["data"] = rows
            result["record_count"] = len(rows)

    async def _extract_json_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from JSON file."""
        encoding = source_config.get('encoding', 'utf-8')

        with open(file_path, encoding=encoding) as file:
            data = json.load(file)

            if isinstance(data, list):
                # Array of objects
                result["data"] = data
                result["record_count"] = len(data)

                if data:
                    # Extract column names from first record
                    if isinstance(data[0], dict):
                        result["columns"] = list(data[0].keys())
            elif isinstance(data, dict):
                # Single object - treat as one record
                result["data"] = [data]
                result["record_count"] = 1
                result["columns"] = list(data.keys())
            else:
                result["status"] = "error"
                result["error_message"] = "JSON data must be an object or array of objects"

    async def _extract_jsonl_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from JSONL file."""
        encoding = source_config.get('encoding', 'utf-8')
        max_rows = source_config.get('max_rows')

        rows = []
        columns = set()

        with open(file_path, encoding=encoding) as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    row = json.loads(line)
                    if isinstance(row, dict):
                        rows.append(row)
                        columns.update(row.keys())
                    else:
                        # Handle non-dict rows
                        rows.append({"value": row, "line_number": line_num})
                        columns.add("value")
                        columns.add("line_number")

                    if max_rows and len(rows) >= max_rows:
                        break

                except json.JSONDecodeError as e:
                    result["warnings"] = result.get("warnings", [])
                    result["warnings"].append(f"Invalid JSON on line {line_num}: {str(e)}")

        result["data"] = rows
        result["record_count"] = len(rows)
        result["columns"] = list(columns)

    async def _extract_parquet_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from Parquet file."""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            result["status"] = "error"
            result["error_message"] = "pyarrow library required for Parquet support"
            return

        max_rows = source_config.get('max_rows')

        # Read parquet file
        table = pq.read_table(file_path)

        # Apply row limit if specified
        if max_rows:
            table = table.slice(0, max_rows)

        # Convert to pandas DataFrame then to records
        df = table.to_pandas()
        result["data"] = df.to_dict('records')
        result["record_count"] = len(df)
        result["columns"] = list(df.columns)

    async def extract_from_api(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from API sources with comprehensive protocol support."""
        start_time = time.time()

        try:
            # Extract configuration
            url = source_config.get('url')
            api_type = source_config.get('api_type', 'rest')  # rest, graphql, soap
            auth_config = source_config.get('authentication', {})
            rate_limiting = source_config.get('rate_limiting', {})
            pagination_config = source_config.get('pagination', {})
            timeout = source_config.get('timeout', 30)
            max_retries = source_config.get('max_retries', 3)

            if not url:
                return {
                    "extraction_method": "api",
                    "status": "error",
                    "error_message": "Missing URL in source_config",
                    "extraction_time": time.time() - start_time
                }

            # Set up rate limiting
            rate_limiter = self._create_rate_limiter(rate_limiting)

            # Set up authentication
            headers = await self._setup_authentication(auth_config)

            # Initialize result
            result = {
                "extraction_method": "api",
                "source_config": source_config,
                "api_type": api_type,
                "status": "completed",
                "data": [],
                "record_count": 0,
                "pages_processed": 0,
                "extraction_time": 0,
                "rate_limited": False
            }

            # Extract based on API type
            if api_type == 'rest':
                method = source_config.get('method', 'GET')
                await self._extract_rest_api(url, method, headers, source_config, result, rate_limiter)
            elif api_type == 'graphql':
                query_data = source_config.get('query', {})
                await self._extract_graphql_api(url, query_data, headers, source_config, result, rate_limiter)
            elif api_type == 'soap':
                soap_data = source_config.get('soap_action')
                await self._extract_soap_api(url, soap_data, headers, source_config, result, rate_limiter)
            else:
                result["status"] = "error"
                result["error_message"] = f"Unsupported API type: {api_type}"

            result["extraction_time"] = time.time() - start_time
            return result

        except Exception as e:
            return {
                "extraction_method": "api",
                "source_config": source_config,
                "status": "error",
                "error_message": f"API extraction error: {str(e)}",
                "extraction_time": time.time() - start_time
            }

    def _create_rate_limiter(self, rate_config: Dict[str, Any]):
        """Create a simple rate limiter."""
        return {
            'requests_per_second': rate_config.get('requests_per_second', 10),
            'last_request_time': 0
        }

    async def _wait_for_rate_limit(self, rate_limiter: Dict[str, Any]):
        """Wait for rate limiting if needed."""
        current_time = time.time()
        time_since_last = current_time - rate_limiter['last_request_time']
        min_interval = 1.0 / rate_limiter['requests_per_second']

        if time_since_last < min_interval:
            wait_time = min_interval - time_since_last
            await asyncio.sleep(wait_time)

        rate_limiter['last_request_time'] = time.time()

    async def _setup_authentication(self, auth_config: Dict[str, Any]) -> Dict[str, str]:
        """Set up authentication headers."""
        headers = {"User-Agent": "Agent-ETL/1.0"}

        auth_type = auth_config.get('type', 'none')

        if auth_type == 'bearer':
            token = auth_config.get('token')
            if token:
                headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'api_key':
            api_key = auth_config.get('api_key')
            header_name = auth_config.get('header_name', 'X-API-Key')
            if api_key:
                headers[header_name] = api_key
        elif auth_type == 'basic':
            username = auth_config.get('username')
            password = auth_config.get('password')
            if username and password:
                import base64
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers['Authorization'] = f'Basic {credentials}'

        # Add custom headers
        custom_headers = auth_config.get('headers', {})
        headers.update(custom_headers)

        return headers

    async def _extract_rest_api(self, url: str, method: str, headers: Dict[str, str],
                               source_config: Dict[str, Any], result: Dict[str, Any],
                               rate_limiter: Dict[str, Any]) -> None:
        """Extract data from REST API."""
        timeout = source_config.get('timeout', 30)
        max_retries = source_config.get('max_retries', 3)
        pagination_config = source_config.get('pagination', {})
        request_data = source_config.get('data', {})
        params = source_config.get('params', {})

        all_data = []
        current_url = url
        page_count = 0

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            while current_url:
                await self._wait_for_rate_limit(rate_limiter)

                response_data = await self._make_request_with_retry(
                    session, method, current_url, headers, request_data, params, max_retries
                )

                if not response_data:
                    break

                # Extract data from response
                data_path = source_config.get('data_path', '')
                if data_path:
                    # Navigate to nested data using dot notation
                    current_data = response_data
                    for key in data_path.split('.'):
                        if isinstance(current_data, dict) and key in current_data:
                            current_data = current_data[key]
                        else:
                            current_data = []
                            break

                    if isinstance(current_data, list):
                        all_data.extend(current_data)
                    elif current_data:
                        all_data.append(current_data)
                else:
                    # No data path specified, use entire response
                    if isinstance(response_data, list):
                        all_data.extend(response_data)
                    else:
                        all_data.append(response_data)

                page_count += 1

                # Handle pagination
                if pagination_config:
                    current_url = self._get_next_page_url(response_data, pagination_config, current_url)
                    if pagination_config.get('max_pages') and page_count >= pagination_config['max_pages']:
                        break
                else:
                    break

        result["data"] = all_data
        result["record_count"] = len(all_data)
        result["pages_processed"] = page_count

    async def _extract_graphql_api(self, url: str, query_data: Dict[str, Any],
                                  headers: Dict[str, str], source_config: Dict[str, Any],
                                  result: Dict[str, Any], rate_limiter: Dict[str, Any]) -> None:
        """Extract data from GraphQL API."""
        timeout = source_config.get('timeout', 30)
        max_retries = source_config.get('max_retries', 3)

        query = query_data.get('query', '')
        variables = query_data.get('variables', {})

        if not query:
            result["status"] = "error"
            result["error_message"] = "Missing GraphQL query in source_config"
            return

        request_data = {
            'query': query,
            'variables': variables
        }

        headers['Content-Type'] = 'application/json'

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            await self._wait_for_rate_limit(rate_limiter)

            response_data = await self._make_request_with_retry(
                session, 'POST', url, headers, request_data, {}, max_retries
            )

            if response_data:
                # GraphQL responses typically have 'data' and 'errors' fields
                if 'errors' in response_data:
                    result["status"] = "error"
                    result["error_message"] = f"GraphQL errors: {response_data['errors']}"
                    return

                data = response_data.get('data', {})
                data_path = source_config.get('data_path', '')

                if data_path:
                    # Navigate to nested data
                    current_data = data
                    for key in data_path.split('.'):
                        if isinstance(current_data, dict) and key in current_data:
                            current_data = current_data[key]
                        else:
                            current_data = []
                            break

                    if isinstance(current_data, list):
                        result["data"] = current_data
                    else:
                        result["data"] = [current_data] if current_data else []
                else:
                    result["data"] = [data] if data else []

                result["record_count"] = len(result["data"])

    async def _extract_soap_api(self, url: str, soap_data: Any, headers: Dict[str, str],
                               source_config: Dict[str, Any], result: Dict[str, Any],
                               rate_limiter: Dict[str, Any]) -> None:
        """Extract data from SOAP API."""
        timeout = source_config.get('timeout', 30)
        max_retries = source_config.get('max_retries', 3)

        soap_action = source_config.get('soap_action', '')
        soap_body = source_config.get('soap_body', '')

        if not soap_body:
            result["status"] = "error"
            result["error_message"] = "Missing SOAP body in source_config"
            return

        # Prepare SOAP envelope
        soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                {soap_body}
            </soap:Body>
        </soap:Envelope>"""

        headers.update({
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': soap_action
        })

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            await self._wait_for_rate_limit(rate_limiter)

            async with session.post(url, data=soap_envelope, headers=headers) as response:
                if response.status == 200:
                    response_text = await response.text()

                    # Parse XML response
                    try:
                        root = ET.fromstring(response_text)

                        # Extract data from SOAP response (this is simplified)
                        # In practice, you'd need more sophisticated XML parsing
                        data_elements = []
                        for elem in root.iter():
                            if elem.text and elem.text.strip():
                                data_elements.append({
                                    'tag': elem.tag,
                                    'text': elem.text.strip(),
                                    'attributes': elem.attrib
                                })

                        result["data"] = data_elements
                        result["record_count"] = len(data_elements)
                    except ET.ParseError as e:
                        result["status"] = "error"
                        result["error_message"] = f"XML parsing error: {str(e)}"
                else:
                    result["status"] = "error"
                    result["error_message"] = f"SOAP request failed with status {response.status}"

    async def _make_request_with_retry(self, session: aiohttp.ClientSession, method: str,
                                     url: str, headers: Dict[str, str],
                                     data: Dict[str, Any], params: Dict[str, str],
                                     max_retries: int) -> Optional[Dict[str, Any]]:
        """Make an HTTP request with retry logic."""
        for attempt in range(max_retries + 1):
            try:
                request_kwargs = {
                    'headers': headers,
                    'params': params
                }

                if method.upper() in ['POST', 'PUT', 'PATCH'] and data:
                    if isinstance(data, dict):
                        request_kwargs['json'] = data
                    else:
                        request_kwargs['data'] = data

                async with session.request(method, url, **request_kwargs) as response:
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '').lower()

                        if 'application/json' in content_type:
                            return await response.json()
                        else:
                            text = await response.text()
                            try:
                                return json.loads(text)
                            except json.JSONDecodeError:
                                # Return as text if not JSON
                                return {"text_response": text}
                    elif response.status == 429:  # Rate limited
                        if attempt < max_retries:
                            # Exponential backoff
                            wait_time = (2 ** attempt) * 1
                            await asyncio.sleep(wait_time)
                            continue
                    elif response.status >= 500:  # Server error
                        if attempt < max_retries:
                            await asyncio.sleep(1)
                            continue

                    # Client error or final attempt
                    return None

            except Exception as e:
                if attempt < max_retries:
                    await asyncio.sleep(1)
                    continue
                else:
                    raise e

        return None

    def _get_next_page_url(self, response_data: Any, pagination_config: Dict[str, Any],
                          current_url: str) -> Optional[str]:
        """Extract the next page URL from response data."""
        pagination_type = pagination_config.get('type', 'link')

        if pagination_type == 'link':
            # Look for next page link in response
            next_url_path = pagination_config.get('next_url_path', 'pagination.next_url')

            current_data = response_data
            for key in next_url_path.split('.'):
                if isinstance(current_data, dict) and key in current_data:
                    current_data = current_data[key]
                else:
                    return None

            return current_data if isinstance(current_data, str) else None

        elif pagination_type == 'offset':
            # Offset-based pagination
            current_offset = pagination_config.get('current_offset', 0)
            limit = pagination_config.get('limit', 100)

            # Check if we have more data
            data_path = pagination_config.get('data_path', '')
            if data_path:
                current_data = response_data
                for key in data_path.split('.'):
                    if isinstance(current_data, dict) and key in current_data:
                        current_data = current_data[key]
                    else:
                        return None

                if isinstance(current_data, list) and len(current_data) >= limit:
                    # Build next URL with incremented offset
                    import urllib.parse as urlparse
                    parsed = urlparse.urlparse(current_url)
                    query_params = urlparse.parse_qs(parsed.query)
                    query_params['offset'] = [str(current_offset + limit)]
                    new_query = urlparse.urlencode(query_params, doseq=True)
                    return urlparse.urlunparse(parsed._replace(query=new_query))

        return None
