"""ETL extraction operations module for performing data extraction tasks."""

from __future__ import annotations

import asyncio
import csv
import json
import os
import time
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import aiohttp
import psutil

from ..exceptions import AgentException, DataProcessingException
from .base_agent import AgentTask


class ETLExtractionOperations:
    """Class containing all ETL extraction operations that can be mixed into ETLAgent."""

    async def _extract_data(self, task: AgentTask) -> Dict[str, Any]:
        """Extract data from specified sources."""
        self.logger.info("Starting data extraction")

        try:
            source_config = task.inputs.get("source_config", {})
            source_type = source_config.get("type", "unknown")
            source_path = source_config.get("path")
            extraction_id = task.inputs.get("extraction_id", f"extract_{int(time.time())}")

            if not source_path:
                raise AgentException("source_path is required for data extraction")

            # Register active extraction
            extraction_info = {
                "extraction_id": extraction_id,
                "source_config": source_config,
                "status": "in_progress",
                "started_at": time.time(),
                "records_extracted": 0,
            }
            self.active_extractions.start_operation(extraction_id, extraction_info)

            # Perform extraction based on specialization and source type
            if self.specialization == "database" and source_type in ["postgres", "mysql", "sqlite"]:
                result = await self._extract_from_database(source_config)
            elif self.specialization == "file" and source_type in ["csv", "json", "xml", "parquet"]:
                result = await self._extract_from_file(source_config)
            elif self.specialization == "api" and source_type in ["rest", "graphql", "soap"]:
                result = await self._extract_from_api(source_config)
            else:
                # Use generic data source analysis tool
                result = await self._use_tool("analyze_data_source", {
                    "source_path": source_path,
                    "source_type": source_type,
                    "include_samples": True,
                })

            # Update extraction info
            extraction_info["status"] = "completed"
            extraction_info["completed_at"] = time.time()
            extraction_info["result"] = result
            extraction_info["records_extracted"] = result.get("record_count", 0)

            # Update metrics
            self._update_etl_metrics("extract", extraction_info)

            # Store extraction results in memory
            await self._store_etl_memory("extraction", extraction_info)

            # Remove from active extractions
            self.active_extractions.finish_operation(extraction_id)

            return {
                "extraction_id": extraction_id,
                "status": "completed",
                "source_type": source_type,
                "records_extracted": extraction_info["records_extracted"],
                "extraction_time": extraction_info["completed_at"] - extraction_info["started_at"],
                "result": result,
            }

        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}", exc_info=e)

            # Update extraction status
            self.active_extractions.update_operation(extraction_id, {
                "status": "failed",
                "error": str(e)
            })

            raise DataProcessingException(f"Data extraction failed: {e}") from e

    async def _extract_from_database(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
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
                error_type = "execution"

            return {
                "extraction_method": "database",
                "status": "error",
                "error_message": f"{error_type.title()} error: {error_message}",
                "extraction_time": extraction_time
            }

    async def _extract_from_file(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from file sources.
        
        Supports CSV, JSON, JSONL, and Parquet file formats with comprehensive
        error handling, performance monitoring, and data profiling.
        
        Args:
            source_config: Configuration containing file path, format, and options
            
        Returns:
            Dict containing extraction results, metadata, and performance metrics
        """
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss

        try:
            # Extract configuration parameters
            file_path = source_config.get('path')
            file_format = source_config.get('format', '').lower()
            encoding = source_config.get('encoding', 'utf-8')
            sample_size = source_config.get('sample_size', 100)
            infer_types = source_config.get('infer_types', False)

            if not file_path:
                raise ValueError("File path is required")

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")

            # Get file metadata
            file_stat = os.stat(file_path)
            file_size = file_stat.st_size

            # Initialize result structure
            result = {
                "extraction_method": "file",
                "format": file_format,
                "status": "completed",
                "file_path": file_path,
                "file_size": file_size,
                "encoding": encoding,
                "record_count": 0,
                "columns": [],
                "sample_data": [],
                "schema": {},
                "extraction_time": 0,
                "memory_usage": 0
            }

            # Handle empty files
            if file_size == 0:
                result["extraction_time"] = time.time() - start_time
                result["memory_usage"] = psutil.Process().memory_info().rss - start_memory
                return result

            # Extract data based on format
            if file_format == 'csv':
                await self._extract_csv_data(file_path, source_config, result)
            elif file_format == 'json':
                await self._extract_json_data(file_path, source_config, result)
            elif file_format == 'jsonl':
                await self._extract_jsonl_data(file_path, source_config, result)
            elif file_format == 'parquet':
                await self._extract_parquet_data(file_path, source_config, result)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            # Limit sample data size
            if len(result["sample_data"]) > sample_size:
                result["sample_data"] = result["sample_data"][:sample_size]

            # Infer schema if requested
            if infer_types and result["sample_data"]:
                result["schema"] = self._infer_schema(result["sample_data"], result["columns"])

            # Calculate performance metrics
            result["extraction_time"] = time.time() - start_time
            result["memory_usage"] = psutil.Process().memory_info().rss - start_memory

            return result

        except FileNotFoundError as e:
            return {
                "extraction_method": "file",
                "status": "error",
                "error_message": f"File not found: {str(e)}",
                "extraction_time": time.time() - start_time,
                "memory_usage": psutil.Process().memory_info().rss - start_memory
            }
        except ValueError as e:
            return {
                "extraction_method": "file",
                "status": "error",
                "error_message": str(e),
                "extraction_time": time.time() - start_time,
                "memory_usage": psutil.Process().memory_info().rss - start_memory
            }
        except Exception as e:
            return {
                "extraction_method": "file",
                "status": "error",
                "error_message": f"Unexpected error during file extraction: {str(e)}",
                "extraction_time": time.time() - start_time,
                "memory_usage": psutil.Process().memory_info().rss - start_memory
            }

    async def _extract_csv_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from CSV file."""
        encoding = source_config.get('encoding', 'utf-8')
        delimiter = source_config.get('delimiter', ',')

        with open(file_path, encoding=encoding, newline='') as f:
            # Detect delimiter if not specified
            if delimiter == ',':
                sample = f.read(1024)
                f.seek(0)
                sniffer = csv.Sniffer()
                try:
                    dialect = sniffer.sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    delimiter = ','

            reader = csv.reader(f, delimiter=delimiter)
            rows = list(reader)

            if rows:
                result["columns"] = rows[0] if rows else []
                data_rows = rows[1:] if len(rows) > 1 else []
                result["record_count"] = len(data_rows)

                # Convert rows to list of dicts for sample data
                result["sample_data"] = [
                    dict(zip(result["columns"], row))
                    for row in data_rows[:100]
                ]

    async def _extract_json_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from JSON file."""
        encoding = source_config.get('encoding', 'utf-8')

        with open(file_path, encoding=encoding) as f:
            data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list):
                result["record_count"] = len(data)
                result["sample_data"] = data[:100]

                # Extract column names from first record
                if data and isinstance(data[0], dict):
                    result["columns"] = list(data[0].keys())
            elif isinstance(data, dict):
                result["record_count"] = 1
                result["sample_data"] = [data]
                result["columns"] = list(data.keys())
            else:
                raise ValueError("JSON file must contain an object or array of objects")

    async def _extract_jsonl_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from JSON Lines file."""
        encoding = source_config.get('encoding', 'utf-8')

        records = []
        columns_set = set()

        with open(file_path, encoding=encoding) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    records.append(record)

                    if isinstance(record, dict):
                        columns_set.update(record.keys())

                    # Limit records for sample
                    if len(records) >= 100:
                        break

                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON on line {line_num}: {e}")

        result["record_count"] = len(records)
        result["sample_data"] = records
        result["columns"] = list(columns_set)

    async def _extract_parquet_data(self, file_path: str, source_config: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Extract data from Parquet file."""
        try:
            import pandas as pd
        except ImportError:
            raise ValueError("pandas is required for Parquet file support")

        try:
            # Read parquet file
            df = pd.read_parquet(file_path)

            result["record_count"] = len(df)
            result["columns"] = df.columns.tolist()

            # Convert sample to list of dicts
            sample_df = df.head(100)
            result["sample_data"] = sample_df.to_dict('records')

        except Exception as e:
            raise ValueError(f"Error reading Parquet file: {e}")

    def _infer_schema(self, sample_data: List[Dict[str, Any]], columns: List[str]) -> Dict[str, str]:
        """Infer data types from sample data."""
        schema = {}

        for column in columns:
            column_values = []
            for record in sample_data:
                if column in record and record[column] is not None:
                    column_values.append(record[column])

            if not column_values:
                schema[column] = "unknown"
                continue

            # Check if all values are numeric
            try:
                numeric_values = [float(v) for v in column_values if str(v).strip()]
                if all(float(v).is_integer() for v in numeric_values):
                    schema[column] = "integer"
                else:
                    schema[column] = "float"
            except (ValueError, TypeError):
                # Check if boolean
                str_values = [str(v).lower() for v in column_values]
                if all(v in ['true', 'false', '1', '0'] for v in str_values):
                    schema[column] = "boolean"
                else:
                    schema[column] = "string"

        return schema

    async def _extract_from_api(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from API sources with comprehensive support for REST, GraphQL, and SOAP."""
        import time

        start_time = time.time()

        try:
            # Extract and validate configuration
            url = source_config.get('url')
            api_type = source_config.get('api_type', 'rest').lower()
            method = source_config.get('method', 'GET').upper()
            headers = source_config.get('headers', {})
            params = source_config.get('params', {})
            data = source_config.get('data')
            auth_config = source_config.get('authentication', {})
            rate_limit = source_config.get('rate_limit', {'requests_per_second': 10})
            pagination = source_config.get('pagination', {})
            timeout = source_config.get('timeout', 30)
            max_retries = source_config.get('max_retries', 3)
            max_records = source_config.get('max_records', 10000)

            if not url:
                return {
                    "extraction_method": "api",
                    "status": "error",
                    "error_message": "Missing 'url' in API source configuration",
                    "extraction_time": time.time() - start_time
                }

            # Initialize rate limiter
            rate_limiter = self._create_rate_limiter(rate_limit)

            # Set up authentication
            auth_headers = await self._setup_authentication(auth_config)
            headers.update(auth_headers)

            # Extract data based on API type
            if api_type == 'rest':
                result = await self._extract_rest_api(
                    url, method, headers, params, data, pagination,
                    timeout, max_retries, max_records, rate_limiter
                )
            elif api_type == 'graphql':
                result = await self._extract_graphql_api(
                    url, data or {}, headers, timeout, max_retries, rate_limiter
                )
            elif api_type == 'soap':
                result = await self._extract_soap_api(
                    url, data, headers, timeout, max_retries, rate_limiter
                )
            else:
                return {
                    "extraction_method": "api",
                    "status": "error",
                    "error_message": f"Unsupported API type: {api_type}",
                    "extraction_time": time.time() - start_time
                }

            # Add common metadata
            result.update({
                "extraction_method": "api",
                "api_type": api_type,
                "source_url": url,
                "extraction_time": time.time() - start_time,
                "source_config": {
                    k: v for k, v in source_config.items()
                    if k not in ['authentication']  # Don't log sensitive data
                }
            })

            return result

        except Exception as e:
            return {
                "extraction_method": "api",
                "status": "error",
                "error_message": str(e),
                "extraction_time": time.time() - start_time,
                "source_config": {
                    k: v for k, v in source_config.items()
                    if k not in ['authentication']
                }
            }

    def _create_rate_limiter(self, rate_config: Dict[str, Any]):
        """Create a simple rate limiter."""
        requests_per_second = rate_config.get('requests_per_second', 10)
        return {'min_interval': 1.0 / requests_per_second, 'last_request': 0}

    async def _wait_for_rate_limit(self, rate_limiter: Dict[str, Any]):
        """Wait for rate limit if necessary."""
        current_time = time.time()
        time_since_last = current_time - rate_limiter['last_request']
        if time_since_last < rate_limiter['min_interval']:
            await asyncio.sleep(rate_limiter['min_interval'] - time_since_last)
        rate_limiter['last_request'] = time.time()

    async def _setup_authentication(self, auth_config: Dict[str, Any]) -> Dict[str, str]:
        """Set up authentication headers based on configuration."""
        headers = {}
        auth_type = auth_config.get('type', '').lower()

        if auth_type == 'bearer':
            token = auth_config.get('token')
            if token:
                headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'api_key':
            key = auth_config.get('key')
            header_name = auth_config.get('header', 'X-API-Key')
            if key:
                headers[header_name] = key
        elif auth_type == 'basic':
            username = auth_config.get('username')
            password = auth_config.get('password')
            if username and password:
                import base64
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers['Authorization'] = f'Basic {credentials}'
        elif auth_type == 'oauth2':
            # For OAuth2, assume token is provided (full OAuth flow would be complex)
            token = auth_config.get('access_token')
            if token:
                headers['Authorization'] = f'Bearer {token}'

        return headers

    async def _extract_rest_api(self, url: str, method: str, headers: Dict[str, str],
                               params: Dict[str, Any], data: Any, pagination: Dict[str, Any],
                               timeout: int, max_retries: int, max_records: int,
                               rate_limiter: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from REST API with pagination support."""
        import aiohttp

        all_data = []
        total_requests = 0
        current_url = url
        current_params = params.copy()

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            while len(all_data) < max_records:
                await self._wait_for_rate_limit(rate_limiter)

                # Make request with retry logic
                response_data = await self._make_request_with_retry(
                    session, method, current_url, headers, current_params, data, max_retries
                )

                if response_data.get('status') == 'error':
                    return response_data

                total_requests += 1
                page_data = response_data['data']

                # Handle different response structures
                if isinstance(page_data, list):
                    all_data.extend(page_data)
                elif isinstance(page_data, dict):
                    # Look for common data keys
                    data_key = None
                    for key in ['data', 'results', 'items', 'records']:
                        if key in page_data and isinstance(page_data[key], list):
                            data_key = key
                            break

                    if data_key:
                        all_data.extend(page_data[data_key])
                    else:
                        all_data.append(page_data)
                else:
                    all_data.append(page_data)

                # Handle pagination
                next_url = self._get_next_page_url(page_data, pagination, current_url)
                if not next_url or len(page_data) == 0:
                    break

                current_url = next_url
                current_params = {}  # URL already contains params for next page

        # Analyze extracted data
        sample_data = all_data[:100] if all_data else []
        columns = []
        if all_data and isinstance(all_data[0], dict):
            columns = list(all_data[0].keys())

        return {
            "status": "completed",
            "data": all_data,
            "record_count": len(all_data),
            "sample_data": sample_data,
            "columns": columns,
            "total_requests": total_requests,
            "pagination_info": {
                "pages_fetched": total_requests,
                "pagination_used": bool(pagination)
            }
        }

    async def _extract_graphql_api(self, url: str, query_data: Dict[str, Any],
                                  headers: Dict[str, str], timeout: int, max_retries: int,
                                  rate_limiter: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from GraphQL API."""
        import aiohttp

        query = query_data.get('query')
        variables = query_data.get('variables', {})

        if not query:
            return {
                "status": "error",
                "error_message": "GraphQL query is required in 'data.query'"
            }

        payload = {
            "query": query,
            "variables": variables
        }

        headers['Content-Type'] = 'application/json'

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            await self._wait_for_rate_limit(rate_limiter)

            response_data = await self._make_request_with_retry(
                session, 'POST', url, headers, {}, payload, max_retries
            )

            if response_data.get('status') == 'error':
                return response_data

            graphql_response = response_data['data']

            # Handle GraphQL response structure
            if 'errors' in graphql_response:
                return {
                    "status": "error",
                    "error_message": f"GraphQL errors: {graphql_response['errors']}"
                }

            data = graphql_response.get('data', {})

            # Try to flatten the response to extract actual data
            extracted_data = []
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, list):
                        extracted_data.extend(value)
                    elif isinstance(value, dict):
                        extracted_data.append(value)
            elif isinstance(data, list):
                extracted_data = data

            sample_data = extracted_data[:100] if extracted_data else []
            columns = []
            if extracted_data and isinstance(extracted_data[0], dict):
                columns = list(extracted_data[0].keys())

            return {
                "status": "completed",
                "data": extracted_data,
                "record_count": len(extracted_data),
                "sample_data": sample_data,
                "columns": columns,
                "graphql_response": graphql_response
            }

    async def _extract_soap_api(self, url: str, soap_data: Any, headers: Dict[str, str],
                               timeout: int, max_retries: int, rate_limiter: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from SOAP API."""
        import aiohttp

        if not soap_data:
            return {
                "status": "error",
                "error_message": "SOAP envelope/body is required in 'data'"
            }

        # Set SOAP headers
        headers['Content-Type'] = 'text/xml; charset=utf-8'
        if 'SOAPAction' not in headers:
            headers['SOAPAction'] = '""'  # Default empty SOAPAction

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            await self._wait_for_rate_limit(rate_limiter)

            response_data = await self._make_request_with_retry(
                session, 'POST', url, headers, {}, soap_data, max_retries
            )

            if response_data.get('status') == 'error':
                return response_data

            soap_response = response_data['data']

            # Parse SOAP XML response
            try:
                if isinstance(soap_response, str):
                    root = ET.fromstring(soap_response)
                else:
                    return {
                        "status": "error",
                        "error_message": "SOAP response is not XML string"
                    }

                # Extract data from SOAP body (simplified parsing)
                extracted_data = []
                for elem in root.iter():
                    if elem.text and elem.text.strip():
                        extracted_data.append({
                            'tag': elem.tag,
                            'text': elem.text.strip(),
                            'attributes': elem.attrib
                        })

                return {
                    "status": "completed",
                    "data": extracted_data,
                    "record_count": len(extracted_data),
                    "sample_data": extracted_data[:100],
                    "soap_response": soap_response
                }

            except ET.ParseError as e:
                return {
                    "status": "error",
                    "error_message": f"Failed to parse SOAP XML response: {e}"
                }

    async def _make_request_with_retry(self, session: aiohttp.ClientSession, method: str,
                                      url: str, headers: Dict[str, str], params: Dict[str, Any],
                                      data: Any, max_retries: int) -> Dict[str, Any]:
        """Make HTTP request with exponential backoff retry logic."""
        import json

        import aiohttp

        for attempt in range(max_retries + 1):
            try:
                request_kwargs = {
                    'headers': headers,
                    'params': params
                }

                if data is not None:
                    if isinstance(data, dict):
                        if headers.get('Content-Type', '').startswith('application/json'):
                            request_kwargs['json'] = data
                        else:
                            request_kwargs['data'] = data
                    else:
                        request_kwargs['data'] = data

                async with session.request(method, url, **request_kwargs) as response:
                    response_text = await response.text()

                    # Check for successful status codes
                    if 200 <= response.status < 300:
                        # Try to parse as JSON first
                        try:
                            response_data = json.loads(response_text)
                        except json.JSONDecodeError:
                            # If not JSON, return as text
                            response_data = response_text

                        return {
                            "status": "completed",
                            "data": response_data,
                            "http_status": response.status,
                            "response_headers": dict(response.headers)
                        }
                    else:
                        # Handle HTTP error status codes
                        if attempt < max_retries:
                            # Exponential backoff: 1s, 2s, 4s, etc.
                            wait_time = 2 ** attempt
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            return {
                                "status": "error",
                                "error_message": f"HTTP {response.status}: {response_text}",
                                "http_status": response.status
                            }

            except aiohttp.ClientError as e:
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    return {
                        "status": "error",
                        "error_message": f"Network error: {str(e)}",
                        "error_type": type(e).__name__
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "error_message": f"Unexpected error: {str(e)}",
                    "error_type": type(e).__name__
                }

        return {
            "status": "error",
            "error_message": f"Failed after {max_retries + 1} attempts"
        }

    def _get_next_page_url(self, response_data: Any, pagination_config: Dict[str, Any],
                          current_url: str) -> Optional[str]:
        """Extract next page URL from API response based on pagination configuration."""
        if not pagination_config:
            return None

        pagination_type = pagination_config.get('type', 'offset')

        if pagination_type == 'cursor':
            # Cursor-based pagination
            cursor_key = pagination_config.get('cursor_key', 'next_cursor')
            if isinstance(response_data, dict) and cursor_key in response_data:
                cursor_value = response_data[cursor_key]
                if cursor_value:
                    base_url = current_url.split('?')[0]
                    cursor_param = pagination_config.get('cursor_param', 'cursor')
                    return f"{base_url}?{cursor_param}={cursor_value}"

        elif pagination_type == 'link':
            # Link header pagination (GitHub style)
            next_link_key = pagination_config.get('next_link_key', 'next')
            if isinstance(response_data, dict) and next_link_key in response_data:
                return response_data[next_link_key]

        elif pagination_type == 'offset':
            # Offset/limit pagination (need to construct next URL)
            if isinstance(response_data, dict):
                total_key = pagination_config.get('total_key', 'total')
                current_page_key = pagination_config.get('page_key', 'page')
                page_size_key = pagination_config.get('size_key', 'size')

                total = response_data.get(total_key, 0)
                current_page = pagination_config.get('current_page', 1)
                page_size = pagination_config.get('page_size', 100)

                if (current_page * page_size) < total:
                    next_page = current_page + 1
                    base_url = current_url.split('?')[0]
                    return f"{base_url}?{current_page_key}={next_page}&{page_size_key}={page_size}"

        return None

    # Helper methods that need to be available to extraction methods
    # These will be provided by the ETLAgent class that mixes in this class:
    # - _use_tool: For using tools from the registry
    # - _store_etl_memory: For storing operation information in memory
    # - _update_etl_metrics: For updating ETL performance metrics
    #
    # The following class attributes are also expected to be available:
    # - self.active_extractions: Dict for tracking active extraction operations
    # - self.specialization: String indicating the agent's specialization
    # - self.logger: Logger instance for logging
    # - self.tool_registry: Tool registry for accessing tools
    # - self.memory: Memory instance for storing operation data
    # - self.etl_metrics: Dict for tracking performance metrics
