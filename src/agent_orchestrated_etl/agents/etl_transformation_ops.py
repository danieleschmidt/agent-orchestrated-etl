"""ETL transformation operations module containing all data transformation methods."""

from __future__ import annotations

import re
import statistics
import time
from typing import Any, Dict, List, Optional

from ..exceptions import DataProcessingException
from .etl_thread_safety import ThreadSafeOperationTracker


class ETLTransformationOperations:
    """Comprehensive ETL transformation operations class."""

    def __init__(self, logger=None):
        """Initialize transformation operations."""
        self.logger = logger
        self.active_transformations = ThreadSafeOperationTracker()
        self.transformation_metrics = {
            "total_transformations": 0,
            "successful_transformations": 0,
            "failed_transformations": 0,
            "total_records_transformed": 0,
            "total_transformation_time": 0.0,
        }

    async def transform_data(self, task_inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Main transformation orchestration method."""
        if self.logger:
            self.logger.info("Starting data transformation")

        try:
            transformation_config = task_inputs.get("transformation_config", {})
            source_data = task_inputs.get("source_data")
            transformation_id = task_inputs.get("transformation_id", f"transform_{int(time.time())}")

            if not source_data and not transformation_config.get("source_reference"):
                raise DataProcessingException("source_data or source_reference is required for transformation")

            # Register active transformation
            transformation_info = {
                "transformation_id": transformation_id,
                "transformation_config": transformation_config,
                "status": "in_progress",
                "started_at": time.time(),
                "records_transformed": 0,
            }
            self.active_transformations.start_operation(transformation_id, transformation_info)

            # Perform transformation based on type and rules
            transformation_rules = transformation_config.get("rules", [])
            transformation_type = transformation_config.get("type", "generic")

            if transformation_type == "mapping":
                result = await self.apply_field_mapping(source_data, transformation_rules)
            elif transformation_type == "aggregation":
                result = await self.apply_aggregation(source_data, transformation_rules)
            elif transformation_type == "filtering":
                result = await self.apply_filtering(source_data, transformation_rules)
            elif transformation_type == "enrichment":
                result = await self.apply_enrichment(source_data, transformation_rules)
            elif transformation_type == "cleaning":
                result = await self.apply_data_cleaning(source_data, transformation_rules)
            elif transformation_type == "validation":
                result = await self.apply_data_validation(source_data, transformation_rules)
            elif transformation_type == "normalization":
                result = await self.apply_data_normalization(source_data, transformation_rules)
            elif transformation_type == "deduplication":
                result = await self.apply_deduplication(source_data, transformation_rules)
            else:
                result = await self.apply_generic_transformation(source_data, transformation_rules)

            # Update transformation info
            transformation_info["status"] = "completed"
            transformation_info["completed_at"] = time.time()
            transformation_info["result"] = result
            transformation_info["records_transformed"] = result.get("record_count", 0)

            # Update metrics
            self._update_transformation_metrics(transformation_info)

            # Remove from active transformations
            self.active_transformations.finish_operation(transformation_id)

            return {
                "transformation_id": transformation_id,
                "status": "completed",
                "transformation_type": transformation_type,
                "records_transformed": transformation_info["records_transformed"],
                "transformation_time": transformation_info["completed_at"] - transformation_info["started_at"],
                "result": result,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Data transformation failed: {e}", exc_info=e)

            self.active_transformations.update_operation(transformation_id, {
                "status": "failed",
                "error": str(e)
            })

            self.transformation_metrics["failed_transformations"] += 1
            raise DataProcessingException(f"Data transformation failed: {e}") from e

    async def apply_field_mapping(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply field mapping transformations."""
        if self.logger:
            self.logger.info(f"Applying field mapping with {len(rules)} rules")

        try:
            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format
                    return await self._apply_field_mapping_columnar(records, rules)
                else:
                    # Row-based format
                    return await self._apply_field_mapping_rows(records, rules)
            elif isinstance(source_data, list):
                # Direct list of records
                return await self._apply_field_mapping_rows(source_data, rules)
            else:
                # Single record
                mapped_record = self._map_single_record(source_data, rules)
                return {
                    "transformation_type": "field_mapping",
                    "rules_applied": len(rules),
                    "record_count": 1,
                    "status": "completed",
                    "data": [mapped_record],
                }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Field mapping failed: {e}")
            raise DataProcessingException(f"Field mapping transformation failed: {e}") from e

    async def _apply_field_mapping_columnar(self, data: Dict[str, List], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply field mapping to columnar data format."""
        mapped_data = {}
        record_count = len(list(data.values())[0]) if data else 0

        for rule in rules:
            source_field = rule.get("source_field")
            target_field = rule.get("target_field")
            transformation = rule.get("transformation")
            default_value = rule.get("default_value")

            if source_field in data:
                if transformation:
                    mapped_data[target_field] = [
                        self._apply_field_transformation(value, transformation)
                        for value in data[source_field]
                    ]
                else:
                    mapped_data[target_field] = data[source_field][:]
            elif default_value is not None:
                mapped_data[target_field] = [default_value] * record_count

        return {
            "transformation_type": "field_mapping",
            "rules_applied": len(rules),
            "record_count": record_count,
            "status": "completed",
            "data": mapped_data,
        }

    async def _apply_field_mapping_rows(self, records: List[Dict], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply field mapping to row-based data format."""
        mapped_records = []

        for record in records:
            mapped_record = self._map_single_record(record, rules)
            mapped_records.append(mapped_record)

        return {
            "transformation_type": "field_mapping",
            "rules_applied": len(rules),
            "record_count": len(mapped_records),
            "status": "completed",
            "data": mapped_records,
        }

    def _map_single_record(self, record: Dict, rules: List[Dict[str, Any]]) -> Dict:
        """Map a single record according to field mapping rules."""
        mapped_record = {}

        for rule in rules:
            source_field = rule.get("source_field")
            target_field = rule.get("target_field")
            transformation = rule.get("transformation")
            default_value = rule.get("default_value")

            if source_field in record:
                value = record[source_field]
                if transformation:
                    value = self._apply_field_transformation(value, transformation)
                mapped_record[target_field] = value
            elif default_value is not None:
                mapped_record[target_field] = default_value

        return mapped_record

    def _apply_field_transformation(self, value: Any, transformation: Dict[str, Any]) -> Any:
        """Apply a specific transformation to a field value."""
        transform_type = transformation.get("type")

        if transform_type == "uppercase":
            return str(value).upper() if value is not None else value
        elif transform_type == "lowercase":
            return str(value).lower() if value is not None else value
        elif transform_type == "trim":
            return str(value).strip() if value is not None else value
        elif transform_type == "date_format":
            # Simplified date formatting - in practice would use proper date parsing
            return str(value) if value is not None else value
        elif transform_type == "numeric":
            try:
                return float(value) if value is not None else value
            except (ValueError, TypeError):
                return value
        elif transform_type == "regex_replace":
            pattern = transformation.get("pattern")
            replacement = transformation.get("replacement", "")
            if pattern and value is not None:
                return re.sub(pattern, replacement, str(value))
            return value
        else:
            return value

    async def apply_aggregation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply aggregation transformations."""
        if self.logger:
            self.logger.info(f"Applying aggregation with {len(rules)} rules")

        try:
            # Parse aggregation rules
            group_by_fields = []
            aggregation_functions = {}

            for rule in rules:
                if rule.get("type") == "group_by":
                    group_by_fields.extend(rule.get("fields", []))
                elif rule.get("type") == "aggregate":
                    field = rule.get("field")
                    function = rule.get("function")
                    alias = rule.get("alias", f"{function}_{field}")
                    aggregation_functions[alias] = {"field": field, "function": function}

            # Apply aggregation based on data format
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format
                    aggregated_data = self._aggregate_columnar_data(records, group_by_fields, aggregation_functions)
                else:
                    # Row-based format
                    aggregated_data = self._aggregate_row_data(records, group_by_fields, aggregation_functions)
            elif isinstance(source_data, list):
                aggregated_data = self._aggregate_row_data(source_data, group_by_fields, aggregation_functions)
            else:
                # Single record - no meaningful aggregation
                aggregated_data = [source_data] if source_data else []

            return {
                "transformation_type": "aggregation",
                "rules_applied": len(rules),
                "record_count": len(aggregated_data) if isinstance(aggregated_data, list) else len(list(aggregated_data.values())[0]) if aggregated_data else 0,
                "status": "completed",
                "data": aggregated_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Aggregation failed: {e}")
            raise DataProcessingException(f"Aggregation transformation failed: {e}") from e

    def _aggregate_row_data(self, records: List[Dict], group_by_fields: List[str], aggregation_functions: Dict[str, Dict]) -> List[Dict]:
        """Aggregate row-based data."""
        if not group_by_fields:
            # No grouping - aggregate entire dataset
            return [self._calculate_aggregations(records, aggregation_functions)]

        # Group records by specified fields
        groups = {}
        for record in records:
            key = tuple(record.get(field) for field in group_by_fields)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        # Calculate aggregations for each group
        aggregated_records = []
        for key, group_records in groups.items():
            aggregated_record = {}

            # Add group by fields
            for i, field in enumerate(group_by_fields):
                aggregated_record[field] = key[i]

            # Calculate aggregations
            aggregations = self._calculate_aggregations(group_records, aggregation_functions)
            aggregated_record.update(aggregations)
            aggregated_records.append(aggregated_record)

        return aggregated_records

    def _aggregate_columnar_data(self, data: Dict[str, List], group_by_fields: List[str], aggregation_functions: Dict[str, Dict]) -> Dict[str, List]:
        """Aggregate columnar data format."""
        if not data:
            return {}

        record_count = len(list(data.values())[0])

        if not group_by_fields:
            # No grouping - aggregate entire columns
            aggregated_data = {}
            for alias, agg_config in aggregation_functions.items():
                field = agg_config["field"]
                function = agg_config["function"]
                if field in data:
                    aggregated_data[alias] = [self._apply_aggregation_function(data[field], function)]
            return aggregated_data

        # Convert to row format for grouping, then back to columnar
        records = []
        for i in range(record_count):
            record = {col: values[i] for col, values in data.items()}
            records.append(record)

        aggregated_records = self._aggregate_row_data(records, group_by_fields, aggregation_functions)

        # Convert back to columnar format
        if not aggregated_records:
            return {}

        columnar_result = {}
        for field in aggregated_records[0].keys():
            columnar_result[field] = [record[field] for record in aggregated_records]

        return columnar_result

    def _calculate_aggregations(self, records: List[Dict], aggregation_functions: Dict[str, Dict]) -> Dict[str, Any]:
        """Calculate aggregation functions for a group of records."""
        aggregations = {}

        for alias, agg_config in aggregation_functions.items():
            field = agg_config["field"]
            function = agg_config["function"]

            # Extract values for the field
            values = [record.get(field) for record in records if record.get(field) is not None]

            aggregations[alias] = self._apply_aggregation_function(values, function)

        return aggregations

    def _apply_aggregation_function(self, values: List[Any], function: str) -> Any:
        """Apply a specific aggregation function to a list of values."""
        if not values:
            return None

        try:
            if function == "count":
                return len(values)
            elif function == "sum":
                numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                return sum(numeric_values)
            elif function == "avg" or function == "mean":
                numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                return statistics.mean(numeric_values) if numeric_values else None
            elif function == "min":
                return min(values)
            elif function == "max":
                return max(values)
            elif function == "median":
                numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                return statistics.median(numeric_values) if numeric_values else None
            elif function == "std" or function == "stddev":
                numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                return statistics.stdev(numeric_values) if len(numeric_values) > 1 else None
            elif function == "distinct_count":
                return len(set(values))
            elif function == "concat":
                return ", ".join(str(v) for v in values)
            else:
                return len(values)  # Default to count
        except Exception:
            return None

    async def apply_filtering(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply filtering transformations."""
        if self.logger:
            self.logger.info(f"Applying filtering with {len(rules)} rules")

        try:
            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format
                    filtered_data = self._filter_columnar_data(records, rules)
                else:
                    # Row-based format
                    filtered_data = self._filter_row_data(records, rules)
            elif isinstance(source_data, list):
                filtered_data = self._filter_row_data(source_data, rules)
            else:
                # Single record
                if self._record_passes_filters(source_data, rules):
                    filtered_data = [source_data]
                else:
                    filtered_data = []

            record_count = len(filtered_data) if isinstance(filtered_data, list) else len(list(filtered_data.values())[0]) if filtered_data else 0

            return {
                "transformation_type": "filtering",
                "rules_applied": len(rules),
                "record_count": record_count,
                "status": "completed",
                "data": filtered_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Filtering failed: {e}")
            raise DataProcessingException(f"Filtering transformation failed: {e}") from e

    def _filter_row_data(self, records: List[Dict], rules: List[Dict[str, Any]]) -> List[Dict]:
        """Filter row-based data."""
        filtered_records = []

        for record in records:
            if self._record_passes_filters(record, rules):
                filtered_records.append(record)

        return filtered_records

    def _filter_columnar_data(self, data: Dict[str, List], rules: List[Dict[str, Any]]) -> Dict[str, List]:
        """Filter columnar data format."""
        if not data:
            return {}

        record_count = len(list(data.values())[0])
        keep_indices = []

        for i in range(record_count):
            record = {col: values[i] for col, values in data.items()}
            if self._record_passes_filters(record, rules):
                keep_indices.append(i)

        # Filter each column
        filtered_data = {}
        for col, values in data.items():
            filtered_data[col] = [values[i] for i in keep_indices]

        return filtered_data

    def _record_passes_filters(self, record: Dict, rules: List[Dict[str, Any]]) -> bool:
        """Check if a record passes all filter rules."""
        for rule in rules:
            if not self._evaluate_filter_rule(record, rule):
                return False
        return True

    def _evaluate_filter_rule(self, record: Dict, rule: Dict[str, Any]) -> bool:
        """Evaluate a single filter rule against a record."""
        field = rule.get("field")
        operator = rule.get("operator")
        value = rule.get("value")

        if field not in record:
            return rule.get("null_handling", "exclude") == "include"

        record_value = record[field]

        if record_value is None:
            return rule.get("null_handling", "exclude") == "include"

        try:
            if operator == "equals" or operator == "==":
                return record_value == value
            elif operator == "not_equals" or operator == "!=":
                return record_value != value
            elif operator == "greater_than" or operator == ">":
                return float(record_value) > float(value)
            elif operator == "greater_equal" or operator == ">=":
                return float(record_value) >= float(value)
            elif operator == "less_than" or operator == "<":
                return float(record_value) < float(value)
            elif operator == "less_equal" or operator == "<=":
                return float(record_value) <= float(value)
            elif operator == "in":
                return record_value in value if isinstance(value, (list, tuple)) else False
            elif operator == "not_in":
                return record_value not in value if isinstance(value, (list, tuple)) else True
            elif operator == "contains":
                return str(value) in str(record_value)
            elif operator == "not_contains":
                return str(value) not in str(record_value)
            elif operator == "starts_with":
                return str(record_value).startswith(str(value))
            elif operator == "ends_with":
                return str(record_value).endswith(str(value))
            elif operator == "regex":
                return bool(re.search(str(value), str(record_value)))
            elif operator == "is_null":
                return record_value is None
            elif operator == "is_not_null":
                return record_value is not None
            else:
                return True  # Unknown operator - don't filter
        except (ValueError, TypeError):
            return False

    async def apply_enrichment(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data enrichment transformations."""
        if self.logger:
            self.logger.info(f"Applying enrichment with {len(rules)} rules")

        try:
            enriched_data = source_data
            fields_enriched = 0

            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format
                    enriched_data, fields_enriched = self._enrich_columnar_data(records, rules)
                else:
                    # Row-based format
                    enriched_data, fields_enriched = self._enrich_row_data(records, rules)
            elif isinstance(source_data, list):
                enriched_data, fields_enriched = self._enrich_row_data(source_data, rules)
            else:
                # Single record
                enriched_record = self._enrich_single_record(source_data, rules)
                enriched_data = enriched_record
                fields_enriched = len([rule for rule in rules if rule.get("target_field")])

            record_count = 1
            if isinstance(enriched_data, list):
                record_count = len(enriched_data)
            elif isinstance(enriched_data, dict) and any(isinstance(v, list) for v in enriched_data.values()):
                record_count = len(list(enriched_data.values())[0]) if enriched_data else 0

            return {
                "transformation_type": "enrichment",
                "rules_applied": len(rules),
                "record_count": record_count,
                "fields_enriched": fields_enriched,
                "status": "completed",
                "data": enriched_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Enrichment failed: {e}")
            raise DataProcessingException(f"Enrichment transformation failed: {e}") from e

    def _enrich_row_data(self, records: List[Dict], rules: List[Dict[str, Any]]) -> tuple[List[Dict], int]:
        """Enrich row-based data."""
        enriched_records = []
        fields_enriched = 0

        for record in records:
            enriched_record = self._enrich_single_record(record, rules)
            enriched_records.append(enriched_record)

        # Count unique fields enriched
        enriched_fields = set()
        for rule in rules:
            if rule.get("target_field"):
                enriched_fields.add(rule["target_field"])
        fields_enriched = len(enriched_fields)

        return enriched_records, fields_enriched

    def _enrich_columnar_data(self, data: Dict[str, List], rules: List[Dict[str, Any]]) -> tuple[Dict[str, List], int]:
        """Enrich columnar data format."""
        enriched_data = data.copy()
        record_count = len(list(data.values())[0]) if data else 0
        fields_enriched = 0

        for rule in rules:
            enrichment_type = rule.get("type")
            target_field = rule.get("target_field")
            source_fields = rule.get("source_fields", [])

            if not target_field:
                continue

            new_column = []

            for i in range(record_count):
                # Create record for enrichment
                record = {col: values[i] for col, values in data.items()}
                enriched_record = self._enrich_single_record(record, [rule])
                new_column.append(enriched_record.get(target_field))

            enriched_data[target_field] = new_column
            fields_enriched += 1

        return enriched_data, fields_enriched

    def _enrich_single_record(self, record: Dict, rules: List[Dict[str, Any]]) -> Dict:
        """Enrich a single record according to enrichment rules."""
        enriched_record = record.copy()

        for rule in rules:
            enrichment_type = rule.get("type")
            target_field = rule.get("target_field")

            if not target_field:
                continue

            if enrichment_type == "calculated_field":
                enriched_record[target_field] = self._calculate_field(record, rule)
            elif enrichment_type == "lookup":
                enriched_record[target_field] = self._lookup_value(record, rule)
            elif enrichment_type == "concatenation":
                enriched_record[target_field] = self._concatenate_fields(record, rule)
            elif enrichment_type == "date_derived":
                enriched_record[target_field] = self._derive_date_field(record, rule)
            elif enrichment_type == "conditional":
                enriched_record[target_field] = self._apply_conditional_logic(record, rule)
            elif enrichment_type == "constant":
                enriched_record[target_field] = rule.get("value")

        return enriched_record

    def _calculate_field(self, record: Dict, rule: Dict[str, Any]) -> Any:
        """Calculate a field based on other fields and an expression."""
        expression = rule.get("expression", "")
        source_fields = rule.get("source_fields", [])

        # Simple expression evaluation - in practice would use safer evaluation
        try:
            for field in source_fields:
                if field in record and record[field] is not None:
                    # Replace field names with values in expression
                    expression = expression.replace(f"{{{field}}}", str(record[field]))

            # Basic arithmetic evaluation (simplified and unsafe - for demo only)
            if all(c in "0123456789+-*/.() " for c in expression):
                return eval(expression)
            else:
                return expression
        except Exception:
            return None

    def _lookup_value(self, record: Dict, rule: Dict[str, Any]) -> Any:
        """Lookup value from a reference table/dictionary."""
        lookup_field = rule.get("lookup_field")
        lookup_table = rule.get("lookup_table", {})
        default_value = rule.get("default_value")

        if lookup_field in record:
            lookup_key = record[lookup_field]
            return lookup_table.get(lookup_key, default_value)

        return default_value

    def _concatenate_fields(self, record: Dict, rule: Dict[str, Any]) -> str:
        """Concatenate multiple fields with a separator."""
        source_fields = rule.get("source_fields", [])
        separator = rule.get("separator", " ")

        values = []
        for field in source_fields:
            if field in record and record[field] is not None:
                values.append(str(record[field]))

        return separator.join(values)

    def _derive_date_field(self, record: Dict, rule: Dict[str, Any]) -> Any:
        """Derive date-related fields from a date column."""
        source_field = rule.get("source_field")
        date_part = rule.get("date_part", "year")  # year, month, day, etc.

        if source_field not in record or record[source_field] is None:
            return None

        # Simplified date parsing - in practice would use proper date libraries
        date_str = str(record[source_field])

        try:
            # Basic date pattern matching
            if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
                date_parts = date_str.split('-')
                if date_part == "year":
                    return int(date_parts[0])
                elif date_part == "month":
                    return int(date_parts[1])
                elif date_part == "day":
                    return int(date_parts[2])
        except (ValueError, IndexError):
            pass

        return None

    def _apply_conditional_logic(self, record: Dict, rule: Dict[str, Any]) -> Any:
        """Apply conditional logic to determine field value."""
        conditions = rule.get("conditions", [])
        default_value = rule.get("default_value")

        for condition in conditions:
            if self._evaluate_condition(record, condition):
                return condition.get("value")

        return default_value

    def _evaluate_condition(self, record: Dict, condition: Dict[str, Any]) -> bool:
        """Evaluate a conditional expression."""
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")

        if field not in record:
            return False

        record_value = record[field]

        # Use the same evaluation logic as filtering
        mock_rule = {"field": field, "operator": operator, "value": value}
        return self._evaluate_filter_rule(record, mock_rule)

    async def apply_data_cleaning(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data cleaning transformations."""
        if self.logger:
            self.logger.info(f"Applying data cleaning with {len(rules)} rules")

        try:
            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format
                    cleaned_data = self._clean_columnar_data(records, rules)
                else:
                    # Row-based format
                    cleaned_data = self._clean_row_data(records, rules)
            elif isinstance(source_data, list):
                cleaned_data = self._clean_row_data(source_data, rules)
            else:
                # Single record
                cleaned_data = [self._clean_single_record(source_data, rules)]

            record_count = len(cleaned_data) if isinstance(cleaned_data, list) else len(list(cleaned_data.values())[0]) if cleaned_data else 0

            return {
                "transformation_type": "cleaning",
                "rules_applied": len(rules),
                "record_count": record_count,
                "status": "completed",
                "data": cleaned_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Data cleaning failed: {e}")
            raise DataProcessingException(f"Data cleaning transformation failed: {e}") from e

    def _clean_row_data(self, records: List[Dict], rules: List[Dict[str, Any]]) -> List[Dict]:
        """Clean row-based data."""
        cleaned_records = []

        for record in records:
            cleaned_record = self._clean_single_record(record, rules)
            cleaned_records.append(cleaned_record)

        return cleaned_records

    def _clean_columnar_data(self, data: Dict[str, List], rules: List[Dict[str, Any]]) -> Dict[str, List]:
        """Clean columnar data format."""
        cleaned_data = {}
        record_count = len(list(data.values())[0]) if data else 0

        for col, values in data.items():
            cleaned_column = []
            for i, value in enumerate(values):
                record = {col: value}
                cleaned_record = self._clean_single_record(record, rules)
                cleaned_column.append(cleaned_record.get(col, value))
            cleaned_data[col] = cleaned_column

        return cleaned_data

    def _clean_single_record(self, record: Dict, rules: List[Dict[str, Any]]) -> Dict:
        """Clean a single record according to cleaning rules."""
        cleaned_record = record.copy()

        for rule in rules:
            cleaning_type = rule.get("type")
            fields = rule.get("fields", [])

            # Apply to all fields if none specified
            if not fields:
                fields = list(cleaned_record.keys())

            for field in fields:
                if field in cleaned_record:
                    cleaned_record[field] = self._apply_cleaning_rule(cleaned_record[field], cleaning_type, rule)

        return cleaned_record

    def _apply_cleaning_rule(self, value: Any, cleaning_type: str, rule: Dict[str, Any]) -> Any:
        """Apply a specific cleaning rule to a value."""
        if value is None:
            return value

        try:
            if cleaning_type == "trim_whitespace":
                return str(value).strip()
            elif cleaning_type == "remove_duplicates":
                # For string values, remove duplicate characters/words
                if isinstance(value, str):
                    words = value.split()
                    return " ".join(dict.fromkeys(words))  # Preserve order
                return value
            elif cleaning_type == "standardize_case":
                case_type = rule.get("case", "lower")
                if case_type == "upper":
                    return str(value).upper()
                elif case_type == "lower":
                    return str(value).lower()
                elif case_type == "title":
                    return str(value).title()
                return value
            elif cleaning_type == "remove_special_chars":
                pattern = rule.get("pattern", r"[^a-zA-Z0-9\s]")
                replacement = rule.get("replacement", "")
                return re.sub(pattern, replacement, str(value))
            elif cleaning_type == "normalize_numbers":
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return value
            elif cleaning_type == "fill_nulls":
                if value is None or str(value).lower() in ['null', 'none', 'n/a', 'na', '']:
                    return rule.get("fill_value", "")
                return value
            elif cleaning_type == "validate_format":
                pattern = rule.get("pattern")
                if pattern and not re.match(pattern, str(value)):
                    return rule.get("default_value", value)
                return value
            else:
                return value
        except Exception:
            return value

    async def apply_data_validation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data validation transformations."""
        if self.logger:
            self.logger.info(f"Applying data validation with {len(rules)} rules")

        try:
            validation_results = {
                "total_records": 0,
                "valid_records": 0,
                "invalid_records": 0,
                "validation_errors": [],
                "error_summary": {},
            }

            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format - convert to rows for validation
                    record_count = len(list(records.values())[0]) if records else 0
                    row_records = []
                    for i in range(record_count):
                        row_records.append({col: values[i] for col, values in records.items()})
                    validation_results = self._validate_records(row_records, rules)
                else:
                    # Row-based format
                    validation_results = self._validate_records(records, rules)
            elif isinstance(source_data, list):
                validation_results = self._validate_records(source_data, rules)
            else:
                # Single record
                validation_results = self._validate_records([source_data], rules)

            return {
                "transformation_type": "validation",
                "rules_applied": len(rules),
                "record_count": validation_results["total_records"],
                "status": "completed",
                "validation_results": validation_results,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Data validation failed: {e}")
            raise DataProcessingException(f"Data validation transformation failed: {e}") from e

    def _validate_records(self, records: List[Dict], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate a list of records against validation rules."""
        validation_results = {
            "total_records": len(records),
            "valid_records": 0,
            "invalid_records": 0,
            "validation_errors": [],
            "error_summary": {},
        }

        for i, record in enumerate(records):
            record_errors = []

            for rule in rules:
                error = self._validate_single_rule(record, rule, i)
                if error:
                    record_errors.append(error)

            if record_errors:
                validation_results["invalid_records"] += 1
                validation_results["validation_errors"].extend(record_errors)

                # Update error summary
                for error in record_errors:
                    error_type = error["rule_type"]
                    validation_results["error_summary"][error_type] = validation_results["error_summary"].get(error_type, 0) + 1
            else:
                validation_results["valid_records"] += 1

        return validation_results

    def _validate_single_rule(self, record: Dict, rule: Dict[str, Any], record_index: int) -> Optional[Dict[str, Any]]:
        """Validate a single rule against a record."""
        rule_type = rule.get("type")
        field = rule.get("field")

        if field and field not in record:
            return {
                "record_index": record_index,
                "field": field,
                "rule_type": rule_type,
                "error": "Field not found",
                "value": None,
            }

        value = record.get(field) if field else None

        try:
            if rule_type == "required" and (value is None or value == ""):
                return {
                    "record_index": record_index,
                    "field": field,
                    "rule_type": rule_type,
                    "error": "Required field is missing or empty",
                    "value": value,
                }
            elif rule_type == "data_type":
                expected_type = rule.get("expected_type")
                if not self._validate_data_type(value, expected_type):
                    return {
                        "record_index": record_index,
                        "field": field,
                        "rule_type": rule_type,
                        "error": f"Invalid data type, expected {expected_type}",
                        "value": value,
                    }
            elif rule_type == "range":
                min_val = rule.get("min_value")
                max_val = rule.get("max_value")
                if value is not None and not self._validate_range(value, min_val, max_val):
                    return {
                        "record_index": record_index,
                        "field": field,
                        "rule_type": rule_type,
                        "error": f"Value out of range [{min_val}, {max_val}]",
                        "value": value,
                    }
            elif rule_type == "pattern":
                pattern = rule.get("pattern")
                if value is not None and pattern and not re.match(pattern, str(value)):
                    return {
                        "record_index": record_index,
                        "field": field,
                        "rule_type": rule_type,
                        "error": f"Value does not match pattern: {pattern}",
                        "value": value,
                    }
            elif rule_type == "enum":
                allowed_values = rule.get("allowed_values", [])
                if value is not None and value not in allowed_values:
                    return {
                        "record_index": record_index,
                        "field": field,
                        "rule_type": rule_type,
                        "error": f"Value not in allowed list: {allowed_values}",
                        "value": value,
                    }
        except Exception as e:
            return {
                "record_index": record_index,
                "field": field,
                "rule_type": rule_type,
                "error": f"Validation error: {str(e)}",
                "value": value,
            }

        return None

    def _validate_data_type(self, value: Any, expected_type: str) -> bool:
        """Validate that a value matches the expected data type."""
        if value is None:
            return True  # Null values are handled by required validation

        try:
            if expected_type == "string":
                return isinstance(value, str)
            elif expected_type == "integer":
                return isinstance(value, int) or (isinstance(value, str) and value.isdigit())
            elif expected_type == "float":
                float(value)
                return True
            elif expected_type == "boolean":
                return isinstance(value, bool) or str(value).lower() in ['true', 'false', '1', '0']
            elif expected_type == "date":
                # Simple date validation
                return bool(re.match(r'^\d{4}-\d{2}-\d{2}', str(value)))
            else:
                return True
        except (ValueError, TypeError):
            return False

    def _validate_range(self, value: Any, min_val: Any, max_val: Any) -> bool:
        """Validate that a value is within the specified range."""
        try:
            numeric_value = float(value)
            if min_val is not None and numeric_value < float(min_val):
                return False
            if max_val is not None and numeric_value > float(max_val):
                return False
            return True
        except (ValueError, TypeError):
            return False

    async def apply_data_normalization(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data normalization transformations."""
        if self.logger:
            self.logger.info(f"Applying data normalization with {len(rules)} rules")

        try:
            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format
                    normalized_data = self._normalize_columnar_data(records, rules)
                else:
                    # Row-based format
                    normalized_data = self._normalize_row_data(records, rules)
            elif isinstance(source_data, list):
                normalized_data = self._normalize_row_data(source_data, rules)
            else:
                # Single record
                normalized_data = [self._normalize_single_record(source_data, rules)]

            record_count = len(normalized_data) if isinstance(normalized_data, list) else len(list(normalized_data.values())[0]) if normalized_data else 0

            return {
                "transformation_type": "normalization",
                "rules_applied": len(rules),
                "record_count": record_count,
                "status": "completed",
                "data": normalized_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Data normalization failed: {e}")
            raise DataProcessingException(f"Data normalization transformation failed: {e}") from e

    def _normalize_row_data(self, records: List[Dict], rules: List[Dict[str, Any]]) -> List[Dict]:
        """Normalize row-based data."""
        # First pass: collect statistics for normalization
        field_stats = self._calculate_normalization_stats(records, rules)

        # Second pass: apply normalization
        normalized_records = []
        for record in records:
            normalized_record = self._normalize_single_record(record, rules, field_stats)
            normalized_records.append(normalized_record)

        return normalized_records

    def _normalize_columnar_data(self, data: Dict[str, List], rules: List[Dict[str, Any]]) -> Dict[str, List]:
        """Normalize columnar data format."""
        # Calculate stats for each field
        field_stats = {}
        for rule in rules:
            field = rule.get("field")
            if field in data:
                field_stats[field] = self._calculate_column_stats(data[field], rule)

        # Apply normalization
        normalized_data = {}
        for col, values in data.items():
            normalized_values = []
            for value in values:
                normalized_value = self._normalize_single_value(value, col, rules, field_stats)
                normalized_values.append(normalized_value)
            normalized_data[col] = normalized_values

        return normalized_data

    def _normalize_single_record(self, record: Dict, rules: List[Dict[str, Any]], field_stats: Dict = None) -> Dict:
        """Normalize a single record according to normalization rules."""
        normalized_record = record.copy()

        for rule in rules:
            field = rule.get("field")
            if field in normalized_record:
                normalized_record[field] = self._normalize_single_value(
                    normalized_record[field], field, [rule], field_stats or {}
                )

        return normalized_record

    def _normalize_single_value(self, value: Any, field: str, rules: List[Dict[str, Any]], field_stats: Dict) -> Any:
        """Normalize a single value according to rules."""
        if value is None:
            return value

        for rule in rules:
            if rule.get("field") == field:
                normalization_type = rule.get("type")

                try:
                    if normalization_type == "min_max":
                        # Min-max scaling: (x - min) / (max - min)
                        stats = field_stats.get(field, {})
                        min_val = stats.get("min", 0)
                        max_val = stats.get("max", 1)
                        if max_val != min_val:
                            return (float(value) - min_val) / (max_val - min_val)
                        return 0
                    elif normalization_type == "z_score":
                        # Z-score normalization: (x - mean) / std
                        stats = field_stats.get(field, {})
                        mean_val = stats.get("mean", 0)
                        std_val = stats.get("std", 1)
                        if std_val != 0:
                            return (float(value) - mean_val) / std_val
                        return 0
                    elif normalization_type == "decimal_scaling":
                        # Decimal scaling: x / 10^d where d is number of digits
                        max_val = field_stats.get(field, {}).get("max", 1)
                        decimal_places = len(str(int(abs(max_val)))) if max_val != 0 else 1
                        return float(value) / (10 ** decimal_places)
                    elif normalization_type == "unit_vector":
                        # Unit vector scaling: x / ||x||
                        magnitude = field_stats.get(field, {}).get("magnitude", 1)
                        return float(value) / magnitude if magnitude != 0 else 0
                except (ValueError, TypeError):
                    pass

        return value

    def _calculate_normalization_stats(self, records: List[Dict], rules: List[Dict[str, Any]]) -> Dict[str, Dict]:
        """Calculate statistics needed for normalization."""
        field_stats = {}

        for rule in rules:
            field = rule.get("field")
            if not field:
                continue

            # Collect numeric values for the field
            values = []
            for record in records:
                if field in record and record[field] is not None:
                    try:
                        values.append(float(record[field]))
                    except (ValueError, TypeError):
                        pass

            if values:
                field_stats[field] = {
                    "min": min(values),
                    "max": max(values),
                    "mean": statistics.mean(values),
                    "std": statistics.stdev(values) if len(values) > 1 else 1,
                    "magnitude": (sum(v**2 for v in values) ** 0.5),
                }

        return field_stats

    def _calculate_column_stats(self, values: List[Any], rule: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate statistics for a single column."""
        numeric_values = []
        for value in values:
            if value is not None:
                try:
                    numeric_values.append(float(value))
                except (ValueError, TypeError):
                    pass

        if not numeric_values:
            return {"min": 0, "max": 1, "mean": 0, "std": 1, "magnitude": 1}

        return {
            "min": min(numeric_values),
            "max": max(numeric_values),
            "mean": statistics.mean(numeric_values),
            "std": statistics.stdev(numeric_values) if len(numeric_values) > 1 else 1,
            "magnitude": (sum(v**2 for v in numeric_values) ** 0.5),
        }

    async def apply_deduplication(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply deduplication transformations."""
        if self.logger:
            self.logger.info(f"Applying deduplication with {len(rules)} rules")

        try:
            # Handle different source data formats
            if isinstance(source_data, dict) and "data" in source_data:
                records = source_data["data"]
                if isinstance(records, dict):
                    # Column-based format - convert to rows, deduplicate, convert back
                    record_count = len(list(records.values())[0]) if records else 0
                    row_records = []
                    for i in range(record_count):
                        row_records.append({col: values[i] for col, values in records.items()})

                    deduplicated_records = self._deduplicate_records(row_records, rules)

                    # Convert back to columnar format
                    if deduplicated_records:
                        deduplicated_data = {}
                        for field in deduplicated_records[0].keys():
                            deduplicated_data[field] = [record[field] for record in deduplicated_records]
                    else:
                        deduplicated_data = {col: [] for col in records.keys()}
                else:
                    # Row-based format
                    deduplicated_data = self._deduplicate_records(records, rules)
            elif isinstance(source_data, list):
                deduplicated_data = self._deduplicate_records(source_data, rules)
            else:
                # Single record - no deduplication needed
                deduplicated_data = [source_data]

            original_count = 0
            if isinstance(source_data, dict) and "data" in source_data:
                if isinstance(source_data["data"], list):
                    original_count = len(source_data["data"])
                elif isinstance(source_data["data"], dict):
                    original_count = len(list(source_data["data"].values())[0]) if source_data["data"] else 0
            elif isinstance(source_data, list):
                original_count = len(source_data)
            else:
                original_count = 1

            deduplicated_count = len(deduplicated_data) if isinstance(deduplicated_data, list) else len(list(deduplicated_data.values())[0]) if deduplicated_data else 0
            duplicates_removed = original_count - deduplicated_count

            return {
                "transformation_type": "deduplication",
                "rules_applied": len(rules),
                "original_record_count": original_count,
                "record_count": deduplicated_count,
                "duplicates_removed": duplicates_removed,
                "status": "completed",
                "data": deduplicated_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Deduplication failed: {e}")
            raise DataProcessingException(f"Deduplication transformation failed: {e}") from e

    def _deduplicate_records(self, records: List[Dict], rules: List[Dict[str, Any]]) -> List[Dict]:
        """Deduplicate records based on specified rules."""
        if not records or not rules:
            return records

        deduplicated_records = []
        seen_keys = set()

        for record in records:
            # Generate a deduplication key based on rules
            dedup_key = self._generate_dedup_key(record, rules)

            if dedup_key not in seen_keys:
                seen_keys.add(dedup_key)
                deduplicated_records.append(record)

        return deduplicated_records

    def _generate_dedup_key(self, record: Dict, rules: List[Dict[str, Any]]) -> str:
        """Generate a deduplication key for a record based on rules."""
        key_parts = []

        for rule in rules:
            dedup_type = rule.get("type", "exact")
            fields = rule.get("fields", [])

            if dedup_type == "exact":
                # Exact match on specified fields
                for field in fields:
                    value = record.get(field, "")
                    key_parts.append(f"{field}:{value}")
            elif dedup_type == "fuzzy":
                # Fuzzy matching (simplified - normalize case and whitespace)
                for field in fields:
                    value = str(record.get(field, "")).lower().strip()
                    # Remove extra whitespace
                    value = re.sub(r'\s+', ' ', value)
                    key_parts.append(f"{field}:{value}")
            elif dedup_type == "numeric_threshold":
                # Numeric matching within threshold
                threshold = rule.get("threshold", 0.01)
                for field in fields:
                    try:
                        value = float(record.get(field, 0))
                        # Round to threshold precision
                        rounded_value = round(value / threshold) * threshold
                        key_parts.append(f"{field}:{rounded_value}")
                    except (ValueError, TypeError):
                        key_parts.append(f"{field}:{record.get(field, '')}")

        return "|".join(key_parts)

    async def apply_generic_transformation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply generic transformations."""
        if self.logger:
            self.logger.info(f"Applying generic transformation with {len(rules)} rules")

        try:
            # Generic transformation can apply multiple transformation types
            result_data = source_data

            for rule in rules:
                transformation_type = rule.get("type")

                if transformation_type == "field_mapping":
                    result = await self.apply_field_mapping(result_data, [rule])
                    result_data = result.get("data", result_data)
                elif transformation_type == "filtering":
                    result = await self.apply_filtering(result_data, [rule])
                    result_data = result.get("data", result_data)
                elif transformation_type == "cleaning":
                    result = await self.apply_data_cleaning(result_data, [rule])
                    result_data = result.get("data", result_data)
                elif transformation_type == "enrichment":
                    result = await self.apply_enrichment(result_data, [rule])
                    result_data = result.get("data", result_data)

            record_count = 1
            if isinstance(result_data, list):
                record_count = len(result_data)
            elif isinstance(result_data, dict) and any(isinstance(v, list) for v in result_data.values()):
                record_count = len(list(result_data.values())[0]) if result_data else 0

            return {
                "transformation_type": "generic",
                "rules_applied": len(rules),
                "record_count": record_count,
                "status": "completed",
                "data": result_data,
            }

        except Exception as e:
            if self.logger:
                self.logger.error(f"Generic transformation failed: {e}")
            raise DataProcessingException(f"Generic transformation failed: {e}") from e

    def _update_transformation_metrics(self, transformation_info: Dict[str, Any]) -> None:
        """Update transformation performance metrics."""
        self.transformation_metrics["total_transformations"] += 1

        if transformation_info.get("status") == "completed":
            self.transformation_metrics["successful_transformations"] += 1
            records_transformed = transformation_info.get("records_transformed", 0)
            self.transformation_metrics["total_records_transformed"] += records_transformed

            # Update processing time
            processing_time = (
                transformation_info.get("completed_at", 0) -
                transformation_info.get("started_at", 0)
            )
            self.transformation_metrics["total_transformation_time"] += processing_time
        else:
            self.transformation_metrics["failed_transformations"] += 1

    def get_transformation_metrics(self) -> Dict[str, Any]:
        """Get transformation performance metrics."""
        metrics = self.transformation_metrics.copy()

        # Calculate derived metrics
        if metrics["total_transformations"] > 0:
            metrics["success_rate"] = (
                metrics["successful_transformations"] / metrics["total_transformations"]
            )
        else:
            metrics["success_rate"] = 0.0

        if metrics["total_transformation_time"] > 0:
            metrics["average_throughput"] = (
                metrics["total_records_transformed"] / metrics["total_transformation_time"]
            )
        else:
            metrics["average_throughput"] = 0.0

        return metrics

    def get_active_transformations(self) -> Dict[str, Dict[str, Any]]:
        """Get information about currently active transformations."""
        return self.active_transformations.get_all_operations()
