"""Data transformation functionality for ETL operations."""

from __future__ import annotations

import re
from typing import Any, Dict, List


class DataTransformer:
    """Handles data transformation operations."""

    def __init__(self):
        """Initialize the data transformer."""
        pass

    async def apply_field_mapping(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply field mapping transformations."""
        try:
            if not isinstance(source_data, (list, dict)):
                return {
                    "status": "error",
                    "error_message": "Source data must be a list or dictionary",
                    "transformed_data": None
                }

            transformed_data = []

            # Convert single dict to list for uniform processing
            if isinstance(source_data, dict):
                source_data = [source_data]

            for record in source_data:
                if not isinstance(record, dict):
                    continue

                transformed_record = {}

                for rule in rules:
                    source_field = rule.get('source_field')
                    target_field = rule.get('target_field')
                    default_value = rule.get('default_value')

                    if source_field in record:
                        transformed_record[target_field] = record[source_field]
                    elif default_value is not None:
                        transformed_record[target_field] = default_value

                transformed_data.append(transformed_record)

            return {
                "status": "completed",
                "transformation_type": "field_mapping",
                "rules_applied": len(rules),
                "records_processed": len(transformed_data),
                "transformed_data": transformed_data
            }

        except Exception as e:
            return {
                "status": "error",
                "error_message": f"Field mapping error: {str(e)}",
                "transformed_data": None
            }

    async def apply_aggregation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply aggregation transformations."""
        try:
            if not isinstance(source_data, list):
                return {
                    "status": "error",
                    "error_message": "Source data must be a list for aggregation",
                    "transformed_data": None
                }

            result = {}

            for rule in rules:
                operation = rule.get('operation')  # sum, count, avg, min, max, group_by
                field = rule.get('field')
                group_by_field = rule.get('group_by')

                if operation == 'count':
                    result[f"{field}_count"] = len(source_data)
                elif operation in ['sum', 'avg', 'min', 'max'] and field:
                    values = []
                    for record in source_data:
                        if isinstance(record, dict) and field in record:
                            try:
                                value = float(record[field])
                                values.append(value)
                            except (ValueError, TypeError):
                                continue

                    if values:
                        if operation == 'sum':
                            result[f"{field}_sum"] = sum(values)
                        elif operation == 'avg':
                            result[f"{field}_avg"] = sum(values) / len(values)
                        elif operation == 'min':
                            result[f"{field}_min"] = min(values)
                        elif operation == 'max':
                            result[f"{field}_max"] = max(values)

                elif operation == 'group_by' and group_by_field:
                    groups = {}
                    for record in source_data:
                        if isinstance(record, dict) and group_by_field in record:
                            group_key = str(record[group_by_field])
                            if group_key not in groups:
                                groups[group_key] = []
                            groups[group_key].append(record)

                    result[f"grouped_by_{group_by_field}"] = groups

            return {
                "status": "completed",
                "transformation_type": "aggregation",
                "rules_applied": len(rules),
                "records_processed": len(source_data),
                "transformed_data": result
            }

        except Exception as e:
            return {
                "status": "error",
                "error_message": f"Aggregation error: {str(e)}",
                "transformed_data": None
            }

    async def apply_filtering(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply filtering transformations."""
        try:
            if not isinstance(source_data, list):
                return {
                    "status": "error",
                    "error_message": "Source data must be a list for filtering",
                    "transformed_data": None
                }

            filtered_data = source_data.copy()

            for rule in rules:
                field = rule.get('field')
                operator = rule.get('operator')  # eq, ne, gt, lt, gte, lte, contains, regex
                value = rule.get('value')

                new_filtered_data = []

                for record in filtered_data:
                    if not isinstance(record, dict) or field not in record:
                        continue

                    record_value = record[field]
                    include_record = False

                    try:
                        if operator == 'eq':
                            include_record = record_value == value
                        elif operator == 'ne':
                            include_record = record_value != value
                        elif operator == 'gt':
                            include_record = float(record_value) > float(value)
                        elif operator == 'lt':
                            include_record = float(record_value) < float(value)
                        elif operator == 'gte':
                            include_record = float(record_value) >= float(value)
                        elif operator == 'lte':
                            include_record = float(record_value) <= float(value)
                        elif operator == 'contains':
                            include_record = str(value) in str(record_value)
                        elif operator == 'regex':
                            include_record = bool(re.search(str(value), str(record_value)))
                    except (ValueError, TypeError):
                        # Skip records that can't be compared
                        continue

                    if include_record:
                        new_filtered_data.append(record)

                filtered_data = new_filtered_data

            return {
                "status": "completed",
                "transformation_type": "filtering",
                "rules_applied": len(rules),
                "records_input": len(source_data),
                "records_output": len(filtered_data),
                "records_filtered": len(source_data) - len(filtered_data),
                "transformed_data": filtered_data
            }

        except Exception as e:
            return {
                "status": "error",
                "error_message": f"Filtering error: {str(e)}",
                "transformed_data": None
            }

    async def apply_enrichment(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data enrichment transformations."""
        try:
            if not isinstance(source_data, list):
                return {
                    "status": "error",
                    "error_message": "Source data must be a list for enrichment",
                    "transformed_data": None
                }

            enriched_data = []

            for record in source_data:
                if not isinstance(record, dict):
                    continue

                enriched_record = record.copy()

                for rule in rules:
                    enrichment_type = rule.get('type')  # calculated_field, lookup, constant
                    target_field = rule.get('target_field')

                    if enrichment_type == 'calculated_field':
                        expression = rule.get('expression')
                        source_fields = rule.get('source_fields', [])

                        # Simple expression evaluation (extend as needed)
                        if expression and all(field in record for field in source_fields):
                            try:
                                # Replace field names with values in expression
                                eval_expression = expression
                                for field in source_fields:
                                    field_value = record[field]
                                    if isinstance(field_value, str):
                                        eval_expression = eval_expression.replace(f"{{{field}}}", f'"{field_value}"')
                                    else:
                                        eval_expression = eval_expression.replace(f"{{{field}}}", str(field_value))

                                # Simple evaluation (be careful with eval in production)
                                result = eval(eval_expression)
                                enriched_record[target_field] = result
                            except Exception:
                                # Skip calculation if there's an error
                                pass

                    elif enrichment_type == 'constant':
                        constant_value = rule.get('value')
                        enriched_record[target_field] = constant_value

                    elif enrichment_type == 'lookup':
                        lookup_table = rule.get('lookup_table', {})
                        lookup_field = rule.get('lookup_field')

                        if lookup_field in record:
                            lookup_key = str(record[lookup_field])
                            if lookup_key in lookup_table:
                                enriched_record[target_field] = lookup_table[lookup_key]

                enriched_data.append(enriched_record)

            return {
                "status": "completed",
                "transformation_type": "enrichment",
                "rules_applied": len(rules),
                "records_processed": len(enriched_data),
                "transformed_data": enriched_data
            }

        except Exception as e:
            return {
                "status": "error",
                "error_message": f"Enrichment error: {str(e)}",
                "transformed_data": None
            }

    async def apply_generic_transformation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply generic transformations based on rule types."""
        try:
            result = {
                "status": "completed",
                "transformations_applied": 0,
                "transformed_data": source_data
            }

            current_data = source_data

            for rule in rules:
                transformation_type = rule.get('type')

                if transformation_type == 'field_mapping':
                    mapping_result = await self.apply_field_mapping(current_data, [rule])
                    if mapping_result["status"] == "completed":
                        current_data = mapping_result["transformed_data"]
                        result["transformations_applied"] += 1

                elif transformation_type == 'filtering':
                    filter_result = await self.apply_filtering(current_data, [rule])
                    if filter_result["status"] == "completed":
                        current_data = filter_result["transformed_data"]
                        result["transformations_applied"] += 1

                elif transformation_type == 'enrichment':
                    enrich_result = await self.apply_enrichment(current_data, [rule])
                    if enrich_result["status"] == "completed":
                        current_data = enrich_result["transformed_data"]
                        result["transformations_applied"] += 1

                elif transformation_type == 'aggregation':
                    agg_result = await self.apply_aggregation(current_data, [rule])
                    if agg_result["status"] == "completed":
                        current_data = agg_result["transformed_data"]
                        result["transformations_applied"] += 1

            result["transformed_data"] = current_data
            return result

        except Exception as e:
            return {
                "status": "error",
                "error_message": f"Generic transformation error: {str(e)}",
                "transformed_data": None
            }
