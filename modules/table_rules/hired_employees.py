from datetime import datetime


def is_iso_datetime(date_str: str) -> bool:
    if not isinstance(date_str, str):
        return False

    try:
        datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return True
    except ValueError:
        pass

    iso_formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",   # With milliseconds and Z (UTC)
        "%Y-%m-%dT%H:%M:%S%z",     # With seconds and time zone offset (e.g. +0100)
        "%Y-%m-%dT%H:%M:%SZ",      # With seconds and Z (UTC)
    ]

    for fmt in iso_formats:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue
    return False


class HiredEmployees:

    def __init__(self, data: list):
        if not isinstance(data, list):
            raise TypeError("The data must be a list of dictionaries.")
        
        self.data = data
        self.insert_columns = ['name', 'datetime', 'department_id', 'job_id']
        self.schema = {
            'name': str,
            'datetime': str,
            'department_id': int,
            'job_id': int
        }
    
    def validate_schema(self) -> tuple[list, list]:
        accepted_rows = []
        rejected_rows = []

        expected_keys_set = set(self.schema.keys())

        for index, row in enumerate(self.data):
            reasons = []

            if not isinstance(row, dict):
                reasons.append(f"The row is not a dictionary, it is of type {type(row).__name__}.")
                rejected_rows.append({"index": index, "row_data": row, "reason": reasons})
                continue

            row_keys_set = set(row.keys())

            if row_keys_set != expected_keys_set:
                missing_keys = expected_keys_set - row_keys_set
                if missing_keys:
                    reasons.append(f"The following keys are missing: {', '.join(sorted(missing_keys))}.")
                
                extra_keys = row_keys_set - expected_keys_set
                if extra_keys:
                    reasons.append(f"Contains unexpected keys: {', '.join(sorted(extra_keys))}.")
            
            for key, expected_type in self.schema.items():
                if key in row:
                    if not isinstance(row[key], expected_type):
                        reasons.append(
                            f"The field '{key}' has the type '{type(row[key]).__name__}', "
                            f"'{expected_type.__name__}' was expected."
                        )
                    
                    if key == 'datetime' and expected_type is str:
                        if not is_iso_datetime(row[key]):
                            reasons.append(
                                f"The field 'datetime' has an invalid ISO 8601 format."
                            )
                elif key not in row and key in expected_keys_set:
                    pass

            if reasons:
                rejected_rows.append({
                    "index": index,
                    "row_data": row,
                    "reason": reasons
                })
            else:
                accepted_rows.append(row)
        
        return accepted_rows, rejected_rows

