class Job:
    def __init__(self, data):
        self.data = data
        self.insert_columns = ['job']
        self.schema = {
            'job': str,
        }
    
    def validate_schema(self):
        accepted_rows = []
        rejected_rows = []

        expected_keys = list(self.schema.keys())
        expected_keys_set = set(expected_keys)

        for index, row in enumerate(self.data):
            if not isinstance(row, dict):
                rejected_rows.append({
                    "index": index,
                    "row_data": row,
                    "reason": [f"The row is not a dictionary, it is of type {type(row).__name__}."]
                })
                continue

            row_keys_set = set(row.keys())
            reasons = []

            if row_keys_set != expected_keys_set:
                missing_keys = expected_keys_set - row_keys_set
                if missing_keys:
                    reasons.append(f"The following keys are missing: {', '.join(sorted(missing_keys))}.")
                
                extra_keys = row_keys_set - expected_keys_set
                if extra_keys:
                    reasons.append(f"Contains unexpected clues: {', '.join(sorted(extra_keys))}.")
            
                for key, expected_type in self.schema.items():
                    if key in row:
                        if not isinstance(row[key], expected_type):
                            reasons.append(
                                f"The flied '{key}' has the type '{type(row[key]).__name__}', "
                                f"was expected '{expected_type.__name__}'."
                            )
                    elif key not in row and not missing_keys:
                         reasons.append(f"The field '{key}' is absent.")

            if reasons:
                rejected_rows.append({
                    "index": index,
                    "row_data": row,
                    "reason": reasons
                })
            else:
                accepted_rows.append(row)
        
        return accepted_rows, rejected_rows
