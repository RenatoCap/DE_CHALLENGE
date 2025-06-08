from datetime import datetime


def is_iso_datetime(date_str: str) -> bool:
    """
    Valida si una cadena de texto es una fecha y hora válida en formato ISO 8601.

    Esta función intenta parsear la cadena utilizando varios formatos ISO 8601 comunes.
    Prioriza `datetime.fromisoformat()` para mayor robustez si la versión de Python lo permite.

    Args:
        date_str (str): La cadena de texto a validar.

    Returns:
        bool: True si la cadena es un datetime ISO 8601 válido, False en caso contrario.
    """
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
    """
    Gestiona la validación de un conjunto de datos de departamentos contra un esquema predefinido.

    Esta clase toma una lista de diccionarios (donde cada diccionario representa un departamento)
    y valida si cada "fila" (diccionario) cumple con el esquema esperado en términos de
    nombres de campos y tipos de datos. Además, realiza una validación específica
    para el campo 'datetime' asegurando que esté en formato ISO 8601.
    Divide los datos en filas que cumplen con el esquema (aceptadas)
    y las que no (rechazadas), proporcionando razones detalladas.

    Atributos:
        data (list): La lista de diccionarios a validar, donde cada diccionario
                     es una fila de datos.
        schema (dict): Un diccionario que define el esquema esperado. Las claves son
                       los nombres de los campos esperados y los valores son los
                       tipos de datos esperados (por ejemplo, str, int, float).
    """

    def __init__(self, data: list):
        """
        Inicializa la instancia de Departments con los datos a validar.

        Args:
            data (list): Una lista de diccionarios. Cada diccionario debe representar
                         una fila con la estructura de un departamento.
        """
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
        """
        Valida cada fila de los datos contra el esquema predefinido y los tipos de datos.

        Itera sobre cada diccionario en `self.data` y realiza las siguientes validaciones:
        1. Asegura que la fila sea un diccionario.
        2. Verifica que las claves del diccionario de la fila coincidan exactamente
           con las claves del esquema (ni faltantes ni inesperadas).
        3. Comprueba si el tipo de dato de cada valor en la fila coincide con el tipo
           esperado en el esquema para esa clave.
        4. Realiza una validación especial para el campo 'datetime' para asegurar
           que esté en formato ISO 8601.

        Returns:
            tuple[list, list]: Una tupla que contiene:
                - accepted_rows (list): Lista de diccionarios que cumplen con el esquema.
                - rejected_rows (list): Lista de diccionarios que NO cumplen con el esquema,
                                        incluyendo información sobre por qué fueron rechazadas.
                                        Cada elemento es un diccionario con:
                                        - 'index': El índice original de la fila.
                                        - 'row_data': El diccionario de la fila original.
                                        - 'reason': Una lista de cadenas que describen los problemas.
        """
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

