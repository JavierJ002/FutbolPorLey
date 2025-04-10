
from typing import Any, Optional, Union, Dict
import logging

def _safe_to_float(value: Any) -> Optional[float]:
    if value is None: return None
    try: return float(str(value).replace(',', '.'))
    except (ValueError, TypeError): return None

def _safe_to_int(value: Any) -> Optional[int]:
    if value is None: return None
    try: return int(float(str(value).replace(',', '.'))) 
    except (ValueError, TypeError): return None

def _convert_to_numeric(value: Any) -> Optional[Union[int, float, Dict[str, Any], str]]:
    """
    Converts a raw stat value from SofaScore API into a numeric type or dict.
    Handles formats like "123", "12.3", "75%", "12/18 (67%)".
    """
    if value is None: return None
    value_str = str(value).strip()
    if not value_str: return None

    # Format: "Successful/Total (Percentage%)"
    if '/' in value_str and '(' in value_str and value_str.endswith(')'):
        try:
            parts = value_str.split('(')
            fraction_part = parts[0].strip()
            percentage_part = parts[1].split(')')[0].strip('% ')
            successful, total = map(int, fraction_part.split('/'))
            # Ensure percentage is derived correctly, handle potential format variations
            percentage = round(float(percentage_part) / 100.0, 4) if percentage_part else None
            # Recalculate percentage if possible and seems incorrect
            if total > 0 and percentage is not None:
                 calculated_perc = round(successful / total, 4)
                 # Allow small tolerance for rounding differences
                 if abs(percentage - calculated_perc) > 0.005:
                      logging.debug(f"Adjusting percentage for {value_str}. API: {percentage}, Calc: {calculated_perc}")
                      percentage = calculated_perc
            elif total > 0 and percentage is None:
                 percentage = round(successful / total, 4)

            return {"successful": successful, "total": total, "percentage": percentage}
        except (ValueError, IndexError, TypeError, ZeroDivisionError):
             logging.warning(f"Could not parse complex stat: {value_str}")
             return {"successful": None, "total": None, "percentage": None} # Return dict with None

    # Format: "Percentage%"
    elif value_str.endswith('%'):
        try:
            return round(float(value_str.strip('% ')) / 100.0, 4)
        except ValueError:
            logging.warning(f"Could not parse percentage: {value_str}")
            return None

    # Format: "Integer" or "Float"
    else:
        try:
            # Try float first to handle "1.0" etc. then try int
            float_val = float(value_str.replace(',', '.'))
            if float_val.is_integer():
                return int(float_val)
            else:
                return float_val
        except ValueError:
            logging.warning(f"Could not parse simple numeric: {value_str}")
            return None # Return None if not clearly numeric