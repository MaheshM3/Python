class ValidationError(Exception):
    pass

def validate_parameters(param1, param2, param3):
    errors = []
    if not isinstance(param1, int) or param1 <= 0:
        errors.append("param1 must be a positive integer")
    if not isinstance(param2, str) or not param2:
        errors.append("param2 must be a non-empty string")
    if not isinstance(param3, list) or not param3:
        errors.append("param3 must be a non-empty list")
    
    if errors:
        raise ValidationError("\n".join(errors))
    
    logging.info("All parameters are valid")
    return True

# Example usage
try:
    validate_parameters(0, "", [])
except ValidationError as e:
    logging.error(f"Validation failed:\n{e}")
    sys.exit(1)