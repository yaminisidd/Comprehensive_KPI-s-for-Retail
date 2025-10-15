
def calculate_level_adjusted(row):
    non_null_count = sum(value is not None for value in row)
    # Assign levels based on the number of non-null values
    return f"level{non_null_count - 2}"