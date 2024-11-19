def hex_to_decimal(hex_str):
    """Convert hex string to decimal."""
    return int(hex_str, 16)

def decimal_to_formatted_hex(decimal, length=16):
    """Convert decimal number to a formatted hex string with leading zeros."""
    return f"{decimal:0{length}X}".lower()

def process_file(file_path):
    """Read file, process each line, and check for consecutive hex differences."""
    with open(file_path, 'r') as file:
        previous_hex = None
        missing_ranges = []  # List to store all missing ranges
        
        for line in file:
            # Split the line by '|' and extract the first part (which contains the hex numbers)
            parts = line.split('|')
            if len(parts) < 4:
                continue  # Skip any malformed lines
            
            # Extract the first hex number from the range in the first part of the split line
            try:
                hex_range = parts[0].strip().split('_')[2]  # Get the correct hex number
            except IndexError:
                print(f"Could not extract hex number from line: {line.strip()}")
                continue  # Skip if the format is incorrect
            
            # Convert the hex number to decimal
            try:
                current_decimal = hex_to_decimal(hex_range)
            except ValueError:
                print(f"Invalid hex number found in line: {line.strip()}")
                continue  # Skip lines with invalid hex numbers
            
            # If there is a previous number, compare the difference
            if previous_hex is not None:
                difference = current_decimal - previous_hex
                if difference > 1000:
                    # starting from previous_hex to current_decimal, there are missing slots,
                    # print each 1000 incremental rage until current_decimal
                    for i in range(previous_hex, current_decimal + 1000, 1000):
                        missing_ranges.append(f"{decimal_to_formatted_hex(i)}_{decimal_to_formatted_hex(i + 1000)}")
                    missing_ranges.append("---------------------")
            
            # Store the current decimal for the next comparison
            previous_hex = current_decimal
        
        # Print all missing ranges
        if missing_ranges:
            print("Missing ranges:")
            for range_str in missing_ranges:
                print(range_str)
        else:
            print("No missing slots found.")

# Example usage
file_path = 'live_sync_entries.txt'  # Replace with your actual file path
process_file(file_path)

