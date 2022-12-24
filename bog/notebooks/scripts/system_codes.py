# Copyright 2021 Blue Ocean Gear.
# Non-transferable license granted to students & staff of University of Chicago Data Science Institute
# under terms of signed document "Data Science Institute Project Agreement"

FLAG_BITS = {
    "in_water": 0x1,
    "update_reason_resurfaced": 0x100,
    "update_reason_sws": 0x200, # SWS == Salt Water Sensor
    "update_reason_scheduled": 0x400,
    "update_reason_moved": 0x800,
    "update_reason_retry": 0x1000,
    "fault_gps_no_fix": 0x2000,
}

def parse_system_flags(flags_uint):
    """ Parses BOG buoy system status flag
    
    Args:
        flags_uint (int): An integer return by a BOG buoy 'system_status'
    Returns:
        system_flags (list of str): A list of strings representing
            each system flag present.
    """
    system_flags = []
    for flag in FLAG_BITS:
        if FLAG_BITS[flag] & flags_uint:
            system_flags.append(flag)
    return system_flags

def human_readable_reason(system_flags):
    """ Adds additional context to update reason from system flags list """
    if "update_reason_sws" in system_flags:
        if "in_water" in system_flags:
            return "Entered Water"
        else:
            return "Left Water"
    elif "update_reason_resurfaced" in system_flags:
        return "Resurfaced"
    elif "update_reason_scheduled" in system_flags:
        return "Scheduled"
    elif "update_reason_moved" in system_flags:
        return "Moved"
    elif "update_reason_retry" in system_flags:
        return "Retry"
    else:
        return "Unknown"

if __name__ == "__main__":
    sample_codes = [1557, 528, 132629, 33296, 7701, 8720, 5653, 5648, 536, 1045, 2069, 784, 1813, 67110421]
    for code in sample_codes:
        print(parse_system_flags(code))


