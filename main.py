"""
Heartbeat monitoring system that detects service failures.

Processes heartbeat events from JSON and identifies services that miss
too many consecutive heartbeats.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List


def is_valid_event(event: Dict) -> bool:
    """
    Check if an event has the required fields and valid timestamp.
    
    Args:
        event: Event dict to validate
    
    Returns:
        True if valid, False otherwise
    """
    # Check for required fields
    if "service" not in event or "timestamp" not in event:
        return False
    
    # Validate timestamp format (ISO 8601)
    try:
        datetime.fromisoformat(event["timestamp"].replace('Z', '+00:00'))
        return True
    except (ValueError, AttributeError):
        return False


def load_events(file_path: str) -> List[Dict[str, str]]:
    """
    Load events from JSON file and filter out invalid ones.
    
    Args:
        file_path: Path to the JSON file
    
    Returns:
        List of valid events only
    
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If JSON is malformed
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Filter to only valid events
    if isinstance(data, list):
        return [event for event in data if is_valid_event(event)]
    else:
        return []


def group_and_sort_events(events: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Organize events by service, sorted by time.
    
    Args:
        events: List of heartbeat events
    
    Returns:
        Dict mapping service name -> sorted events
    """
    # Group events by service
    grouped = {}
    for event in events:
        service = event["service"]
        if service not in grouped:
            grouped[service] = []
        grouped[service].append(event)
    
    # Sort each service's events chronologically by timestamp
    for service in grouped:
        grouped[service].sort(key=lambda e: datetime.fromisoformat(e["timestamp"].replace('Z', '+00:00')))
    
    return grouped


def detect_missed_heartbeats(
    service_events: Dict[str, List[Dict]], 
    expected_interval: int, 
    allowed_misses: int
) -> List[Dict[str, str]]:
    """
    Find services that missed too many heartbeats and create alerts.
    
    Args:
        service_events: Service name -> sorted events
        expected_interval: Seconds between expected heartbeats
        allowed_misses: How many consecutive misses trigger an alert
    
    Returns:
        List of alerts with 'service' and 'alert_at' fields
    """
    alerts = []
    
    for service, events in service_events.items():
        if len(events) < 2:
            # Need at least 2 events to detect missed heartbeats
            continue
        
        consecutive_misses = 0
        
        for i in range(len(events) - 1):
            current_time = datetime.fromisoformat(events[i]["timestamp"].replace('Z', '+00:00'))
            next_time = datetime.fromisoformat(events[i + 1]["timestamp"].replace('Z', '+00:00'))
            
            # How long between these two heartbeats?
            time_gap = (next_time - current_time).total_seconds()
            
            # If gap is too large, we missed some heartbeats
            if time_gap > expected_interval:
                missed_count = int((time_gap - expected_interval) / expected_interval)
                consecutive_misses += missed_count
                
                # Did we cross the threshold?
                if consecutive_misses >= allowed_misses:
                    # Alert timestamp is when the Nth miss happened
                    alert_timestamp = current_time + timedelta(seconds=allowed_misses * expected_interval)
                    
                    alerts.append({
                        "service": service,
                        "alert_at": alert_timestamp.isoformat().replace('+00:00', 'Z')
                    })
                    
                    # Reset after alerting (only alert once per miss sequence)
                    consecutive_misses = 0
                else:
                    # Got a heartbeat before hitting threshold - reset the counter
                    consecutive_misses = 0
            else:
                # Heartbeat came on time, reset
                consecutive_misses = 0
    
    return alerts


def monitor_heartbeats(file_path: str, expected_interval: int, allowed_misses: int) -> List[Dict[str, str]]:
    """
    Main function - load events, process them, and return alerts.
    
    Args:
        file_path: JSON file with heartbeat events
        expected_interval: Seconds between expected heartbeats
        allowed_misses: Consecutive misses needed to trigger alert
    
    Returns:
        List of alerts (service + timestamp)
    
    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If JSON is invalid
    """
    try:
        # Load events and filter out invalid ones
        events = load_events(file_path)
        
        # Organize by service and sort by time
        service_events = group_and_sort_events(events)
        
        # Find services that missed too many heartbeats
        alerts = detect_missed_heartbeats(service_events, expected_interval, allowed_misses)
        
        return alerts
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Heartbeat events file not found: {file_path}") from e
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in heartbeat events file: {file_path}", e.doc, e.pos) from e


if __name__ == "__main__":
    # Run with default settings
    alerts = monitor_heartbeats("events.json", 60, 3)
    print(f"Alerts: {alerts}")
