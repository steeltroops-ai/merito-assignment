# Heartbeat Monitor

A Python system that monitors service health by detecting missed consecutive heartbeats and generating alerts.

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

Processes `events.json` with default settings (60s interval, 3 allowed misses).

## Test

```bash
python -m pytest tests/
```

## Input Format

```json
[
    {"service": "email", "timestamp": "2025-08-04T10:00:00Z"},
    {"service": "sms", "timestamp": "2025-08-04T10:01:00Z"}
]
```

## Output Format

```json
[
    {"service": "push", "alert_at": "2025-08-04T10:09:00Z"}
]
```

## Implementation

- `main.py` - Core monitoring logic
- `tests/test_heartbeat_monitor.py` - Unit tests
- `tests/test_properties.py` - Property-based tests (Hypothesis)
