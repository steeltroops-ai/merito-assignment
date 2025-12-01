"""
Unit tests for the heartbeat monitoring system.
Tests specific scenarios to make sure the core logic works correctly.
"""

import unittest
import json
import tempfile
import os
import sys

# Add parent directory to path to import main module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import monitor_heartbeats


class TestHeartbeatMonitor(unittest.TestCase):
    """Test cases for heartbeat monitoring"""
    
    def test_working_alert_case(self):
        """Service that misses exactly 3 heartbeats should trigger an alert"""
        # Setup: service sends heartbeat at 10:00, then nothing until 10:04
        # That's a 240s gap = 60s + 3*60s, so 3 missed heartbeats
        # Should trigger alert at 10:03 (when 3rd miss happened)
        events = [
            {"service": "api-service", "timestamp": "2025-08-04T10:00:00Z"},
            {"service": "api-service", "timestamp": "2025-08-04T10:04:00Z"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(events, f)
            temp_file = f.name
        
        try:
            alerts = monitor_heartbeats(temp_file, 60, 3)
            
            # Should get exactly one alert
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0]["service"], "api-service")
            self.assertEqual(alerts[0]["alert_at"], "2025-08-04T10:03:00Z")
        finally:
            os.unlink(temp_file)
    
    def test_near_miss_case(self):
        """Service that misses only 2 heartbeats shouldn't trigger alert"""
        # Only 2 misses, threshold is 3, so no alert
        events = [
            {"service": "api-service", "timestamp": "2025-08-04T10:00:00Z"},
            {"service": "api-service", "timestamp": "2025-08-04T10:03:00Z"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(events, f)
            temp_file = f.name
        
        try:
            alerts = monitor_heartbeats(temp_file, 60, 3)
            
            # No alert since we're below threshold
            self.assertEqual(len(alerts), 0)
        finally:
            os.unlink(temp_file)
    
    def test_unordered_input(self):
        """Events can arrive in any order - system should sort them correctly"""
        # Events are intentionally out of order
        events = [
            {"service": "api-service", "timestamp": "2025-08-04T10:04:00Z"},
            {"service": "api-service", "timestamp": "2025-08-04T10:00:00Z"},
            {"service": "db-service", "timestamp": "2025-08-04T10:05:00Z"},
            {"service": "db-service", "timestamp": "2025-08-04T10:01:00Z"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(events, f)
            temp_file = f.name
        
        try:
            alerts = monitor_heartbeats(temp_file, 60, 3)
            
            # Both services should alert
            self.assertEqual(len(alerts), 2)
            alert_services = {alert["service"] for alert in alerts}
            self.assertEqual(alert_services, {"api-service", "db-service"})
            
            # Check timestamps are correct
            for alert in alerts:
                if alert["service"] == "api-service":
                    self.assertEqual(alert["alert_at"], "2025-08-04T10:03:00Z")
                elif alert["service"] == "db-service":
                    self.assertEqual(alert["alert_at"], "2025-08-04T10:04:00Z")
        finally:
            os.unlink(temp_file)
    
    def test_malformed_events(self):
        """System should skip bad events and process the good ones"""
        # Mix of valid and invalid events
        events = [
            {"service": "api-service", "timestamp": "2025-08-04T10:00:00Z"},
            {"service": "api-service"},  # missing timestamp
            {"timestamp": "2025-08-04T10:01:00Z"},  # missing service
            {"service": "api-service", "timestamp": "invalid-timestamp"},
            {"service": "api-service", "timestamp": "2025-08-04T10:04:00Z"},
            {"service": "db-service", "timestamp": "2025-08-04T10:00:00Z"},
            {"service": "db-service", "timestamp": None},
            {"service": "db-service", "timestamp": "2025-08-04T10:04:00Z"}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(events, f)
            temp_file = f.name
        
        try:
            # Should handle bad events gracefully
            alerts = monitor_heartbeats(temp_file, 60, 3)
            
            # Only valid events processed, both services should alert
            self.assertEqual(len(alerts), 2)
            alert_services = {alert["service"] for alert in alerts}
            self.assertEqual(alert_services, {"api-service", "db-service"})
        finally:
            os.unlink(temp_file)


if __name__ == "__main__":
    unittest.main()
