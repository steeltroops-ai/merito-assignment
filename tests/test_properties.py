"""
Property-based tests using Hypothesis.
These test general properties that should hold for any input.
"""

from hypothesis import given, settings, HealthCheck, assume
import hypothesis.strategies as st
from datetime import datetime, timezone, timedelta
import sys
import os
import random

# Add parent directory to path to import main module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from main import is_valid_event, group_and_sort_events, detect_missed_heartbeats, monitor_heartbeats


class TestHeartbeatProperties:
    """Property tests - verify behavior across many random inputs"""
    
    @given(
        service=st.text(min_size=1),
        timestamp=st.datetimes(min_value=datetime(2000, 1, 1), max_value=datetime(2100, 12, 31))
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_valid_event_acceptance(self, service, timestamp):
        """
        Feature: heartbeat-monitor, Property 1: Valid event acceptance
     
        Any event with both required fields should be accepted.
        """
        event = {
            "service": service,
            "timestamp": timestamp.isoformat()
        }
        
        assert is_valid_event(event) == True
    
    @given(
        events=st.lists(
            st.fixed_dictionaries({
                "service": st.text(min_size=1, max_size=20),
                "timestamp": st.datetimes(
                    min_value=datetime(2000, 1, 1),
                    max_value=datetime(2100, 12, 31)
                ).map(lambda dt: dt.isoformat())
            }),
            min_size=1,
            max_size=50
        )
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_order_independence(self, events):
        """
        Feature: heartbeat-monitor, Property 2: Deterministic processing regardless of input order
        
        Shuffling the input shouldn't change the final result.
        """
        result1 = group_and_sort_events(events)
        
        # Try again with shuffled input
        shuffled_events = events.copy()
        random.shuffle(shuffled_events)
        result2 = group_and_sort_events(shuffled_events)
        
        # Should get same results
        assert set(result1.keys()) == set(result2.keys())
        
        for service in result1:
            assert len(result1[service]) == len(result2[service])
            for i in range(len(result1[service])):
                assert result1[service][i]["service"] == result2[service][i]["service"]
                assert result1[service][i]["timestamp"] == result2[service][i]["timestamp"]
    
    @given(
        expected_interval=st.integers(min_value=10, max_value=300),
        time_gap_multiplier=st.floats(min_value=2.5, max_value=10.0)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_missed_heartbeat_calculation(self, expected_interval, time_gap_multiplier):
        """
        Feature: heartbeat-monitor, Property 3: Missed heartbeat calculation accuracy

        The math for counting missed heartbeats should be correct.
        """
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = int(expected_interval * time_gap_multiplier)
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = {
            "test_service": [
                {"service": "test_service", "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
                {"service": "test_service", "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
            ]
        }
        
        expected_misses = int((time_gap - expected_interval) / expected_interval)
        assume(expected_misses >= 1)
        
        allowed_misses = expected_misses
        alerts = detect_missed_heartbeats(events, expected_interval, allowed_misses)
        
        assert len(alerts) == 1
        assert alerts[0]["service"] == "test_service"
        
        # Check alert timestamp is right
        expected_alert_time = start_time + timedelta(seconds=allowed_misses * expected_interval)
        alert_time = datetime.fromisoformat(alerts[0]["alert_at"].replace('Z', '+00:00'))
        assert alert_time == expected_alert_time
    
    @given(
        expected_interval=st.integers(min_value=10, max_value=300),
        allowed_misses=st.integers(min_value=2, max_value=10),
        actual_misses=st.integers(min_value=1, max_value=10)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_no_alert_below_threshold(self, expected_interval, allowed_misses, actual_misses):
        """
        Feature: heartbeat-monitor, Property 4: No alert below threshold
  
        If we don't hit the threshold, no alert should fire.
        """
        assume(actual_misses < allowed_misses)
        
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = expected_interval + (actual_misses * expected_interval)
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = {
            "test_service": [
                {"service": "test_service", "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
                {"service": "test_service", "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
            ]
        }
        
        alerts = detect_missed_heartbeats(events, expected_interval, allowed_misses)
        assert len(alerts) == 0
    
    @given(
        expected_interval=st.integers(min_value=10, max_value=300),
        allowed_misses=st.integers(min_value=1, max_value=10)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_alert_at_threshold(self, expected_interval, allowed_misses):
        """
        Feature: heartbeat-monitor, Property 5: Alert triggered at threshold
   
        Hitting exactly the threshold should trigger an alert.
        """
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = expected_interval + (allowed_misses * expected_interval)
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = {
            "test_service": [
                {"service": "test_service", "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
                {"service": "test_service", "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
            ]
        }
        
        alerts = detect_missed_heartbeats(events, expected_interval, allowed_misses)
        
        assert len(alerts) == 1
        assert alerts[0]["service"] == "test_service"
        
        expected_alert_time = start_time + timedelta(seconds=allowed_misses * expected_interval)
        alert_time = datetime.fromisoformat(alerts[0]["alert_at"].replace('Z', '+00:00'))
        assert alert_time == expected_alert_time
    
    @given(
        expected_interval=st.integers(min_value=10, max_value=300),
        allowed_misses=st.integers(min_value=3, max_value=10),
        first_misses=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_miss_counter_reset(self, expected_interval, allowed_misses, first_misses):
        """
        Feature: heartbeat-monitor, Property 6: Miss counter reset on heartbeat

        Getting a heartbeat should reset the miss counter.
        """
        assume(first_misses < allowed_misses)
        
        # Three heartbeats, each with some misses but not enough to alert
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        time_gap_1 = expected_interval + (first_misses * expected_interval)
        second_time = start_time + timedelta(seconds=time_gap_1)
        
        time_gap_2 = expected_interval + (first_misses * expected_interval)
        third_time = second_time + timedelta(seconds=time_gap_2)
        
        events = {
            "test_service": [
                {"service": "test_service", "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
                {"service": "test_service", "timestamp": second_time.isoformat().replace('+00:00', 'Z')},
                {"service": "test_service", "timestamp": third_time.isoformat().replace('+00:00', 'Z')}
            ]
        }
        
        alerts = detect_missed_heartbeats(events, expected_interval, allowed_misses)
        
        # No alert - counter resets after each heartbeat
        assert len(alerts) == 0

    @given(
        expected_interval=st.integers(min_value=10, max_value=300),
        allowed_misses=st.integers(min_value=1, max_value=10),
        num_services=st.integers(min_value=1, max_value=5)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_alert_structure_completeness(self, expected_interval, allowed_misses, num_services):
        """
        Feature: heartbeat-monitor, Property 9: Alert structure completeness

        Every alert should have the right fields.
        """
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = expected_interval + (allowed_misses * expected_interval)
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = {}
        for i in range(num_services):
            service_name = f"service_{i}"
            events[service_name] = [
                {"service": service_name, "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
                {"service": service_name, "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
            ]
        
        alerts = detect_missed_heartbeats(events, expected_interval, allowed_misses)
        
        # Check each alert has required fields
        for alert in alerts:
            assert "service" in alert
            assert isinstance(alert["service"], str)
            assert len(alert["service"]) > 0
            
            assert "alert_at" in alert
            assert isinstance(alert["alert_at"], str)
            
            # Timestamp should be valid
            try:
                datetime.fromisoformat(alert["alert_at"].replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                assert False, f"Invalid timestamp: {alert['alert_at']}"
    
    @given(
        expected_interval=st.integers(min_value=10, max_value=300),
        allowed_misses=st.integers(min_value=1, max_value=10),
        num_services=st.integers(min_value=2, max_value=10)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_all_alerts_captured(self, expected_interval, allowed_misses, num_services):
        """
        Feature: heartbeat-monitor, Property 10: All alerts captured
    
        If multiple services fail, we should get alerts for all of them.
        """
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = expected_interval + (allowed_misses * expected_interval)
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = {}
        expected_services = set()
        for i in range(num_services):
            service_name = f"service_{i}"
            expected_services.add(service_name)
            events[service_name] = [
                {"service": service_name, "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
                {"service": service_name, "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
            ]
        
        alerts = detect_missed_heartbeats(events, expected_interval, allowed_misses)
        
        # One alert per service
        assert len(alerts) == num_services
        
        alerted_services = {alert["service"] for alert in alerts}
        assert alerted_services == expected_services

    @given(
        interval1=st.integers(min_value=30, max_value=100),
        interval2=st.integers(min_value=101, max_value=300),
        allowed_misses=st.integers(min_value=2, max_value=5)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_configuration_parameter_interval(self, interval1, interval2, allowed_misses):
        """
        Feature: heartbeat-monitor, Property 7: Configuration parameter usage (interval)
  
        Different intervals should produce different results.
        """
        import tempfile
        import json
        
        assume(interval1 != interval2)
        
        # Fixed time gap, different intervals = different miss counts
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = 250
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = [
            {"service": "test_service", "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
            {"service": "test_service", "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(events, f)
            temp_file = f.name
        
        try:
            expected_misses_1 = int((time_gap - interval1) / interval1)
            expected_misses_2 = int((time_gap - interval2) / interval2)
            
            assume(expected_misses_1 != expected_misses_2)
            
            alerts1 = monitor_heartbeats(temp_file, interval1, allowed_misses)
            alerts2 = monitor_heartbeats(temp_file, interval2, allowed_misses)
            
            # Results should differ
            if expected_misses_1 >= allowed_misses and expected_misses_2 < allowed_misses:
                assert len(alerts1) == 1 and len(alerts2) == 0
            elif expected_misses_1 < allowed_misses and expected_misses_2 >= allowed_misses:
                assert len(alerts1) == 0 and len(alerts2) == 1
            elif expected_misses_1 >= allowed_misses and expected_misses_2 >= allowed_misses:
                assert len(alerts1) == 1 and len(alerts2) == 1
                assert alerts1[0]["alert_at"] != alerts2[0]["alert_at"]
        finally:
            os.unlink(temp_file)
    
    @given(
        expected_interval=st.integers(min_value=30, max_value=100),
        threshold1=st.integers(min_value=2, max_value=5),
        threshold2=st.integers(min_value=6, max_value=10)
    )
    @settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
    def test_threshold_parameter_usage(self, expected_interval, threshold1, threshold2):
        """
        Feature: heartbeat-monitor, Property 8: Threshold parameter usage
 
        Different thresholds should produce different alert behavior.
        """
        import tempfile
        import json
        
        assume(threshold1 != threshold2)
        assume(threshold1 < threshold2)
        
        # Create misses between the two thresholds
        target_misses = (threshold1 + threshold2) // 2
        assume(threshold1 < target_misses < threshold2)
        
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        time_gap = expected_interval + (target_misses * expected_interval)
        end_time = start_time + timedelta(seconds=time_gap)
        
        events = [
            {"service": "test_service", "timestamp": start_time.isoformat().replace('+00:00', 'Z')},
            {"service": "test_service", "timestamp": end_time.isoformat().replace('+00:00', 'Z')}
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(events, f)
            temp_file = f.name
        
        try:
            alerts1 = monitor_heartbeats(temp_file, expected_interval, threshold1)
            alerts2 = monitor_heartbeats(temp_file, expected_interval, threshold2)
            
            # Lower threshold should alert
            assert len(alerts1) == 1
            assert alerts1[0]["service"] == "test_service"
            
            # Higher threshold should not
            assert len(alerts2) == 0
        finally:
            os.unlink(temp_file)
