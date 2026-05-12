import csv

from om_pipeline.analysis.event_qa import EventQaConfig, audit_event_outputs, format_qa_summary


def write_csv(path, rows):
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def test_audit_event_outputs_summarises_pilot_metrics(tmp_path):
    events_csv = tmp_path / "OM_Events_July_2024.csv"
    registry_csv = tmp_path / "Fleet_Registry_July_2024.csv"

    write_csv(
        events_csv,
        [
            {
                "MMSI": "111",
                "Name": "CT Alice",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "found_id": "A01",
                "start": "2024-07-01 00:00:00",
                "end": "2024-07-01 00:30:00",
                "duration_min": "30",
                "min_dist": "12.5",
                "length": "25",
            },
            {
                "MMSI": "111",
                "Name": "CT Alice",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "found_id": "A02",
                "start": "2024-07-01 01:00:00",
                "end": "2024-07-01 01:45:00",
                "duration_min": "45",
                "min_dist": "20",
                "length": "25",
            },
            {
                "MMSI": "222",
                "Name": "Bulk Maybe",
                "Ship type": "Cargo",
                "wind_farm": "Beta",
                "found_id": "B01",
                "start": "2024-07-02 00:00:00",
                "end": "2024-07-02 00:20:00",
                "duration_min": "20",
                "min_dist": "95",
                "length": "220",
            },
        ],
    )
    write_csv(
        registry_csv,
        [
            {
                "MMSI": "111",
                "Name": "CT Alice",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "Total_Dwell_Time": "75",
                "Event_Count": "2",
                "length": "25",
            },
            {
                "MMSI": "222",
                "Name": "Bulk Maybe",
                "Ship type": "Cargo",
                "wind_farm": "Beta",
                "Total_Dwell_Time": "20",
                "Event_Count": "1",
                "length": "220",
            },
        ],
    )

    summary = audit_event_outputs(events_csv, registry_csv)

    assert summary["row_counts"]["events"] == 3
    assert summary["duration_min"]["median"] == 30
    assert summary["min_dist_m"]["max"] == 95
    assert summary["events_by_wind_farm"][0]["wind_farm"] == "Alpha"
    assert summary["events_by_wind_farm"][0]["event_count"] == 2
    assert summary["top_mmsis_by_dwell_time"][0]["mmsi"] == 111
    assert summary["top_mmsis_by_dwell_time"][0]["total_dwell_min"] == 75
    assert summary["suspect_vessels"]["suspect_count"] == 1
    assert "suspect ship_type" in summary["suspect_vessels"]["examples"][0]["reason"]
    assert "length above" in summary["suspect_vessels"]["examples"][0]["reason"]

    text = format_qa_summary(summary)
    assert "Pilot event QA summary" in text
    assert "Top MMSIs by dwell time" in text


def test_impossible_event_jumps_use_coordinates_when_available(tmp_path):
    events_csv = tmp_path / "OM_Events_July_2024.csv"
    registry_csv = tmp_path / "Fleet_Registry_July_2024.csv"

    write_csv(
        events_csv,
        [
            {
                "MMSI": "333",
                "Name": "Fast Claim",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "found_id": "A01",
                "start": "2024-07-01 00:00:00",
                "end": "2024-07-01 00:30:00",
                "duration_min": "30",
                "min_dist": "10",
                "length": "30",
                "latitude": "54.0",
                "longitude": "7.0",
            },
            {
                "MMSI": "333",
                "Name": "Fast Claim",
                "Ship type": "Service Vessel",
                "wind_farm": "Far Farm",
                "found_id": "F01",
                "start": "2024-07-01 00:40:00",
                "end": "2024-07-01 01:00:00",
                "duration_min": "20",
                "min_dist": "15",
                "length": "30",
                "latitude": "55.0",
                "longitude": "8.0",
            },
        ],
    )
    write_csv(
        registry_csv,
        [
            {
                "MMSI": "333",
                "Name": "Fast Claim",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "Total_Dwell_Time": "50",
                "Event_Count": "2",
                "length": "30",
            },
        ],
    )

    summary = audit_event_outputs(
        events_csv,
        registry_csv,
        config=EventQaConfig(jump_speed_knots=45),
    )

    jumps = summary["impossible_event_jumps"]
    assert jumps["available"] is True
    assert jumps["jump_count"] == 1
    assert jumps["examples"][0]["reason"] == "required speed exceeds threshold"
    assert jumps["examples"][0]["required_speed_knots"] > 45


def test_impossible_event_jumps_report_skip_without_enough_data(tmp_path):
    events_csv = tmp_path / "OM_Events_July_2024.csv"
    registry_csv = tmp_path / "Fleet_Registry_July_2024.csv"

    write_csv(
        events_csv,
        [
            {
                "MMSI": "444",
                "Name": "Normal",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "found_id": "A01",
                "start": "2024-07-01 00:00:00",
                "end": "2024-07-01 00:30:00",
                "duration_min": "30",
                "min_dist": "10",
                "length": "30",
            },
            {
                "MMSI": "444",
                "Name": "Normal",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "found_id": "A02",
                "start": "2024-07-01 02:00:00",
                "end": "2024-07-01 02:30:00",
                "duration_min": "30",
                "min_dist": "10",
                "length": "30",
            },
        ],
    )
    write_csv(
        registry_csv,
        [
            {
                "MMSI": "444",
                "Name": "Normal",
                "Ship type": "Service Vessel",
                "wind_farm": "Alpha",
                "Total_Dwell_Time": "60",
                "Event_Count": "2",
                "length": "30",
            },
        ],
    )

    summary = audit_event_outputs(events_csv, registry_csv)

    jumps = summary["impossible_event_jumps"]
    assert jumps["available"] is False
    assert jumps["jump_count"] == 0
    assert "need coordinate columns" in jumps["reason"]
