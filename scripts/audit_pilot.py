import argparse
import json

from om_pipeline.analysis.event_qa import EventQaConfig, audit_event_outputs, format_qa_summary


def main():
    parser = argparse.ArgumentParser(
        description="Audit pilot O&M dwell event and fleet registry CSV outputs."
    )
    parser.add_argument("events_csv", help="Path to an OM_Events_*.csv file.")
    parser.add_argument("registry_csv", help="Path to a Fleet_Registry_*.csv file.")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a JSON summary instead of plain text.",
    )
    parser.add_argument(
        "--top-mmsi-limit",
        type=int,
        default=10,
        help="Number of top dwell-time MMSIs to include.",
    )
    parser.add_argument(
        "--suspect-limit",
        type=int,
        default=20,
        help="Number of suspect vessel examples to include.",
    )
    parser.add_argument(
        "--jump-speed-knots",
        type=float,
        default=45.0,
        help="Required speed threshold for impossible event jump checks.",
    )
    args = parser.parse_args()

    config = EventQaConfig(
        top_mmsi_limit=args.top_mmsi_limit,
        suspect_limit=args.suspect_limit,
        jump_speed_knots=args.jump_speed_knots,
    )
    summary = audit_event_outputs(args.events_csv, args.registry_csv, config=config)

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(format_qa_summary(summary))


if __name__ == "__main__":
    main()
