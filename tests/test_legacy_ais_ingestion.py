import csv
import io
import zipfile

from om_pipeline.ingestion.ais import filter_zip_to_writer


def test_filter_headerless_legacy_ais_zip(tmp_path):
    csv_file = tmp_path / "legacy.csv"
    rows = [
        [
            "01/07/2015 00:00:00",
            "Class A",
            "246479000",
            "57,546333",
            "5,894700",
            "Under way using engine",
            "0,0",
            "0,4",
            "167,0",
            "168",
            "Unknown",
            "",
            "LEGACY VESSEL",
            "Undefined",
            "",
            "",
            "",
            "Undefined",
            "",
            "",
            "",
            "AIS",
        ]
    ]
    with csv_file.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)

    zip_path = tmp_path / "legacy.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(csv_file, arcname="legacy.csv")

    output = io.StringIO()
    writer = csv.writer(output)
    stats, _ = filter_zip_to_writer(
        zip_path,
        writer,
        True,
        bounds=(46.5, 60.0, -4.5, 15.0),
        max_sog=2.0,
    )

    assert stats["seen"] == 1
    assert stats["kept"] == 1
    assert output.getvalue().splitlines()[0].startswith("Timestamp,Type of mobile,MMSI")
    assert "246479000" in output.getvalue()
