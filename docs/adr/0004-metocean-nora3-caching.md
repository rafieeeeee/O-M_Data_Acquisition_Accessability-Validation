# 0004. Metocean NORA3 Caching and Interpolation Policy

## Status
Accepted

## Context
The O&M AIS pipeline identifies thousands of unique vessel "dwell events" at offshore wind foundations. To evaluate workability limits, each event must be decorated with continuous, high-fidelity Metocean conditions. The first implemented backbone covers wave variables only ($H_s$, $T_p$, wave direction). Wind and current are required for the final multi-parameter model, but are intentionally staged after the wave-only backbone is QA-stable.

We rely on the MET Norway **NORA3** hindcast dataset, accessed remotely via an OPeNDAP/THREDDS server. Because NORA3 data is natively provided at a 1-hour temporal resolution—whereas our downstream SCADA and FINO1 data uses a rigid 10-minute grid—we must download and upscale the NORA3 data.

Initial approaches pulled exactly the start/end window for every event. This led to three major problems:
1. **Network Thrashing**: Thousands of discrete events within the same month resulted in thousands of overlapping OPeNDAP requests for the same spatial coordinates, severely bottlenecking extraction and threatening rate limits.
2. **Interpolation Edge Effects**: When down-selecting exact boundaries prior to upscaling, the boundaries lacked mathematical "brackets," distorting the cubic spline interpolation at the very beginning and end of the events.
3. **Cross-Month Boundary Loss**: Events crossing a calendar month boundary, or terminating in the final minutes of a month, dropped data because temporal `xarray.sel()` boundaries were misaligned with event reality.

## Decision
We have established a rigid architectural contract for NORA3 extraction and interpolation, divided into two distinct responsibilities:

1. **The Monthly Fetch & Cache (`src/om_pipeline/ingestion/nora3.py`)**
   - The fetcher natively requests a **full calendar month** starting from midnight on the 1st of the event's month, ending at midnight on the 1st of the following month.
   - It explicitly adds a **2-hour padding tail** (`+ pd.Timedelta(hours=2)`) to the fetch boundary.
   - It filters the OPeNDAP request immediately to only download required wave variables (`hs`, `tp`, `thq`/`wave_direction`).
   - This full array is cached locally to disk (`.csv`). Any subsequent event at the same foundation within that month hits the local cache instantly.
   - The fetcher returns a subset to the orchestrator that spans the exact event window *plus* a strict 2-hour bracket on both sides.

2. **The Orchestrator & Upscaler (`scripts/extract_metocean.py`)**
   - The orchestrator batches events by `found_id`, `YEAR(start)`, and `MONTH(start)`. This prevents multi-year continuous extraction blocks.
   - It runs the 2-hour padded subset through the upscaler, which applies **Cubic Splines** to scalars ($H_s, T_p$) and **Circular Vector Interpolation** to the wave direction.
   - *Only after interpolation is complete* does the orchestrator clip the array back to the strict `start <= time <= end` bounds of the actual vessel event.

## Consequences
- **Positive:** Network requests are minimized to at most 1 per foundation-month.
- **Positive:** Interpolation is mathematically sound across the entirety of every event due to the guaranteed 2-hour bracketing.
- **Positive:** Data integrity is maintained across calendar month boundaries.
- **Negative:** The local cache uses slightly more disk space by caching the entire month (744 hours per spatial point) rather than purely the event windows. Given the minimal size of single-coordinate CSV files, this is considered a negligible tradeoff for the massive performance gains.

## Future Agents Note
Do not modify the separation of concerns between `extract_metocean.py` and `nora3.py`. `nora3.py` is responsible for handling the cache hits and providing mathematically safe bracketed windows. `extract_metocean.py` is responsible for performing the interpolation and making the final strict temporal slice.

Wind and current should be added as a schema extension, not by replacing the wave contract. Wind should preferably be represented as vector components during interpolation (`u10`/`v10` or equivalent), then reconstructed to speed/direction if needed. Current should follow the same vector-first rule when a suitable hindcast/source is selected. Update QA checks and downstream join schemas when these variables are introduced.
