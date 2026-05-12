# Metocean Data Acquisition Guide

This document details the acquisition paths for FINO1 and NORA3 wave data to support joining AIS dwell events with environmental conditions.

## 1. FINO1 (In-Situ Ground Truth)

FINO1 provides high-fidelity, 10-minute wave records at the Alpha Ventus pilot site.

- **Access Method:** BSH specialist procedure "Insitu".
- **Portal:** [BSH Service Portal](https://fino.bsh.de/) (Direct access via [Insitu Portal](https://fino.bsh.de/))
- **Credentials:** Requires registration on the **BSH-Login** system and specific request for "Insitu" specialist procedure access.
- **Variables:**
    - `Hs`: Significant wave height (m).
    - `Tp`: Peak wave period (s).
    - `theta`: Mean wave direction (degrees, from which waves arrive).
- **Format:** CSV or ASCII (preferred for 10-minute alignment).
- **Resolution:** 10-minute intervals.
- **License:** Free for research and scientific purposes; requires source acknowledgment (BSH and FINO project). **Note:** Final license terms require confirmation after BSH Insitu access is granted.

## 2. NORA3 (Regional Hindcast)

NORA3 provides regional wave fields and serves as a backup or scaling source.

- **Access Method:** MET Norway THREDDS Data Server.
- **Base URL:** [thredds.met.no](https://thredds.met.no/thredds/catalog/windsurfer/mywavewam3km_files/catalog.html)
- **Usage/Licensing Guidance:** [MET Norway Data Policy](https://www.met.no/en/free-met-data/Licensing) (Creative Commons Attribution 4.0 International).
- **Protocols:** OPeNDAP or NCSS (NetCDF Subset Service).
- **Variables:**
    - `hs`: Significant wave height (m).
    - `tp`: Peak wave period (s).
    - `mwd`: Mean wave direction (degrees).
- **Spatial Resolution:** 3 km grid.
- **Temporal Resolution:** 1-hour intervals.
- **Download Strategy:** 
    - Use `metocean-api` or OPeNDAP to extract specific coordinates.
    - **Policy:** One-at-a-time requests with identifying User-Agent and local caching. Access must be rate-limited, serialized, and cache-aware following MET Norway terms.
- **Interpolation:** Use cubic spline interpolation to upscale 1-hour blocks to the 10-minute backbone.

## 3. Recommended Implementation Strategy

### Pilot Validation (Alpha Ventus)
- **Primary Source:** **FINO1** (10-minute ground truth).
- **Secondary Source:** **NORA3** (Used to validate spatial consistency and fill gaps).

### Temporal Alignment
- AIS dwell events should be snapped to the rigid 10-minute temporal grid.
- Metocean data must be aligned to the same grid.
- **Directional Data:** Treat wave direction $\theta$ as circular. Do not interpolate raw degrees directly; convert to unit vectors ($u = \cos(\theta), v = \sin(\theta)$), interpolate vectors, then reconstruct the angle.

## 4. Blockers & Action Items
- [ ] **Human Action Required:** Register for a BSH-Login account and request "Insitu" access.
- [ ] **Technical Task:** Implement the circular interpolation utility for NORA3 upscaling.
