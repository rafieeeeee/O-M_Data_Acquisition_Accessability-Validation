# Copernicus Marine Credentials

Do not commit credentials.

Use one of these local credential paths:

1. `copernicusmarine login`
2. Environment variables:
   - `COPERNICUSMARINE_USERNAME`
   - `COPERNICUSMARINE_PASSWORD`
3. A local ignored `.env` file loaded by your shell or notebook environment.

Add any local credential files to `.gitignore`.

Never place a Copernicus Marine username or password in Python scripts, notebooks,
YAML configs, or committed JSON request files.

The Baltic downloader reads credentials only from the existing
`copernicusmarine` login/config or the two environment variables above. It never
prints credential values.
