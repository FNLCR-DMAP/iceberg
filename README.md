# Apache Iceberg ETL Demo (Lean Version)

Single, working demonstration of Apache Iceberg features (time travel, branching, schema evolution, ACID) via the script `etl_pipeline_demo.py`.

## Quick Start
```bash
python -m venv .venv && . .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
python scripts/download_iceberg_jars.py
python etl_pipeline_demo.py
```

## What You Get
- Raw → Intermediate → Final metric tables
- Time travel snapshot comparison
- Branching (dev + staging) using Iceberg refs
- Schema evolution (adds `discount_amount`)
- Snapshot / files metadata inspection

## Requirements
Version-pinned baseline (reproducible):

| Component | Version | Pin / Artifact | Notes |
|-----------|---------|----------------|-------|
| Python | 3.9.x | `environment-minimal.yml` | Tested with 3.9.23 |
| Java (OpenJDK) | 11.x | `java -version` | JDK 17 also works |
| PySpark | 3.4.1 | `pyspark==3.4.1` | Ships Scala 2.12 jars |
| Scala runtime | 2.12.17 | `scala-library-2.12.17.jar` | Must match `_2.12` in Iceberg JAR name |
| Iceberg runtime | 1.4.2 | `iceberg-spark-runtime-3.4_2.12-1.4.2.jar` | Auto-downloaded |
| PyIceberg | 0.5.1 | `pyiceberg==0.5.1` | Python client utilities |
| Pandas | 2.0.3 | `pandas==2.0.3` | Convenience only |
| Jupyter (meta) | 1.0.0 | `jupyter==1.0.0` | Optional (not used by script) |

The `scripts/download_iceberg_jars.py` script picks the correct runtime JAR for the pinned PySpark/Scala combination.

## Adjusting Versions
| Target | New Versions | Actions |
|--------|--------------|---------|
| Spark + Iceberg upgrade | PySpark 3.5.1 + Iceberg 1.10.0 | `pip install pyspark==3.5.1`; ensure Scala 2.13; download `iceberg-spark-runtime-3.5_2.13-1.10.0.jar` |
| PyIceberg only | 0.5.2+ | Bump `pyiceberg` pin in `requirements.txt` / env file |
| Java | 17 | Use JDK 17; no code changes expected |

After changing PySpark version always re-run the download script and remove outdated JARs to avoid mixed Scala artifacts.

## More Detail
See `SETUP.md` for: environment creation, troubleshooting, CI hints, and optional Hadoop-only vs Hive-backed catalog notes.

## Cleanup
```bash
rm -rf iceberg_warehouse *.pyc .venv
```

Enjoy exploring Iceberg! If something breaks, open an issue with your Spark + PySpark + Scala + Iceberg versions.

## Docker Usage

Build the image:
```bash
docker build -t iceberg-demo .
```

Run the demo (warehouse persisted to a host directory):
```bash
docker run --rm -v "$PWD/iceberg_warehouse:/app/iceberg_warehouse" \
	--name iceberg-etl iceberg-demo
```

Override command (e.g., open a shell):
```bash
docker run --rm -it iceberg-demo bash
```

Use a custom Iceberg runtime jar (mount + env var):
```bash
docker run --rm -v "$PWD/custom_jars:/jars_custom" \
	-e ICEBERG_JAR=/jars_custom/iceberg-spark-runtime-3.5_2.13-1.10.0.jar \
	iceberg-demo
```

Rebuild after dependency/version changes:
```bash
docker build --no-cache -t iceberg-demo .
```

The container runs `python etl_pipeline_demo.py` by default.

## Makefile & CI Helpers

A `Makefile` is included for convenience (all targets are thin wrappers around Docker commands):

| Target | Command Performed | Notes |
|--------|-------------------|-------|
| `make build` | `docker build -t iceberg-demo .` | Accepts `PYTHON_VERSION` & `IMAGE` overrides |
| `make run` | Run container and persist warehouse volume | Uses `./iceberg_warehouse` on host |
| `make shell` | Open interactive bash shell | Good for ad‑hoc Spark SQL |
| `make warehouse` | Recreate (clean) local `iceberg_warehouse/` dir | Does not touch image |
| `make clean` | Remove local image `iceberg-demo` | Ignores errors if missing |

Examples:
```bash
# Build with a different Python base (example: 3.10-slim)
make build PYTHON_VERSION=3.10-slim IMAGE=iceberg-demo:py310

# Run the ETL and persist data
make run

# Interactive exploration
make shell

# Reset the local warehouse (start fresh)
make warehouse
```

You can also override the image name when running:
```bash
IMAGE=my-registry/iceberg-demo:latest make build
```

### Multi-Arch (Optional)
If you need a multi-architecture build (e.g., for both `amd64` and `arm64`):
```bash
docker buildx create --use --name iceberg-builder || true
docker buildx build --platform linux/amd64,linux/arm64 \
	-t my-org/iceberg-demo:latest --push .
```

### CI Workflow
The GitHub Actions workflow at `.github/workflows/docker-ci.yml`:
1. Sets up Buildx & QEMU
2. Builds the image (local load only, not pushing)
3. Runs a tiny smoke check (imports PySpark, starts the demo and truncates output)

If you later want to push to a registry, add a login step (e.g., `docker/login-action`) and set `push: true` plus tags.

### Upgrading Components
For a PySpark + Iceberg upgrade:
1. Update `pyspark` in `requirements.txt`
2. Rebuild (`make build`)
3. Verify the new Iceberg runtime JAR auto-download; if not, adjust `scripts/download_iceberg_jars.py`
4. Run `make run` and validate snapshots & features

Open an issue if you want an automated matrix (Spark/Iceberg) CI workflow; it's easy to extend from the current single-job file.