"""Pre-build standalone deployment archives for AWS Glue (--extra-py-files) and AWS Lambda
using `uv` cross-platform wheel installation so neither AWS Glue nor AWS Lambda ever runs
`pip install` at runtime.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT_DIR / "dist"
BUILD_TMP = ROOT_DIR / "build" / "uv_staging"


def run_cmd(cmd: list[str]) -> None:
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=ROOT_DIR, check=True)


def zip_directory(source_dir: Path, output_zip: Path) -> None:
    output_zip.parent.mkdir(parents=True, exist_ok=True)
    if output_zip.exists():
        output_zip.unlink()
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path in sorted(source_dir.rglob("*")):
            if file_path.is_file():
                rel = file_path.relative_to(source_dir)
                if "__pycache__" in rel.parts or file_path.suffix == ".pyc":
                    continue
                zf.write(file_path, rel.as_posix())
    print(f"Created archive: {output_zip} ({output_zip.stat().st_size:,} bytes)")


def build_glue_dependencies_zip(uv_cmd: list[str]) -> Path:
    """Build dist/glue_dependencies.zip containing our project package + pre-built Linux wheels
    (pydantic, pydantic-core, urllib3<2, requests, etc.) for AWS Glue 4.0 (Python 3.10, x86_64 Linux).
    """
    glue_stage = BUILD_TMP / "glue_stage"
    if glue_stage.exists():
        shutil.rmtree(glue_stage)
    glue_stage.mkdir(parents=True, exist_ok=True)

    # 1. Install third-party wheels for AWS Glue 4.0 (Python 3.10 on x86_64 manylinux2014)
    run_cmd(
        [
            *uv_cmd,
            "pip",
            "install",
            "--target",
            str(glue_stage),
            "--python-version",
            "3.10",
            "--python-platform",
            "x86_64-manylinux2014",
            "--only-binary",
            ":all:",
            "pydantic>=2.6.0",
            "urllib3<2.0.0",
            "requests>=2.31.0",
        ]
    )

    # 2. Copy our project's glue_jobs package into the top level of the bundle
    shutil.copytree(ROOT_DIR / "src" / "glue_jobs", glue_stage / "glue_jobs", dirs_exist_ok=True)

    output_zip = DIST_DIR / "glue_dependencies.zip"
    zip_directory(glue_stage, output_zip)
    return output_zip


def build_lambda_deployment_zip(uv_cmd: list[str]) -> Path:
    """Build dist/ingestion_lambda.zip containing handler.py + any requirements in
    src/ingestion_lambda/requirements.txt (excluding boto3/botocore which are built into the
    AWS Lambda Python 3.12 runtime) for x86_64 Linux.
    """
    lambda_stage = BUILD_TMP / "lambda_stage"
    if lambda_stage.exists():
        shutil.rmtree(lambda_stage)
    lambda_stage.mkdir(parents=True, exist_ok=True)

    req_file = ROOT_DIR / "src" / "ingestion_lambda" / "requirements.txt"
    third_party_reqs = [
        line.strip()
        for line in req_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
        and not line.strip().startswith("#")
        and not line.strip().lower().startswith(("boto3", "botocore"))
    ]

    if third_party_reqs:
        run_cmd(
            [
                *uv_cmd,
                "pip",
                "install",
                "--target",
                str(lambda_stage),
                "--python-version",
                "3.12",
                "--python-platform",
                "x86_64-manylinux2014",
                "--only-binary",
                ":all:",
                *third_party_reqs,
            ]
        )

    shutil.copy2(ROOT_DIR / "src" / "ingestion_lambda" / "handler.py", lambda_stage / "handler.py")
    shutil.copy2(ROOT_DIR / "src" / "ingestion_lambda" / "__init__.py", lambda_stage / "__init__.py")

    output_zip = DIST_DIR / "ingestion_lambda.zip"
    zip_directory(lambda_stage, output_zip)
    return output_zip


def main() -> None:
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    uv_bin = shutil.which("uv")
    uv_cmd = [uv_bin] if uv_bin else [sys.executable, "-m", "uv"]

    # Also build the pure-Python project wheel into dist/
    run_cmd([*uv_cmd, "build", "--wheel", "--out-dir", str(DIST_DIR)])

    build_glue_dependencies_zip(uv_cmd)
    build_lambda_deployment_zip(uv_cmd)

    if BUILD_TMP.exists():
        shutil.rmtree(BUILD_TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
