#!/usr/bin/env python3
"""Gera calendario epidemiologico deterministico para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/vigilancia/calendario_epidemiologico"
CURRENT_DIR = REFERENCE_DIR / "current"
PARQUET_PATH = CURRENT_DIR / "calendario_epidemiologico.parquet"
REFERENCE_MANIFEST = REFERENCE_DIR / "manifest.json"
GLOBAL_MANIFEST = PUBLISH_ROOT / "manifest.json"

REFERENCE_PATHS = {
    "cnes_estabelecimentos": "referencias/cnes/estabelecimentos/manifest.json",
    "uf": "referencias/ibge/uf/manifest.json",
    "municipios": "referencias/ibge/municipios/manifest.json",
    "populacao": "referencias/ibge/populacao/manifest.json",
    "populacao_raca_censo": "referencias/ibge/populacao_raca_censo/manifest.json",
    "matriz_pesos_raca": "referencias/ibge/matriz_pesos_raca/manifest.json",
    "calendario_epidemiologico": (
        "referencias/vigilancia/calendario_epidemiologico/manifest.json"
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def epidemiological_year_start(year: int) -> date:
    """Domingo que inicia a semana epidemiologica 1 do ano informado."""
    jan_4 = date(year, 1, 4)
    days_since_sunday = (jan_4.weekday() + 1) % 7
    return jan_4 - timedelta(days=days_since_sunday)


def epidemiological_year_and_week(day: date) -> tuple[int, int]:
    year = day.year
    start = epidemiological_year_start(year)

    if day < start:
        year -= 1
        start = epidemiological_year_start(year)
    elif day >= epidemiological_year_start(year + 1):
        year += 1
        start = epidemiological_year_start(year)

    week = ((day - start).days // 7) + 1
    return year, week


def build_calendar(start_year: int, end_year: int) -> pd.DataFrame:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    days = pd.date_range(start=start, end=end, freq="D")

    rows = []
    for timestamp in days:
        current_date = timestamp.date()
        ano_epi, semana_epi = epidemiological_year_and_week(current_date)
        rows.append(
            {
                "data": current_date,
                "ano": current_date.year,
                "mes": current_date.month,
                "dia": current_date.day,
                "ano_epi": ano_epi,
                "semana_epi": semana_epi,
                "ano_semana_epi": f"{ano_epi}{semana_epi:02d}",
                "ano_semana_epi_num": (ano_epi * 100) + semana_epi,
            }
        )

    return pd.DataFrame(rows)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_if_changed(path: Path, payload: dict) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return False
    path.write_text(content, encoding="utf-8")
    return True


def write_global_manifest() -> None:
    manifest = {
        "generated_at_utc": utc_now(),
        "references": REFERENCE_PATHS,
    }
    write_json_if_changed(GLOBAL_MANIFEST, manifest)


def read_existing_file_metadata() -> tuple[str | None, int | None]:
    if not REFERENCE_MANIFEST.exists():
        return None, None

    with REFERENCE_MANIFEST.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)

    file_info = manifest.get("file")
    if not isinstance(file_info, dict):
        return None, None

    sha = file_info.get("sha256")
    rows = file_info.get("rows")
    return (sha if isinstance(sha, str) else None, rows if isinstance(rows, int) else None)


def publish_calendar(df: pd.DataFrame, start_year: int, end_year: int) -> bool:
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = PARQUET_PATH.with_suffix(".parquet.tmp")

    df.to_parquet(temp_path, index=False, engine="pyarrow")
    new_sha = sha256_file(temp_path)
    new_rows = len(df)
    old_sha, old_rows = read_existing_file_metadata()

    changed = new_sha != old_sha or new_rows != old_rows or not PARQUET_PATH.exists()
    if changed:
        temp_path.replace(PARQUET_PATH)
    else:
        temp_path.unlink()

    if changed or not REFERENCE_MANIFEST.exists():
        manifest = {
            "reference_id": "calendario_epidemiologico",
            "title": "Calendario Epidemiologico",
            "version": f"{start_year}-{end_year}",
            "generated_at_utc": utc_now(),
            "rule": (
                "Semanas epidemiologicas de domingo a sabado; a semana 1 e a "
                "semana que contem a maioria dos dias em janeiro."
            ),
            "file": {
                "path": PARQUET_PATH.relative_to(PUBLISH_ROOT).as_posix(),
                "sha256": new_sha,
                "rows": new_rows,
            },
        }
        write_json_if_changed(REFERENCE_MANIFEST, manifest)
        write_global_manifest()

    return changed


def validate_calendar() -> None:
    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
    expected_columns = [
        "data",
        "ano",
        "mes",
        "dia",
        "ano_epi",
        "semana_epi",
        "ano_semana_epi",
        "ano_semana_epi_num",
    ]
    if list(df.columns) != expected_columns:
        raise ValueError(f"Colunas invalidas: {list(df.columns)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera calendario epidemiologico em Parquet para o VigiSUS-BR."
    )
    parser.add_argument("--start-year", type=int, default=1900)
    parser.add_argument("--end-year", type=int, default=2100)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        df = build_calendar(args.start_year, args.end_year)
        changed = publish_calendar(df, args.start_year, args.end_year)
        validate_calendar()
    except Exception as exc:
        print(f"Erro ao gerar calendario epidemiologico: {exc}")
        return 1

    status = "atualizado" if changed else "sem alteracao"
    print(f"Calendario epidemiologico {status}: {PARQUET_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
