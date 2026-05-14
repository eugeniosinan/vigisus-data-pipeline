#!/usr/bin/env python3
"""Gera referencia de bairros do Censo 2022 para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from zipfile import ZipFile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_URL = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/"
    "Agregados_por_Setores_Censitarios/Agregados_por_Bairro_csv/"
    "Agregados_por_bairros_basico_BR_20250417.zip"
)
ZIP_NAME = "Agregados_por_bairros_basico_BR_20250417.zip"
CSV_NAME = "Agregados_por_bairros_basico_BR.csv"

PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/ibge/bairros_censo"
SOURCE_DIR = REFERENCE_DIR / "source"
CURRENT_DIR = REFERENCE_DIR / "current"
ZIP_PATH = SOURCE_DIR / ZIP_NAME
CSV_PATH = SOURCE_DIR / CSV_NAME
PARQUET_PATH = CURRENT_DIR / "bairros_censo_2022.parquet"
REFERENCE_MANIFEST = REFERENCE_DIR / "manifest.json"
GLOBAL_MANIFEST = PUBLISH_ROOT / "manifest.json"

REFERENCE_PATHS = {
    "cnes_estabelecimentos": "referencias/cnes/estabelecimentos/manifest.json",
    "uf": "referencias/ibge/uf/manifest.json",
    "municipios": "referencias/ibge/municipios/manifest.json",
    "populacao": "referencias/ibge/populacao/manifest.json",
    "populacao_raca_censo": "referencias/ibge/populacao_raca_censo/manifest.json",
    "matriz_pesos_raca": "referencias/ibge/matriz_pesos_raca/manifest.json",
    "bairros_censo": "referencias/ibge/bairros_censo/manifest.json",
    "cid10": "referencias/saude/cid10/manifest.json",
    "calendario_epidemiologico": (
        "referencias/vigilancia/calendario_epidemiologico/manifest.json"
    ),
}

TEXT_COLUMNS = [
    "cd_bairro",
    "nm_bairro",
    "cd_regiao",
    "nm_regiao",
    "cd_uf",
    "nm_uf",
    "cd_mun",
    "nm_mun",
    "cd_dist",
    "nm_dist",
    "cd_subdist",
    "nm_subdist",
    "cd_nu",
    "nm_nu",
    "cd_aglom",
    "nm_aglom",
    "cd_rgint",
    "nm_rgint",
    "cd_rgi",
    "nm_rgi",
    "cd_concurb",
    "nm_concurb",
]
INTEGER_COLUMNS = ["v0001", "v0002", "v0003", "v0004", "v0007"]
FLOAT_COLUMNS = ["area_km2", "v0005", "v0006"]
OUTPUT_COLUMNS = TEXT_COLUMNS + FLOAT_COLUMNS[:1] + INTEGER_COLUMNS[:4] + FLOAT_COLUMNS[1:] + ["v0007"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(overwrite: bool = False) -> Path:
    ZIP_PATH.parent.mkdir(parents=True, exist_ok=True)
    if ZIP_PATH.exists() and not overwrite:
        return ZIP_PATH

    request = Request(SOURCE_URL, headers={"User-Agent": "vigisus-data-pipeline"})
    with urlopen(request, timeout=180) as response:
        content = response.read()

    temp_path = ZIP_PATH.with_suffix(".zip.part")
    temp_path.write_bytes(content)
    temp_path.replace(ZIP_PATH)
    return ZIP_PATH


def extract_csv(zip_path: Path, overwrite: bool = False) -> Path:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    if CSV_PATH.exists() and not overwrite:
        return CSV_PATH

    with ZipFile(zip_path) as zip_file:
        if CSV_NAME not in zip_file.namelist():
            raise FileNotFoundError(f"{CSV_NAME} nao encontrado em {zip_path.name}.")
        with zip_file.open(CSV_NAME) as source:
            temp_path = CSV_PATH.with_suffix(".csv.part")
            temp_path.write_bytes(source.read())
            temp_path.replace(CSV_PATH)
    return CSV_PATH


def normalize_columns(columns: list[str]) -> list[str]:
    return [column.strip().lower() for column in columns]


def clean_text(value: object) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text in {"", "."}:
        return None
    return text


def read_bairros_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        dtype="string",
        keep_default_na=False,
        decimal=",",
    )
    df.columns = normalize_columns(list(df.columns))

    for column in TEXT_COLUMNS:
        df[column] = df[column].map(clean_text).astype("string")

    for column in INTEGER_COLUMNS:
        df[column] = pd.to_numeric(df[column].replace("", pd.NA), errors="coerce").astype(
            "Int64"
        )

    for column in FLOAT_COLUMNS:
        df[column] = pd.to_numeric(
            df[column].replace("", pd.NA).str.replace(",", ".", regex=False),
            errors="coerce",
        )

    return df[OUTPUT_COLUMNS].sort_values("cd_bairro").reset_index(drop=True)


def bairros_schema() -> pa.Schema:
    fields = [pa.field(column, pa.string()) for column in TEXT_COLUMNS]
    fields.append(pa.field("area_km2", pa.float64()))
    for column in ["v0001", "v0002", "v0003", "v0004"]:
        fields.append(pa.field(column, pa.int64()))
    fields.append(pa.field("v0005", pa.float64()))
    fields.append(pa.field("v0006", pa.float64()))
    fields.append(pa.field("v0007", pa.int64()))
    return pa.schema(fields)


def write_parquet(df: pd.DataFrame) -> None:
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=bairros_schema(), preserve_index=False)
    pq.write_table(table, PARQUET_PATH)


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


def write_reference_manifest(rows: int) -> None:
    manifest = {
        "reference_id": "bairros_censo",
        "title": "IBGE Censo 2022 - Agregados por Bairros",
        "version": "2022-20250417",
        "generated_at_utc": utc_now(),
        "source": SOURCE_URL,
        "update_policy": "Referencia estatica sem cron; copia mantida no repositorio.",
        "file": {
            "path": PARQUET_PATH.relative_to(PUBLISH_ROOT).as_posix(),
            "sha256": sha256_file(PARQUET_PATH),
            "rows": rows,
            "columns": OUTPUT_COLUMNS,
        },
        "source_files": {
            "zip_path": ZIP_PATH.relative_to(PUBLISH_ROOT).as_posix(),
            "zip_sha256": sha256_file(ZIP_PATH),
            "csv_path": CSV_PATH.relative_to(PUBLISH_ROOT).as_posix(),
            "csv_sha256": sha256_file(CSV_PATH),
        },
        "main_fields": {
            "neighborhood_name": "nm_bairro",
            "municipality_code": "cd_mun",
            "municipality_name": "nm_mun",
            "population_2022": "v0001",
        },
    }
    write_json_if_changed(REFERENCE_MANIFEST, manifest)
    write_global_manifest()


def validate() -> None:
    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
    if list(df.columns) != OUTPUT_COLUMNS:
        raise ValueError(f"Colunas invalidas: {list(df.columns)}")
    if df.empty:
        raise ValueError("Tabela de bairros vazia.")
    if "3304557001" not in set(df["cd_bairro"].dropna().astype(str)):
        raise ValueError("Registro de bairro esperado do Rio de Janeiro nao encontrado.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera referencia de bairros do Censo 2022.")
    parser.add_argument("--overwrite", action="store_true", help="Baixa novamente o ZIP.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        zip_path = download(overwrite=args.overwrite)
        csv_path = extract_csv(zip_path, overwrite=args.overwrite)
        df = read_bairros_csv(csv_path)
        write_parquet(df)
        write_reference_manifest(rows=int(len(df)))
        validate()
    except Exception as exc:
        print(f"Erro ao gerar bairros do Censo: {exc}")
        return 1

    print("Bairros do Censo publicados.")
    print(f"Linhas: {len(df)}")
    print(f"Manifest: {REFERENCE_MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
