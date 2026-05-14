#!/usr/bin/env python3
"""Gera a referencia CID-10 para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/saude/cid10"
CURRENT_DIR = REFERENCE_DIR / "current"
SOURCE_DIR = REFERENCE_DIR / "source"
REFERENCE_MANIFEST = REFERENCE_DIR / "manifest.json"
GLOBAL_MANIFEST = PUBLISH_ROOT / "manifest.json"

SOURCES = {
    "capitulos": {
        "url": "https://raw.githubusercontent.com/bigdata-icict/ETL-Dataiku-DSS/master/SIM/cid10_tabela_capitulos.csv",
        "csv": "cid10_tabela_capitulos.csv",
        "parquet": "capitulos.parquet",
    },
    "grupos": {
        "url": "https://raw.githubusercontent.com/bigdata-icict/ETL-Dataiku-DSS/master/SIM/cid10_tabela_grupos.csv",
        "csv": "cid10_tabela_grupos.csv",
        "parquet": "grupos.parquet",
    },
    "categorias": {
        "url": "https://raw.githubusercontent.com/bigdata-icict/ETL-Dataiku-DSS/master/SIM/CID-10-CATEGORIAS.CSV.utf8",
        "csv": "CID-10-CATEGORIAS.CSV.utf8",
        "parquet": "categorias.parquet",
    },
    "subcategorias": {
        "url": "https://raw.githubusercontent.com/bigdata-icict/ETL-Dataiku-DSS/master/SIM/CID-10-SUBCATEGORIAS.CSV.utf8",
        "csv": "CID-10-SUBCATEGORIAS.CSV.utf8",
        "parquet": "subcategorias.parquet",
    },
}

REFERENCE_PATHS = {
    "cnes_estabelecimentos": "referencias/cnes/estabelecimentos/manifest.json",
    "uf": "referencias/ibge/uf/manifest.json",
    "municipios": "referencias/ibge/municipios/manifest.json",
    "populacao": "referencias/ibge/populacao/manifest.json",
    "populacao_raca_censo": "referencias/ibge/populacao_raca_censo/manifest.json",
    "matriz_pesos_raca": "referencias/ibge/matriz_pesos_raca/manifest.json",
    "cid10": "referencias/saude/cid10/manifest.json",
    "calendario_epidemiologico": (
        "referencias/vigilancia/calendario_epidemiologico/manifest.json"
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url: str, path: Path, overwrite: bool = False) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        return path

    request = Request(url, headers={"User-Agent": "vigisus-data-pipeline"})
    with urlopen(request, timeout=120) as response:
        content = response.read()

    temp_path = path.with_suffix(path.suffix + ".part")
    temp_path.write_bytes(content)
    temp_path.replace(path)
    return path


def normalize_columns(columns: list[str]) -> list[str]:
    return [column.strip().lower() for column in columns]


def read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        sep=";",
        encoding="utf-8-sig",
        dtype="string",
        keep_default_na=False,
        lineterminator="\n",
    )
    df.columns = normalize_columns(list(df.columns))
    df = df.rename(columns={column: column.replace("\r", "") for column in df.columns})
    df = df[[column for column in df.columns if column]]
    for column in df.columns:
        df[column] = df[column].astype("string").str.replace("\r", "", regex=False)
    return df


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([pa.field(column, pa.string()) for column in df.columns])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, path)


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


def publish(overwrite: bool = False) -> dict[str, dict[str, object]]:
    files = {}
    for table_name, config in SOURCES.items():
        csv_path = SOURCE_DIR / str(config["csv"])
        parquet_path = CURRENT_DIR / str(config["parquet"])

        download(str(config["url"]), csv_path, overwrite=overwrite)
        df = read_csv(csv_path)
        write_parquet(df, parquet_path)

        files[table_name] = {
            "csv_path": csv_path.relative_to(PUBLISH_ROOT).as_posix(),
            "parquet_path": parquet_path.relative_to(PUBLISH_ROOT).as_posix(),
            "source_url": config["url"],
            "sha256": sha256_file(parquet_path),
            "csv_sha256": sha256_file(csv_path),
            "rows": int(len(df)),
            "columns": list(df.columns),
        }

    return files


def write_reference_manifest(files: dict[str, dict[str, object]]) -> None:
    manifest = {
        "reference_id": "cid10",
        "title": "CID-10",
        "version": "static",
        "generated_at_utc": utc_now(),
        "source": "bigdata-icict/ETL-Dataiku-DSS SIM CID-10 CSV files",
        "update_policy": "Referencia estatica sem cron; copia mantida no repositorio.",
        "files": files,
    }
    write_json_if_changed(REFERENCE_MANIFEST, manifest)
    write_global_manifest()


def validate(files: dict[str, dict[str, object]]) -> None:
    for table_name, metadata in files.items():
        path = PUBLISH_ROOT / str(metadata["parquet_path"])
        df = pd.read_parquet(path, engine="pyarrow")
        if df.empty:
            raise ValueError(f"Tabela CID-10 vazia: {table_name}")
        if list(df.columns) != metadata["columns"]:
            raise ValueError(f"Colunas invalidas na tabela CID-10: {table_name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera referencia CID-10 em Parquet.")
    parser.add_argument("--overwrite", action="store_true", help="Baixa novamente os CSVs.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        files = publish(overwrite=args.overwrite)
        write_reference_manifest(files)
        validate(files)
    except Exception as exc:
        print(f"Erro ao gerar CID-10: {exc}")
        return 1

    print("CID-10 publicado.")
    print(f"Tabelas: {len(files)}")
    print(f"Manifest: {REFERENCE_MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
