#!/usr/bin/env python3
"""Gera a referencia oficial de estabelecimentos CNES para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import csv
import ftplib
import hashlib
import json
import re
import shutil
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/cnes"
FILE_PATTERN = re.compile(r"^BASE_DE_DADOS_CNES_(\d{6})\.zip$", re.IGNORECASE)
ESTABELECIMENTO_PATTERN = re.compile(
    r"^tbEstabelecimento(?P<competencia>\d{6})\.csv$", re.IGNORECASE
)

RAW_DIR = Path("data/raw/cnes/estabelecimentos")
PROCESSED_DIR = Path("data/processed/cnes/estabelecimentos")
PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/cnes/estabelecimentos"
CURRENT_DIR = REFERENCE_DIR / "current"
REFERENCE_MANIFEST = REFERENCE_DIR / "manifest.json"
GLOBAL_MANIFEST = PUBLISH_ROOT / "manifest.json"

DEFAULT_ENCODING = "latin-1"
CSV_SEPARATOR = ";"
CSV_QUOTECHAR = '"'
DEFAULT_RETRIES = 5
DEFAULT_RETRY_DELAY = 20
FTP_RETRY_ERRORS = ftplib.all_errors + (OSError,)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def retry_ftp(operation, description: str, retries: int = DEFAULT_RETRIES):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return operation()
        except FTP_RETRY_ERRORS as exc:
            last_error = exc
            if attempt == retries:
                break
            wait_seconds = DEFAULT_RETRY_DELAY * attempt
            print(
                f"{description} falhou na tentativa {attempt}/{retries}: {exc}. "
                f"Nova tentativa em {wait_seconds}s."
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"{description} falhou apos {retries} tentativas: {last_error}")


def find_latest_cnes_file(ftp: ftplib.FTP) -> str:
    candidates: list[tuple[str, str]] = []

    for name in ftp.nlst():
        file_name = Path(name).name
        match = FILE_PATTERN.match(file_name)
        if match:
            candidates.append((match.group(1), file_name))

    if not candidates:
        raise FileNotFoundError("Nenhum BASE_DE_DADOS_CNES_YYYYMM.zip encontrado.")

    return max(candidates, key=lambda item: item[0])[1]


def get_latest_remote_file() -> str:
    def operation() -> str:
        with ftplib.FTP(FTP_HOST, timeout=180) as ftp:
            ftp.login()
            ftp.cwd(FTP_DIR)
            return find_latest_cnes_file(ftp)

    return retry_ftp(operation, "Consulta do arquivo CNES mais recente")


def file_competencia(file_name: str) -> str:
    match = FILE_PATTERN.match(file_name)
    if not match:
        raise ValueError(f"Arquivo fora do padrao esperado: {file_name}")
    return match.group(1)


def read_reference_version(manifest_path: Path = REFERENCE_MANIFEST) -> str | None:
    if not manifest_path.exists():
        return None

    with manifest_path.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)

    version = manifest.get("version")
    return version if isinstance(version, str) else None


def write_global_manifest() -> None:
    PUBLISH_ROOT.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generated_at_utc": utc_now(),
        "references": {
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
        },
    }
    GLOBAL_MANIFEST.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def download_file(file_name: str, output_dir: Path = RAW_DIR, overwrite: bool = False) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / file_name

    if output_file.exists() and not overwrite:
        return output_file

    temp_file = output_file.with_suffix(output_file.suffix + ".part")
    if temp_file.exists():
        temp_file.unlink()

    def operation() -> Path:
        if temp_file.exists():
            temp_file.unlink()

        with ftplib.FTP(FTP_HOST, timeout=180) as ftp:
            ftp.login()
            ftp.cwd(FTP_DIR)

            with temp_file.open("wb") as file_handle:
                ftp.retrbinary(f"RETR {file_name}", file_handle.write)

        return temp_file

    retry_ftp(operation, f"Download de {file_name}")

    temp_file.replace(output_file)
    return output_file


def find_estabelecimento_member(zip_file: zipfile.ZipFile, competencia: str) -> str:
    for member in zip_file.namelist():
        member_name = Path(member).name
        match = ESTABELECIMENTO_PATTERN.match(member_name)
        if match and match.group("competencia") == competencia:
            return member

    raise FileNotFoundError(f"tbEstabelecimento{competencia}.csv nao encontrado.")


def extract_estabelecimento_csv(zip_path: Path, output_dir: Path = RAW_DIR) -> Path:
    competencia = file_competencia(zip_path.name)
    output_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zip_file:
        member = find_estabelecimento_member(zip_file, competencia)
        csv_path = output_dir / Path(member).name
        temp_file = csv_path.with_suffix(csv_path.suffix + ".part")

        if temp_file.exists():
            temp_file.unlink()

        with zip_file.open(member) as source, temp_file.open("wb") as target:
            shutil.copyfileobj(source, target)

        temp_file.replace(csv_path)

    return csv_path


def normalize_column_name(column_name: str) -> str:
    match = re.fullmatch(
        r"TO_CHAR\((?P<column>[^,]+),'DD/MM/YYYY'\)", column_name, re.IGNORECASE
    )
    if match:
        return match.group("column").strip()
    return column_name


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={column: normalize_column_name(column) for column in df.columns})
    df = df.rename(columns={column: column.lower() for column in df.columns})

    for column in df.columns:
        if column.startswith("dt_"):
            parsed_dates = pd.to_datetime(
                df[column].replace("", pd.NA), format="%d/%m/%Y", errors="coerce"
            )
            dates = parsed_dates.dt.date.astype("object")
            dates[parsed_dates.isna()] = None
            df[column] = dates

    return df


def estabelecimento_schema(columns: list[str]) -> pa.Schema:
    fields = []
    for column in columns:
        field_type = pa.date32() if column.startswith("dt_") else pa.string()
        fields.append(pa.field(column, field_type))
    return pa.schema(fields)


def write_estabelecimento_parquet(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(
        df, schema=estabelecimento_schema(list(df.columns)), preserve_index=False
    )
    pq.write_table(table, output_path)


def csv_to_processed_parquet(
    csv_path: Path, competencia: str, encoding: str = DEFAULT_ENCODING
) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = PROCESSED_DIR / f"tbEstabelecimento{competencia}.parquet"

    df = pd.read_csv(
        csv_path,
        sep=CSV_SEPARATOR,
        quotechar=CSV_QUOTECHAR,
        encoding=encoding,
        dtype="string",
        keep_default_na=False,
    )
    df = transform_dataframe(df)
    write_estabelecimento_parquet(df, parquet_path)
    return parquet_path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def publish_by_uf(parquet_path: Path, competencia: str) -> dict[str, dict[str, object]]:
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    if "co_estado_gestor" not in df.columns or "co_municipio_gestor" not in df.columns:
        raise ValueError("Parquet CNES nao possui colunas de UF/municipio gestor.")

    if CURRENT_DIR.exists():
        shutil.rmtree(CURRENT_DIR)
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)

    files: dict[str, dict[str, object]] = {}
    for uf, uf_df in sorted(df.groupby("co_estado_gestor", dropna=False)):
        uf_code = "sem_estado" if pd.isna(uf) or uf == "" else str(uf)
        output_path = CURRENT_DIR / f"{uf_code}.parquet"
        write_estabelecimento_parquet(uf_df, output_path)
        relative_path = output_path.relative_to(PUBLISH_ROOT).as_posix()
        files[uf_code] = {
            "path": relative_path,
            "sha256": sha256_file(output_path),
            "rows": int(len(uf_df)),
        }

    return files


def write_reference_manifest(competencia: str, files: dict[str, dict[str, object]]) -> None:
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {
        "reference_id": "cnes_estabelecimentos",
        "title": "CNES - Estabelecimentos",
        "version": competencia,
        "partition": "uf",
        "municipality_filter_column": "co_municipio_gestor",
        "generated_at_utc": utc_now(),
        "files": files,
    }
    REFERENCE_MANIFEST.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def validate_published_files(files: dict[str, dict[str, object]]) -> None:
    for metadata in files.values():
        path = PUBLISH_ROOT / str(metadata["path"])
        pd.read_parquet(path, engine="pyarrow")


def cleanup_temp_files(*paths: Path) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera a referencia oficial CNES - Estabelecimentos."
    )
    parser.add_argument("--force", action="store_true", help="Reprocessa mesmo sem versao nova.")
    parser.add_argument(
        "--encoding",
        default=DEFAULT_ENCODING,
        help=f"Encoding do CSV extraido. Padrao: {DEFAULT_ENCODING}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        remote_file = get_latest_remote_file()
        remote_version = file_competencia(remote_file)
        published_version = read_reference_version()

        if published_version and published_version >= remote_version and not args.force:
            print(f"CNES ja atualizado: {published_version} >= {remote_version}")
            if not GLOBAL_MANIFEST.exists():
                write_global_manifest()
            return 0

        zip_path = download_file(remote_file, overwrite=args.force)
        csv_path = extract_estabelecimento_csv(zip_path)
        parquet_path = csv_to_processed_parquet(csv_path, remote_version, encoding=args.encoding)
        files = publish_by_uf(parquet_path, remote_version)
        validate_published_files(files)
        write_reference_manifest(remote_version, files)
        write_global_manifest()
        cleanup_temp_files(zip_path, csv_path)
    except Exception as exc:
        print(f"Erro ao gerar CNES: {exc}")
        return 1

    print(f"CNES publicado: {remote_version}")
    print(f"Arquivos por UF: {len(files)}")
    print(f"Manifest: {REFERENCE_MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
