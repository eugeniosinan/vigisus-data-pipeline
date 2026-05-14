#!/usr/bin/env python3
"""Gera a referencia oficial de populacao POPSVS para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import ftplib
import hashlib
import json
import re
import shutil
import struct
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/IBGE/POPSVS"
FILE_PATTERN = re.compile(r"^POPSBR(?P<yy>\d{2})\.zip$", re.IGNORECASE)
DEFAULT_START_YEAR = 2019
DEFAULT_RECENT_YEARS = 3

RAW_DIR = Path("data/raw/ibge/populacao")
PROCESSED_DIR = Path("data/processed/ibge/populacao")
PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/ibge/populacao"
CURRENT_DIR = REFERENCE_DIR / "current"
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

OUTPUT_COLUMNS = [
    "co_municipio_ibge",
    "co_municipio",
    "co_uf",
    "ano",
    "sexo",
    "idade",
    "pop",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def zip_year(file_name: str) -> int:
    match = FILE_PATTERN.match(Path(file_name).name)
    if not match:
        raise ValueError(f"Arquivo fora do padrao esperado: {file_name}")
    return 2000 + int(match.group("yy"))


def list_remote_files(start_year: int) -> dict[int, str]:
    with ftplib.FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        ftp.cwd(FTP_DIR)
        names = ftp.nlst()

    files: dict[int, str] = {}
    for name in names:
        file_name = Path(name).name
        if FILE_PATTERN.match(file_name):
            year = zip_year(file_name)
            if year >= start_year:
                files[year] = file_name

    if not files:
        raise FileNotFoundError(f"Nenhum POPSBRYY.zip encontrado a partir de {start_year}.")

    return dict(sorted(files.items()))


def read_reference_years() -> list[int]:
    if not REFERENCE_MANIFEST.exists():
        return []

    with REFERENCE_MANIFEST.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)

    years = manifest.get("years")
    if not isinstance(years, list):
        return []

    return [int(year) for year in years]


def read_reference_files() -> dict[str, dict[str, object]]:
    if not REFERENCE_MANIFEST.exists():
        return {}

    with REFERENCE_MANIFEST.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)

    files = manifest.get("files")
    return files if isinstance(files, dict) else {}


def published_files_exist(files: dict[str, dict[str, object]]) -> bool:
    if not files:
        return False
    return all((PUBLISH_ROOT / str(metadata.get("path", ""))).exists() for metadata in files.values())


def download_file(file_name: str, overwrite: bool = False) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    output_file = RAW_DIR / file_name
    if output_file.exists() and not overwrite:
        return output_file

    temp_file = output_file.with_suffix(output_file.suffix + ".part")
    temp_file.unlink(missing_ok=True)

    with ftplib.FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        ftp.cwd(FTP_DIR)
        with temp_file.open("wb") as file_handle:
            ftp.retrbinary(f"RETR {file_name}", file_handle.write)

    temp_file.replace(output_file)
    return output_file


def extract_dbf(zip_path: Path, year: int) -> Path:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zip_file:
        dbf_members = [
            member
            for member in zip_file.namelist()
            if Path(member).name.lower().endswith(".dbf")
        ]
        if len(dbf_members) != 1:
            raise FileNotFoundError(
                f"Esperado exatamente um DBF em {zip_path.name}, encontrados {len(dbf_members)}."
            )

        member = dbf_members[0]
        dbf_path = RAW_DIR / f"populacao_{year}.dbf"
        temp_file = dbf_path.with_suffix(".dbf.part")
        temp_file.unlink(missing_ok=True)

        with zip_file.open(member) as source, temp_file.open("wb") as target:
            shutil.copyfileobj(source, target)

        temp_file.replace(dbf_path)

    return dbf_path


def dbf_fields(file_handle) -> tuple[int, int, list[dict[str, object]]]:
    header = file_handle.read(32)
    if len(header) != 32:
        raise ValueError("Cabecalho DBF invalido.")

    record_count, header_length, record_length = struct.unpack("<IHH", header[4:12])
    fields: list[dict[str, object]] = []

    while True:
        descriptor = file_handle.read(32)
        if not descriptor:
            raise ValueError("Descritores DBF sem terminador.")
        if descriptor[0] == 0x0D:
            break

        raw_name = descriptor[:11].split(b"\x00", 1)[0]
        name = raw_name.decode("latin-1").strip().lower()
        field_type = chr(descriptor[11])
        length = descriptor[16]
        decimals = descriptor[17]
        fields.append(
            {
                "name": name,
                "type": field_type,
                "length": length,
                "decimals": decimals,
            }
        )

    file_handle.seek(header_length)
    return record_count, record_length, fields


def decode_dbf_value(raw: bytes, field: dict[str, object]) -> str | int | None:
    text = raw.decode("latin-1", errors="ignore").strip()
    if text == "":
        return None

    if field["type"] in {"N", "F"} and int(field.get("decimals", 0)) == 0:
        return int(text)

    return text


def iter_dbf_chunks(dbf_path: Path, chunk_size: int) -> Iterator[pd.DataFrame]:
    with dbf_path.open("rb") as file_handle:
        record_count, record_length, fields = dbf_fields(file_handle)
        required = {"cod_mun", "ano", "sexo", "idade", "pop"}
        available = {str(field["name"]) for field in fields}
        missing = required - available
        if missing:
            raise ValueError(f"Campos ausentes no DBF {dbf_path.name}: {sorted(missing)}")

        rows = []
        for _ in range(record_count):
            record = file_handle.read(record_length)
            if len(record) != record_length:
                break
            if record[:1] == b"*":
                continue

            offset = 1
            values = {}
            for field in fields:
                length = int(field["length"])
                name = str(field["name"])
                values[name] = decode_dbf_value(record[offset : offset + length], field)
                offset += length

            co_municipio_ibge = str(values["cod_mun"]).zfill(7)
            rows.append(
                {
                    "co_municipio_ibge": co_municipio_ibge,
                    "co_municipio": co_municipio_ibge[:6],
                    "co_uf": co_municipio_ibge[:2],
                    "ano": int(values["ano"]),
                    "sexo": int(values["sexo"]),
                    "idade": str(values["idade"]).zfill(3),
                    "pop": int(values["pop"]),
                }
            )

            if len(rows) >= chunk_size:
                yield pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
                rows = []

        if rows:
            yield pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def population_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("co_municipio_ibge", pa.string()),
            pa.field("co_municipio", pa.string()),
            pa.field("co_uf", pa.string()),
            pa.field("ano", pa.int16()),
            pa.field("sexo", pa.int8()),
            pa.field("idade", pa.string()),
            pa.field("pop", pa.int64()),
        ]
    )


def write_dataframe(writer: pq.ParquetWriter, df: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(df, schema=population_schema(), preserve_index=False)
    writer.write_table(table)


def process_year_to_parquet(year: int, zip_file: str, force: bool, chunk_size: int) -> Path:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DIR / f"populacao_{year}.parquet"
    if output_path.exists() and not force:
        return output_path

    zip_path = download_file(zip_file, overwrite=force)
    dbf_path = extract_dbf(zip_path, year)
    temp_path = output_path.with_suffix(".parquet.tmp")
    temp_path.unlink(missing_ok=True)

    writer = pq.ParquetWriter(temp_path, population_schema())
    rows_written = 0
    try:
        for chunk in iter_dbf_chunks(dbf_path, chunk_size):
            rows_written += len(chunk)
            write_dataframe(writer, chunk)
    finally:
        writer.close()

    if rows_written == 0:
        temp_path.unlink(missing_ok=True)
        raise ValueError(f"Nenhum registro processado para {year}.")

    temp_path.replace(output_path)
    dbf_path.unlink(missing_ok=True)
    zip_path.unlink(missing_ok=True)
    return output_path


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


def publish_by_uf(year_files: dict[int, Path]) -> tuple[dict[str, dict[str, object]], int]:
    if CURRENT_DIR.exists():
        shutil.rmtree(CURRENT_DIR)
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.concat(
        [pd.read_parquet(year_files[year], engine="pyarrow") for year in sorted(year_files)],
        ignore_index=True,
    )
    if df.empty:
        raise ValueError("Populacao consolidada ficaria vazia.")

    files: dict[str, dict[str, object]] = {}
    for uf, uf_df in sorted(df.groupby("co_uf", dropna=False)):
        uf_code = "sem_uf" if pd.isna(uf) or uf == "" else str(uf)
        output_path = CURRENT_DIR / f"{uf_code}.parquet"
        table = pa.Table.from_pandas(uf_df, schema=population_schema(), preserve_index=False)
        pq.write_table(table, output_path)
        files[uf_code] = {
            "path": output_path.relative_to(PUBLISH_ROOT).as_posix(),
            "sha256": sha256_file(output_path),
            "rows": int(len(uf_df)),
        }

    return files, int(len(df))


def write_reference_manifest(
    years: list[int],
    source_files: dict[int, str],
    files: dict[str, dict[str, object]],
    rows: int,
) -> None:
    manifest = {
        "reference_id": "populacao",
        "title": "IBGE/DATASUS POPSVS - Populacao",
        "version": f"{min(years)}-{max(years)}",
        "years": years,
        "partition": "uf",
        "municipality_filter_column": "co_municipio_ibge",
        "generated_at_utc": utc_now(),
        "source": f"ftp://{FTP_HOST}{FTP_DIR}/",
        "source_files": {str(year): source_files[year] for year in years},
        "rows": rows,
        "files": files,
    }
    write_json_if_changed(REFERENCE_MANIFEST, manifest)
    write_global_manifest()


def validate() -> None:
    files = read_reference_files()
    if not files:
        raise ValueError("Manifest de populacao sem arquivos publicados.")

    for metadata in files.values():
        path = PUBLISH_ROOT / str(metadata["path"])
        df = pd.read_parquet(path, engine="pyarrow")
        if list(df.columns) != OUTPUT_COLUMNS:
            raise ValueError(f"Colunas invalidas em {path}: {list(df.columns)}")
        if df.empty:
            raise ValueError(f"Parquet de populacao vazio: {path}")
        if not all(column == column.lower() for column in df.columns):
            raise ValueError(f"Ha colunas fora do padrao minusculo em {path}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera referencia POPSVS em Parquet.")
    parser.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    parser.add_argument("--recent-years", type=int, default=DEFAULT_RECENT_YEARS)
    parser.add_argument("--force", action="store_true", help="Reprocessa todos os anos.")
    parser.add_argument("--chunk-size", type=int, default=250_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        source_files = list_remote_files(args.start_year)
        years = sorted(source_files)[-args.recent_years :]
        if not years:
            raise ValueError("Nenhum ano selecionado para processamento.")
        source_files = {year: source_files[year] for year in years}
        published_years = read_reference_years()
        published_files = read_reference_files()
        if (
            not args.force
            and published_years == years
            and published_files_exist(published_files)
            and REFERENCE_MANIFEST.exists()
        ):
            print(f"Populacao ja atualizada: {min(years)}-{max(years)}")
            if not GLOBAL_MANIFEST.exists():
                write_global_manifest()
            return 0

        processed_files = {
            year: process_year_to_parquet(
                year,
                source_files[year],
                force=args.force or year not in published_years,
                chunk_size=args.chunk_size,
            )
            for year in years
        }
        files, rows = publish_by_uf(processed_files)
        write_reference_manifest(years, source_files, files, rows)
        validate()
    except Exception as exc:
        print(f"Erro ao gerar populacao: {exc}")
        return 1

    print(f"Populacao publicada: {min(years)}-{max(years)}")
    print(f"Linhas: {rows}")
    print(f"Arquivos por UF: {len(files)}")
    print(f"Manifest: {REFERENCE_MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
