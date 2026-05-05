#!/usr/bin/env python3
"""Download the latest CNES ZIP and extract/read tbEstabelecimentoYYYYMM.csv."""

from __future__ import annotations

import argparse
import csv
import ftplib
import json
import re
import shutil
import zipfile
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
DEFAULT_OUTPUT_DIR = Path("data/raw/cnes")
DEFAULT_PROCESSED_DIR = Path("data/processed/cnes/estabelecimentos")
DEFAULT_MANIFEST_PATH = Path("data/github/cnes/estabelecimentos/manifest.json")
DEFAULT_ENCODING = "latin-1"
CSV_SEPARATOR = ";"
CSV_QUOTECHAR = '"'


def find_latest_cnes_file(ftp: ftplib.FTP) -> str:
    """Return the newest CNES database file available in the current FTP dir."""
    candidates: list[tuple[str, str]] = []

    for name in ftp.nlst():
        file_name = Path(name).name
        match = FILE_PATTERN.match(file_name)
        if match:
            candidates.append((match.group(1), file_name))

    if not candidates:
        raise FileNotFoundError(
            "Nenhum arquivo encontrado no padrao BASE_DE_DADOS_CNES_YYYYMM.zip."
        )

    return max(candidates, key=lambda item: item[0])[1]


def get_zip_competencia(zip_path: Path) -> str:
    match = FILE_PATTERN.match(zip_path.name)
    if not match:
        raise ValueError(f"Nome do ZIP fora do padrao esperado: {zip_path.name}")
    return match.group(1)


def get_file_competencia(file_name: str) -> str:
    match = FILE_PATTERN.match(file_name)
    if not match:
        raise ValueError(f"Nome do arquivo fora do padrao esperado: {file_name}")
    return match.group(1)


def download_latest_cnes_database(output_dir: Path, overwrite: bool = False) -> Path:
    """Download the latest CNES database ZIP and return the local file path."""
    output_dir.mkdir(parents=True, exist_ok=True)

    with ftplib.FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        ftp.cwd(FTP_DIR)

        file_name = find_latest_cnes_file(ftp)
        output_file = output_dir / file_name

        if output_file.exists() and not overwrite:
            return output_file

        temp_file = output_file.with_suffix(output_file.suffix + ".part")
        if temp_file.exists():
            temp_file.unlink()

        with temp_file.open("wb") as file_handle:
            ftp.retrbinary(f"RETR {file_name}", file_handle.write)

        temp_file.replace(output_file)
    return output_file


def get_latest_remote_cnes_file() -> str:
    with ftplib.FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        ftp.cwd(FTP_DIR)
        return find_latest_cnes_file(ftp)


def download_cnes_database_file(
    file_name: str, output_dir: Path, overwrite: bool = False
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / file_name

    if output_file.exists() and not overwrite:
        return output_file

    temp_file = output_file.with_suffix(output_file.suffix + ".part")
    if temp_file.exists():
        temp_file.unlink()

    with ftplib.FTP(FTP_HOST, timeout=120) as ftp:
        ftp.login()
        ftp.cwd(FTP_DIR)

        with temp_file.open("wb") as file_handle:
            ftp.retrbinary(f"RETR {file_name}", file_handle.write)

    temp_file.replace(output_file)
    return output_file


def find_estabelecimento_member(zip_file: zipfile.ZipFile, competencia: str) -> str:
    expected_name = f"tbEstabelecimento{competencia}.csv"

    for member in zip_file.namelist():
        member_name = Path(member).name
        match = ESTABELECIMENTO_PATTERN.match(member_name)
        if match and match.group("competencia") == competencia:
            return member

    raise FileNotFoundError(f"Arquivo nao encontrado dentro do ZIP: {expected_name}")


def extract_estabelecimento_csv(
    zip_path: Path, output_dir: Path, overwrite: bool = False
) -> Path:
    """Extract only tbEstabelecimentoYYYYMM.csv from the CNES ZIP."""
    output_dir.mkdir(parents=True, exist_ok=True)
    competencia = get_zip_competencia(zip_path)

    with zipfile.ZipFile(zip_path) as zip_file:
        member = find_estabelecimento_member(zip_file, competencia)
        csv_name = Path(member).name
        output_file = output_dir / csv_name

        if output_file.exists() and not overwrite:
            return output_file

        temp_file = output_file.with_suffix(output_file.suffix + ".part")
        if temp_file.exists():
            temp_file.unlink()

        with zip_file.open(member) as source, temp_file.open("wb") as target:
            shutil.copyfileobj(source, target)

        temp_file.replace(output_file)

    return output_file


def preview_estabelecimento_csv(
    csv_path: Path, encoding: str = DEFAULT_ENCODING, rows: int = 3
) -> tuple[list[str], list[dict[str, str]]]:
    """Read a small preview from tbEstabelecimentoYYYYMM.csv."""
    with csv_path.open("r", encoding=encoding, newline="") as file_handle:
        reader = csv.DictReader(file_handle, delimiter=";", quotechar='"')
        preview_rows = []
        for index, row in enumerate(reader):
            if index >= rows:
                break
            preview_rows.append(row)

        return reader.fieldnames or [], preview_rows


def normalize_column_name(column_name: str) -> str:
    match = re.fullmatch(
        r"TO_CHAR\((?P<column>[^,]+),'DD/MM/YYYY'\)", column_name, re.IGNORECASE
    )
    if match:
        return match.group("column").strip()
    return column_name


def transform_estabelecimento_dataframe(df: pd.DataFrame) -> pd.DataFrame:
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


def write_partitioned_by_gestor(
    df: pd.DataFrame, output_root: Path, parquet_name: str
) -> int:
    partition_count = 0

    for (estado, municipio), municipio_df in df.groupby(
        ["co_estado_gestor", "co_municipio_gestor"], dropna=False
    ):
        estado_dir = "sem_estado" if pd.isna(estado) or estado == "" else str(estado)
        municipio_dir = (
            "sem_municipio" if pd.isna(municipio) or municipio == "" else str(municipio)
        )
        output_dir = output_root / estado_dir / municipio_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        write_estabelecimento_parquet(municipio_df, output_dir / parquet_name)
        partition_count += 1

    return partition_count


def get_estabelecimento_schema(columns: list[str]) -> pa.Schema:
    fields = []
    for column in columns:
        field_type = pa.date32() if column.startswith("dt_") else pa.string()
        fields.append(pa.field(column, field_type))
    return pa.schema(fields)


def write_estabelecimento_parquet(df: pd.DataFrame, output_path: Path) -> None:
    schema = get_estabelecimento_schema(list(df.columns))
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    pq.write_table(table, output_path)


def convert_estabelecimento_csv_to_parquet(
    csv_path: Path, processed_dir: Path, encoding: str = DEFAULT_ENCODING
) -> tuple[Path, Path, int]:
    competencia_match = ESTABELECIMENTO_PATTERN.match(csv_path.name)
    if not competencia_match:
        raise ValueError(f"Nome do CSV fora do padrao esperado: {csv_path.name}")

    competencia = competencia_match.group("competencia")
    parquet_name = f"tbEstabelecimento{competencia}.parquet"
    processed_dir.mkdir(parents=True, exist_ok=True)

    main_parquet = processed_dir / parquet_name
    partition_root = processed_dir / competencia

    df = pd.read_csv(
        csv_path,
        sep=CSV_SEPARATOR,
        quotechar=CSV_QUOTECHAR,
        encoding=encoding,
        dtype="string",
        keep_default_na=False,
    )
    df = transform_estabelecimento_dataframe(df)

    write_estabelecimento_parquet(df, main_parquet)

    if partition_root.exists():
        shutil.rmtree(partition_root)
    partition_count = write_partitioned_by_gestor(df, partition_root, parquet_name)

    return main_parquet, partition_root, partition_count


def write_parquet_sample(
    parquet_path: Path, rows: int = 10, output_path: Path | None = None
) -> Path:
    if output_path is None:
        output_path = parquet_path.with_name(f"{parquet_path.stem}_sample{rows}.parquet")

    sample_df = pd.read_parquet(parquet_path, engine="pyarrow").head(rows)
    write_estabelecimento_parquet(sample_df, output_path)
    return output_path


def find_latest_local_parquet(processed_dir: Path) -> Path | None:
    candidates: list[tuple[str, Path]] = []

    for path in processed_dir.glob("tbEstabelecimento*.parquet"):
        match = ESTABELECIMENTO_PATTERN.match(path.with_suffix(".csv").name)
        if match:
            candidates.append((match.group("competencia"), path))

    if not candidates:
        return None

    return max(candidates, key=lambda item: item[0])[1]


def read_manifest_competencia(manifest_path: Path) -> str | None:
    if not manifest_path.exists():
        return None

    with manifest_path.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)

    competencia = manifest.get("competencia")
    if not isinstance(competencia, str):
        return None

    return competencia


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Baixa a base CNES mais recente do FTP publico do DATASUS."
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Diretorio temporario para ZIP/CSV. Padrao: {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "-p",
        "--processed-dir",
        type=Path,
        default=DEFAULT_PROCESSED_DIR,
        help=f"Diretorio onde os Parquets serao salvos. Padrao: {DEFAULT_PROCESSED_DIR}",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help=f"Manifesto publicado usado no checker. Padrao: {DEFAULT_MANIFEST_PATH}",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Baixa e extrai novamente mesmo que os arquivos ja existam.",
    )
    parser.add_argument(
        "--encoding",
        default=DEFAULT_ENCODING,
        help=f"Encoding usado para ler o CSV. Padrao: {DEFAULT_ENCODING}",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=10,
        help="Quantidade de linhas do arquivo Parquet de amostra. Padrao: 10",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        remote_file = get_latest_remote_cnes_file()
        remote_competencia = get_file_competencia(remote_file)
        manifest_competencia = read_manifest_competencia(args.manifest_path)

        if (
            manifest_competencia
            and manifest_competencia >= remote_competencia
            and not args.overwrite
        ):
            print(
                "Manifest ja esta atualizado: "
                f"{manifest_competencia} >= {remote_competencia}"
            )
            return 0

        local_parquet = find_latest_local_parquet(args.processed_dir)

        if local_parquet and not args.overwrite:
            local_competencia = local_parquet.stem.removeprefix("tbEstabelecimento")
            if local_competencia >= remote_competencia:
                print(f"Parquet local ja esta atualizado: {local_parquet}")
                return 0

        zip_file = download_cnes_database_file(
            remote_file, args.output_dir, overwrite=args.overwrite
        )
        csv_file = extract_estabelecimento_csv(
            zip_file, args.output_dir, overwrite=args.overwrite
        )
        parquet_file, partition_root, partition_count = convert_estabelecimento_csv_to_parquet(
            csv_file, args.processed_dir, encoding=args.encoding
        )
        sample_file = write_parquet_sample(parquet_file, rows=args.preview_rows)

        csv_file.unlink(missing_ok=True)
        zip_file.unlink(missing_ok=True)
    except Exception as exc:
        print(f"Erro ao processar a base CNES: {exc}")
        return 1

    print(f"Parquet principal: {parquet_file}")
    print(f"Amostra parquet: {sample_file}")
    print(f"Particoes por UF/municipio: {partition_root}")
    print(f"Arquivos municipais gerados: {partition_count}")
    print("ZIP e CSV temporario removidos.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
