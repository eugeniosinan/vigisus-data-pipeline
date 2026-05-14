#!/usr/bin/env python3
"""Gera populacao censitaria por raca/cor para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SIDRA_TABLE = "9606"
SIDRA_VARIABLE = "93"
DEFAULT_CENSUS_YEAR = 2022
CENSUS_YEAR = DEFAULT_CENSUS_YEAR
SOURCE_URL = "https://apisidra.ibge.gov.br/values"
METADATA_URL = f"https://servicodados.ibge.gov.br/api/v3/agregados/{SIDRA_TABLE}/metadados"
PERIODS_URL = f"https://servicodados.ibge.gov.br/api/v3/agregados/{SIDRA_TABLE}/periodos"
DEFAULT_SLEEP_SECONDS = 0.2
DEFAULT_RETRIES = 3
DEFAULT_AGE_CHUNK_SIZE = 25

RACES = {
    "2776": ("1", "branca"),
    "2777": ("2", "preta"),
    "2778": ("3", "amarela"),
    "2779": ("4", "parda"),
    "2780": ("5", "indigena"),
}
SEXES = {
    "4": ("1", "masculino"),
    "5": ("2", "feminino"),
}
AGE_IDS = list(range(6557, 6654)) + [6656, 6657, 6658, 6659]
DEFAULT_UFS = [
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "31",
    "32",
    "33",
    "35",
    "41",
    "42",
    "43",
    "50",
    "51",
    "52",
    "53",
]

RAW_DIR = Path("data/raw/ibge/populacao_raca_censo")
PROCESSED_DIR = Path("data/processed/ibge/populacao_raca_censo")
PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/ibge/populacao_raca_censo"
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
    "ano_censo",
    "co_municipio_ibge",
    "co_municipio",
    "co_uf",
    "sexo",
    "no_sexo",
    "idade",
    "co_raca_cor",
    "co_raca_cor_sidra",
    "no_raca_cor",
    "pop",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(url: str) -> object:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "vigisus-data-pipeline",
        },
    )
    with urlopen(request, timeout=120) as response:
        raw = response.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8-sig"))


def periods_from_payload(payload: object) -> list[int]:
    periods = []
    if isinstance(payload, dict):
        raw_periods = payload.get("periodos")
        if isinstance(raw_periods, list):
            for item in raw_periods:
                if isinstance(item, dict) and str(item.get("id", "")).isdigit():
                    periods.append(int(item["id"]))
                elif str(item).isdigit():
                    periods.append(int(str(item)))

        periodicity = payload.get("periodicidade")
        if isinstance(periodicity, dict) and str(periodicity.get("fim", "")).isdigit():
            periods.append(int(periodicity["fim"]))

    elif isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and str(item.get("id", "")).isdigit():
                periods.append(int(item["id"]))
            elif str(item).isdigit():
                periods.append(int(str(item)))

    return sorted(set(periods))


def available_periods() -> list[int]:
    periods = periods_from_payload(fetch_json(METADATA_URL))
    if len(periods) <= 1:
        periods = sorted(set(periods + periods_from_payload(fetch_json(PERIODS_URL))))
    if not periods:
        raise ValueError("Nenhum periodo disponivel encontrado na API de metadados.")
    return periods


def latest_available_period() -> int:
    return max(available_periods())


def read_reference_version() -> int | None:
    if not REFERENCE_MANIFEST.exists():
        return None
    with REFERENCE_MANIFEST.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)
    version = manifest.get("version")
    return int(version) if str(version).isdigit() else None


def published_files_exist(files: dict[str, dict[str, object]]) -> bool:
    if not files:
        return False
    return all((PUBLISH_ROOT / str(metadata.get("path", ""))).exists() for metadata in files.values())


def sidra_url(uf: str, race_id: str, sex_id: str, age_ids: list[int]) -> str:
    territorial_selector = quote(f"in n3 {uf}")
    age_ids_param = ",".join(str(age_id) for age_id in age_ids)
    return (
        f"{SOURCE_URL}/t/{SIDRA_TABLE}/n6/{territorial_selector}/v/{SIDRA_VARIABLE}"
        f"/p/{CENSUS_YEAR}/c86/{race_id}/c2/{sex_id}/c287/{age_ids_param}"
    )


def fetch_sidra(url: str, retries: int) -> list[dict[str, str]]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "vigisus-data-pipeline",
                },
            )
            with urlopen(request, timeout=180) as response:
                raw = response.read().decode("utf-8-sig")
            payload = json.loads(raw)
            if not isinstance(payload, list):
                raise ValueError("Resposta SIDRA nao e uma lista.")
            return payload
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(attempt * 2)

    raise RuntimeError(f"Falha ao consultar SIDRA ({url}): {last_error}")


def chunks(values: list[int], size: int) -> list[list[int]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def parse_age(age_text: str) -> int:
    if "Menos de 1" in age_text:
        return 0
    if "100 anos" in age_text:
        return 100
    numbers = re.findall(r"\d+", age_text)
    return int(numbers[0]) if numbers else 0


def parse_population(value: str) -> int:
    if value in {"-", "X", "...", ""}:
        return 0
    return int(float(str(value).replace(",", ".")))


def dataframe_from_sidra(payload: list[dict[str, str]]) -> pd.DataFrame:
    if len(payload) <= 1:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    rows = []
    for item in payload[1:]:
        race_id = item["D4C"]
        sex_id = item["D5C"]
        race_code, race_name = RACES[race_id]
        sex_code, sex_name = SEXES[sex_id]
        municipality = str(item["D1C"]).zfill(7)
        rows.append(
            {
                "ano_censo": CENSUS_YEAR,
                "co_municipio_ibge": municipality,
                "co_municipio": municipality[:6],
                "co_uf": municipality[:2],
                "sexo": sex_code,
                "no_sexo": sex_name,
                "idade": parse_age(item["D6N"]),
                "co_raca_cor": race_code,
                "co_raca_cor_sidra": race_id,
                "no_raca_cor": race_name,
                "pop": parse_population(item["V"]),
            }
        )

    return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


def census_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("ano_censo", pa.int16()),
            pa.field("co_municipio_ibge", pa.string()),
            pa.field("co_municipio", pa.string()),
            pa.field("co_uf", pa.string()),
            pa.field("sexo", pa.string()),
            pa.field("no_sexo", pa.string()),
            pa.field("idade", pa.int16()),
            pa.field("co_raca_cor", pa.string()),
            pa.field("co_raca_cor_sidra", pa.string()),
            pa.field("no_raca_cor", pa.string()),
            pa.field("pop", pa.int64()),
        ]
    )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=census_schema(), preserve_index=False)
    pq.write_table(table, path)


def extract_uf(
    uf: str,
    sleep_seconds: float,
    retries: int,
    age_chunk_size: int,
    force: bool,
) -> Path:
    processed_path = PROCESSED_DIR / f"{uf}.parquet"
    raw_path = RAW_DIR / str(CENSUS_YEAR) / f"{uf}.parquet"
    if processed_path.exists() and raw_path.exists() and not force:
        return processed_path

    data_chunks = []
    for race_id in RACES:
        for sex_id in SEXES:
            for age_ids in chunks(AGE_IDS, age_chunk_size):
                url = sidra_url(uf, race_id, sex_id, age_ids)
                payload = fetch_sidra(url, retries=retries)
                chunk = dataframe_from_sidra(payload)
                if not chunk.empty:
                    data_chunks.append(chunk)
                time.sleep(sleep_seconds)

    if not data_chunks:
        raise ValueError(f"Nenhum dado retornado para a UF {uf}.")

    df = pd.concat(data_chunks, ignore_index=True)
    df = df.sort_values(
        ["co_municipio_ibge", "sexo", "idade", "co_raca_cor"]
    ).reset_index(drop=True)

    write_parquet(df, raw_path)
    write_parquet(df, processed_path)
    return processed_path


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


def publish(processed_files: dict[str, Path]) -> tuple[dict[str, dict[str, object]], int]:
    if CURRENT_DIR.exists():
        shutil.rmtree(CURRENT_DIR)
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)

    files: dict[str, dict[str, object]] = {}
    total_rows = 0
    for uf, source_path in sorted(processed_files.items()):
        output_path = CURRENT_DIR / f"{uf}.parquet"
        shutil.copyfile(source_path, output_path)
        df = pd.read_parquet(output_path, engine="pyarrow", columns=["ano_censo"])
        rows = int(len(df))
        total_rows += rows
        files[uf] = {
            "path": output_path.relative_to(PUBLISH_ROOT).as_posix(),
            "sha256": sha256_file(output_path),
            "rows": rows,
        }

    return files, total_rows


def write_reference_manifest(files: dict[str, dict[str, object]], rows: int) -> None:
    manifest = {
        "reference_id": "populacao_raca_censo",
        "title": f"IBGE Censo {CENSUS_YEAR} - Populacao por Raca/Cor",
        "version": str(CENSUS_YEAR),
        "partition": "uf",
        "municipality_filter_column": "co_municipio_ibge",
        "generated_at_utc": utc_now(),
        "source": "SIDRA IBGE v1",
        "source_table": SIDRA_TABLE,
        "source_variable": SIDRA_VARIABLE,
        "source_url": SOURCE_URL,
        "metadata_url": METADATA_URL,
        "periods_url": PERIODS_URL,
        "rows": rows,
        "files": files,
    }
    write_json_if_changed(REFERENCE_MANIFEST, manifest)
    write_global_manifest()


def read_reference_files() -> dict[str, dict[str, object]]:
    if not REFERENCE_MANIFEST.exists():
        return {}
    with REFERENCE_MANIFEST.open("r", encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)
    files = manifest.get("files")
    return files if isinstance(files, dict) else {}


def validate(files: dict[str, dict[str, object]]) -> None:
    for metadata in files.values():
        path = PUBLISH_ROOT / str(metadata["path"])
        df = pd.read_parquet(path, engine="pyarrow")
        if list(df.columns) != OUTPUT_COLUMNS:
            raise ValueError(f"Colunas invalidas em {path}: {list(df.columns)}")
        if df.empty:
            raise ValueError(f"Arquivo vazio: {path}")
        if not all(column == column.lower() for column in df.columns):
            raise ValueError(f"Colunas fora do padrao minusculo em {path}")


def parse_ufs(value: str | None) -> list[str]:
    if not value:
        return DEFAULT_UFS
    return [uf.strip().zfill(2) for uf in value.split(",") if uf.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera populacao censitaria por raca/cor em Parquet."
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Ano SIDRA a extrair. Se omitido, usa o periodo mais recente da API.",
    )
    parser.add_argument("--ufs", help="Lista de UFs separadas por virgula. Ex.: 11,33")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--age-chunk-size", type=int, default=DEFAULT_AGE_CHUNK_SIZE)
    parser.add_argument("--force", action="store_true", help="Reextrai UFs ja processadas.")
    return parser.parse_args()


def main() -> int:
    global CENSUS_YEAR

    args = parse_args()
    ufs = parse_ufs(args.ufs)

    try:
        CENSUS_YEAR = args.year or latest_available_period()
        published_version = read_reference_version()
        published_files = read_reference_files()
        if (
            not args.force
            and published_version is not None
            and published_version >= CENSUS_YEAR
            and published_files_exist(published_files)
        ):
            print(f"Populacao por raca/cor ja atualizada: {published_version} >= {CENSUS_YEAR}")
            if not GLOBAL_MANIFEST.exists():
                write_global_manifest()
            return 0

        processed_files = {
            uf: extract_uf(
                uf,
                sleep_seconds=args.sleep_seconds,
                retries=args.retries,
                age_chunk_size=args.age_chunk_size,
                force=args.force,
            )
            for uf in ufs
        }
        files, rows = publish(processed_files)
        write_reference_manifest(files, rows)
        validate(files)
    except Exception as exc:
        print(f"Erro ao gerar populacao por raca/cor do Censo: {exc}")
        return 1

    print(f"Populacao por raca/cor do Censo publicada: {CENSUS_YEAR}")
    print(f"Arquivos por UF: {len(files)}")
    print(f"Linhas: {rows}")
    print(f"Manifest: {REFERENCE_MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
