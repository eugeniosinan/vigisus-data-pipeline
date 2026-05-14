#!/usr/bin/env python3
"""Gera a referencia oficial de UFs do IBGE para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


SOURCE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados?orderBy=nome"
PUBLISH_ROOT = Path("data/publish")
REFERENCE_DIR = PUBLISH_ROOT / "referencias/ibge/uf"
CURRENT_DIR = REFERENCE_DIR / "current"
PARQUET_PATH = CURRENT_DIR / "uf.parquet"
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


def fetch_json(url: str) -> list[dict]:
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

    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Resposta inesperada da API de Localidades do IBGE.")
    return payload


def build_dataframe(payload: list[dict]) -> pd.DataFrame:
    rows = []
    for item in payload:
        regiao = item.get("regiao") or {}
        rows.append(
            {
                "co_uf": f"{int(item['id']):02d}",
                "sg_uf": str(item["sigla"]),
                "no_uf": str(item["nome"]),
                "co_regiao": str(regiao.get("id", "")),
                "sg_regiao": str(regiao.get("sigla", "")),
                "no_regiao": str(regiao.get("nome", "")),
            }
        )

    df = pd.DataFrame(rows)
    return df.sort_values("co_uf").reset_index(drop=True)


def parquet_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("co_uf", pa.string()),
            pa.field("sg_uf", pa.string()),
            pa.field("no_uf", pa.string()),
            pa.field("co_regiao", pa.string()),
            pa.field("sg_regiao", pa.string()),
            pa.field("no_regiao", pa.string()),
        ]
    )


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


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(df, schema=parquet_schema(), preserve_index=False)
    pq.write_table(table, path)


def publish(df: pd.DataFrame) -> bool:
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)
    temp_path = PARQUET_PATH.with_suffix(".parquet.tmp")
    write_parquet(df, temp_path)

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
            "reference_id": "uf",
            "title": "IBGE - Unidades da Federacao",
            "version": "ibge-localidades",
            "generated_at_utc": utc_now(),
            "source": SOURCE_URL,
            "file": {
                "path": PARQUET_PATH.relative_to(PUBLISH_ROOT).as_posix(),
                "sha256": new_sha,
                "rows": new_rows,
            },
        }
        write_json_if_changed(REFERENCE_MANIFEST, manifest)
        write_global_manifest()

    return changed


def validate() -> None:
    df = pd.read_parquet(PARQUET_PATH, engine="pyarrow")
    expected_columns = ["co_uf", "sg_uf", "no_uf", "co_regiao", "sg_regiao", "no_regiao"]
    if list(df.columns) != expected_columns:
        raise ValueError(f"Colunas invalidas: {list(df.columns)}")
    if len(df) != 27:
        raise ValueError(f"Quantidade invalida de UFs: {len(df)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera referencia de UFs em Parquet.")
    parser.add_argument("--source-url", default=SOURCE_URL)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        payload = fetch_json(args.source_url)
        df = build_dataframe(payload)
        changed = publish(df)
        validate()
    except Exception as exc:
        print(f"Erro ao gerar UF: {exc}")
        return 1

    status = "atualizada" if changed else "sem alteracao"
    print(f"Referencia UF {status}: {PARQUET_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
