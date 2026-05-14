#!/usr/bin/env python3
"""Gera matriz de pesos raciais do Censo 2022 para o VigiSUS-BR."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


CENSUS_YEAR = 2022
PUBLISH_ROOT = Path("data/publish")
SOURCE_REFERENCE_DIR = PUBLISH_ROOT / "referencias/ibge/populacao_raca_censo"
SOURCE_CURRENT_DIR = SOURCE_REFERENCE_DIR / "current"
REFERENCE_DIR = PUBLISH_ROOT / "referencias/ibge/matriz_pesos_raca"
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
    "pop_raca",
    "pop_total_grupo",
    "peso_raca",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def matrix_schema() -> pa.Schema:
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
            pa.field("pop_raca", pa.int64()),
            pa.field("pop_total_grupo", pa.int64()),
            pa.field("peso_raca", pa.float64()),
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


def source_files() -> dict[str, Path]:
    if not SOURCE_CURRENT_DIR.exists():
        raise FileNotFoundError(
            "Referencia populacao_raca_censo nao encontrada. Execute "
            "gerar_populacao_raca_censo.py antes."
        )
    files = {
        path.stem: path
        for path in SOURCE_CURRENT_DIR.glob("*.parquet")
        if path.stem.isdigit()
    }
    if not files:
        raise FileNotFoundError("Nenhum Parquet censitario por UF encontrado.")
    return dict(sorted(files.items()))


def build_matrix(df: pd.DataFrame) -> pd.DataFrame:
    group_columns = ["ano_censo", "co_municipio_ibge", "sexo", "idade"]
    totals = (
        df.groupby(group_columns, dropna=False)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "pop_total_grupo"})
    )
    matrix = df.merge(totals, on=group_columns, how="left")
    matrix = matrix.rename(columns={"pop": "pop_raca"})
    matrix["peso_raca"] = 0.0
    valid = matrix["pop_total_grupo"] > 0
    matrix.loc[valid, "peso_raca"] = (
        matrix.loc[valid, "pop_raca"] / matrix.loc[valid, "pop_total_grupo"]
    )
    return matrix[OUTPUT_COLUMNS].sort_values(
        ["co_municipio_ibge", "sexo", "idade", "co_raca_cor"]
    )


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, schema=matrix_schema(), preserve_index=False)
    pq.write_table(table, path)


def publish(files_by_uf: dict[str, Path]) -> tuple[dict[str, dict[str, object]], int]:
    if CURRENT_DIR.exists():
        shutil.rmtree(CURRENT_DIR)
    CURRENT_DIR.mkdir(parents=True, exist_ok=True)

    files: dict[str, dict[str, object]] = {}
    total_rows = 0
    for uf, source_path in files_by_uf.items():
        df = pd.read_parquet(source_path, engine="pyarrow")
        matrix = build_matrix(df)
        output_path = CURRENT_DIR / f"{uf}.parquet"
        write_parquet(matrix, output_path)
        rows = int(len(matrix))
        total_rows += rows
        files[uf] = {
            "path": output_path.relative_to(PUBLISH_ROOT).as_posix(),
            "sha256": sha256_file(output_path),
            "rows": rows,
        }

    return files, total_rows


def write_reference_manifest(files: dict[str, dict[str, object]], rows: int) -> None:
    manifest = {
        "reference_id": "matriz_pesos_raca",
        "title": "IBGE Censo 2022 - Matriz de Pesos por Raca/Cor",
        "version": str(CENSUS_YEAR),
        "partition": "uf",
        "municipality_filter_column": "co_municipio_ibge",
        "generated_at_utc": utc_now(),
        "method": "peso_raca = pop_raca / pop_total_grupo",
        "formula": (
            "PopEstimada(ano,mun,sexo,idade,raca) = "
            "TotalProjetadoDATASUS(ano,mun,sexo,idade) * PesoRacaCenso2022"
        ),
        "source_reference": "referencias/ibge/populacao_raca_censo/manifest.json",
        "rows": rows,
        "files": files,
    }
    write_json_if_changed(REFERENCE_MANIFEST, manifest)
    write_global_manifest()


def validate(files: dict[str, dict[str, object]]) -> None:
    for metadata in files.values():
        path = PUBLISH_ROOT / str(metadata["path"])
        df = pd.read_parquet(path, engine="pyarrow")
        if list(df.columns) != OUTPUT_COLUMNS:
            raise ValueError(f"Colunas invalidas em {path}: {list(df.columns)}")
        if df.empty:
            raise ValueError(f"Arquivo vazio: {path}")

        sums = (
            df.groupby(["ano_censo", "co_municipio_ibge", "sexo", "idade"])["peso_raca"]
            .sum()
            .reset_index()
        )
        invalid = sums[(sums["peso_raca"] > 0) & ((sums["peso_raca"] - 1.0).abs() > 1e-9)]
        if not invalid.empty:
            raise ValueError(f"Soma de pesos invalida em {path}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera matriz de pesos raciais do Censo 2022 em Parquet."
    )
    parser.add_argument("--ufs", help="Lista de UFs separadas por virgula. Ex.: 11,33")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        files_by_uf = source_files()
        if args.ufs:
            selected = {uf.strip().zfill(2) for uf in args.ufs.split(",") if uf.strip()}
            files_by_uf = {uf: path for uf, path in files_by_uf.items() if uf in selected}
            if not files_by_uf:
                raise ValueError("Nenhuma UF selecionada possui arquivo censitario.")
        files, rows = publish(files_by_uf)
        write_reference_manifest(files, rows)
        validate(files)
    except Exception as exc:
        print(f"Erro ao gerar matriz de pesos raciais: {exc}")
        return 1

    print(f"Matriz de pesos raciais publicada: {CENSUS_YEAR}")
    print(f"Arquivos por UF: {len(files)}")
    print(f"Linhas: {rows}")
    print(f"Manifest: {REFERENCE_MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
