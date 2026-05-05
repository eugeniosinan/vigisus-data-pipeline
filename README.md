# VIGSUS Data Pipeline

Pipeline para publicar dados de estabelecimentos CNES do DATASUS em Parquet.

## Como funciona

O fluxo consulta o FTP publico do DATASUS, identifica a competencia mais recente do arquivo `BASE_DE_DADOS_CNES_YYYYMM.ZIP` e compara com o manifesto publicado em:

```text
data/github/cnes/estabelecimentos/manifest.json
```

Se a competencia do manifesto ja for igual ou maior que a competencia disponivel no FTP, o processamento e ignorado. Se houver uma competencia nova, o pipeline:

1. Baixa o ZIP mais recente de `ftp.datasus.gov.br/cnes`.
2. Extrai somente `tbEstabelecimentoYYYYMM.csv`.
3. Corrige colunas no padrao `TO_CHAR(COLUNA,'DD/MM/YYYY')`.
4. Converte todas as colunas para minusculo.
5. Converte colunas `dt_*` para date.
6. Salva um Parquet local em `data/processed/cnes/estabelecimentos`.
7. Publica um Parquet por UF em `data/github/cnes/estabelecimentos/YYYYMM`.
8. Mantem `co_municipio_gestor` dentro dos arquivos por UF para filtro por municipio.
9. Remove ZIP e CSV temporarios.

## Estrutura publicada

```text
data/github/cnes/estabelecimentos/
  manifest.json
  202603/
    11/tbEstabelecimento202603_UF11.parquet
    12/tbEstabelecimento202603_UF12.parquet
    ...
    53/tbEstabelecimento202603_UF53.parquet
```

Somente `data/github/` deve ser versionado no Git. As pastas `data/raw/` e `data/processed/` sao cache local e ficam no `.gitignore`.

## Requisitos

```powershell
pip install pandas pyarrow
```

## Rodar localmente

```powershell
cd C:\Projetos\vigisus-data-pipeline

python scripts\download_cnes_estabelecimentos.py
python scripts\publish_latest_cnes_estabelecimentos.py

git add data\github scripts README.md .gitignore
git commit -m "Update CNES estabelecimentos"
git push origin main
```

Para testar publicando poucas UFs:

```powershell
python scripts\publish_latest_cnes_estabelecimentos.py --limit-ufs 3
```

## GitHub Actions

O repositorio ja inclui o workflow `.github/workflows/update-cnes.yml`:

```yaml
name: Atualizar CNES Estabelecimentos

on:
  workflow_dispatch:
  schedule:
    - cron: "0 8 * * *"

permissions:
  contents: write

jobs:
  update-cnes:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pandas pyarrow

      - name: Process CNES
        run: |
          python scripts/download_cnes_estabelecimentos.py
          python scripts/publish_latest_cnes_estabelecimentos.py

      - name: Commit changes
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/github/cnes/estabelecimentos
          git diff --cached --quiet || git commit -m "Update CNES estabelecimentos"
          git push
```

Se nao existir competencia nova no FTP, o primeiro script para no checker pelo `manifest.json`, o segundo script mantem a publicacao atual e o passo de commit nao encontra alteracoes.
