# vigisus-data-pipeline

Fonte oficial de arquivos de referencia consumidos pelo VigiSUS-BR.

O VigiSUS-BR e voltado principalmente para municipios. Este repositorio publica bases de referencia em Parquet com caminhos estaveis, manifests e hashes SHA256. A pasta oficial consumida pelo sistema e `data/publish/`.

## Estrutura

```text
data/
  raw/
    cnes/estabelecimentos/
    ibge/populacao/
    ibge/uf/
    ibge/municipios/
    vigilancia/calendario_epidemiologico/

  processed/
    cnes/estabelecimentos/
    ibge/populacao/
    ibge/uf/
    ibge/municipios/
    vigilancia/calendario_epidemiologico/

  publish/
    manifest.json
    referencias/
      cnes/estabelecimentos/current/
      ibge/populacao/current/
      ibge/uf/current/
      ibge/municipios/current/
      vigilancia/calendario_epidemiologico/current/
```

`data/raw/` e `data/processed/` sao cache de execucao e nao entram no Git. `data/publish/` e a fonte oficial versionada.

## Referencias

| Referencia | Status | Atualizacao |
| --- | --- | --- |
| CNES - Estabelecimentos | Implementado | Diaria via GitHub Actions |
| Populacao | Planejado | Mensal via GitHub Actions |
| UF | Planejado | Estavel, sem cron |
| Municipios | Planejado | Estavel, sem cron |
| Calendario epidemiologico | Implementado | Estavel, sem cron |

## CNES - Estabelecimentos

Script principal:

```text
gerar_cnes.py
```

Fonte:

```text
ftp.datasus.gov.br/cnes
```

Funcionamento:

1. Consulta o FTP publico do DATASUS.
2. Identifica o arquivo mais recente `BASE_DE_DADOS_CNES_YYYYMM.ZIP`.
3. Compara `YYYYMM` com `data/publish/referencias/cnes/estabelecimentos/manifest.json`.
4. Se a versao publicada ja estiver atualizada, encerra sem baixar o ZIP.
5. Se houver versao nova, extrai apenas `tbEstabelecimentoYYYYMM.csv`.
6. Corrige colunas `TO_CHAR(COLUNA,'DD/MM/YYYY')`.
7. Converte nomes de colunas para minusculo.
8. Converte colunas `dt_*` para date.
9. Publica um Parquet por UF em `current`.
10. Gera SHA256 e contagem de linhas no manifest.

Publicacao:

```text
data/publish/referencias/cnes/estabelecimentos/current/11.parquet
data/publish/referencias/cnes/estabelecimentos/current/12.parquet
data/publish/referencias/cnes/estabelecimentos/current/33.parquet
```

O VigiSUS-BR baixa o arquivo da UF configurada e filtra o municipio pela coluna:

```text
co_municipio_gestor
```

Nao ha historico publicado de CNES. A pasta `current/` sempre representa a versao mais recente.

## Manifests

Manifest global:

```text
data/publish/manifest.json
```

Formato:

```json
{
  "generated_at_utc": "ISO_DATETIME",
  "references": {
    "cnes_estabelecimentos": "referencias/cnes/estabelecimentos/manifest.json",
    "uf": "referencias/ibge/uf/manifest.json",
    "municipios": "referencias/ibge/municipios/manifest.json",
    "populacao": "referencias/ibge/populacao/manifest.json",
    "calendario_epidemiologico": "referencias/vigilancia/calendario_epidemiologico/manifest.json"
  }
}
```

Manifest CNES:

```text
data/publish/referencias/cnes/estabelecimentos/manifest.json
```

Inclui:

- `reference_id`
- `title`
- `version`
- `partition`
- `municipality_filter_column`
- `generated_at_utc`
- `files.{uf}.path`
- `files.{uf}.sha256`
- `files.{uf}.rows`

## Rodar localmente

Instale dependencias:

```powershell
pip install pandas pyarrow
```

Execute:

```powershell
python gerar_cnes.py
```

Publique alteracoes:

```powershell
git add data/publish gerar_cnes.py README.md .github/workflows/update-cnes.yml .gitignore
git commit -m "Update CNES reference"
git push origin main
```

## GitHub Actions

CNES roda diariamente as 13:00 no horario de Brasilia:

```text
.github/workflows/update-cnes.yml
```

O cron do GitHub usa UTC:

```yaml
schedule:
  - cron: "0 16 * * *"
```

O workflow:

1. Instala Python, pandas e pyarrow.
2. Executa `python gerar_cnes.py`.
3. Faz `git add data/publish`.
4. Commita e faz push somente se houver mudanca real.
5. Envia mensagem ao Discord somente quando houver commit.

Para habilitar a notificacao no Discord, configure o secret:

```text
Settings > Secrets and variables > Actions > New repository secret
Name: DISCORD_WEBHOOK_URL
Secret: https://discord.com/api/webhooks/...
```

## Fontes de dados

CNES:

- FTP DATASUS CNES: `ftp.datasus.gov.br/cnes`

Populacao:

- FTP DATASUS/IBGE POPSVS: `ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/POPSVS/`
- O TABNET oficial informa que a fonte POPSVS pode ser baixada nesse FTP e descreve a realizacao por CGI Demografico/RIPSA e CGIAE/SVSA/Ministerio da Saude: https://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge%2Fcnv%2Fpopsvs2024br.def=

Relatorio Anual de Gestao 2024:

- Pagina oficial do RAG: https://www.gov.br/saude/pt-br/acesso-a-informacao/gestao-do-sus/instrumentos-de-planejamento/rag
- PDF do Relatorio Anual de Gestao 2024 na BVS MS: https://bvsms.saude.gov.br/bvs/publicacoes/relatorio_anual_gestao_2024.pdf

O RAG 2024 menciona a RIPSA e bases oficiais usadas como referencia. O repositorio indica o link original do PDF em vez de armazenar o arquivo, para evitar duplicacao de documento oficial e manter a referencia na fonte primaria.

UF e municipios:

- Serao gerados a partir de fonte oficial a definir na etapa de implementacao dos scripts `gerar_uf.py` e `gerar_municipios.py`.

Calendario epidemiologico:

- Gerado por regra deterministica no script `gerar_calendario_epidemiologico.py`, cobrindo 1900 a 2100.
- As semanas epidemiologicas vao de domingo a sabado.
- A semana epidemiologica 1 e a semana que contem a maioria dos dias em janeiro, equivalente a semana que contem 4 de janeiro.
