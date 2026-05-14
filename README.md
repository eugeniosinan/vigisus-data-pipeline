# vigisus-data-pipeline

Fonte oficial de arquivos de referencia consumidos pelo VigiSUS-BR.

O VigiSUS-BR e voltado principalmente para municipios. Este repositorio publica
bases de referencia em Parquet com caminhos estaveis, manifests, hashes SHA256 e
contagem de linhas. A pasta oficial consumida pelo sistema e `data/publish/`.

Nao ha historico publicado das referencias variaveis. Quando uma base muda, a
pasta `current/` e substituida pela versao mais recente.

## Estrutura

```text
.
  gerar_cnes.py
  gerar_populacao.py
  gerar_populacao_raca_censo.py
  gerar_matriz_pesos_raca.py
  gerar_uf.py
  gerar_municipios.py
  gerar_calendario_epidemiologico.py
  README.md

  .github/
    workflows/
      update-cnes.yml
      update-populacao.yml
      update-populacao-raca-censo.yml

  data/
    raw/
      cnes/estabelecimentos/
      ibge/populacao/
      ibge/populacao_raca_censo/
      ibge/matriz_pesos_raca/
      ibge/uf/
      ibge/municipios/
      vigilancia/calendario_epidemiologico/

    processed/
      cnes/estabelecimentos/
      ibge/populacao/
      ibge/populacao_raca_censo/
      ibge/matriz_pesos_raca/
      ibge/uf/
      ibge/municipios/
      vigilancia/calendario_epidemiologico/

    publish/
      manifest.json
      referencias/
        cnes/estabelecimentos/
          manifest.json
          current/{uf}.parquet

        ibge/populacao/
          manifest.json
          current/{uf}.parquet

        ibge/populacao_raca_censo/
          manifest.json
          current/{uf}.parquet

        ibge/matriz_pesos_raca/
          manifest.json
          current/{uf}.parquet

        ibge/uf/
          manifest.json
          current/uf.parquet

        ibge/municipios/
          manifest.json
          current/municipios.parquet

        vigilancia/calendario_epidemiologico/
          manifest.json
          current/calendario_epidemiologico.parquet
```

`data/raw/` e `data/processed/` sao cache de execucao local e nao entram no Git.
`data/publish/` e a fonte oficial versionada e consumida pelo VigiSUS-BR.

## Referencias

| Referencia | Script | Publicacao | Atualizacao |
| --- | --- | --- | --- |
| CNES - Estabelecimentos | `gerar_cnes.py` | Parquet por UF | Diaria via GitHub Actions |
| Populacao POPSVS | `gerar_populacao.py` | Parquet por UF | Mensal via GitHub Actions |
| Populacao por raca/cor Censo 2022 | `gerar_populacao_raca_censo.py` | Parquet por UF | Estavel, sem cron |
| Matriz de pesos por raca/cor | `gerar_matriz_pesos_raca.py` | Parquet por UF | Verificacao semestral via GitHub Actions |
| UF | `gerar_uf.py` | Arquivo nacional unico | Estavel, sem cron |
| Municipios | `gerar_municipios.py` | Arquivo nacional unico | Estavel, sem cron |
| Calendario epidemiologico | `gerar_calendario_epidemiologico.py` | Arquivo nacional unico | Estavel, sem cron |

## Manifests

O manifest global fica em:

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
    "populacao_raca_censo": "referencias/ibge/populacao_raca_censo/manifest.json",
    "matriz_pesos_raca": "referencias/ibge/matriz_pesos_raca/manifest.json",
    "calendario_epidemiologico": "referencias/vigilancia/calendario_epidemiologico/manifest.json"
  }
}
```

Cada referencia tem seu proprio `manifest.json`. Os manifests especificos
informam a versao publicada, fonte dos dados, caminhos dos arquivos, SHA256 e
contagem de linhas.

Referencias particionadas por UF usam o campo `files`:

```json
{
  "partition": "uf",
  "files": {
    "33": {
      "path": "referencias/.../current/33.parquet",
      "sha256": "...",
      "rows": 123
    }
  }
}
```

## CNES - Estabelecimentos

Fonte:

```text
ftp.datasus.gov.br/cnes
```

Padrao do arquivo no FTP:

```text
BASE_DE_DADOS_CNES_YYYYMM.ZIP
```

Arquivo usado dentro do ZIP:

```text
tbEstabelecimentoYYYYMM.csv
```

Funcionamento:

1. Consulta o FTP publico do DATASUS.
2. Identifica a competencia mais recente pelo padrao `BASE_DE_DADOS_CNES_YYYYMM.ZIP`.
3. Compara a competencia remota com o campo `version` do manifest publicado.
4. Se ja estiver atualizado, encerra sem baixar o ZIP.
5. Se houver versao nova, baixa o ZIP e extrai apenas o CSV de estabelecimentos.
6. Le o CSV com `sep=";"`, `quotechar='"'` e encoding `latin-1`.
7. Corrige colunas geradas como `TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')`.
8. Converte nomes de colunas para minusculo.
9. Converte colunas `dt_*` para date.
10. Publica um Parquet por UF.
11. Remove ZIP e CSV temporarios apos o processamento.

Publicacao:

```text
data/publish/referencias/cnes/estabelecimentos/current/11.parquet
data/publish/referencias/cnes/estabelecimentos/current/12.parquet
data/publish/referencias/cnes/estabelecimentos/current/33.parquet
```

Manifest:

```text
data/publish/referencias/cnes/estabelecimentos/manifest.json
```

O VigiSUS-BR baixa o arquivo da UF configurada e filtra o municipio localmente
pela coluna:

```text
co_municipio_gestor
```

## Populacao POPSVS

Fonte:

```text
ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/POPSVS/
```

Padrao dos arquivos no FTP:

```text
POPSBRYY.zip
```

Exemplo:

```text
POPSBR25.zip
```

Cada ZIP contem um unico arquivo DBF, normalmente no padrao:

```text
POPYY.dbf
```

Funcionamento:

1. Consulta o FTP publico POPSVS.
2. Identifica todos os arquivos `POPSBRYY.zip` a partir de 2019.
3. Seleciona somente os 3 anos mais recentes disponiveis no FTP.
4. Baixa cada ZIP necessario.
5. Extrai o unico DBF interno.
6. Le o DBF diretamente em Python.
7. Normaliza os dados para colunas em minusculo.
8. Publica um Parquet por UF.
9. Mantem `data/processed/ibge/populacao/` como cache local por ano.

Publicacao atual:

```text
data/publish/referencias/ibge/populacao/current/11.parquet
data/publish/referencias/ibge/populacao/current/12.parquet
data/publish/referencias/ibge/populacao/current/33.parquet
```

Colunas publicadas:

```text
co_municipio_ibge
co_municipio
co_uf
ano
sexo
idade
pop
```

O manifest informa:

- `version`, por exemplo `2023-2025`;
- `years`, com os anos publicados;
- `partition: "uf"`;
- `municipality_filter_column: "co_municipio_ibge"`;
- `source_files`, com o ZIP usado por ano;
- `rows`, com o total nacional;
- `files.{uf}`, com caminho, SHA256 e linhas de cada UF.

Quando aparecer `POPSBR26.zip`, o checker mensal passara a publicar `2024-2026`
e removera `2023` da pasta publicada.

## Populacao por Raca/Cor Censo 2022

Fonte:

```text
SIDRA IBGE v1
Tabela 9606
Variavel 93 - Populacao residente
Censo Demografico 2022
```

Script:

```text
gerar_populacao_raca_censo.py
```

Funcionamento:

1. Consulta a API de metadados da tabela 9606.
2. Verifica o ultimo periodo disponivel sem baixar a tabela completa.
3. Se o periodo publicado ja for o mais recente, encerra sem alterar arquivos.
4. Se houver periodo novo, consulta a API SIDRA para cada UF.
5. Usa municipio, sexo, idade simples e raca/cor.
6. Quebra as consultas por blocos de idade para respeitar limites da API.
7. Publica um Parquet por UF.
8. Mantem `data/raw/ibge/populacao_raca_censo/{ano}/` e
   `data/processed/ibge/populacao_raca_censo/` como cache local.

Endpoint de verificacao:

```text
https://servicodados.ibge.gov.br/api/v3/agregados/9606/metadados
```

Publicacao:

```text
data/publish/referencias/ibge/populacao_raca_censo/current/11.parquet
data/publish/referencias/ibge/populacao_raca_censo/current/12.parquet
data/publish/referencias/ibge/populacao_raca_censo/current/33.parquet
```

Colunas publicadas:

```text
ano_censo
co_municipio_ibge
co_municipio
co_uf
sexo
no_sexo
idade
co_raca_cor
co_raca_cor_sidra
no_raca_cor
pop
```

Codificacao interna de raca/cor:

```text
1 branca
2 preta
3 amarela
4 parda
5 indigena
```

Esta referencia representa o dado censitario atomico de 2022. Ela nao e uma
projecao anual.

## Matriz de Pesos por Raca/Cor

Script:

```text
gerar_matriz_pesos_raca.py
```

Entrada:

```text
data/publish/referencias/ibge/populacao_raca_censo/current/{uf}.parquet
```

Publicacao:

```text
data/publish/referencias/ibge/matriz_pesos_raca/current/11.parquet
data/publish/referencias/ibge/matriz_pesos_raca/current/12.parquet
data/publish/referencias/ibge/matriz_pesos_raca/current/33.parquet
```

Colunas publicadas:

```text
ano_censo
co_municipio_ibge
co_municipio
co_uf
sexo
no_sexo
idade
co_raca_cor
co_raca_cor_sidra
no_raca_cor
pop_raca
pop_total_grupo
peso_raca
```

Calculo:

```text
peso_raca = pop_raca / pop_total_grupo
```

Onde `pop_total_grupo` e o total da celula:

```text
ano_censo + municipio + sexo + idade
```

Uso metodologico no VigiSUS-BR:

```text
PopEstimada(ano, mun, sexo, idade, raca) =
  TotalProjetadoDATASUS(ano, mun, sexo, idade) * PesoRacaCenso2022(mun, sexo, idade, raca)
```

Para os anos posteriores a 2022, a matriz usa os pesos raciais estaveis do
Censo 2022. O Censo 2010 nao e usado nesta versao do pipeline porque a
referencia operacional de populacao do VigiSUS-BR trabalha com os anos recentes
do POPSVS.

## UF

Fonte:

```text
https://servicodados.ibge.gov.br/api/v1/localidades/estados?orderBy=nome
```

Script:

```text
gerar_uf.py
```

Publicacao:

```text
data/publish/referencias/ibge/uf/current/uf.parquet
```

Colunas publicadas:

```text
co_uf
sg_uf
no_uf
co_regiao
sg_regiao
no_regiao
```

Esta referencia e deterministica/estavel e nao roda em cron.

## Municipios

Fonte:

```text
https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome
```

Script:

```text
gerar_municipios.py
```

Publicacao:

```text
data/publish/referencias/ibge/municipios/current/municipios.parquet
```

Colunas publicadas:

```text
co_municipio_ibge
co_municipio
no_municipio
co_uf
sg_uf
no_uf
co_regiao
sg_regiao
no_regiao
co_microrregiao
no_microrregiao
co_mesorregiao
no_mesorregiao
co_regiao_imediata
no_regiao_imediata
co_regiao_intermediaria
no_regiao_intermediaria
```

`co_municipio_ibge` tem 7 digitos. `co_municipio` tem 6 digitos, formato usado
em varias bases DATASUS.

Esta referencia e deterministica/estavel e nao roda em cron.

## Calendario Epidemiologico

Script:

```text
gerar_calendario_epidemiologico.py
```

Publicacao:

```text
data/publish/referencias/vigilancia/calendario_epidemiologico/current/calendario_epidemiologico.parquet
```

Colunas publicadas:

```text
data
ano
mes
dia
ano_epi
semana_epi
ano_semana_epi
ano_semana_epi_num
```

Regra implementada:

- semanas epidemiologicas vao de domingo a sabado;
- a semana epidemiologica 1 e a semana que contem a maioria dos dias em janeiro;
- isso equivale a usar a semana que contem 4 de janeiro;
- o arquivo cobre 1900 a 2100.

O VigiSUS-BR usa esse arquivo como fonte preferencial e pode gerar fallback local
se o arquivo remoto estiver indisponivel.

## GitHub Actions

CNES roda diariamente as 13:00 no horario de Brasilia:

```text
.github/workflows/update-cnes.yml
```

Cron em UTC:

```yaml
schedule:
  - cron: "0 16 * * *"
```

Populacao roda mensalmente as 13:00 no horario de Brasilia, no dia 5:

```text
.github/workflows/update-populacao.yml
```

Cron em UTC:

```yaml
schedule:
  - cron: "0 16 5 * *"
```

Populacao por raca/cor e matriz racial rodam a cada 6 meses, as 13:00 no
horario de Brasilia, no dia 15 de janeiro e julho:

```text
.github/workflows/update-populacao-raca-censo.yml
```

Cron em UTC:

```yaml
schedule:
  - cron: "0 16 15 1,7 *"
```

Os workflows:

1. Fazem checkout do repositorio.
2. Instalam Python 3.12.
3. Instalam `pandas` e `pyarrow`.
4. Executam o script correspondente.
5. Fazem `git add data/publish`.
6. Commitam e fazem push somente se houver mudanca real.
7. Enviam mensagem ao Discord em toda execucao concluida, com ou sem dados novos.

Para habilitar notificacao no Discord, configure o secret:

```text
Settings > Secrets and variables > Actions > New repository secret
Name: DISCORD_WEBHOOK_URL
Secret: https://discord.com/api/webhooks/...
```

## Rodar Localmente

Instale dependencias:

```powershell
pip install pandas pyarrow
```

Executar todas as referencias:

```powershell
python gerar_cnes.py
python gerar_populacao.py
python gerar_populacao_raca_censo.py
python gerar_matriz_pesos_raca.py
python gerar_uf.py
python gerar_municipios.py
python gerar_calendario_epidemiologico.py
```

Executar apenas referencias estaveis:

```powershell
python gerar_uf.py
python gerar_municipios.py
python gerar_calendario_epidemiologico.py
python gerar_populacao_raca_censo.py
python gerar_matriz_pesos_raca.py
```

Validar JSON dos manifests:

```powershell
python -m json.tool data/publish/manifest.json > $null
python -m json.tool data/publish/referencias/cnes/estabelecimentos/manifest.json > $null
python -m json.tool data/publish/referencias/ibge/populacao/manifest.json > $null
python -m json.tool data/publish/referencias/ibge/populacao_raca_censo/manifest.json > $null
python -m json.tool data/publish/referencias/ibge/matriz_pesos_raca/manifest.json > $null
python -m json.tool data/publish/referencias/ibge/uf/manifest.json > $null
python -m json.tool data/publish/referencias/ibge/municipios/manifest.json > $null
python -m json.tool data/publish/referencias/vigilancia/calendario_epidemiologico/manifest.json > $null
```

Validar sintaxe dos scripts:

```powershell
python -m py_compile gerar_cnes.py gerar_populacao.py gerar_populacao_raca_censo.py gerar_matriz_pesos_raca.py gerar_uf.py gerar_municipios.py gerar_calendario_epidemiologico.py
```

## Como o VigiSUS-BR Consome

1. Baixa `data/publish/manifest.json`.
2. Localiza o manifest especifico da referencia desejada.
3. Para bases por UF, usa `files.{co_uf}.path`.
4. Baixa apenas o Parquet da UF configurada.
5. Filtra localmente pelo municipio quando necessario.

Exemplos:

- CNES: baixar `referencias/cnes/estabelecimentos/current/33.parquet` e filtrar
  por `co_municipio_gestor`.
- Populacao: baixar `referencias/ibge/populacao/current/33.parquet` e filtrar
  por `co_municipio_ibge`.
- Matriz racial: baixar `referencias/ibge/matriz_pesos_raca/current/33.parquet`
  e aplicar `peso_raca` sobre a populacao POPSVS projetada.
- UF, municipios e calendario: baixar o arquivo nacional unico.

## Fontes de Dados

CNES:

- FTP DATASUS CNES: `ftp.datasus.gov.br/cnes`

Populacao:

- FTP DATASUS/IBGE POPSVS:
  `ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/POPSVS/`
- TABNET oficial do Ministerio da Saude informa a fonte POPSVS:
  https://tabnet.datasus.gov.br/cgi/deftohtm.exe?ibge%2Fcnv%2Fpopsvs2024br.def=

Populacao por raca/cor e matriz de pesos:

- SIDRA IBGE v1:
  https://apisidra.ibge.gov.br/
- Tabela 9606, Censo Demografico 2022, populacao residente por municipio,
  sexo, idade e cor ou raca.

UF e municipios:

- API oficial de Localidades do IBGE:
  https://servicodados.ibge.gov.br/api/v1/localidades/estados
- API oficial de Localidades do IBGE:
  https://servicodados.ibge.gov.br/api/v1/localidades/municipios

Calendario epidemiologico:

- Gerado por regra deterministica no proprio repositorio.

Relatorio Anual de Gestao 2024:

- Pagina oficial do RAG:
  https://www.gov.br/saude/pt-br/acesso-a-informacao/gestao-do-sus/instrumentos-de-planejamento/rag
- PDF do Relatorio Anual de Gestao 2024 na BVS MS:
  https://bvsms.saude.gov.br/bvs/publicacoes/relatorio_anual_gestao_2024.pdf

O RAG 2024 menciona a RIPSA e bases oficiais usadas como referencia. Este
repositorio cita o link original do PDF em vez de armazenar o arquivo, para
evitar duplicacao de documento oficial e manter a referencia na fonte primaria.
