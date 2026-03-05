# Listar Eventos - Relatorios BLiP

Utilitario em Python para consultar eventos no BLiP Analytics e gerar planilhas Excel com:
- inconsistencias por tracking;
- eventos por `action`;
- relatorio consolidado (inconsistencias + eventos por action);
- relatorio analitico consolidado (base bruta da API, sem deduplicacao).

Os scripts podem ser executados individualmente ou pelo menu interativo.

## Pre-requisitos

- Python 3.10+ (recomendado 3.12+).
- Dependencias de `requirements.txt`.
- Credenciais BLiP no arquivo `.env`.

### Variaveis de ambiente

Configure na raiz do projeto:

```env
URL=
KEY_DUDA_PRD=
KEY_FGTS_PRD=
KEY_CLT_PRD=
KEY_DUDA_DEV=
```

## Instalacao

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Como executar

### Menu unificado

```bash
python menu_relatorios.py
```

Opcoes atuais do menu:

- `1` - `getEventDeatilsV3.py`: relatorio de inconsistencias por tracking + resumo de inconsistencias + action/extras expandidos.
- `2` - `getEventCategoriesV2.py`: eventos por `action`, com deduplicacao por `contactIdentity + action`.
- `3` - `getEventDeatilsV3_com_actions.py`: consolidado (inconsistencias + consulta por actions em paralelo).
- `4` - `getEventDeatilsV3_analitico.py`: consolidado analitico com metricas e base bruta (sem deduplicacao).
- `0` - sair.

### Arquivos usados pelo menu

O `menu_relatorios.py` executa somente estes arquivos:

- opcao `1` -> `getEventDeatilsV3.py`
- opcao `2` -> `getEventCategoriesV2.py`
- opcao `3` -> `getEventDeatilsV3_com_actions.py`
- opcao `4` -> `getEventDeatilsV3_analitico.py`

Arquivos como `relatorio.py`, `resetBlip.py`, `event_counters.csv` e `event_details_all.csv` nao fazem parte do fluxo do menu.

### Execucao direta dos scripts

```bash
python getEventDeatilsV3.py
python getEventCategoriesV2.py
python getEventDeatilsV3_com_actions.py
python getEventDeatilsV3_analitico.py
```

## Fluxo de uso (resumo)

1. Informar data inicial e final (`DD-MM-AAAA` ou `YYYY-MM-DD`).
2. Informar tracking (ou aceitar o padrao).
3. Escolher o router/chave de autorizacao.
4. Confirmar geracao de Excel (`s` ou `n`).

No `getEventCategoriesV2.py`, tambem e necessario colar a `ACTION` completa.

No `getEventDeatilsV3_analitico.py`, os dados de eventos por action sao mantidos como a API retorna (sem deduplicacao).

## O que cada script gera

- `getEventDeatilsV3.py`
  - Aba `relatorio por eventos`
  - Aba `resumo inconsistencias`
  - Aba `action raw expandido`

- `getEventCategoriesV2.py`
  - Aba `relatorio por action` (eventos deduplicados por contato + action)

- `getEventDeatilsV3_com_actions.py`
  - Aba `relatorio por eventos`
  - Aba `relatorio por action`
  - Aba `actions falharam` (quando houver erro nas consultas por action)

- `getEventDeatilsV3_analitico.py`
  - Aba `relatorio por eventos`
  - Aba `relatorio por action`
  - Abas de metricas analiticas (`resumo_geral`, `top_actions`, `erros_por_status`, `categorias`, `serie_temporal`, `resumo_incons`)
  - Aba `actions_falharam` (quando houver erro nas consultas por action)

## Status do analitico (em ajuste)

O `getEventDeatilsV3_analitico.py` ainda esta em evolucao. Objetivo esperado:

- Manter base bruta da API para analise (sem deduplicacao na aba de eventos por action).
- Consolidar indicadores de volume, erros e taxa de erro por action/categoria/dia.
- Facilitar leitura executiva no `resumo_geral` sem perder rastreabilidade para o detalhe.

## Saida dos relatorios

Pasta base:

- `~/Documentos/relatorio de eventos/` (ou `~/Documents/relatorio de eventos/`, dependendo do sistema)

Estrutura gerada automaticamente:

- `eventDetails/<router>/relatorio_AAAAMMDD/`
- `eventCategories/<router>/relatorio_AAAAMMDD/`

Os arquivos recebem sufixo `_vN` quando ja existe um relatorio com o mesmo nome no dia.

## Estrutura do projeto

- `menu_relatorios.py`: menu principal.
- `getEventDeatilsV3.py`: inconsistencias por tracking com abas de resumo e expansao.
- `getEventCategoriesV2.py`: eventos por action (deduplicado por contato + action).
- `getEventDeatilsV3_com_actions.py`: consolidado de inconsistencias + eventos por action em paralelo.
- `getEventDeatilsV3_analitico.py`: consolidado analitico (em ajuste).
- `requirements.txt`: dependencias.
- `.env`: configuracao local (nao versionar chaves).

## Problemas comuns

- `401` ou `403`: chave incorreta ou router errado.
- `400`: tracking/action/data invalidos na consulta.
- Nenhum item retornado: periodo sem eventos ou filtro muito restritivo.

## Observacoes

- O nome dos scripts contem `Deatils` por compatibilidade com o historico do projeto.
- Se a acentuacao aparecer incorreta no terminal, verifique a codificacao UTF-8 no editor/console.
