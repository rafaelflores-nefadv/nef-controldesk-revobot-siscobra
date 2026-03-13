# Documentacao - Revo360 Bot

## Visao Geral

O projeto evoluiu de `1 ciclo = 1 arquivo` para `1 ciclo = N sources`.

No modelo atual:

- `DOWNLOAD_SOURCES` define os sources do ciclo
- cada source roda pipeline proprio e independente
- a execucao e sequencial
- falha de um source nao interrompe os demais
- state/checkpoint/retry sao por source
- notificacoes sao disparadas individualmente por source

Orquestracao: [../main.py](../main.py)

## Configuracao

Arquivo: [../src/config/settings.py](../src/config/settings.py)

### Campos por source em `DOWNLOAD_SOURCES`

- `id`: identificador unico
- `enabled`: habilita/desabilita o source
- `remote_folder`: pasta remota no File Manager
- `filename_template`: nome esperado do arquivo do ciclo
- `prepared_prefix`: prefixo do arquivo preparado
- `copy_dir`: destino local/servidor para `send_server`
- `ftp_dir`: destino FTP para `send_ftp`
- `send_to_server`: ativa/desativa envio local
- `send_to_ftp`: ativa/desativa envio FTP

### Configuracoes globais (compartilhadas)

- login/autenticacao
- e-mail
- Google Chat
- WhatsApp/WAHA
- parametros gerais de execucao, agenda, retry e logs

## Exemplo de Configuracao (2 sources)

```python
DOWNLOAD_SOURCES = [
    {
        "id": "default_0914",
        "enabled": True,
        "remote_folder": "Exportacao Siscobra 0914",
        "filename_template": "Exportacao_Siscobra_0914_{date:%Y%m%d}.csv",
        "prepared_prefix": "LOCAL_0914_...",
        "copy_dir": r"Z:\\control_desk\\RETORNO\\0914",
        "ftp_dir": "/ftp_empresa/SISCOBRA/RETORNO/0914",
        "send_to_server": True,
        "send_to_ftp": True,
    },
    {
        "id": "siscobra_planalto",
        "enabled": True,
        "remote_folder": "Exportacao Siscobra Planalto",
        "filename_template": "Exportacao_Siscobra_Planalto_{date:%Y%m%d}.csv",
        "prepared_prefix": "LOCAL_PLANALTO_...",
        "copy_dir": r"Z:\\control_desk\\RETORNO\\PLANALTO",
        "ftp_dir": "/ftp_empresa/SISCOBRA/RETORNO/PLANALTO",
        "send_to_server": True,
        "send_to_ftp": False,
    },
]
```

## Fluxo de Execucao por Source

1. discover/listagem remota
2. validacao do arquivo esperado
3. download
4. prepare
5. send_server
6. send_ftp
7. notify

Regras importantes:

- download so ocorre se o arquivo esperado existir na listagem
- sources sao processados em sequencia
- falha de um source nao interrompe os demais

## Download via Navegador Autenticado

O bot nao faz download direto por HTTP para o arquivo final.
Ele usa o navegador autenticado (Selenium) no endpoint do File Manager:

- `POST /api/file-manager-file-system?path=..\UPLOAD`
- `command=Download`
- `arguments` com `pathInfoList`

Formato de `arguments`:

```json
{
  "pathInfoList": [[
    { "key": "<folder>", "name": "<folder_name>" },
    { "key": "<folder>\\<filename>", "name": "<filename>" }
  ]]
}
```

Fluxo real:

`LOGIN -> contexto same-origin -> GetDirContents -> validacao do nome esperado -> form submit -> aguardar arquivo em DOWNLOAD_DIR`

## Stage Prepare e `remover_cabecalho_csv`

No stage `prepare`:

- copia o arquivo baixado para um arquivo temporario
- executa `remover_cabecalho_csv(...)`
- grava o arquivo preparado sem cabecalho

Comportamento atual da funcao:

- le CSV com pandas
- mantem todas as linhas de dados
- grava com `header=False` e `index=False`

Exemplo:

```python
df = pd.read_csv(caminho_arquivo, sep=";", dtype=str)
df.to_csv(caminho_arquivo, sep=";", index=False, header=False)
```

Observacao: a implementacao anterior removia incorretamente a primeira linha de dados. A atual remove apenas o cabecalho.

## State, Checkpoint e Retomada

- state diario: `downloads/logs/state_YYYY-MM-DD.json`
- progresso por source em `items[source_id]`
- cada item guarda `paths`, `stages`, `status`, `last_error`, `file`, `timestamps`
- source concluido nao e reprocessado
- source com falha/pendente retoma do ponto necessario

## Compatibilidade (Legado)

Quando aplicavel, o projeto ainda aceita configuracao legado baseada em variaveis globais (`FILE_MANAGER_EXPORT_FOLDER`, `FILE_PREFIX`, `COPY_DIR`, etc.).
States legados (sem `items[source_id]`) sao normalizados para o formato multi-source no carregamento.
O modo recomendado daqui em diante e usar `DOWNLOAD_SOURCES` como configuracao principal.

## Logs e Notificacoes

- log geral: `downloads/logs/execucao_YYYYMMDD_HHMMSS.log`
- log por source: `downloads/logs/sources/YYYY-MM-DD_<source_id>.log`
- notificacoes: configuracao global e envio individual por source

## Execucao Manual (CLI)

Executar todos os sources:

```bash
python main.py --run-anytime 12-03-2026
```

Executar apenas um source:

```bash
python main.py --run-anytime 12-03-2026 --source siscobra_planalto
```

Executar apenas envio local:

```bash
python main.py --run-anytime 12-03-2026 --source siscobra_planalto --local
```

Forcar execucao ignorando state:

```bash
python main.py --run-anytime 12-03-2026 --source siscobra_planalto --force-run
```

## Nota Operacional

Os nomes de pasta e arquivo devem coincidir exatamente com os retornados pelo File Manager do REVO360.
Diferencas de acentuacao ou case (ex.: `PLANALTO` vs `Planalto`) impedem a localizacao correta do arquivo.

## Modulos

- Arquitetura geral: [ARCHITECTURE.md](ARCHITECTURE.md)
- Download e listagem no browser autenticado: [modules/download.md](modules/download.md)

