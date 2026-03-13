# Architecture

## 1. Visao Geral

Modelo atual do ciclo:

- antes: `1 ciclo = 1 arquivo`
- agora: `1 ciclo = N sources`

Cada source tem configuracao propria e pipeline independente, com execucao sequencial.
Falha em um source nao interrompe os demais.

Orquestrador principal: [../main.py](../main.py)

## 2. Configuracao

Arquivo: [../src/config/settings.py](../src/config/settings.py)

### 2.1 `DOWNLOAD_SOURCES`

Campos suportados por item:

- `id`
- `enabled`
- `remote_folder`
- `filename_template`
- `prepared_prefix`
- `copy_dir`
- `ftp_dir`
- `send_to_server`
- `send_to_ftp`

### 2.2 Configuracoes globais

Permanecem globais:

- login/autenticacao
- e-mail
- Google Chat
- WhatsApp/WAHA
- agenda, retry, timeouts e logs

## 3. Exemplo de Configuracao

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

## 4. Fluxo do Pipeline por Source

1. discover/listagem remota
2. validacao do arquivo esperado
3. download
4. prepare
5. send_server
6. send_ftp
7. notify

Download so e disparado quando o nome esperado e encontrado na listagem.

## 5. Download no Navegador Autenticado

O fluxo de producao usa o browser autenticado (Selenium), nao download final direto por HTTP no Python.

Passos:

1. login no REVO360
2. garantir contexto same-origin do File Manager
3. listar via `GetDirContents`
4. validar existencia do arquivo esperado
5. submeter formulario de download no browser (`command=Download`)
6. aguardar arquivo no `DOWNLOAD_DIR`

Formato de `arguments` no submit:

```json
{
  "pathInfoList": [[
    { "key": "<folder>", "name": "<folder_name>" },
    { "key": "<folder>\\<filename>", "name": "<filename>" }
  ]]
}
```

## 6. Stage Prepare e Tratamento do CSV

Fluxo do stage `prepare`:

`downloaded_file -> tmp_file -> remover_cabecalho_csv(tmp_file) -> prepared_file`

Comportamento atual de `remover_cabecalho_csv`:

- le CSV com pandas
- mantem todas as linhas de dados
- grava sem cabecalho (`header=False`)

Exemplo:

```python
df = pd.read_csv(caminho_arquivo, sep=";", dtype=str)
df.to_csv(caminho_arquivo, sep=";", index=False, header=False)
```

## 7. State e Checkpoint

Arquivo:

- `downloads/logs/state_YYYY-MM-DD.json`

Estrutura:

- state diario do ciclo
- `items[source_id]` com subestado por source
- campos por item incluem `paths`, `stages`, `status`, `last_error`, `file`, `timestamps`

## 8. Retry e Retomada

- retry e retomada por source
- source concluido nao e reprocessado
- source com falha/pendente continua do ponto necessario
- falhas em envio nao exigem novo download se artefatos continuam validos

## 9. Logs e Notificacoes

- log geral da execucao
- logs individuais por source
- configuracao de notificacao global
- envio de notificacao individual por source

## 10. Nota Operacional

Nomes de pasta/arquivo precisam bater exatamente com o File Manager do REVO360.
Diferencas de acentuacao ou case (ex.: `PLANALTO` vs `Planalto`) impedem match na descoberta.

## 11. Referencias

- Guia operacional: [README.md](README.md)
- Detalhes do download: [modules/download.md](modules/download.md)

## 12. Compatibilidade (Legado)

- configuracao legado com variaveis globais ainda e aceita quando aplicavel
- state legado (sem `items`) e convertido para o formato atual no carregamento
- recomendacao: manter `DOWNLOAD_SOURCES` como padrao de configuracao

