# Revo360 Bot

Automacao do REVO360 com pipeline diario multi-source orientado a configuracao.

## Modelo Atual

- 1 ciclo diario = N sources configuraveis (`DOWNLOAD_SOURCES`)
- pipeline independente por source (`download -> prepare -> send_server -> send_ftp`)
- execucao sequencial (sem paralelismo)
- retry/checkpoint por source no state diario
- notificacao individual por source

## Exemplo de DOWNLOAD_SOURCES

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

Cada source e processado de forma independente. Falha em um source nao interrompe os demais. O state diario e os logs sao separados por source.

## Como o Download Funciona

O bot nao faz download binario direto por HTTP no Python.
O download ocorre no navegador autenticado (Selenium), via form submit:

- endpoint: `POST /api/file-manager-file-system?path=..\UPLOAD`
- `command=Download`
- `arguments` com `pathInfoList`

Estrutura de `arguments`:

```json
{
  "pathInfoList": [[
    { "key": "<folder>", "name": "<folder_name>" },
    { "key": "<folder>\\<filename>", "name": "<filename>" }
  ]]
}
```

Fluxo real:

1. LOGIN REVO360
2. abrir contexto same-origin do File Manager
3. listar arquivos via `GetDirContents`
4. validar se o arquivo esperado existe
5. executar form submit no navegador
6. aguardar arquivo aparecer em `DOWNLOAD_DIR`

## Stage Prepare

- cria uma copia temporaria do arquivo baixado
- executa `remover_cabecalho_csv(...)`
- grava o arquivo preparado sem cabecalho

Correcao atual: apenas o cabecalho e removido, mantendo todas as linhas de dados.

## Execucao Manual

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

## Instalacao

```bash
pip install -r requirements.txt
```

## Documentacao Completa

- [docs/README.md](docs/README.md)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/modules/download.md](docs/modules/download.md)


# nef-controldesk-revobot-siscobra
