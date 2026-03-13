# Modulo `download`

## Objetivo

Documentar o fluxo real de discover/download no REVO360 e sua integracao com o stage `prepare`.

Arquivos principais:

- [../../src/core/download_api.py](../../src/core/download_api.py)
- [../../src/core/download.py](../../src/core/download.py)
- [../../main.py](../../main.py)

## Pipeline por Source

No source atual, a etapa `download` executa:

1. login no REVO360
2. abrir contexto same-origin do File Manager no navegador autenticado
3. listar arquivos da pasta remota (`GetDirContents`)
4. validar o arquivo esperado do ciclo
5. submeter download via formulario no navegador
6. aguardar arquivo final em `DOWNLOAD_DIR`

Se o arquivo esperado nao estiver na listagem, o download nao e disparado.

## Descoberta do Arquivo

- com `filename_template`: resolve nome esperado e exige match exato
- sem template: fallback para selecao por data no nome

Metadados gravados em `item_state["file"]`:

- `expected_name`
- `resolved_name`
- `found_in_listing`
- `listed_count`
- `listed_at`

## Download via Navegador (Form Submit)

O bot nao faz download final direto por HTTP no Python.
O download e disparado pelo navegador autenticado (Selenium) com formulario:

- `method=POST`
- endpoint: `/api/file-manager-file-system?path=..\UPLOAD`
- campo `command=Download`
- campo `arguments` no formato abaixo

```json
{
  "pathInfoList": [[
    { "key": "<folder>", "name": "<folder_name>" },
    { "key": "<folder>\\<filename>", "name": "<filename>" }
  ]]
}
```

Fluxo real:

`LOGIN -> same-origin File Manager -> GetDirContents -> validar nome -> form submit -> aguardar DOWNLOAD_DIR`

## Stage `prepare`

Depois do download, o pipeline chama `prepare`:

`downloaded_file -> tmp_file -> remover_cabecalho_csv(tmp_file) -> prepared_file`

O stage:

- copia o arquivo baixado para temporario
- aplica limpeza de cabecalho
- grava arquivo final preparado

## Funcao `remover_cabecalho_csv`

Comportamento atual:

- le CSV com pandas (`sep=";"`, `dtype=str`)
- mantem todas as linhas de dados
- grava sem cabecalho (`header=False`) e sem indice (`index=False`)

Exemplo:

```python
df = pd.read_csv(caminho_arquivo, sep=";", dtype=str)
df.to_csv(caminho_arquivo, sep=";", index=False, header=False)
```

Nota de historico: a versao anterior removia indevidamente a primeira linha de dados. A versao atual remove apenas o cabecalho.

## Confirmacao de Conclusao do Download

O arquivo so e considerado valido quando:

- aparece no `DOWNLOAD_DIR`
- tem tamanho maior que zero
- nao existe `.crdownload` pendente correspondente

## Integracao com Multi-Source

A etapa usa os campos do source atual (`item_state["source"]`), como:

- `id`
- `remote_folder`
- `filename_template`

Resultado e persistido por source em `items[source_id]`, sem bloquear outros sources.

## Falhas Esperadas

- arquivo esperado ausente na listagem
- sessao expirada/autenticacao invalida
- erro ao submeter formulario
- arquivo nao apareceu no `DOWNLOAD_DIR`
- arquivo vazio

Essas falhas entram no state/checkpoint e no retry por source.

## Nota Operacional

Nomes de pasta e arquivo devem coincidir exatamente com o retorno do File Manager do REVO360.
Diferencas de acentuacao ou case (ex.: `PLANALTO` vs `Planalto`) impedem a localizacao do arquivo.
