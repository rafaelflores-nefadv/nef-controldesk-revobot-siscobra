from pathlib import Path

# REVO360 CONFIG
BASE_URL = "https://nef.revo360.io/login"
USERNAME = "rafael.flores"
PASSWORD = "nef@2212"

HEADLESS = False
DEFAULT_TIMEOUT = 20 # TEMPO DE ESPERA PARA O BOT AGUARDAR A PÁGINA CARREGAR

DOWNLOAD_DIR = Path(__file__).resolve().parents[2] / "downloads"
LOG_DIR = DOWNLOAD_DIR / "logs"

# AGENDAMENTO
# 0=segunda ... 6=domingo
RUN_DAYS = [0, 1, 2, 3, 4]
# horario no formato HH:MM (24h)
RUN_TIME = "18:40"
# se True, o bot ignora execucoes fora do agendamento
ENFORCE_SCHEDULE = True
# data inicial opcional para começar a executar (YYYY-MM-DD)
RUN_START_DATE = None
# intervalo de verificacao do agendamento (segundos)
SCHEDULE_CHECK_SECONDS = 10
# modo daemon: processo permanece vivo para executar diariamente
DAEMON_MODE_ENABLED = True
# fallback de espera quando nao for possivel calcular o proximo agendamento
DAEMON_POLL_SECONDS = 30
# pequena espera apos o ciclo para evitar dupla execucao na borda da janela
DAEMON_AFTER_RUN_SLEEP_SECONDS = 60

# RETRY DE EXECUCAO
# Tentativa 1 = execucao normal do dia.
# Tentativas seguintes = retries.
# Exemplo: RETRY_MAX_ATTEMPTS=3 => no maximo 3 execucoes naquele dia.
RETRY_ON_FAILURE_ENABLED = True
RETRY_MAX_ATTEMPTS = 10
RETRY_DELAY_SECONDS = 1800 # 30 minutos entre cada tentativa

# NOME DO ARQUIVO A SER BAIXADO
FILE_PREFIX = "LOCAL_0914_80_NABARRETEFERRO_ACIONAMENTOS_"
FILE_MANAGER_EXPORT_FOLDER = "Exportação Siscobra 0914"
FILE_NAME_TEMPLATE = "Exportacao_Siscobra_0914_{date:%Y%m%d}.csv"
FILE_MANAGER_API_BASE_URL = "https://nef.revo360.io:10024"
FILE_MANAGER_HTTP_TIMEOUT_CONNECT = 15
FILE_MANAGER_HTTP_TIMEOUT_READ = 120
FILE_MANAGER_HTTP_RETRY_ATTEMPTS = 3
FILE_MANAGER_HTTP_RETRY_BACKOFF_SECONDS = 1.0
DOWNLOAD_WAIT_TIMEOUT_SECONDS = 120
DOWNLOAD_WAIT_POLL_SECONDS = 0.5

# DIR SERVIDOR NABARRETE & FERRO
COPY_DIR = r"Z:\recuperação_extrajudicial\control_desk\SICREDI CELEIRO - SISCOBRA\2026\RETORNO\01-2026"

# CONTROLE DE COPIAS
ENABLE_COPIES = True
COPY_TO_SERVER = True
COPY_TO_FTP = True

# FTP CONFIG
FTP_HOST = "172.25.68.13"
FTP_USER = "silva_cunha"
FTP_PASSWORD = "Nef2025*"
FTP_DIR = "/ftp_nabarreteferro_adv/SISCOBRA/RETORNO"
FTP_USE_TLS = True
FTP_PORT = 990
FTP_TLS_IMPLICIT = True
FTP_TLS_VERIFY = False
FTP_TIMEOUT = 30
FTP_TLS_LEGACY = True
FTP_DATA_PROTECTION = "P"  # P=TLS no canal de dados, C=canal limpo
FTP_PASSIVE = True

# MULTI-SOURCE DOWNLOAD (modelo novo)
# Mantem compatibilidade com o modelo legado de 1 arquivo.
DOWNLOAD_SOURCES = [
    {
        "id": "siscobra_celeiro",
        "enabled": True,
        "remote_folder": "Exportação Siscobra 0914",
        "filename_template": "Exportacao_Siscobra_0914_{date:%Y%m%d}.csv",
        "prepared_prefix": "LOCAL_0914_80_NABARRETEFERRO_ACIONAMENTOS_",
        "copy_dir": r"Z:\\control_desk\\RETORNO\\0914",
        "ftp_dir": "/ftp_nabarreteferro_adv/SISCOBRA/RETORNO",
        "send_to_server": True,
        "send_to_ftp": True,
    },
    {
        "id": "siscobra_planalto",
        "enabled": True,
        "remote_folder": "Exportação Siscobra Planalto",
        "filename_template": "Exportacao_Siscobra_Planalto_{date:%Y%m%d}.csv",
        "prepared_prefix": "LOCAL_3953_190_NABARRETEFERRO_ACIONAMENTOS_",
        "copy_dir": r"Z:\\control_desk\\RETORNO\\3953",
        "ftp_dir": "/ftp_nabarreteferro_adv/SISCOBRA/RETORNO",
        "send_to_server": True,
        "send_to_ftp": True,
    }
]

GOOGLE_CHAT_WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQAlFBeLe4/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=73TMA4x-dtsV1sroy0s9zOenMMPOTgqHKlE-1L3-XEs"
GOOGLE_CHAT_TIMEOUT = 10

# WhatsApp notifications (WAHA)
ENABLE_WHATSAPP_NOTIFICATION = True
WAHA_BASE_URL = "http://69.6.221.156:3000/"
WAHA_SESSION = "default"
WAHA_CHAT_IDS = ['556793087866', '556781386476', '556781376048']
WAHA_API_KEY = "a6a6311f4a54b1fda7d0b0c9fa7fd8a4b523bb0de67674695bf3ea545d36255b"
WAHA_TIMEOUT = 10
WAHA_FAIL_FAST = False
NOTIFY_ASCII_ONLY = False

# Email notifications (Gmail SMTP)
ENABLE_EMAIL_NOTIFICATION = True

EMAIL_SMTP_HOST = "smtp.gmail.com"
EMAIL_SMTP_PORT = 587
EMAIL_USE_TLS = True

EMAIL_USER = "max.ia@nefadv.com.br"
EMAIL_PASSWORD = "duav ppuj momp eyri" # senha do gmail: Maxai#2026
EMAIL_FROM = "max.ia@nefadv.com.br"

# Lista de destinatários configurável
EMAIL_TO = [
    "rafaelflores@nefadv.com.br ",
    "juliogoncalves@nefadv.com.br",
    "mauriciooliveira@nefadv.com.br",
    "vitor.niz@extranef.com.br",
    "talisson.inacio@extranef.com.br",
]

EMAIL_TIMEOUT = 10
