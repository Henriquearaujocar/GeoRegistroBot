# -*- coding: utf-8 -*-
import logging
import os
import time
import pytz  # Melhor biblioteca para timezones
from datetime import datetime, timedelta, date
from collections import defaultdict
import gspread  # Para Google Sheets
from google.oauth2.service_account import Credentials  # Para autenticação Google
from google.auth.exceptions import RefreshError
from telegram import Update, Location
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    JobQueue,
)
from telegram.constants import ParseMode
from dotenv import load_dotenv
import json # <--- ADICIONADO PARA PERSISTÊNCIA
import copy # <--- ADICIONADO PARA PERSISTÊNCIA

# --- Carregar Variáveis de Ambiente ---
load_dotenv()
print("Tentando carregar variáveis de ambiente do arquivo .env...")

# --- Constantes de Configuração (Lidas do Ambiente) ---
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
SPREADSHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME")
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE_PATH")
LOG_LEVEL_STR = os.environ.get("LOG_LEVEL", "INFO").upper()
try:
    target_group_id_env = os.environ.get("TELEGRAM_TARGET_GROUP_ID")
    TARGET_GROUP_ID = int(target_group_id_env) if target_group_id_env else 0
except (ValueError, TypeError):
    TARGET_GROUP_ID = 0
try:
    admin_ids_str = os.environ.get("TELEGRAM_ADMIN_IDS", "")
    ADMIN_USER_IDS = set()
    if admin_ids_str:
        for admin_id in admin_ids_str.split(','):
            try:
                if admin_id.strip():
                    ADMIN_USER_IDS.add(int(admin_id.strip()))
            except ValueError:
                print(f"AVISO: Ignorando Admin ID inválido '{admin_id.strip()}' em TELEGRAM_ADMIN_IDS.")
except Exception as e:
     print(f"ERRO ao processar TELEGRAM_ADMIN_IDS: {e}")
     ADMIN_USER_IDS = set()
try:
    MAX_INACTIVE_HOURS = int(os.environ.get("MAX_INACTIVE_HOURS", 9))
except (ValueError, TypeError):
    MAX_INACTIVE_HOURS = 9
try:
    CLEANUP_JOB_INTERVAL_MINUTES = int(os.environ.get("CLEANUP_JOB_INTERVAL_MINUTES", 60))
except (ValueError, TypeError):
    CLEANUP_JOB_INTERVAL_MINUTES = 60

# --- Constantes do Código ---
COL_USERID = 'UserID'
COL_USERNAME = 'Username'
COL_START_BR = 'StartTimeBR'
COL_END_BR = 'EndTimeBR'
COL_DURATION = 'Duration'
COL_START_UTC = 'StartTimeUTC'
COL_END_UTC = 'EndTimeUTC'
COL_DURATION_SEC = 'DurationSeconds'
REQUIRED_HEADERS = {COL_USERID, COL_USERNAME, COL_START_BR, COL_END_BR, COL_DURATION, COL_START_UTC, COL_END_UTC, COL_DURATION_SEC}
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
UTC_TZ = pytz.utc
BR_TZ = pytz.timezone('America/Sao_Paulo') # Horário de Brasília

# <<< NOVO: Nome do arquivo para salvar o estado >>>
STATE_FILENAME = "active_shares_state.json"

# --- Configuração de Logging ---
numeric_level = getattr(logging, LOG_LEVEL_STR, logging.INFO)
log_format = "%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s"
logging.basicConfig(format=log_format, level=numeric_level)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google.auth.transport.requests").setLevel(logging.WARNING)
logging.getLogger("google.oauth2.credentials").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Estado Global (Conexão Planilha e Partilhas Ativas) ---
gs_client: gspread.Client | None = None
gs_worksheet: gspread.Worksheet | None = None
# active_shares será inicializado pelo load_state agora
active_shares: dict = {}

# --- Funções Auxiliares ---

# ==============================================
# NOVAS FUNÇÕES PARA PERSISTÊNCIA JSON
# ==============================================

def save_state(data: dict, filename: str) -> None:
    """Salva o dicionário active_shares em um arquivo JSON de forma segura."""
    logger.debug(f"Tentando salvar estado no arquivo: {filename}")
    temp_filename = filename + ".tmp"

    # Cria uma cópia profunda para não modificar o dicionário original durante o processo
    # e converte chaves de tupla para string e datetimes para ISO string
    data_to_save = {}
    try:
        data_copy = copy.deepcopy(data) # Trabalha na cópia
        for key_tuple, value_dict in data_copy.items():
            # Converte chave (chat_id, message_id) para string "chat_id:message_id"
            str_key = f"{key_tuple[0]}:{key_tuple[1]}"

            # Converte datetimes dentro do dicionário de valor para ISO string
            if 'start_time' in value_dict and isinstance(value_dict['start_time'], datetime):
                value_dict['start_time'] = value_dict['start_time'].isoformat()
            if 'last_update' in value_dict and isinstance(value_dict['last_update'], datetime):
                value_dict['last_update'] = value_dict['last_update'].isoformat()

            data_to_save[str_key] = value_dict

    except Exception as e:
         logger.error(f"Erro ao preparar dados para salvar estado: {e}", exc_info=True)
         return # Não tenta salvar se a preparação falhar

    # Salva no arquivo temporário e depois renomeia (atomic replace)
    try:
        with open(temp_filename, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False) # indent=4 para legibilidade
        os.replace(temp_filename, filename) # Renomeia atomicamente
        logger.info(f"Estado salvo com sucesso em {filename} ({len(data_to_save)} entradas)")
    except IOError as e:
        logger.error(f"Erro de I/O ao salvar estado em {temp_filename} ou {filename}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao salvar estado: {e}", exc_info=True)
    finally:
        # Garante que o arquivo temporário seja removido se algo der errado após criá-lo
        if os.path.exists(temp_filename):
            try:
                os.remove(temp_filename)
                logger.debug(f"Arquivo temporário {temp_filename} removido.")
            except OSError as e:
                logger.error(f"Erro ao remover arquivo temporário {temp_filename}: {e}")


def load_state(filename: str) -> dict:
    """Carrega o dicionário active_shares de um arquivo JSON."""
    logger.info(f"Tentando carregar estado do arquivo: {filename}")
    state_data = {}
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            loaded_data = json.load(f)

        # Converte chaves de string de volta para tupla e ISO strings para datetime
        for str_key, value_dict in loaded_data.items():
             try:
                 # Converte chave "chat_id:message_id" para tupla (int(chat_id), int(message_id))
                 chat_id_str, message_id_str = str_key.split(':')
                 key_tuple = (int(chat_id_str), int(message_id_str))

                 # Converte ISO strings de volta para datetime OBJETOS AWARE (UTC)
                 if 'start_time' in value_dict and isinstance(value_dict['start_time'], str):
                     dt_obj = datetime.fromisoformat(value_dict['start_time'])
                     # Garante que seja UTC (fromisoformat já deve fazer isso se o offset estiver na string)
                     value_dict['start_time'] = dt_obj.astimezone(UTC_TZ)
                 if 'last_update' in value_dict and isinstance(value_dict['last_update'], str):
                     dt_obj = datetime.fromisoformat(value_dict['last_update'])
                     value_dict['last_update'] = dt_obj.astimezone(UTC_TZ)

                 state_data[key_tuple] = value_dict
             except (ValueError, KeyError, TypeError) as e:
                 logger.warning(f"Erro ao processar entrada do estado carregado (chave: {str_key}): {e}. Ignorando entrada.")
                 continue # Pula para a próxima entrada se esta estiver mal formada

        logger.info(f"Estado carregado com sucesso de {filename} ({len(state_data)} entradas)")
        return state_data

    except FileNotFoundError:
        logger.info(f"Arquivo de estado {filename} não encontrado. Iniciando com estado vazio.")
        return {} # Retorna dicionário vazio se o arquivo não existe
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do arquivo de estado {filename}: {e}. Iniciando com estado vazio.")
        return {} # Retorna dicionário vazio se o arquivo estiver corrompido
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar estado de {filename}: {e}", exc_info=True)
        return {} # Segurança: retorna dicionário vazio em caso de outros erros

# ==============================================
# FIM DAS NOVAS FUNÇÕES
# ==============================================

def format_duration(total_seconds: int) -> str:
    """Formata segundos totais em HH:MM:SS (considerando dias)."""
    if total_seconds < 0: total_seconds = 0
    td = timedelta(seconds=total_seconds)
    mm, ss = divmod(td.seconds, 60)
    hh, mm = divmod(mm, 60)
    total_hours = td.days * 24 + hh
    return f"{total_hours:02}:{mm:02}:{ss:02}"

def setup_google_sheets() -> bool:
    """Configura a conexão com o Google Sheets. Retorna True se sucesso, False caso contrário."""
    global gs_client, gs_worksheet
    logger.info("Tentando conectar ao Google Sheets...")
    try:
        if not SERVICE_ACCOUNT_FILE:
            raise ValueError("Caminho do arquivo de credenciais (GOOGLE_SERVICE_ACCOUNT_FILE_PATH) não configurado.")
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(f"Arquivo de credenciais '{SERVICE_ACCOUNT_FILE}' não encontrado.")
        if not SPREADSHEET_ID:
            raise ValueError("ID da Planilha Google (GOOGLE_SHEET_ID) não configurado.")
        if not SHEET_NAME:
            raise ValueError("Nome da Aba (GOOGLE_SHEET_NAME) não configurado.")

        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        headers = worksheet.row_values(1)
        logger.info(f"Leitura inicial da planilha '{spreadsheet.title}' -> Aba '{worksheet.title}' bem-sucedida. Cabeçalhos: {headers}")
        missing_headers = REQUIRED_HEADERS - set(h.strip() for h in headers)
        if missing_headers:
            logger.error(f"Erro CRÍTICO: Cabeçalho(s) obrigatório(s) não encontrado(s) na planilha: {missing_headers}. Verifique a primeira linha da aba '{SHEET_NAME}'.")
            return False
        gs_client = client
        gs_worksheet = worksheet
        logger.info("Conexão com Google Sheets estabelecida e validada com sucesso.")
        return True
    # ... (resto do seu tratamento de erro para setup_google_sheets) ...
    except FileNotFoundError as fnf_error:
         logger.error(f"Erro de configuração Google Sheets: {fnf_error}")
    except ValueError as val_error:
        logger.error(f"Erro de configuração Google Sheets: {val_error}")
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"Erro CRÍTICO: Planilha com ID '{SPREADSHEET_ID}' não encontrada...")
    except gspread.exceptions.WorksheetNotFound:
        logger.error(f"Erro CRÍTICO: Aba/Página '{SHEET_NAME}' não encontrada...")
    except RefreshError as auth_error:
        logger.error(f"Erro de Autenticação Google: {auth_error}...")
    except gspread.exceptions.APIError as api_error:
        logger.error(f"Erro de API Google Sheets (código: {api_error.response.status_code}): {api_error}...")
    except Exception as e:
        logger.error(f"Erro inesperado ao conectar com Google Sheets: {e}", exc_info=True)
    gs_client = None
    gs_worksheet = None
    return False


def get_worksheet_with_retry(max_retries=2, delay=5) -> gspread.Worksheet | None:
    """Tenta obter o worksheet global, reconectando se necessário, com retentativas."""
    global gs_worksheet, gs_client
    if gs_worksheet and gs_client:
        try:
            gs_worksheet.acell('A1', value_render_option='FORMATTED_VALUE')
            logger.debug("Worksheet existente ainda válido.")
            return gs_worksheet
        except (gspread.exceptions.APIError, RefreshError, ConnectionError, Exception) as e:
            logger.warning(f"Worksheet existente inválido ({type(e).__name__}). Tentando reconectar...")
            gs_worksheet = None
            gs_client = None
    logger.info("Tentando obter worksheet (nova conexão ou reconexão)...")
    for attempt in range(max_retries + 1):
        if setup_google_sheets():
            return gs_worksheet
        if attempt < max_retries:
            logger.warning(f"Falha ao obter worksheet. Tentando novamente em {delay}s... (Tentativa {attempt + 1}/{max_retries + 1})")
            time.sleep(delay)
    logger.error("Não foi possível obter worksheet funcional após múltiplas tentativas.")
    return None

async def append_row_with_retry(data: list, max_retries=2, delay=5) -> bool:
    """Adiciona uma linha na planilha com retentativas em caso de erro de API."""
    worksheet = get_worksheet_with_retry()
    if not worksheet:
        logger.error(f"Falha CRÍTICA: Não foi possível obter worksheet para adicionar dados: {data}")
        return False
    username_log = data[1] if len(data) > 1 else "N/A"
    userid_log = data[0] if len(data) > 0 else "N/A"
    for attempt in range(max_retries + 1):
        try:
            worksheet.append_row(data, value_input_option='USER_ENTERED')
            logger.info(f"Dados gravados com sucesso na planilha: {username_log} ({userid_log})")
            return True
        except gspread.exceptions.APIError as api_error:
            logger.error(f"Erro de API ao gravar na planilha para {username_log} (Tentativa {attempt + 1}/{max_retries + 1}): {api_error}")
            if api_error.response.status_code in [429, 500, 503]:
                if attempt < max_retries:
                    logger.warning(f"Tentando novamente em {delay}s...")
                    time.sleep(delay)
                    worksheet = get_worksheet_with_retry()
                    if not worksheet:
                        logger.error("Falha ao obter worksheet antes de retentativa de escrita.")
                        break
                else:
                    logger.error(f"Falha ao gravar dados de {username_log} após {max_retries + 1} tentativas.")
                    return False
            else:
                logger.error(f"Erro de API não recuperável ({api_error.response.status_code}). Não tentando novamente.")
                return False
        except Exception as e:
            logger.error(f"Erro inesperado ao gravar na planilha para {username_log} (Tentativa {attempt + 1}): {e}", exc_info=True)
            return False
    return False

# --- Handlers do Telegram ---

async def debug_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Loga todas as atualizações recebidas (nível DEBUG)."""
    if logger.isEnabledFor(logging.DEBUG):
        chat_id = update.effective_chat.id if update.effective_chat else "N/A"
        user_id = update.effective_user.id if update.effective_user else "N/A"
        update_type = update.__class__.__name__
        logger.debug(f"--- RAW UPDATE --- Type: {update_type}, ChatID: {chat_id}, UserID: {user_id}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler para /start (apenas Admins em PV)."""
    user = update.effective_user
    if not user or not update.message or update.message.chat.type != 'private' or user.id not in ADMIN_USER_IDS:
        logger.warning(f"Usuário {user.id if user else 'Desconhecido'} tentou usar /start sem permissão ou fora do PV.")
        return
    await update.message.reply_html(
        f"Olá Admin {user.mention_html()}!\n"
        f"Monitorando localizações em tempo real no grupo ID: <code>{TARGET_GROUP_ID}</code>.\n"
        f"Planilha: <code>{SPREADSHEET_ID}</code> | Aba: <code>{SHEET_NAME}</code>\n"
        f"Use /status para ver registros ou /help para comandos."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler para /help (apenas Admins em PV)."""
    user = update.effective_user
    if not user or not update.message or update.message.chat.type != 'private' or user.id not in ADMIN_USER_IDS:
        logger.warning(f"Usuário {user.id if user else 'Desconhecido'} tentou usar /help sem permissão ou fora do PV.")
        return
    await update.message.reply_text(
        "Comandos disponíveis (apenas Admins no PV):\n"
        "/start - Mensagem de boas-vindas.\n"
        "/status [dd/mm/yyyy] - Mostra compartilhamentos concluídos (da planilha) e ativos (da memória) para a data especificada (ou hoje se omitido).\n"
        "/help - Mostra esta mensagem.\n\n"
        "Funcionamento:\n"
        "1. Colaboradores enviam 'Localização em Tempo Real' no grupo configurado.\n"
        "2. O bot registra início e fim e salva na Planilha Google.\n"
        "3. Admins usam /status neste chat privado para ver os dados."
    )

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processa o envio inicial de uma localização DENTRO DO GRUPO ALVO."""
    if not update.message or not update.message.from_user or not update.message.location:
        logger.debug("handle_location: Ignorando update sem mensagem, remetente ou localização.")
        return

    message = update.message
    user = message.from_user
    current_location: Location = message.location

    if current_location.live_period and current_location.live_period > 0:
        chat_id = message.chat_id
        message_id = message.message_id
        user_id = user.id
        username = user.username if user.username else user.first_name
        now_utc = datetime.now(UTC_TZ)
        share_key = (chat_id, message_id)

        if share_key in active_shares:
            logger.warning(f"handle_location: Recebido início de partilha para chave {share_key} que já estava ativa. Sobrescrevendo.")

        active_shares[share_key] = {
            'user_id': user_id, 'username': username,
            'start_time': now_utc, 'last_update': now_utc,
        }
        logger.info(f"INÍCIO Partilha: Usuário {username} ({user_id}) no Grupo {chat_id}. Msg ID: {message_id}.")

        # <<< MODIFICADO: Salvar estado após adicionar >>>
        save_state(active_shares, STATE_FILENAME)
        # <<< FIM DA MODIFICAÇÃO >>>

    else:
        logger.debug(f"handle_location: Localização recebida não é 'em tempo real' (live_period={current_location.live_period}). Ignorando.")


async def handle_edited_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processa atualizações (edições) de uma mensagem de localização DENTRO DO GRUPO ALVO."""
    if not update.edited_message or not update.edited_message.chat or not update.edited_message.message_id:
        logger.debug("handle_edited_location: Ignorando update editado sem dados essenciais.")
        return

    edited_message = update.edited_message
    chat_id = edited_message.chat_id
    message_id = edited_message.message_id
    share_key = (chat_id, message_id)

    if share_key in active_shares:
        now_utc = datetime.now(UTC_TZ)
        # Acessa a cópia local para evitar race conditions se o dict global mudar
        share_info = active_shares.get(share_key)
        if not share_info: # Verifica se foi removido por outra thread/job entre o check e aqui
             logger.warning(f"handle_edited_location: Share key {share_key} desapareceu inesperadamente.")
             return

        username = share_info.get('username', 'N/A')
        user_id = share_info.get('user_id', 'N/A')

        # A partilha terminou se a mensagem editada NÃO tem mais 'location' ou 'live_period'
        if not edited_message.location or not getattr(edited_message.location, 'live_period', None):
            logger.info(f"FIM Partilha: Detectado fim para {username} ({user_id}). Msg ID: {message_id}")
            end_time_utc = now_utc
            start_time_utc = share_info['start_time']
            if start_time_utc.tzinfo is None: start_time_utc = UTC_TZ.localize(start_time_utc)
            duration = end_time_utc - start_time_utc
            duration_seconds = max(0, int(duration.total_seconds()))
            start_br = start_time_utc.astimezone(BR_TZ)
            end_br = end_time_utc.astimezone(BR_TZ)
            duration_str = format_duration(duration_seconds)

            row_data = [
                user_id, username,
                start_br.strftime('%d/%m/%Y %H:%M:%S'), end_br.strftime('%d/%m/%Y %H:%M:%S'),
                duration_str,
                start_time_utc.strftime('%Y-%m-%d %H:%M:%S'), end_time_utc.strftime('%Y-%m-%d %H:%M:%S'),
                duration_seconds
            ]

            success = await append_row_with_retry(row_data)
            if not success:
                logger.error(f"Falha definitiva ao gravar dados da partilha de {username} ({user_id}) na planilha.")

            # Remove da memória ativa
            removed_from_memory = False
            try:
                # Verifica novamente antes de deletar
                if share_key in active_shares:
                     del active_shares[share_key]
                     logger.info(f"Partilha {share_key} removida da memória ativa.")
                     removed_from_memory = True
            except KeyError:
                logger.warning(f"Tentativa de remover share_key {share_key} que já não estava em active_shares.")

            # <<< MODIFICADO: Salvar estado após remover >>>
            if removed_from_memory:
                 save_state(active_shares, STATE_FILENAME)
            # <<< FIM DA MODIFICAÇÃO >>>

        else:
             # Atualização da localização ativa
             # Atualiza diretamente no dicionário global (com cuidado)
             try:
                 active_shares[share_key]['last_update'] = now_utc
                 logger.debug(f"Partilha ATUALIZADA: Localização de {username} ({user_id}). Msg ID: {message_id}")

                 # <<< MODIFICADO: Salvar estado após atualizar >>>
                 save_state(active_shares, STATE_FILENAME)
                 # <<< FIM DA MODIFICAÇÃO >>>
             except KeyError:
                  logger.warning(f"handle_edited_location: Share key {share_key} desapareceu antes de atualizar last_update.")

    else:
        logger.debug(f"handle_edited_location: Edição da msg {message_id} ignorada (não corresponde a uma partilha ativa conhecida).")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handler para /status (apenas Admins em PV). Permite filtro por data."""
    user = update.effective_user
    if not user or not update.message or update.message.chat.type != 'private' or user.id not in ADMIN_USER_IDS:
        logger.warning(f"Usuário {user.id if user else 'Desconhecido'} tentou usar /status sem permissão ou fora do PV.")
        return

    admin_username = user.username if user.username else user.first_name
    target_date = datetime.now(BR_TZ).date()
    target_date_str_filter = ""

    if context.args:
        try:
            date_arg = context.args[0]
            try: target_date = datetime.strptime(date_arg, '%d/%m/%Y').date()
            except ValueError: target_date = datetime.strptime(date_arg, '%Y-%m-%d').date()
            target_date_str_filter = target_date.strftime('%d/%m/%Y')
            logger.info(f"/status solicitado por {admin_username} para data: {target_date_str_filter}")
        except ValueError:
            await update.message.reply_text("Formato de data inválido. Use DD/MM/YYYY ou YYYY-MM-DD. Mostrando dados de hoje.")
            target_date_str_filter = target_date.strftime('%d/%m/%Y')
    else:
        target_date_str_filter = target_date.strftime('%d/%m/%Y')
        logger.info(f"/status solicitado por {admin_username} para data de hoje: {target_date_str_filter}")

    reply_parts = []
    reply_parts.append(f"📊 *Status Geral para o dia {target_date_str_filter}*")
    reply_parts.append(f"_(Solicitado por {admin_username})_\n")

    # 1. Processar dados da Planilha
    reply_parts.append("*Compartilhamentos concluídos (Planilha):*")
    worksheet = get_worksheet_with_retry()
    found_in_sheet_for_date = False
    records_by_user = defaultdict(list)
    total_geral_seconds_sheet = 0

    if worksheet:
        try:
            all_values = worksheet.get_all_values()
            logger.info(f"Handler status: Lendo {len(all_values)} linhas da planilha para data {target_date_str_filter}.")
            if len(all_values) > 1:
                headers = [h.strip() for h in all_values[0]]
                missing_headers = REQUIRED_HEADERS - set(headers)
                if missing_headers:
                    logger.error(f"Erro CRÍTICO no status: Cabeçalho(s) não encontrado(s): {missing_headers}.")
                    reply_parts.append(f"\n⚠️ *Erro: Coluna(s) {missing_headers} não encontrada(s). Verifique planilha.*")
                else:
                    idx_username = headers.index(COL_USERNAME)
                    idx_startbr = headers.index(COL_START_BR)
                    idx_endbr = headers.index(COL_END_BR)
                    idx_duration = headers.index(COL_DURATION)
                    idx_duration_sec = headers.index(COL_DURATION_SEC)
                    idx_userid = headers.index(COL_USERID) if COL_USERID in headers else -1

                    for i, row in enumerate(all_values[1:], start=2):
                        min_cols_needed = max(idx_username, idx_startbr, idx_endbr, idx_duration, idx_duration_sec, (idx_userid if idx_userid !=-1 else 0)) + 1
                        if len(row) >= min_cols_needed:
                            start_time_br_str = row[idx_startbr].strip()
                            if start_time_br_str.startswith(target_date_str_filter):
                                found_in_sheet_for_date = True
                                colab_username = row[idx_username].strip()
                                if not colab_username and idx_userid != -1:
                                    colab_username = f"UserID {row[idx_userid].strip()}"
                                if not colab_username: colab_username = f"Linha {i}"
                                try: duration_seconds = int(row[idx_duration_sec].strip() or 0)
                                except ValueError: duration_seconds = 0
                                start_hour = start_time_br_str.split(' ')[1] if ' ' in start_time_br_str else '??:??:??'
                                end_br_str = row[idx_endbr].strip()
                                end_hour = end_br_str.split(' ')[1] if ' ' in end_br_str else '??:??:??'
                                record_info = (start_hour, end_hour, row[idx_duration].strip(), duration_seconds)
                                records_by_user[colab_username].append(record_info)

            if not found_in_sheet_for_date:
                reply_parts.append("_Nenhum registro concluído encontrado para esta data._")
            else:
                for colab_name in sorted(records_by_user.keys()):
                    records = records_by_user[colab_name]
                    reply_parts.append(f"\n👤 **{colab_name}:**")
                    user_total_seconds = 0
                    records.sort(key=lambda x: x[0])
                    for record in records:
                        reply_parts.append(f"  - 🕰️ {record[0]} até {record[1]} | ⏳ {record[2]}")
                        user_total_seconds += record[3]
                    user_total_fmt = format_duration(user_total_seconds)
                    reply_parts.append(f"  _Total {colab_name}: {user_total_fmt}_")
                    total_geral_seconds_sheet += user_total_seconds
                geral_total_fmt = format_duration(total_geral_seconds_sheet)
                reply_parts.append(f"\n\n*Tempo total GERAL concluído ({target_date_str_filter}): {geral_total_fmt}*")

        except gspread.exceptions.APIError as api_error:
            logger.error(f"Erro de API Google ao ler planilha para /status: {api_error}")
            reply_parts.append("\n⚠️ *Erro ao comunicar com a API do Google Sheets.*")
        except Exception as e:
            logger.error(f"Erro inesperado ao ler/processar planilha para /status: {e}", exc_info=True)
            reply_parts.append("\n⚠️ *Erro ao ler/processar o histórico da planilha.*")
    else:
        reply_parts.append("\n⚠️ *Histórico indisponível (planilha não conectada). Verifique os logs.*")

    # 2. Processar Partilhas Ativas (em memória) - Mostra sempre as ativas AGORA
    reply_parts.append("\n\n*Compartilhamentos ativos AGORA (Memória):*")
    found_active = False
    # Cria cópia das chaves para iterar com segurança
    active_shares_keys_copy = list(active_shares.keys())
    now_br_status = datetime.now(BR_TZ)

    for key in active_shares_keys_copy:
         # Re-obtém o valor atual do dicionário global a cada iteração
         active_share = active_shares.get(key)
         if not active_share: # Verifica se foi removido enquanto iterava
              continue

         last_update_utc = active_share.get('last_update', datetime.now(UTC_TZ))
         if last_update_utc.tzinfo is None: last_update_utc = UTC_TZ.localize(last_update_utc) # Garante timezone
         time_since_last_update = datetime.now(UTC_TZ) - last_update_utc
         if time_since_last_update > timedelta(hours=MAX_INACTIVE_HOURS):
             logger.warning(f"/status: Encontrada partilha inativa de {active_share.get('username','N/A')} (MsgID: {key[1]}) - Última att: {last_update_utc}. Job de limpeza deve remover.")
             continue

         found_active = True
         colab_username_active = active_share.get('username', 'N/A')
         start_utc_active = active_share['start_time']
         if start_utc_active.tzinfo is None: start_utc_active = UTC_TZ.localize(start_utc_active)

         start_br_active = start_utc_active.astimezone(BR_TZ)
         active_duration = now_br_status - start_br_active # Usa now_br_status que está fora do loop
         active_duration_str = format_duration(int(active_duration.total_seconds()))

         reply_parts.append(f"  - ▶️ **{colab_username_active}** (iniciou às {start_br_active.strftime('%H:%M:%S')} BRT, ativo por {active_duration_str})")

    if not found_active:
        reply_parts.append("_Nenhuma localização ativa no momento._")

    # Enviar resposta
    final_reply = "\n".join(reply_parts)
    if len(final_reply) > 4096:
        final_reply = final_reply[:4090] + "\n(...)"
        logger.warning("Mensagem /status truncada por exceder limite de tamanho.")
    try:
        await update.message.reply_text(final_reply, parse_mode=ParseMode.MARKDOWN)
    except Exception as send_error:
        logger.error(f"Erro ao enviar mensagem /status (Markdown): {send_error}")
        try:
            plain_text = final_reply.replace('*','').replace('_','').replace('`','')
            await update.message.reply_text(plain_text)
        except Exception as send_error2: logger.error(f"Erro ao enviar mensagem /status (texto plano): {send_error2}")


# --- Tarefa Agendada (JobQueue) ---

async def cleanup_inactive_shares(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job periódico para remover partilhas inativas da memória."""
    logger.info("JOB: Iniciando limpeza de partilhas inativas...")
    now_utc = datetime.now(UTC_TZ)
    inactive_limit = timedelta(hours=MAX_INACTIVE_HOURS)
    keys_to_remove = []
    active_shares_keys = list(active_shares.keys()) # Copia chaves para iterar
    state_changed = False # Flag para saber se algo foi removido

    for key in active_shares_keys:
        try:
            # Re-obtém o valor atual a cada iteração para segurança
            share_info = active_shares.get(key)
            if not share_info: continue # Se foi removido entre pegar as chaves e agora

            last_update = share_info.get('last_update', share_info.get('start_time'))
            if last_update.tzinfo is None: last_update = UTC_TZ.localize(last_update)

            if now_utc - last_update > inactive_limit:
                keys_to_remove.append(key)
                logger.info(f"JOB: Marcando partilha inativa para remoção: {share_info.get('username','N/A')} (MsgID: {key[1]}) - Última att: {last_update}")
        except KeyError:
            logger.warning(f"JOB: Chave {key} desapareceu durante a verificação.")
            continue
        except Exception as e: # Captura outros erros inesperados durante a verificação
             logger.error(f"JOB: Erro inesperado ao verificar chave {key}: {e}", exc_info=True)

    removed_count = 0
    if keys_to_remove: # Só entra se houver chaves a remover
        for key in keys_to_remove:
            try:
                # Verifica novamente antes de deletar
                if key in active_shares:
                     del active_shares[key]
                     removed_count += 1
                     state_changed = True # Marca que o estado mudou
            except KeyError: pass # Já foi removida

        logger.info(f"JOB: Limpeza concluída. {removed_count} partilhas inativas removidas da memória.")

        # <<< MODIFICADO: Salvar estado apenas se algo foi removido >>>
        if state_changed:
            save_state(active_shares, STATE_FILENAME)
        # <<< FIM DA MODIFICAÇÃO >>>

    else:
         logger.info("JOB: Limpeza concluída. Nenhuma partilha inativa encontrada.")


# --- Função Principal ---

def main() -> None:
    """Inicia o bot e configura os handlers."""
    global active_shares # Necessário para modificar a variável global

    logger.info("--- Iniciando Configuração do Bot ---")

    # Validação da Configuração Essencial
    valid_config = True
    if not BOT_TOKEN: logger.critical("ERRO CRÍTICO: Token do Bot não configurado."); valid_config = False
    if not SPREADSHEET_ID: logger.critical("ERRO CRÍTICO: ID da Planilha não configurado."); valid_config = False
    if TARGET_GROUP_ID == 0: logger.critical("ERRO CRÍTICO: ID do Grupo Alvo inválido."); valid_config = False
    if not ADMIN_USER_IDS: logger.warning("AVISO: Nenhum ID de Administrador configurado.")
    if not SERVICE_ACCOUNT_FILE: logger.critical("ERRO CRÍTICO: Caminho do arquivo de credenciais não configurado."); valid_config = False
    if not valid_config:
        print("Configuração inválida encontrada. Verifique os logs CRÍTICOS. Bot não iniciado.")
        return

    # Loga as configurações lidas
    logger.info(f"Token: {'Configurado (termina em ...' + BOT_TOKEN[-4:] + ')' if BOT_TOKEN else 'NÃO CONFIGURADO'}")
    logger.info(f"Grupo Alvo ID: {TARGET_GROUP_ID}")
    logger.info(f"Admins IDs: {ADMIN_USER_IDS}")
    logger.info(f"Planilha ID: {SPREADSHEET_ID}")
    logger.info(f"Aba Planilha: {SHEET_NAME}")
    logger.info(f"Arquivo Credenciais: {SERVICE_ACCOUNT_FILE}")
    logger.info(f"Nível de Log: {LOG_LEVEL_STR}")
    logger.info(f"Max Horas Inativas: {MAX_INACTIVE_HOURS}")
    logger.info(f"Intervalo Job Limpeza (min): {CLEANUP_JOB_INTERVAL_MINUTES}")

    # Tenta conectar ao Google Sheets na inicialização
    if not setup_google_sheets():
        logger.error("AVISO CRÍTICO: Conexão inicial com Google Sheets falhou. Funcionalidades da planilha estarão indisponíveis até reconectar.")

    # <<< MODIFICADO: Carregar estado na inicialização >>>
    active_shares = load_state(STATE_FILENAME)
    logger.info(f"Estado carregado: {len(active_shares)} partilhas ativas recuperadas.")
    # <<< FIM DA MODIFICAÇÃO >>>

    # Cria a aplicação Telegram
    try:
        application = Application.builder().token(BOT_TOKEN).build()
    except Exception as e:
        logger.critical(f"Erro CRÍTICO ao criar Application com o token fornecido: {e}", exc_info=True)
        print("Erro ao inicializar a aplicação do Telegram. Verifique o token e a conexão. Bot não iniciado.")
        return

    # --- Configuração dos Handlers ---
    if numeric_level == logging.DEBUG:
        application.add_handler(MessageHandler(filters.ALL, debug_handler), group=0)
        logger.info("Handler DEBUG (debug_handler) adicionado (group=0)")

    admin_private_filter = filters.ChatType.PRIVATE & filters.User(user_id=ADMIN_USER_IDS)
    application.add_handler(CommandHandler("start", start_command, filters=admin_private_filter), group=1)
    application.add_handler(CommandHandler("help", help_command, filters=admin_private_filter), group=1)
    application.add_handler(CommandHandler("status", status_command, filters=admin_private_filter), group=1)
    logger.info("Handlers de comando (/start, /help, /status) para Admins adicionados (group=1)")

    group_filter = filters.Chat(chat_id=TARGET_GROUP_ID)
    application.add_handler(MessageHandler(
        filters.LOCATION & group_filter & filters.UpdateType.MESSAGE & (~filters.UpdateType.EDITED_MESSAGE),
        handle_location
    ), group=1)
    logger.info("Handler para novas localizações no grupo alvo adicionado (group=1)")

    application.add_handler(MessageHandler(
        filters.UpdateType.EDITED_MESSAGE & group_filter,
        handle_edited_location
    ), group=1)
    logger.info("Handler para edições de mensagens no grupo alvo adicionado (group=1)")

    # --- Configuração do Job Queue (Limpeza Periódica) ---
    if CLEANUP_JOB_INTERVAL_MINUTES > 0:
        job_queue = application.job_queue
        if job_queue:
            interval_seconds = CLEANUP_JOB_INTERVAL_MINUTES * 60
            job_queue.run_repeating(cleanup_inactive_shares, interval=interval_seconds, first=interval_seconds)
            logger.info(f"Job de limpeza agendado a cada {CLEANUP_JOB_INTERVAL_MINUTES} minutos.")
        else:
            logger.error("Falha ao obter JobQueue da aplicação. Job de limpeza não agendado.")
    else:
        logger.info("Job de limpeza de partilhas desativado (intervalo <= 0).")

    # --- Iniciar o Bot ---
    logger.info("=== BOT INICIADO E PRONTO PARA RECEBER UPDATES ===")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.critical(f"Erro fatal durante o polling do bot: {e}", exc_info=True)
    finally:
         # <<< MODIFICADO: Salvar estado ao encerrar >>>
         logger.info("Tentando salvar estado final antes de encerrar...")
         save_state(active_shares, STATE_FILENAME)
         # <<< FIM DA MODIFICAÇÃO >>>
         logger.info("--- Bot Encerrado ---")

if __name__ == "__main__":
    try:
        import telegram
        import gspread
        import pytz
        import google.oauth2
        import dotenv
    except ImportError as import_error:
        print(f"ERRO: Biblioteca necessária não encontrada: {import_error.name}.")
        print("Instale as dependências com: pip install python-telegram-bot python-dotenv google-api-python-client google-auth-httplib2 google-auth-oauthlib gspread pytz")
    else:
        main()