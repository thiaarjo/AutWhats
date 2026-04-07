#!/usr/bin/env python3
"""
WhatsApp Business Listener - Automação Profissional de Captura de Dados
=======================================================================
Monitora grupos do WhatsApp Business e captura mensagens, mídias e áudios
automaticamente, salvando tudo em um banco SQLite local.

Autor: Automação ADB via uiautomator2
Versão: 2.0 (Produção)
"""

import uiautomator2 as u2
import sqlite3
import hashlib
import random
import time
import os
import sys
import signal
import logging
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date
from typing import List, Dict, Optional
from dotenv import load_dotenv
from supabase import create_client, Client

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "whatsapp_capture.db")
LOG_PATH = os.path.join(BASE_DIR, "scraper.log")
CSV_PATH = os.path.join(BASE_DIR, "mensagens_capturadas.csv")

DEVICE_SERIAL = "RX8RC0HXX3F"
PACKAGE_NAME = "com.whatsapp.w4b"

# Resource IDs mapeados via Weditor
IDS = {
    "group_name":       "com.whatsapp.w4b:id/conversation_contact_name",
    "group_members":    "com.whatsapp.w4b:id/conversation_contact_status",
    "sender":           "com.whatsapp.w4b:id/conversation_row_name",
    "sender_alt":       "com.whatsapp.w4b:id/conversations_row_contact_name",
    "message_text":     "com.whatsapp.w4b:id/message_text",
    "timestamp":        "com.whatsapp.w4b:id/date",
    "date_divider":     "com.whatsapp.w4b:id/conversation_row_date_divider",
    "image":            "com.whatsapp.w4b:id/image",
    "view_once_media":  "com.whatsapp.w4b:id/view_once_media_container_large",
    "main_layout":      "com.whatsapp.w4b:id/main_layout",
    "status":           "com.whatsapp.w4b:id/status",
    "info":             "com.whatsapp.w4b:id/info",
    "unread_badge":     "com.whatsapp.w4b:id/conversations_row_message_count",
    "caption":          "com.whatsapp.w4b:id/caption",
    "edit_label":       "com.whatsapp.w4b:id/edit_label",
    "media_container":  "com.whatsapp.w4b:id/media_container",
    "thumb_0":          "com.whatsapp.w4b:id/thumb_0",
    "thumb_1":          "com.whatsapp.w4b:id/thumb_1",
    "thumb_2":          "com.whatsapp.w4b:id/thumb_2",
    "sender_group":     "com.whatsapp.w4b:id/name_in_group_tv",
    "album_more":       "com.whatsapp.w4b:id/more",
}

# ============================================================================
# LOGGING PROFISSIONAL
# ============================================================================
def setup_logging():
    """Configura logging dual: arquivo + console."""
    logger = logging.getLogger("WhatsAppListener")
    logger.setLevel(logging.DEBUG)
    
    # Formato profissional
    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Handler para arquivo (tudo)
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    
    # Handler para console (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ============================================================================
# CLASSE PRINCIPAL
# ============================================================================
class WhatsAppListener:
    """
    Listener profissional para captura de mensagens do WhatsApp Business.
    
    Funcionalidades:
    - Detecção automática de novas mensagens (trigger por badge)
    - Captura de texto, fotos, áudios e mídias de visualização única
    - Rastreamento de data absoluta via divisores de conversa
    - Deduplicação via hash MD5
    - Logging completo em arquivo
    - Tratamento de erros em todas as operações
    """

    def __init__(self, target_groups, serial=DEVICE_SERIAL):
        self.log = setup_logging()
        self.log.info("=" * 60)
        self.log.info("WHATSAPP LISTENER v2.0 - INICIALIZAÇÃO")
        self.log.info("=" * 60)
        
        # Conexão com dispositivo
        try:
            self.d = u2.connect(serial)
            device_info = self.d.info
            self.device_width = device_info.get('displayWidth', 720)
            self.log.info(f"Dispositivo conectado: {serial}")
            self.log.info(f"Resolução: {self.device_width}x{device_info.get('displayHeight', 1600)}")
        except Exception as e:
            self.log.critical(f"Falha ao conectar ao dispositivo {serial}: {e}")
            raise SystemExit(1)
        
        # Configuração de grupos
        if isinstance(target_groups, str):
            target_groups = [target_groups]
        self.target_groups = target_groups
        self.log.info(f"Grupos alvo: {', '.join(self.target_groups)}")
        
        # Estado interno
        self.db_path = DB_PATH
        self.last_known_sender = "Desconhecido"
        self.group_other_members = []
        self.current_date = date.today().strftime("%Y-%m-%d")
        self.running = True
        
        # Buffer de deduplicação por sequência (Sliding Window)
        # Armazena tuplas (sender, content, timestamp, media_type, date) da última tela
        self._message_buffer = []
        self._saved_sequences = set()
        self._session_timestamp = datetime.now().strftime("%H:%M")
        self.last_seen_timestamp = self._session_timestamp
        
        # Cache de mídias baixadas NESTA SESSÃO para evitar duplicidade de associação
        self.downloaded_remote_paths = set()
        
        # Subpastas de mídia
        self.media_dir = os.path.join(BASE_DIR, "media")
        self.img_dir = os.path.join(self.media_dir, "images")
        self.audio_dir = os.path.join(self.media_dir, "audio")
        os.makedirs(self.img_dir, exist_ok=True)
        os.makedirs(self.audio_dir, exist_ok=True)
        
        # Inicializa banco de dados
        self._init_db()
        
        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        # Conexão Híbrida (Storage Direto + Webhook de Dados)
        load_dotenv()
        sb_url = os.getenv("SUPABASE_URL")
        sb_key = os.getenv("SUPABASE_KEY") # Service Role para Storage
        self.webhook_url = os.getenv("WHATSAPP_WEBHOOK_URL")
        self.webhook_token = os.getenv("WHATSAPP_WEBHOOK_TOKEN")
        self.project_id = os.getenv("PROJECT_ID") # Agora lido dinamicamente
        
        self.supabase: Optional[Client] = None
        if sb_url and sb_key:
            try:
                self.supabase = create_client(sb_url, sb_key)
                self.log.info("Motor de Storage Supabase conectado.")
            except Exception as e:
                self.log.error(f"Erro no Storage Supabase: {e}")

        if self.webhook_url and self.webhook_token:
            self.log.info("Canal de Webhook (receive-whatsapp) configurado.")
        else:
            self.log.warning("Webhook não configurado. Sincronização cloud desativada.")
            
        self.log.info("Listener inicializado com sucesso.")

    # -----------------------------------------------------------------------
    # BANCO DE DADOS
    # -----------------------------------------------------------------------
    def _init_db(self):
        """Cria/atualiza a tabela de mensagens com suporte a mídia e data."""
        try:
            self.conn = sqlite3.connect(self.db_path, timeout=60)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=15000")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    project_id TEXT,
                    group_name TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT,
                    media_type TEXT DEFAULT 'text',
                    timestamp_str TEXT,
                    message_date TEXT,
                    local_path TEXT,
                    cloud_url TEXT,
                    file_hash TEXT,
                    session_number INTEGER DEFAULT 0,
                    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabela de metadados para controlar logs por grupo
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_metadata (
                    group_name TEXT PRIMARY KEY,
                    last_session_number INTEGER DEFAULT 0
                )
            ''')
            # Migração: adicionar colunas novas se não existirem
            cols = [
                ("media_type", "TEXT DEFAULT 'text'"),
                ("message_date", "TEXT"),
                ("project_id", "TEXT"),
                ("local_path", "TEXT"),
                ("cloud_url", "TEXT"),
                ("file_hash", "TEXT"),
                ("session_number", "INTEGER DEFAULT 0")
            ]
            for col_name, col_def in cols:
                try:
                    cursor.execute(f"ALTER TABLE messages ADD COLUMN {col_name} {col_def}")
                except sqlite3.OperationalError:
                    pass  # Já existe
            
            # Tabela de Checkpoints para convergência rápida
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS group_checkpoints (
                    group_name TEXT PRIMARY KEY,
                    last_msg_id TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            self.conn.commit()
            
            if os.path.exists(self.db_path):
                self.log.info(f"Banco de dados OK: {self.db_path}")
            else:
                self.log.critical(f"FALHA ao criar banco: {self.db_path}")
        except Exception as e:
            self.log.critical(f"Erro fatal no banco de dados: {e}")
            raise

    def _start_new_session(self, group_name):
        """
        Incrementa e retorna o número da sessão (Log) para um grupo específico.
        Garante que cada 'pulo' no grupo seja rastreado de forma única.
        """
        try:
            cursor = self.conn.cursor()
            # Pega o último número ou inicializa com 0
            cursor.execute("SELECT last_session_number FROM group_metadata WHERE group_name = ?", (group_name,))
            row = cursor.fetchone()
            
            new_session = 1
            if row:
                new_session = row[0] + 1
                cursor.execute("UPDATE group_metadata SET last_session_number = ? WHERE group_name = ?", (new_session, group_name))
            else:
                cursor.execute("INSERT INTO group_metadata (group_name, last_session_number) VALUES (?, ?)", (group_name, new_session))
            
            self.conn.commit()
            self._current_session_number = new_session
            self.log.info(f">>> [LOG {new_session}] Iniciando nova sessão de captura para: {group_name}")
            return new_session
        except Exception as e:
            self.log.error(f"Erro ao gerar novo session_id para {group_name}: {e}")
            self._current_session_number = 0
            return 0

    def save_message(self, sender, content, timestamp, message_date, media_type="text", seq_index=0, local_path=None, file_hash=None, img_bounds=None):
        """Salva uma mensagem no banco com deduplicação e dispara o Webhook Cloud."""
        # Normaliza sender e content ANTES de gerar o hash e salvar
        sender = self._normalize_sender(sender)
        content = self._normalize(content)
        
        # 0. Limpeza Orfã Absoluta: Se a varredura atual achou o nome real e a anterior o guardou como orfão sem o nome devido ao corte da tela, substitua.
        if sender != "Desconhecido":
            try:
                self.conn.execute(
                    "DELETE FROM messages WHERE group_name = ? AND sender = 'Desconhecido' AND content = ? AND timestamp_str = ? AND message_date = ?",
                    (self._current_group, content, timestamp, message_date)
                )
                self.conn.commit()
            except Exception:
                pass
        
        msg_id = self._hash(sender, content, timestamp, message_date, media_type, seq_index)
        session_num = getattr(self, '_current_session_number', 0)
        
        # 1. Upload para o Cloud Storage (Desativado temporariamente conforme pedido)
        media_url = None
        # if local_path:
        #     ... 

        # 2. Salva localmente (SQLite)
        try:
            self.conn.execute(
                "INSERT INTO messages (id, project_id, group_name, sender, content, media_type, timestamp_str, message_date, local_path, cloud_url, file_hash, session_number) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (msg_id, self.project_id, self._current_group, sender, content, media_type, timestamp, message_date, local_path, media_url, file_hash, session_num)
            )
            self.conn.commit()
            is_new = True
        except sqlite3.IntegrityError:
            is_new = False
        except Exception as e:
            self.log.error(f"Erro ao salvar mensagem local: {e}")
            return False

        # 3. Dispara Webhook (receive-whatsapp)
        # Payload conforme a especificação da Function (v3.0 com whatsapp_group_id)
        import re
        t_clean = re.search(r'\d{1,2}:\d{2}', timestamp)
        t_clean = t_clean.group(0) if t_clean else timestamp

        self._sync_to_supabase({
            "id": msg_id,
            "project_id": self.project_id,
            "group_name": self._current_group,
            "whatsapp_group_id": str(self._current_group),
            "sender": sender,
            "content": content,
            "media_url": media_url,
            "timestamp": f"{self.current_date} {t_clean}"
        })
        
        return is_new

    def _normalize(self, text):
        """
        Remove caracteres de controle, espaços extras e ruídos de truncamento (Ler mais).
        Essencial para que mensagens longas tenham o mesmo Hash independente do scroll.
        """
        if not text: return ""
        import re
        # 1. Remove caracteres invisíveis e controles
        text = text.replace('\u200e', '').replace('\u200f', '').replace('\u200c', '')
        text = "".join(ch for ch in text if ch.isprintable())
        
        # 2. Limpa ruídos de truncamento do WhatsApp (Read more / Ler mais / ...)
        text = text.replace("\u2026", "").replace("...", "")
        text = re.sub(r'(?i)\s+Ler mais\s*$', '', text)
        text = re.sub(r'(?i)\s+Read more\s*$', '', text)
        
        return text.strip()

    def _normalize_sender(self, name):
        """
        Normaliza o nome do remetente para garantir consistência.
        Remove ~ (contatos não salvos), caracteres invisíveis, e dois-pontos finais.
        '~ Vinii' e 'Vinii' viram a mesma pessoa.
        """
        if not name: return "Desconhecido"
        import re
        # Remove caracteres invisíveis Unicode
        name = name.replace('\u200e', '').replace('\u200f', '').replace('\u200c', '')
        # Remove o ~ que o WhatsApp coloca em contatos não salvos
        name = re.sub(r'^~\s*', '', name)
        # Remove dois-pontos finais
        name = name.rstrip(':')
        name = name.strip()
        return name if name else "Desconhecido"

    def _get_db_occurrence_count(self, sender, content, timestamp, msg_date):
        """Consulta o banco para ver quantas mensagens IDÊNTICAS já existem (deduplicação global)."""
        try:
            s_norm = self._normalize_sender(sender)
            c_norm = self._normalize(content)
            
            query = "SELECT COUNT(*) FROM messages WHERE sender = ? AND content = ? AND timestamp_str = ? AND message_date = ?"
            cursor = self.conn.execute(query, (s_norm, c_norm, timestamp, msg_date))
            return cursor.fetchone()[0]
        except Exception as e:
            self.log.error(f"Erro na contagem de ocorrências DB: {e}")
            return 0

    def _hash(self, sender, content, timestamp, message_date, media_type, seq_index=0):
        """
        Gera hash MD5 único e estável para deduplicação.
        """
        s_norm = self._normalize_sender(sender)
        c_norm = self._normalize(content)
        data = f"{self._current_group}|{s_norm}|{c_norm}|{timestamp}|{message_date}|{media_type}|{seq_index}".encode('utf-8')
        return hashlib.md5(data).hexdigest()

    # -----------------------------------------------------------------------
    # GESTÃO DE MÍDIA (ADB PULL)
    # -----------------------------------------------------------------------
    def _calculate_file_hash(self, filepath):
        """Calcula SHA-256 do arquivo para integridade física."""
        sha256_hash = hashlib.sha256()
        try:
            with open(filepath, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception:
            return None

    def _save_media_from_gallery(self, media_type, count, bounds):
        """
        Abre a Galeria do Whatsapp clicando na imagem,
        Clica Menu -> 'Salvar' para forçar a criação de um arquivo fresco na raiz do Android,
        repete para todos os itens do álbum, e retorna ao chat.
        """
        if not bounds: return
        try:
            # 1. Clicar no centro da área da tela da imagem/album
            cx, cy = (bounds['left'] + bounds['right']) // 2, (bounds['top'] + bounds['bottom']) // 2
            self.d.click(cx, cy)
            time.sleep(2) # Espera a foto abrir fullscreen

            iters = count if media_type == "image_album" else 1

            for i in range(iters):
                # 2. Abre opções
                self.d(description="Mais opções").click(timeout=3)
                time.sleep(1)
                
                # 3. Clica em "Salvar"
                self.d(text="Salvar").click(timeout=3)
                time.sleep(2) # Espera a notificação "Salvo..."
                
                # Desliza para esquerda se não for o último do album
                if iters > 1 and i < iters - 1:
                    self.d.swipe_ext("left", scale=0.8)
                    time.sleep(1)
            
            # 4. Voltar para o Chat
            self.d.press("back")
            time.sleep(1)
            
        except Exception as e:
            self.log.debug(f"Erro ao forçar Salvar legado: {e}")
            self.d.press("back") # Failsafe

    # -----------------------------------------------------------------------
    # CLOUD SYNC (WEBHOOK / EDGE FUNCTIONS)
    # -----------------------------------------------------------------------
    def _sync_to_supabase(self, data: dict):
        """Envia os dados via POST para a Edge Function 'receive-whatsapp'."""
        if not self.webhook_url or not self.webhook_token: 
            return
        
        try:
            headers = {
                "Authorization": f"Bearer {self.webhook_token}",
                "Content-Type": "application/json"
            }
            response = requests.post(
                self.webhook_url, 
                json=data, 
                headers=headers,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                self.log.debug(f"Sincronizado via Webhook: {data['id']}")
            else:
                self.log.error(f"Erro na Function ({response.status_code}): {response.text}")
                
        except Exception as e:
            self.log.error(f"Falha na comunicação com Webhook: {e}")

    def _upload_media_to_supabase(self, local_path: str, media_type: str):
        """Faz o upload do arquivo para o storage bucket 'whatsapp_media'."""
        if not self.supabase or not local_path or not os.path.exists(local_path):
            return None
            
        try:
            file_name = os.path.basename(local_path)
            storage_path = f"{media_type}s/{file_name}"
            
            with open(local_path, 'rb') as f:
                self.supabase.storage.from_("whatsapp_media").upload(
                    path=storage_path, 
                    file=f,
                    file_options={"upsert": "true"}
                )
                
            public_url = self.supabase.storage.from_("whatsapp_media").get_public_url(storage_path)
            self.log.info(f"Mídia Cloud: {public_url}")
            return public_url
        except Exception as e:
            self.log.error(f"Falha no upload Storage: {e}")
            return None

    def _get_media_adb_path(self, media_type):
        """Mapeia o tipo de mídia para a pasta correspondente no Android (W4B)."""
        base = "/storage/emulated/0/Android/media/com.whatsapp.w4b/WhatsApp Business/Media/"
        if media_type == "image":
            return base + "WhatsApp Business Images/"
        elif media_type == "audio":
            # Voice notes ficam em subpastas por semana (ex: 202412)
            # Pegamos a pasta principal e o script buscará recursivamente
            return base + "WhatsApp Business Voice Notes/"
        return None

    def _download_recent_media(self, media_type, count=1):
        """
        Localiza o(s) arquivo(s) mais recente(s) via ADB, ignorando os já baixados nesta run.
        """
        adb_folder = self._get_media_adb_path("image" if media_type == "image_album" else media_type)
        if not adb_folder: return None, None

        try:
            # Pega uma lista maior (ex: 20) para podermos filtrar os que já usamos
            if media_type == "audio":
                find_cmd = f"find '{adb_folder}' -type f -name '*.opus' -printf '%T@ %p\\n' | sort -rn | head -20 | cut -d' ' -f2-"
            else:
                find_cmd = f"ls -t '{adb_folder}' | head -20"
            
            output = self.d.shell(find_cmd).output.strip()
            if not output or "not found" in output: return None, None
                
            all_recent = output.split('\n')
            
            # Filtra apenas os que não baixamos ainda nesta sessão
            # Ordena por timestamp real do arquivo para bater com a hierarquia do chat
            remote_paths = []
            # 'all_recent' foi obtido com 'find %T@ %p' ou 'ls -t', ambos dão os mais recentes primeiro
            for r in all_recent:
                r = r.strip()
                if not r: continue
                full_remote = r if media_type == "audio" else os.path.join(adb_folder, r).replace("\\", "/")
                if full_remote not in self.downloaded_remote_paths:
                    remote_paths.append(full_remote)
                if len(remote_paths) >= count: break

            if not remote_paths:
                self.log.debug(f"Nenhum arquivo novo disponível na pasta de {media_type}.")
                return None, None

            paths_downloaded = []
            hashes_downloaded = []
            
            for remote_path in remote_paths:
                # 2. Destino local
                ext = os.path.splitext(remote_path)[1] or (".jpg" if "image" in media_type else ".opus")
                local_temp_path = os.path.abspath(os.path.join(self.img_dir if "image" in media_type else self.audio_dir, f"temp_{random.randint(1000,9999)}{ext}"))

                self.d.pull(remote_path, local_temp_path)
                if not os.path.exists(local_temp_path): continue

                f_hash = self._calculate_file_hash(local_temp_path)
                final_name = f"{f_hash}{ext}" if f_hash else os.path.basename(local_temp_path)
                local_final = os.path.join(os.path.dirname(local_temp_path), final_name)
                
                if not os.path.exists(local_final):
                    os.rename(local_temp_path, local_final)
                else:
                    os.remove(local_temp_path)

                # Marca como baixado para não repetir
                self.downloaded_remote_paths.add(remote_path)
                
                rel_base = "images" if "image" in media_type else "audio"
                rel_path = os.path.join("media", rel_base, os.path.basename(local_final)).replace("\\", "/")
                paths_downloaded.append(rel_path)
                hashes_downloaded.append(f_hash or "unknown")

            return "|".join(paths_downloaded), "|".join(hashes_downloaded)
        except Exception as e:
            self.log.error(f"Erro no download RECENTE ({media_type}): {e}")
            return None, None

    def _download_media_by_date(self, media_type, count, msg_date, msg_time):
        """
        Busca arquivos vinculados à data (YYYYMMDD) e HORA aproximada no Android.
        Garante precisão ao associar fotos e áudios históricos.
        """
        import os, time, random
        from datetime import datetime
        try:
            date_prefix = msg_date.replace("-", "")
            adb_dir = self._get_media_adb_path("image" if media_type == "image_album" else media_type)
            if not adb_dir: return None, None
            
            # 1. Busca todos os arquivos do dia
            find_cmd = f"find '{adb_dir}' -maxdepth 2 -type f -name '*{date_prefix}*' -printf '%T@|%p\\n'"
            output = self.d.shell(find_cmd).output.strip()
            if not output: return None, None
            
            # 2. Converte horário da mensagem para minutos (ex: "18:24" -> 1104)
            try:
                m_hour, m_min = map(int, msg_time.split(':'))
                msg_total_min = m_hour * 60 + m_min
            except: 
                msg_total_min = -1

            files_of_the_day = []
            for line in output.split('\n'):
                if '|' not in line: continue
                ts_str, fpath = line.split('|', 1)
                # Converte timestamp do arquivo para HH:MM do dia
                dt_object = datetime.fromtimestamp(float(ts_str))
                file_total_min = dt_object.hour * 60 + dt_object.minute
                
                diff = abs(file_total_min - msg_total_min) if msg_total_min != -1 else 999
                files_of_the_day.append({
                    'path': fpath.strip(),
                    'diff': diff,
                    'ts': float(ts_str)
                })

            # 3. Ordena pela menor diferença de tempo E depois pelo timestamp ABSOLUTO (cronologia pura)
            # Como estamos no modo LEGADO (do presente para o passado), queremos os arquivos
            # mais recentes do lote primeiro dentro do mesmo minuto.
            files_of_the_day.sort(key=lambda x: (x['diff'], -x['ts']))
            
            # Filtra os N melhores (count) que não foram baixados nesta run
            target_files = []
            for f in files_of_the_day:
                if f['path'] not in self.downloaded_remote_paths:
                    target_files.append(f['path'])
                if len(target_files) >= count: break

            if not target_files: return None, None

            paths_dl = []
            hashes_dl = []
            for remote_path in target_files:
                ext = os.path.splitext(remote_path)[1] or (".jpg" if "image" in media_type else ".opus")
                local_temp = os.path.abspath(os.path.join(self.img_dir if "image" in media_type else self.audio_dir, f"temp_hist_{random.randint(1000,9999)}{ext}"))
                
                self.d.pull(remote_path, local_temp)
                if not os.path.exists(local_temp): continue
                
                f_hash = self._calculate_file_hash(local_temp)
                final_name = f"{f_hash}{ext}" if f_hash else os.path.basename(local_temp)
                local_final = os.path.join(os.path.dirname(local_temp), final_name)
                
                if not os.path.exists(local_final):
                    os.rename(local_temp, local_final)
                else:
                    os.remove(local_temp)

                self.downloaded_remote_paths.add(remote_path)
                rel_base = "images" if "image" in media_type else "audio"
                rel_path = os.path.join("media", rel_base, os.path.basename(local_final)).replace("\\", "/")
                paths_dl.append(rel_path)
                hashes_dl.append(f_hash or "unknown")

            return "|".join(paths_dl), "|".join(hashes_dl)
        except Exception as e:
            self.log.error(f"Erro na calibração histórica de {media_type}: {e}")
            return None, None

    # -----------------------------------------------------------------------
    # NAVEGAÇÃO SEGURA
    # -----------------------------------------------------------------------
    def ensure_app_open(self):
        """
        Garante que o WA Business está aberto e visível.
        Tenta até 3 vezes antes de desistir.
        """
        for attempt in range(1, 4):
            try:
                current = self.d.app_current()
                if current.get('package') == PACKAGE_NAME:
                    return True
                
                self.log.warning(f"WA Business não detectado (tentativa {attempt}/3). Abrindo...")
                self.d.app_start(PACKAGE_NAME)
                time.sleep(4) # Espera um pouco mais para carregar
                
                if self.d.app_current().get('package') == PACKAGE_NAME:
                    self.log.info("WA Business aberto com sucesso.")
                    return True
                
                # Se falhar na abertura direta, tenta pressionar Home e abrir pelo ícone
                self.d.press("home")
                time.sleep(1)
                if self.d(text="WA Business").exists(timeout=2):
                    self.d(text="WA Business").click()
                    time.sleep(4)
                    if self.d.app_current().get('package') == PACKAGE_NAME:
                        return True
                        
            except Exception as e:
                self.log.error(f"Erro ao tentar abrir app (tentativa {attempt}): {e}")
                time.sleep(2)
        
        self.log.critical("CRÍTICO: Não foi possível abrir o WhatsApp após 3 tentativas. Encerrando por segurança.")
        self.running = False
        return False

    def open_group(self, group_name):
        """
        Garante a abertura de um grupo específico.
        Retorna True se entrar, False se falhar.
        """
        self._current_group = group_name
        self.log.info(f"Tentando abrir grupo: {group_name}...")
        
        # 1. Tenta abrir o app no package correto
        self.d.app_start(PACKAGE_NAME)
        time.sleep(1)
        
        # 2. Se já estiver no grupo certo, OK
        if self.d(resourceId=IDS["group_name"], text=group_name).exists(timeout=2):
            self.log.info(f"Já estamos dentro do grupo: {group_name}")
            return True
            
        # 3. Se estiver em OUTRO grupo, volta para a lista
        if self.d(resourceId=IDS["group_name"]).exists(timeout=1):
            self.d.press("back")
            time.sleep(1)
            
        # 4. Procura o grupo na lista de conversas
        target = self.d(resourceId=IDS["sender_alt"], text=group_name)
        if not target.exists:
            # Scroll para achar
            self.log.info(f"Buscando '{group_name}' na lista de chats...")
            self.d(scrollable=True).scroll.to(text=group_name)
            target = self.d(resourceId=IDS["sender_alt"], text=group_name)
            
        if target.exists(timeout=2):
            target.click()
            # Espera carregar o conteúdo interno (o container de mensagens ou álbuns)
            if self.d(resourceId=IDS["message_text"]).wait(timeout=5.0) or \
               self.d(resourceId=IDS["media_container"]).wait(timeout=5.0):
                self._message_buffer = []  # Limpa buffer para nova scan
                self.last_known_sender = "Desconhecido" # Limpa memória do último remetente para o novo grupo
                self._start_new_session(group_name) # Inicializa Log ID para esta visita
                self.log.info(f"Grupo '{group_name}' aberto e carregado.")
                return True
        
        self.log.error(f"Erro: Não foi possível abrir o grupo '{group_name}'.")
        return False

    def return_to_chat_list(self):
        """Retorna à lista de conversas de forma segura."""
        for _ in range(3):
            if self.d(resourceId=IDS["group_name"]).exists(timeout=1):
                self.d.press("back")
                time.sleep(1)
            else:
                return True
        return True

    # -----------------------------------------------------------------------
    # CAPTURA DE MENSAGENS E MÍDIA (via XML Hierarchy Parsing)
    # -----------------------------------------------------------------------
    def scrape_visible_messages(self, is_scrolling_up=False):
        """
        Varredura inteligente da tela usando parse do XML hierarchy.
        Blindagem de Identidade v2.0: content-desc como fonte soberana de remetente.
        """
        messages_found = 0
        
        # Reset de memória apenas se estivermos capturando de baixo para cima (Modo Legado),
        # porque de baixo pra cima nós quebramos a ordem cronológica visual das telas consecutivas.
        if is_scrolling_up:
            self.last_known_sender = "Desconhecido"
        
        try:
            import xml.etree.ElementTree as ET
            
            # 1. Extrai os membros do grupo (apenas uma vez)
            if not self.group_other_members:
                self._extract_group_members()
            
            # 2. Dump do XML da tela inteira (uma única chamada)
            xml_str = self.d.dump_hierarchy()
            root = ET.fromstring(xml_str)
            
            # 3. Processar cada mensagem encontrando blocos de conteúdo
            messages_found += self._parse_messages_from_xml(root, is_scrolling_up)
            
        except Exception as e:
            self.log.error(f"Erro durante varredura XML: {e}")
        
        if messages_found > 0:
            self.log.info(f"Varredura concluída: {messages_found} mensagens novas salvas.")
        else:
            self.log.debug("Varredura concluída: nenhuma mensagem nova.")
        
        return messages_found

    def _parse_date_divider(self, raw_text):
        """Converte textos como 'Hoje', 'Ontem', '24/03/2026' para formato ISO."""
        raw_text = raw_text.strip()
        today = date.today()
        
        if raw_text.lower() == "hoje":
            return today.strftime("%Y-%m-%d")
        elif raw_text.lower() == "ontem":
            from datetime import timedelta
            return (today - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            # Lidar com dias da semana (ex: 'Quinta-feira')
            weekdays = {
                "segunda-feira": 0, "terça-feira": 1, "quarta-feira": 2,
                "quinta-feira": 3, "sexta-feira": 4, "sábado": 5, "domingo": 6
            }
            rt_lower = raw_text.lower()
            if rt_lower in weekdays:
                from datetime import timedelta
                target_day = weekdays[rt_lower]
                current_day = today.weekday()
                diff = (current_day - target_day) % 7
                if diff == 0: diff = 7 # Garante que pegue a semana passada se for o mesmo dia (já que 'Hoje' é tratado acima)
                return (today - timedelta(days=diff)).strftime("%Y-%m-%d")

            # Formatos numéricos clássicos
            for fmt in ["%d/%m/%Y", "%d de %B de %Y", "%d/%m/%y"]:
                try:
                    return datetime.strptime(raw_text, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
                    
            self.log.debug(f"Data não parseada (fallback para hoje): '{raw_text}'")
            return today.strftime("%Y-%m-%d")

    def _parse_messages_from_xml(self, root, is_scrolling_up=False):
        """
        Percorre o XML e identifica mensagens através de seus 'Containers' (nós pais).
        Blindagem de Identidade v2.1: Agrupamento estrutural puro.
        """
        count_saved = 0
        container_map = {} # {id_do_pai: [elementos_filhos]}
        
        active_date = self.current_date

        
        # 1. Primeiro passo: Identifica divisores de data para atualizar o contexto
        for node in root.iter('node'):
            rid = node.get('resource-id', '')
            if 'conversation_row_date_divider' in rid:
                text = node.get('text', '')
                if text:
                    active_date = self._parse_date_divider(text)
                    self.current_date = active_date
        
        # 2. Segundo passo: Mapeia elementos para seus 'containers de mensagem'
        # Em WA Business, o container é o nó que contém 'main_layout' ou 'message_text'
        parent_map = {c: p for p in root.iter() for c in p}
        
        for node in root.iter('node'):
            rid = node.get('resource-id', '')
            if not rid or 'com.whatsapp.w4b' not in rid:
                continue
                
            short_rid = rid.replace('com.whatsapp.w4b:id/', '')
            
            # Sobe na árvore até achar o 'balão' (container)
            curr = node
            message_container = None
            # Procuramos um pai que represente a linha da conversa
            for _ in range(5): 
                p = parent_map.get(curr)
                if p is None: break
                p_rid = p.get('resource-id', '')
                # Busca ALVO EXATO: main_layout agrupa a bolha inteira (nome + texto + hora)
                if p_rid.endswith('id/main_layout') or 'date_divider' in p_rid:
                    message_container = p
                    break
                curr = p
            
            if message_container is None:
                # Fallback: Se não achar container específico, usa o pai direto
                message_container = parent_map.get(node)
                
            if message_container is not None:
                c_id = id(message_container)
                if c_id not in container_map:
                    container_map[c_id] = []
                
                container_map[c_id].append({
                    'rid': short_rid,
                    'text': node.get('text', ''),
                    'desc': node.get('content-desc', ''),
                    'bounds': self._parse_bounds(node.get('bounds', '')),
                    'node': node
                })
        
        # 3. Terceiro passo: Processa cada 'caixa' (container) de mensagem de forma isolada
        # Ordenamos os containers pela posição vertical (Top) para manter a cronologia na tela
        sorted_containers = sorted(container_map.values(), key=lambda els: els[0]['bounds']['top'])
        
        for message_elements in sorted_containers:
            # Processa os elementos desta caixa específica
            result = self._process_row(message_elements)
            if result:
                sender, content, timestamp, media_type, img_bounds = result
                
                # Só salva e envia se não for duplicada
                # v3.1: Ordem correta (sender, content, timestamp, active_date, media_type, img_bounds)
                if self.save_message(sender, content, timestamp, active_date, media_type, img_bounds=img_bounds):
                    count_saved += 1
        
        return count_saved

    def _check_id_exists(self, msg_id):
        """Verifica se um ID (Hash) já existe no banco de dados."""
        try:
            cursor = self.conn.execute("SELECT 1 FROM messages WHERE id = ?", (msg_id,))
            return cursor.fetchone() is not None
        except Exception:
            return False
    
    def _find_overlap(self, buffer, current, is_scrolling_up=False):
        """
        Encontra o maior sufixo de um que coincida com o prefixo do outro.
        v2.9: Strict suffix/prefix matching mas procura a maior coincidência possível no buffer.
        """
        if not buffer or not current:
            return 0
        
        len_c = len(current)
        max_possible = min(len(buffer), len_c)
        
        for size in range(max_possible, 0, -1):
            if is_scrolling_up:
                # Subindo: suffix(current) == prefix(buffer)
                if current[-size:] == buffer[:size]:
                    return size
            else:
                # Descendo: suffix(buffer) == prefix(current)
                if buffer[-size:] == current[:size]:
                    return size
        return 0

    def _parse_bounds(self, bounds_str):
        """Converte '[x1,y1][x2,y2]' em dict."""
        try:
            parts = bounds_str.replace('][', ',').replace('[', '').replace(']', '').split(',')
            if len(parts) == 4:
                return {
                    'left': int(parts[0]),
                    'top': int(parts[1]),
                    'right': int(parts[2]),
                    'bottom': int(parts[3])
                }
        except (ValueError, IndexError):
            pass
        return {'left': 0, 'top': 0, 'right': 0, 'bottom': 0}

        return rows

    def _process_row(self, row_elements):
        """
        Processa um grupo de elementos na mesma linha vertical.
        Retorna (sender, content, timestamp, media_type, img_bounds) ou None.
        """
        has_message_text = None
        has_status = False
        has_timestamp = None
        has_image = None
        has_view_once = None
        has_voice = None
        has_sender_name = None
        has_caption = None
        has_edit_label = False
        has_unread_divider = None
        album_thumbs = []
        extra_count = 0
        main_layout_desc = None  # content-desc do container da mensagem (fonte soberana de identidade)
        
        for el in row_elements:
            rid = el['rid']
            
            if rid == 'message_text':
                has_message_text = el
            elif rid == 'caption':
                has_caption = el
            elif rid == 'edit_label':
                has_edit_label = True
            elif rid == 'status':
                has_status = True  # Minha mensagem
            elif rid in ('date', 'date_wrapper'):
                has_timestamp = el
            elif rid == 'image':
                has_image = el
            elif rid.startswith('thumb_'):
                album_thumbs.append(el)
            elif rid == 'more':
                # Verifica se tem o label de "+X" fotos (Álbum)
                txt = (el['text'] or '').strip()
                if txt.startswith('+') and txt[1:].isdigit():
                    extra_count = int(txt[1:])
            elif rid == 'view_once_media_container_large':
                has_view_once = el
            elif rid == 'main_layout':
                # Sempre captura o content-desc (contém "NomeContato: mensagem, HH:MM")
                raw_desc = (el['desc'] or '').strip()
                if raw_desc:
                    main_layout_desc = raw_desc
                # Também checa se é áudio
                desc_lower = raw_desc.lower()
                if any(kw in desc_lower for kw in ['voz', 'áudio', 'audio', 'segundos', 'minutos']):
                    has_voice = el
            elif rid in ('conversation_row_name', 'conversation_row_contact_name', 'name_in_group_tv', 'conversation_row_name_in_group_name_and_role_container'):
                has_sender_name = el
            elif rid == 'conversation_row_date_divider':
                has_unread_divider = el
        
        # ===================================================================
        # DETERMINAÇÃO DO REMETENTE (Blindagem de Identidade v2.0)
        # Prioridade: 1) status (Eu) → 2) content-desc → 3) nome explícito → 4) last_known_sender
        # ===================================================================
        
        # Método auxiliar: extrair nome do content-desc do main_layout
        sender_from_desc = None
        if main_layout_desc and not has_status:
            clean_desc = main_layout_desc.replace('\u200e', '').replace('\u200f', '').strip()
            if ':' in clean_desc:
                potential_name = clean_desc.split(':')[0].strip()
                if potential_name and len(potential_name) < 40:
                    sender_from_desc = self._normalize_sender(potential_name)
        
        if has_status:
            sender = "Eu"
        elif has_sender_name and (has_sender_name['text'] or has_sender_name['desc']):
            # Fonte de ID Real (WEditor Precision) - aceita tanto texto quanto description
            val = has_sender_name['text'] if has_sender_name['text'] else has_sender_name['desc']
            sender = self._normalize_sender(val)
            self.last_known_sender = sender
        elif sender_from_desc and sender_from_desc != "Desconhecido":
            # content-desc é a segunda fonte (se não for genético)
            sender = sender_from_desc
            self.last_known_sender = sender
        else:
            # Se não achou nada novo ou veio "Desconhecido", usa a memória
            sender = self.last_known_sender

        
        # Determina o timestamp
        if has_timestamp and has_timestamp['text']:
            timestamp = has_timestamp['text']
            self.last_seen_timestamp = timestamp
        else:
            timestamp = self.last_seen_timestamp
        
        # Processa o conteúdo baseado no tipo
        suffix = " (Editada)" if has_edit_label else ""
        
        # Se for um álbum (várias thumbs ou container com extra)
        if album_thumbs or extra_count > 0:
            total_in_album = len(album_thumbs) + extra_count
            content = f"[Álbum] {total_in_album} fotos"
            return (sender, content + suffix, timestamp, "image_album", album_thumbs[0]['bounds'] if album_thumbs else None)
            
        if has_unread_divider and has_unread_divider['text']:
            # Divisor de mensagens não lidas gerado pelo próprio WA
            return ("Sistema", has_unread_divider['text'], timestamp, "system", None)
            
        if has_message_text and has_message_text['text']:
            txt = has_message_text['text']
            if "mensagem apagada" in txt.lower() or "você apagou esta mensagem" in txt.lower():
                return None
            return (sender, txt + suffix, timestamp, "text", None)
            
        elif has_image:
            desc = (has_image['desc'] or '').strip()
            cap = f" | Legenda: {has_caption['text']}" if has_caption and has_caption['text'] else ""
            content = f"[Foto] {desc}{cap}" if desc else f"[Foto]{cap}"
            return (sender, content + suffix, timestamp, "image", has_image['bounds'])
            
        elif has_view_once:
            desc = (has_view_once['desc'] or '').lower()
            if 'voz' in desc or 'reprodução' in desc:
                return (sender, f"[Voz Única] {desc}", timestamp, "view_once_voice", None)
            else:
                return (sender, f"[Foto Única] {desc}", timestamp, "view_once_photo", None)
                
        elif has_voice:
            desc = (has_voice['desc'] or '').strip()
            if sender == self.last_known_sender or sender == "Desconhecido":
                parts = desc.replace('\u200e', '').split(',')
                if len(parts) > 1:
                    potential_name = parts[0].strip()
                    if potential_name and potential_name.lower() not in ('voz', 'áudio'):
                        sender = potential_name
                        self.last_known_sender = sender
            return (sender, f"[Voz] {desc}", timestamp, "audio", None)
    # UTILITÁRIOS DE IDENTIFICAÇÃO
    # -----------------------------------------------------------------------
    def _identify_sender(self, element, bounds):
        """
        Identifica o remetente usando múltiplas heurísticas:
        1. Presença de elemento 'status' (Lida/Entregue) = 'Eu'
        2. Elemento explícito de nome do remetente na UI
        3. Extração de nomes dos membros do grupo via header
        4. Posição na tela como fallback
        Suporta nomes com emojis, números de telefone e caracteres especiais.
        """
        # Heurística 1: Verifica se a mensagem tem um elemento 'status' (Lida/Entregue/Enviada)
        # Só as MINHAS mensagens têm esse elemento
        try:
            status_el = element.sibling(resourceId=IDS["status"])
            if status_el.exists:
                return "Eu"
        except Exception:
            pass
        
        # Heurística 2: Posição na tela (direita = minha mensagem)
        left = bounds.get('left', 0)
        right = bounds.get('right', 0)
        if right > (self.device_width - 60):  # Margem de 60px da borda direita
            return "Eu"
        
        # Heurística 3: Busca pelo remetente explícito na hierarquia (checa ambos os IDs)
        try:
            for sender_id in [IDS["sender"], IDS["sender_alt"]]:
                sender_elements = self.d(resourceId=sender_id)
                if sender_elements.exists:
                    for s in sender_elements:
                        s_bounds = s.info.get('bounds', {})
                        if s_bounds.get('top', 0) <= bounds.get('top', 0):
                            text = s.get_text()
                            if text:
                                self.last_known_sender = text.strip()
                                return self.last_known_sender
        except Exception:
            pass
        
        # Heurística 4: Se nunca encontrou remetente explícito, tenta extrair do header
        if self.last_known_sender == "Desconhecido" and not self.group_other_members:
            self._extract_group_members()
        
        # Se temos membros conhecidos e não é "Eu", retorna o primeiro membro
        if self.group_other_members:
            return self.group_other_members[0]
        
        return self.last_known_sender
    
    def _extract_group_members(self):
        """
        Extrai os nomes dos outros membros do grupo a partir do header da conversa.
        O header 'conversation_contact_status' contém algo como 'Th, Você' ou 'Ana, João, Você'.
        """
        try:
            header = self.d(resourceId=IDS["group_members"])
            if header.exists:
                raw_text = header.get_text()
                if raw_text:
                    # Remove caractere invisivel \u200e
                    cleaned = raw_text.replace('\u200e', '').replace('\u200c', '').strip()
                    # Separa por virgula e filtra 'Voce'
                    members = [m.strip() for m in cleaned.split(',')]
                    self.group_other_members = [
                        m for m in members 
                        if m.lower() not in ('voc\u00ea', 'voce', 'you', '')
                    ]
                    if self.group_other_members:
                        self.last_known_sender = self.group_other_members[0]
                        self.log.info(f"Membros do grupo detectados: {self.group_other_members}")
        except Exception as e:
            self.log.debug(f"Erro ao extrair membros do grupo: {e}")

    def _get_timestamp(self, element):
        """Obtém o timestamp de uma mensagem via sibling ou scan."""
        try:
            time_el = element.sibling(resourceId=IDS["timestamp"])
            if time_el.exists:
                return time_el.get_text()
        except Exception:
            pass
        
        # Fallback: busca o timestamp mais próximo
        try:
            timestamps = self.d(resourceId=IDS["timestamp"])
            if timestamps.exists:
                el_top = element.info.get('bounds', {}).get('top', 0)
                closest = None
                min_dist = float('inf')
                for ts in timestamps:
                    ts_top = ts.info.get('bounds', {}).get('top', 0)
                    dist = abs(ts_top - el_top)
                    if dist < min_dist:
                        min_dist = dist
                        closest = ts
                if closest:
                    return closest.get_text()
        except Exception:
            pass
        
        return None  # Não inventamos horários para evitar duplicatas por ID mutável

    # -----------------------------------------------------------------------
    # INGESTÃO DE LEGADO (HISTÓRICO)
    # -----------------------------------------------------------------------
    def ingest_legacy(self, group_name="Desconhecido", scrolls=5, direction="passado"):
        """
        Sobe ou desce a conversa para capturar mensagens antigas (histórico).
        Inteligência de Checkpoint: Se encontrar mensagens que já foram capturadas,
        ele entende que 'convergiu' com o passado e encerra o scroll imediatamente.
        """
        self.log.info(f"Arqueologia Digital em '{group_name}': {'infinito' if scrolls == -1 else scrolls} scrolls p/ {direction}.")
        total = 0
        swipe_dir = "down" if direction == "passado" else "up"
        is_scrolling_up = (direction == "passado")
        
        limit = 999999 if scrolls == -1 else scrolls
        no_new_consecutive = 0
        MAX_EMPTY = 10 
        
        # 1. Recupera o último ponto de parada conhecido
        last_checkpoint_id = self._get_checkpoint(group_name)
        
        for i in range(limit):
            if not self.running:
                break
                
            found = self.scrape_visible_messages(is_scrolling_up=is_scrolling_up)
            total += found
            
            if found > 0:
                no_new_consecutive = 0
                self.log.info(f"Scroll {i+1} - {found} novas mensagens capturadas.")
            else:
                no_new_consecutive += 1
                
                # TESTE DE CONVERGÊNCIA: Se não há nada novo nos primeiros scrolls e temos checkpoint,
                # e a tela atual tem mensagens (found=0 e buffer não vazio), paramos.
                if i < 3 and last_checkpoint_id and self._message_buffer:
                    self.log.info(f"Convergência detectada em '{group_name}'. Já estamos sincronizados com o passado!")
                    break
                
                self.log.info(f"Scroll {i+1} - Sem novidades ({no_new_consecutive}/{MAX_EMPTY} para parada).")
                if no_new_consecutive >= MAX_EMPTY:
                    break
            
            # Executa o scroll
            scale = 0.85 if no_new_consecutive > 0 else 0.65
            self.d.swipe_ext(swipe_dir, scale=scale)
            time.sleep(2)
        
        # 2. Ao finalizar a Arqueologia, atualiza o checkpoint com a msg mais recente da história
        self._update_checkpoint(group_name)

    def _get_checkpoint(self, group_name):
        """Busca o ID da última mensagem processada para este grupo."""
        try:
            cursor = self.conn.execute("SELECT last_msg_id FROM group_checkpoints WHERE group_name = ?", (group_name,))
            row = cursor.fetchone()
            return row[0] if row else None
        except: return None

    def _update_checkpoint(self, group_name):
        """Salva o ID da mensagem mais recente existente no banco para este grupo."""
        try:
            # Busca a mensagem com a data/hora mais recente salva no banco
            cursor = self.conn.execute(
                "SELECT id FROM messages WHERE group_name = ? ORDER BY message_date DESC, timestamp_str DESC LIMIT 1", 
                (group_name,)
            )
            row = cursor.fetchone()
            if row:
                msg_id = row[0]
                self.conn.execute(
                    "INSERT OR REPLACE INTO group_checkpoints (group_name, last_msg_id, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (group_name, msg_id)
                )
                self.conn.commit()
                self.log.debug(f"Checkpoint atualizado para '{group_name}': {msg_id[:10]}...")
        except Exception as e:
            self.log.debug(f"Erro ao atualizar checkpoint: {e}")

    def _check_unread_badge(self, group_name):
        """
        Verifica se há badge de mensagens não lidas para um grupo na lista de conversas.
        Retorna o número de mensagens não lidas (int). 0 = sem novidades.
        Utiliza parse estruturado do XML e busca difusa por similaridade.
        """
        try:
            xml_str = self.d.dump_hierarchy()
            root = ET.fromstring(xml_str)
            
            # 1. Localiza o nó do grupo
            target_node = None
            for node in root.iter('node'):
                txt = (node.get('text', '') or '').strip()
                # Busca flexível: nome exato, com LTR, ou contido no texto
                if group_name == txt or group_name == txt.replace('\u200e', '') or group_name.lower() in txt.lower():
                    target_node = node
                    break
            
            if target_node is None:
                # Opcional: dump para debug se falhar
                with open("last_node_search_fail.xml", "w", encoding="utf-8") as f:
                    f.write(xml_str)
                return 0

            # 2. Mapeia a árvore para subir níveis (Ancestrais)
            parent_map = {c: p for p in root.iter() for c in p}
            
            curr = target_node
            # Sobe até 5 níveis (o container da linha costuma ser o nível 2 ou 3)
            for _ in range(5):
                parent = parent_map.get(curr)
                if parent is None: break
                
                # Procura o badge de forma difusa em qualquer descendente deste container
                for sub in parent.iter('node'):
                    rid = sub.get('resource-id', '')
                    text = sub.get('text', '')
                    desc = sub.get('content-desc', '').lower()
                    
                    # Critério 1: ID conhecido do Weditor ou palavras-chave no ID
                    has_id = any(k in rid.lower() for k in ['count', 'badge', 'unread'])
                    # Critério 2: Texto numérico curto (badge visual)
                    is_numeric = text and text.isdigit() and len(text) <= 3
                    # Critério 3: Descrição de acessibilidade
                    has_desc = 'não lidas' in desc or 'unread' in desc
                    
                    if has_id or is_numeric or has_desc:
                        # Tenta pegar do texto primeiro
                        if text and text.isdigit():
                            count = int(text)
                        else:
                            # Tenta extrair número do content-desc (ex: "Mensagem não lida: 1")
                            import re
                            nums = re.findall(r'\d+', desc or '')
                            count = int(nums[0]) if nums else 0
                            
                        if count > 0:
                            self.log.info(f"Badge detectado em '{group_name}': {count} nova(s).")
                            return count
                curr = parent

            # Se chegou aqui, não achou badge nos ancestrais
            with open("last_badge_search_fail.xml", "w", encoding="utf-8") as f:
                f.write(xml_str)
            return 0
            
        except Exception as e:
            self.log.debug(f"Erro ao verificar badge XML de '{group_name}': {e}")
            return 0


    # -----------------------------------------------------------------------
    # SHUTDOWN
    # -----------------------------------------------------------------------
    def _shutdown_handler(self, signum, frame):
        """Handler para encerramento gracioso via Ctrl+C."""
        self.log.info("Sinal de encerramento recebido. Finalizando...")
        self.running = False

    def close(self):
        """Fecha a conexão com o banco de dados de forma segura."""
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                self.conn = None
                self.log.info("Conexão com banco de dados fechada com sucesso.")
        except Exception as e:
            self.log.error(f"Erro ao fechar banco: {e}")


# ============================================================================
# EXPORTAÇÃO CSV
# ============================================================================

def load_config():
    """Carrega as configurações de um arquivo JSON se existir."""
    config_path = os.path.join(BASE_DIR, "config.json")
    if os.path.exists(config_path):
        import json
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Erro ao carregar config.json: {e}")
    return {}

# ============================================================================
# PONTO DE ENTRADA
# ============================================================================
if __name__ == "__main__":
    import argparse
    import json
    
    config = load_config()
    
    parser = argparse.ArgumentParser(
        description="AutWhats - Automação Inteligente de WhatsApp Business"
    )
    parser.add_argument(
        "--grupos", 
        nargs="+", 
        default=config.get("grupos", ["Teste"]),
        help="Nome(s) do(s) grupo(s) alvo"
    )
    parser.add_argument(
        "--modo", 
        choices=["auto", "listener", "legado", "export", "combo"],
        default=config.get("modo", "combo"),
        help="Modo: 'auto' (Vigia), 'legado' (Histórico), 'combo' (Histórico depois Vigia)"
    )
    # Nota: No argparse mudei o nome 'listener' interno para 'auto' para o novo modo
    
    parser.add_argument(
        "--gatilhos",
        type=str,
        default=config.get("gatilhos", ""),
        help="Gatilhos JSON. Ex: '{\"Grupo\": [\"Pessoa1\"]}'"
    )
    parser.add_argument(
        "--intervalo", 
        type=int, 
        default=config.get("intervalo", 15),
        help="Segundos entre verificações em modo Standby"
    )
    parser.add_argument(
        "--scrolls", 
        type=int, 
        default=5,
        help="Scrolls no modo legado"
    )
    parser.add_argument(
        "--serial",
        type=str,
        default=config.get("serial", DEVICE_SERIAL),
        help=f"Serial ADB (padrão: {DEVICE_SERIAL})"
    )
    
    args = parser.parse_args()
    
    # Processa gatilhos
    
    if args.modo in ["auto", "listener", "combo"]:
        # ── Modo Contínuo: monitora múltiplos grupos via badge ──
        listener = WhatsAppListener(target_groups=args.grupos, serial=args.serial)
        try:
            # Se for o modo COMBO, roda primeiro o Arqueólogo (Legado) para todos os grupos
            if args.modo == "combo":
                listener.log.info(">>> INICIANDO MODO COMBO: Arqueologia iniciada antes do monitoramento...")
                if listener.ensure_app_open():
                    for grupo in args.grupos:
                        if not listener.running: break
                        if listener.open_group(grupo):
                            listener.ingest_legacy(grupo, scrolls=args.scrolls)
                            listener.return_to_chat_list()
                    listener.log.info(">>> Arqueologia concluída. Migrando para modo VIGILANTE em tempo real...")

            # Inicia o modo Vigilante (Auto)
            if listener.ensure_app_open():
                listener.log.info(f"Modo VIGILANTE ativo — monitorando {len(args.grupos)} grupo(s): {', '.join(args.grupos)}")
                
                while listener.running:
                    # Segurança: Garante que o app está aberto antes de cada ciclo
                    if not listener.ensure_app_open():
                        break
                        
                    listener.return_to_chat_list()
                    time.sleep(1)
                    
                    for grupo in args.grupos:
                        if not listener.running: break
                        
                        unread_count = listener._check_unread_badge(grupo)
                        if unread_count > 0:
                            listener.log.info(f">>> Nova(s) mensagem(ns) em '{grupo}' — entrando para captura...")
                            if listener.open_group(grupo):
                                listener.scrape_visible_messages(is_scrolling_up=False)
                                listener.return_to_chat_list()
                                time.sleep(1)
                    
                    if listener.running:
                        time.sleep(args.intervalo)
        finally:
            listener.close()
    elif args.modo == "legado":
        # ── Modo Legado: captura histórico profundo via scroll ──
        listener = WhatsAppListener(target_groups=args.grupos, serial=args.serial)
        try:
            if listener.ensure_app_open():
                for grupo in args.grupos:
                    if listener.open_group(grupo):
                        listener.ingest_legacy(grupo, scrolls=args.scrolls)
                        listener.return_to_chat_list()
        finally:
            listener.close()
