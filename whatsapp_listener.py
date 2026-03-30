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
import xml.etree.ElementTree as ET
from datetime import datetime, date

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
                    group_name TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT,
                    media_type TEXT DEFAULT 'text',
                    timestamp_str TEXT,
                    message_date TEXT,
                    local_path TEXT,
                    file_hash TEXT,
                    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Migração: adicionar colunas novas se não existirem
            cols = [
                ("media_type", "TEXT DEFAULT 'text'"),
                ("message_date", "TEXT"),
                ("local_path", "TEXT"),
                ("file_hash", "TEXT")
            ]
            for col_name, col_def in cols:
                try:
                    cursor.execute(f"ALTER TABLE messages ADD COLUMN {col_name} {col_def}")
                except sqlite3.OperationalError:
                    pass  # Já existe
            
            self.conn.commit()
            
            if os.path.exists(self.db_path):
                self.log.info(f"Banco de dados OK: {self.db_path}")
            else:
                self.log.critical(f"FALHA ao criar banco: {self.db_path}")
        except Exception as e:
            self.log.critical(f"Erro fatal no banco de dados: {e}")
            raise

    def save_message(self, sender, content, timestamp, message_date, media_type="text", seq_index=0, local_path=None, file_hash=None):
        """Salva uma mensagem no banco com deduplicação por hash + índice sequencial."""
        msg_id = self._hash(sender, content, timestamp, message_date, media_type, seq_index)
        try:
            self.conn.execute(
                "INSERT INTO messages (id, group_name, sender, content, media_type, timestamp_str, message_date, local_path, file_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (msg_id, self._current_group, sender, content, media_type, timestamp, message_date, local_path, file_hash)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Duplicata
        except Exception as e:
            self.log.error(f"Erro ao salvar mensagem: {e}")
            return False

    def _hash(self, sender, content, timestamp, message_date, media_type, seq_index=0):
        """Gera hash MD5 único para deduplicação. NÃO inclui message_date (instável entre sessões)."""
        data = f"{self._current_group}|{sender}|{content}|{timestamp}|{media_type}|{seq_index}".encode('utf-8')
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
        Localiza o(s) arquivo(s) mais recente(s) via ADB na pasta correspondente,
        faz o pull, calcula o hash e renomeia.
        Suporta 'count' para baixar vários de uma vez (ex: álbuns).
        """
        adb_folder = self._get_media_adb_path("image" if media_type == "image_album" else media_type)
        if not adb_folder:
            return None, None

        try:
            # 1. Encontra os N arquivos mais recentes
            # Para áudio (VNs), busca recursiva. IMPORTANTE: Usar aspas para o shell
            if media_type == "audio":
                find_cmd = f"find '{adb_folder}' -type f -name '*.opus' -printf '%T@ %p\\n' | sort -rn | head -{count} | cut -d' ' -f2-"
            else:
                # Para imagens, ls -t e head -N
                find_cmd = f"ls -t '{adb_folder}' | head -{count}"
            
            output = self.d.shell(find_cmd).output.strip()
            if not output or "not found" in output:
                return None, None
                
            remote_paths = output.split('\n')
            
            paths_downloaded = []
            hashes_downloaded = []
            
            for remote_file in remote_paths:
                remote_file = remote_file.strip()
                if not remote_file: continue
                
                # Constrói path completo no Android
                if media_type == "audio":
                    remote_path = remote_file
                else:
                    remote_path = os.path.join(adb_folder, remote_file).replace("\\", "/")
                
                # 2. Define destino local temporário (Caminho absoluto para não errar)
                ext = os.path.splitext(remote_path)[1] or (".jpg" if "image" in media_type else ".opus")
                temp_name = f"temp_{int(time.time()*1000)}_{random.randint(100,999)}{ext}"
                local_dest_dir = self.img_dir if "image" in media_type else self.audio_dir
                local_temp_path = os.path.abspath(os.path.join(local_dest_dir, temp_name))

                # 3. Faz o Pull (Importante: remotepath pode ter espaços, uiautomator2 lida bem se for a string pura)
                self.log.debug(f"Puxando mídia: {remote_path} -> {local_temp_path}")
                try:
                    self.d.pull(remote_path, local_temp_path)
                except Exception as pe:
                    self.log.error(f"Erro no pull adb: {pe}")
                    continue

                if not os.path.exists(local_temp_path):
                    self.log.warning(f"Arquivo não chegou ao PC: {local_temp_path}")
                    continue

                # 4. Hash e Persistência
                f_hash = self._calculate_file_hash(local_temp_path)
                final_name = f"{f_hash}{ext}" if f_hash else temp_name
                local_final_path = os.path.abspath(os.path.join(local_dest_dir, final_name))
                
                if f_hash and os.path.exists(local_final_path):
                    os.remove(local_temp_path)
                else:
                    os.rename(local_temp_path, local_final_path)

                # Caminho relativo para o banco (para ser portátil)
                rel_base = "images" if "image" in media_type else "audio"
                rel_path = os.path.join("media", rel_base, os.path.basename(local_final_path)).replace("\\", "/")
                paths_downloaded.append(rel_path)
                hashes_downloaded.append(f_hash or "unknown")

            # Retorna strings separadas por vírgula se for lote
            return "|".join(paths_downloaded), "|".join(hashes_downloaded)

        except Exception as e:
            self.log.error(f"Erro no download de mídia ({media_type}): {e}")
            return None, None

    # -----------------------------------------------------------------------
    # NAVEGAÇÃO SEGURA
    # -----------------------------------------------------------------------
    def ensure_app_open(self):
        """Garante que o WA Business está aberto, com até 3 tentativas."""
        for attempt in range(1, 4):
            try:
                current = self.d.app_current()
                if current.get('package') == PACKAGE_NAME:
                    self.log.debug("WA Business já está em primeiro plano.")
                    return True
                
                self.log.info(f"Abrindo WA Business (tentativa {attempt}/3)...")
                self.d.app_start(PACKAGE_NAME)
                time.sleep(3)
                
                if self.d.app_current().get('package') == PACKAGE_NAME:
                    self.log.info("WA Business aberto com sucesso.")
                    return True
                
                # Fallback: busca na Home Screen
                self.log.warning("Abertura direta falhou. Buscando na Home Screen...")
                self.d.press("home")
                time.sleep(1)
                
                for page in range(5):
                    if self.d(text="WA Business").exists(timeout=1):
                        self.d(text="WA Business").click()
                        time.sleep(3)
                        if self.d.app_current().get('package') == PACKAGE_NAME:
                            self.log.info("WA Business encontrado na Home Screen.")
                            return True
                    self.d.swipe_ext("left", scale=0.9)
                    time.sleep(0.5)
                    
            except Exception as e:
                self.log.error(f"Erro na tentativa {attempt}: {e}")
                time.sleep(2)
        
        self.log.critical("Não foi possível abrir o WA Business após 3 tentativas.")
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
        Garante identificação correta do remetente via elemento 'status'.
        """
        messages_found = 0
        
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
            for fmt in ["%d/%m/%Y", "%d de %B de %Y", "%d/%m/%y"]:
                try:
                    return datetime.strptime(raw_text, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            self.log.debug(f"Data não parseada: '{raw_text}'")
            return raw_text

    def _parse_messages_from_xml(self, root, is_scrolling_up=False):
        """
        Percorre o XML e identifica mensagens por seus resource-ids.
        Usa Sliding Window bidirecional para determinar quais são as NOVAS.
        """
        import xml.etree.ElementTree as ET
        count_saved = 0
        
        # Coleta TODOS os elementos WhatsApp relevantes com suas posições
        all_elements = []
        for node in root.iter('node'):
            rid = node.get('resource-id', '')
            if not rid or 'com.whatsapp.w4b' not in rid:
                continue
            
            short_rid = rid.replace('com.whatsapp.w4b:id/', '')
            text = node.get('text', '')
            desc = node.get('content-desc', '')
            bounds_str = node.get('bounds', '')
            bounds = self._parse_bounds(bounds_str)
            
            all_elements.append({
                'rid': short_rid,
                'full_rid': rid,
                'text': text,
                'desc': desc,
                'bounds': bounds,
                'node': node
            })
        
        # Agrupa elementos por proximidade vertical
        message_rows = self._group_elements_by_row(all_elements)
        
        # 1. Coleta mensagens da tela atual com rastreamento dinâmico de data
        current_screen_raw = []
        active_date = self.current_date
        
        for row in message_rows:
            # Verifica se esta linha é um divisor de data
            is_divider = False
            for el in row:
                if el['rid'] == 'conversation_row_date_divider':
                    raw_text = el['text']
                    if raw_text:
                        active_date = self._parse_date_divider(raw_text)
                        self.current_date = active_date # Atualiza o global tb
                        is_divider = True
                        break
            
            if is_divider:
                continue
                
            # Processa linha de mensagem
            result = self._process_row(row)
            if result:
                sender, content, timestamp, media_type = result
                # Criamos a 5-tupla estável (incluindo a data ativa no momento)
                current_screen_raw.append((sender, content, timestamp, media_type, active_date))
        
        if not current_screen_raw:
            return 0
        
        # 2. Re-implementação robusta da Janela Deslizante (Overlap)
        overlap_size = self._find_overlap(self._message_buffer, current_screen_raw, is_scrolling_up)
        
        if is_scrolling_up:
            new_messages = current_screen_raw[:len(current_screen_raw) - overlap_size]
            self._message_buffer = new_messages + self._message_buffer
        else:
            new_messages = current_screen_raw[overlap_size:]
            self._message_buffer.extend(new_messages)
            
        # 3. Salva no banco com deduplicação real via Hash MD5 (estável entre sessões)
        for msg in new_messages:
            sender, content, timestamp, media_type, msg_date = msg
            
            # Cálculo de ocorrência para seq_index (deduplicação estável)
            occurrence_in_session = 0
            for prev_msg in self._message_buffer:
                if prev_msg == msg:
                    occurrence_in_session += 1
                if prev_msg is msg:
                    break
            seq_index = occurrence_in_session - 1
            
            msg_id = self._hash(sender, content, timestamp, msg_date, media_type, seq_index)
            
            if not self._check_id_exists(msg_id):
                # Se for mídia, baixa o(s) arquivo(s)
                local_path = None
                f_hash = None
                if media_type in ("image", "audio", "image_album"):
                    count = 1
                    if media_type == "image_album":
                        import re
                        match = re.search(r'(\d+)', content)
                        count = int(match.group(1)) if match else 1
                    
                    self.log.info(f"Nova mídia detectada ({media_type}, {count} arquivos): Baixando...")
                    # Delay para o WA processar os arquivos no celular
                    time.sleep(2.0) 
                    local_path, f_hash = self._download_recent_media(media_type, count=count)
                
                if self.save_message(sender, content, timestamp, msg_date, media_type, seq_index, local_path, f_hash):
                    count_saved += 1
                    status = f"[{media_type.upper()}]"
                    checksum_info = f" [SHA256: {f_hash[:10]}...]" if f_hash else ""
                    self.log.info(f"{status}{checksum_info} {sender}: {content[:40]}... ({timestamp})")
            else:
                self.log.debug(f"Mensagem já existe: {sender} - {content[:20]} ({msg_date} {timestamp})")
        
        if len(self._message_buffer) > 200:
            self._message_buffer = self._message_buffer[:200] if is_scrolling_up else self._message_buffer[-200:]
            
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

    def _group_elements_by_row(self, elements):
        """Agrupa elementos por proximidade vertical (~mesma linha da mensagem)."""
        if not elements:
            return []
        
        # Ordena por posição vertical
        sorted_els = sorted(elements, key=lambda e: e['bounds']['top'])
        
        rows = []
        if not sorted_els: return []
        
        current_row = [sorted_els[0]]
        
        for el in sorted_els[1:]:
            # Se o top está dentro de 80px do primeiro elemento do grupo atual
            if abs(el['bounds']['top'] - current_row[0]['bounds']['top']) < 80:
                current_row.append(el)
            else:
                rows.append(current_row)
                current_row = [el]
        
        if current_row:
            rows.append(current_row)
        
        return rows

    def _process_row(self, row_elements):
        """
        Processa um grupo de elementos na mesma linha vertical.
        Retorna (sender, content, timestamp, media_type) ou None.
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
        album_thumbs = []
        extra_count = 0
        
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
            elif rid == 'date':
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
                desc = (el['desc'] or '').lower()
                if any(kw in desc for kw in ['voz', 'áudio', 'audio', 'segundos', 'minutos']):
                    has_voice = el
            elif rid in ('conversation_row_name', 'conversation_row_contact_name', 'name_in_group_tv'):
                has_sender_name = el
        
        # Determina o remetente
        if has_status:
            sender = "Eu"
        elif has_sender_name and has_sender_name['text']:
            raw_name = has_sender_name['text'].replace('\u200e', '').strip()
            if raw_name.endswith(':'):
                raw_name = raw_name[:-1].strip()
            sender = raw_name
            self.last_known_sender = sender
        elif self.group_other_members:
            sender = self.group_other_members[0]
        else:
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
            return (sender, content + suffix, timestamp, "image_album")
            
        if has_message_text and has_message_text['text']:
            txt = has_message_text['text']
            if "mensagem apagada" in txt.lower():
                return (sender, "[MENSAGEM APAGADA]", timestamp, "deleted")
            return (sender, txt + suffix, timestamp, "text")
            
        elif has_image:
            desc = (has_image['desc'] or '').strip()
            cap = f" | Legenda: {has_caption['text']}" if has_caption and has_caption['text'] else ""
            content = f"[Foto] {desc}{cap}" if desc else f"[Foto]{cap}"
            return (sender, content + suffix, timestamp, "image")
            
        elif has_view_once:
            desc = (has_view_once['desc'] or '').lower()
            if 'voz' in desc or 'reprodução' in desc:
                return (sender, f"[Voz Única] {desc}", timestamp, "view_once_voice")
            else:
                return (sender, f"[Foto Única] {desc}", timestamp, "view_once_photo")
                
        elif has_voice:
            desc = (has_voice['desc'] or '').strip()
            if sender == self.last_known_sender or sender == "Desconhecido":
                parts = desc.replace('\u200e', '').split(',')
                if len(parts) > 1:
                    potential_name = parts[0].strip()
                    if potential_name and potential_name.lower() not in ('voz', 'áudio'):
                        sender = potential_name
                        self.last_known_sender = sender
            return (sender, f"[Áudio] {desc}{suffix}", timestamp, "audio")
        
        return None

    # -----------------------------------------------------------------------
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
        
        return datetime.now().strftime("%H:%M")

    # -----------------------------------------------------------------------
    # INGESTÃO DE LEGADO (HISTÓRICO)
    # -----------------------------------------------------------------------
    def ingest_legacy(self, group_name="Desconhecido", scrolls=5, direction="passado"):
        """
        Sobe ou desce a conversa para capturar mensagens antigas (histórico).
        Para automaticamente se ficar 3 scrolls sem encontrar mensagens novas.
        Usa 'swipe reforçado' quando detecta scrolls vazios para garantir que a tela mudou.
        """
        self.log.info(f"Arqueologia Digital em '{group_name}': {'infinito (smart-stop)' if scrolls == -1 else scrolls} scrolls p/ {direction}.")
        total = 0
        swipe_dir = "down" if direction == "passado" else "up"
        is_scrolling_up = (direction == "passado")
        
        # -1 significa scroll infinito até o critério de parada
        limit = 999999 if scrolls == -1 else scrolls
        no_new_consecutive = 0
        MAX_EMPTY = 3  # Scrolls consecutivos sem novidade antes de parar
        
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
                self.log.info(f"Scroll {i+1} - Sem novidades ({no_new_consecutive}/{MAX_EMPTY} para parada).")
                
                if no_new_consecutive >= MAX_EMPTY:
                    self.log.info(f"Parada automática: {MAX_EMPTY} scrolls consecutivos sem mensagens novas.")
                    break
            
            # Executa o scroll — se estiver sem novidades, usa swipe mais forte
            if no_new_consecutive > 0:
                # Swipe reforçado: mais longo para tentar revelar conteúdo novo
                self.d.swipe_ext(swipe_dir, scale=0.9)
                time.sleep(2)
            else:
                self.d.swipe_ext(swipe_dir, scale=0.8)

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
def export_csv():
    """Exporta os dados do banco para CSV formatado."""
    import pandas as pd
    
    if not os.path.exists(DB_PATH):
        print(f"Erro: Banco de dados não encontrado em: {DB_PATH}")
        return
    
    print(f"Abrindo banco para exportação: {DB_PATH}")
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
    except Exception as e:
        print(f"ERRO ao abrir banco para export: {e}")
        import traceback
        traceback.print_exc()
        return
    query = """
        SELECT 
            id AS ID,
            group_name AS Grupo,
            sender AS Remetente,
            content AS Conteudo,
            media_type AS Tipo_Midia,
            timestamp_str AS Hora,
            message_date AS Data_Mensagem,
            local_path AS Caminho_Media,
            file_hash AS Hash,
            captured_at AS Data_Captura
        FROM messages
        ORDER BY group_name, message_date, timestamp_str
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"Exportado com sucesso: {CSV_PATH}")
    print(f"Total de registros: {len(df)}")
    if len(df) > 0:
        print(f"\nResumo por tipo de mídia:")
        print(df['Tipo_Midia'].value_counts().to_string())


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
        choices=["auto", "listener", "legado", "export"],
        default=config.get("modo", "auto"),
        help="Modo: 'auto' (Gatilhos), 'listener' (Legado), 'export' (CSV)"
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
    
    if args.modo == "export":
        export_csv()
    elif args.modo in ["auto", "listener"]:
        # ── Modo Contínuo: monitora múltiplos grupos via badge ──
        listener = WhatsAppListener(target_groups=args.grupos, serial=args.serial)
        try:
            if listener.ensure_app_open():
                listener.log.info(f"Modo AUTO ativo — monitorando {len(args.grupos)} grupo(s): {', '.join(args.grupos)}")
                listener.log.info(f"Intervalo entre ciclos: {args.intervalo}s")
                
                while listener.running:
                    # Garante que estamos na lista de conversas
                    listener.return_to_chat_list()
                    time.sleep(1)
                    
                    listener.log.debug(f"Ciclo iniciado para grupos: {args.grupos}")
                    for grupo in args.grupos:
                        if not listener.running:
                            break
                        
                        # Scaneia a tela por badges (não precisa passar o grupo por parâmetro aqui)
                        unread_list = listener._check_unread_badge()
                        
                        if grupo in unread_list:
                            listener.log.info(f">>> Nova(s) mensagem(ns) em '{grupo}' — entrando para captura...")
                            if listener.open_group(grupo):
                                listener.scrape_visible_messages(is_scrolling_up=False)
                                listener.return_to_chat_list()
                                time.sleep(1)
                        else:
                            listener.log.debug(f"Sem novidades em '{grupo}'")
                    
                    if listener.running:
                        listener.log.debug(f"Ciclo concluído. Aguardando {args.intervalo}s...")
                        time.sleep(args.intervalo)
        finally:
            listener.close()
            export_csv()
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
            export_csv()
