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
import time
import os
import sys
import signal
import logging
from datetime import datetime, date

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "whatsapp_capture.db")
LOG_PATH = os.path.join(BASE_DIR, "scraper.log")
CSV_PATH = os.path.join(BASE_DIR, "mensagens_capturadas.csv")

DEVICE_SERIAL = "192.168.0.254:33325"
PACKAGE_NAME = "com.whatsapp.w4b"

# Resource IDs mapeados via Weditor
IDS = {
    "group_name":       "com.whatsapp.w4b:id/conversation_contact_name",
    "group_members":    "com.whatsapp.w4b:id/conversation_contact_status",
    "sender":           "com.whatsapp.w4b:id/conversation_row_name",
    "sender_alt":       "com.whatsapp.w4b:id/conversation_row_contact_name",
    "message_text":     "com.whatsapp.w4b:id/message_text",
    "timestamp":        "com.whatsapp.w4b:id/date",
    "date_divider":     "com.whatsapp.w4b:id/conversation_row_date_divider",
    "image":            "com.whatsapp.w4b:id/image",
    "view_once_media":  "com.whatsapp.w4b:id/view_once_media_container_large",
    "main_layout":      "com.whatsapp.w4b:id/main_layout",
    "status":           "com.whatsapp.w4b:id/status",
    "info":             "com.whatsapp.w4b:id/info",
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
        # Armazena tuplas (sender, content, timestamp, media_type) da última tela
        self._message_buffer = []
        self._saved_sequences = set()  # Hashes de sequências já salvas
        
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
                    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Migração: adicionar colunas novas se não existirem
            try:
                cursor.execute("ALTER TABLE messages ADD COLUMN media_type TEXT DEFAULT 'text'")
            except sqlite3.OperationalError:
                pass
            try:
                cursor.execute("ALTER TABLE messages ADD COLUMN message_date TEXT")
            except sqlite3.OperationalError:
                pass
            
            self.conn.commit()
            
            if os.path.exists(self.db_path):
                self.log.info(f"Banco de dados OK: {self.db_path}")
            else:
                self.log.critical(f"FALHA ao criar banco: {self.db_path}")
        except Exception as e:
            self.log.critical(f"Erro fatal no banco de dados: {e}")
            raise

    def save_message(self, sender, content, timestamp, media_type="text", seq_index=0):
        """Salva uma mensagem no banco com deduplicação por hash + índice sequencial."""
        msg_id = self._hash(sender, content, timestamp, media_type, seq_index)
        try:
            self.conn.execute(
                "INSERT INTO messages (id, group_name, sender, content, media_type, timestamp_str, message_date) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (msg_id, self._current_group, sender, content, media_type, timestamp, self.current_date)
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Duplicata
        except Exception as e:
            self.log.error(f"Erro ao salvar mensagem: {e}")
            return False

    def _hash(self, sender, content, timestamp, media_type, seq_index=0):
        """Gera hash MD5 único para deduplicação. Inclui índice sequencial."""
        data = f"{self._current_group}|{sender}|{content}|{timestamp}|{media_type}|{seq_index}".encode('utf-8')
        return hashlib.md5(data).hexdigest()

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

    def navigate_to_group(self, group_name):
        """Navega para um grupo específico na lista de conversas."""
        self._current_group = group_name
        
        # Se já estiver dentro do grupo
        if self.d(resourceId=IDS["group_name"], text=group_name).exists(timeout=2):
            self.log.debug(f"Já estamos dentro do grupo '{group_name}'.")
            return True
        
        # Se estiver dentro de OUTRO grupo, voltar
        if self.d(resourceId=IDS["group_name"]).exists(timeout=1):
            self.log.info("Dentro de outro grupo. Voltando à lista...")
            self.d.press("back")
            time.sleep(1.5)
        
        self.log.info(f"Buscando grupo '{group_name}' na lista de conversas...")
        for scroll in range(10):
            try:
                entry = self.d(text=group_name)
                if entry.exists(timeout=1):
                    entry.click()
                    time.sleep(2)
                    # Confirma que entrou
                    if self.d(resourceId=IDS["group_name"], text=group_name).exists(timeout=2):
                        self.log.info(f"Grupo '{group_name}' aberto com sucesso.")
                        return True
                self.d.swipe_ext("up", scale=0.5)
                time.sleep(0.8)
            except Exception as e:
                self.log.error(f"Erro ao buscar grupo (scroll {scroll}): {e}")
                time.sleep(1)
        
        self.log.error(f"Grupo '{group_name}' NÃO encontrado após 10 scrolls.")
        return False

    def return_to_chat_list(self):
        """Retorna à lista de conversas de forma segura."""
        for _ in range(3):
            try:
                if self.d(resourceId=IDS["group_name"]).exists(timeout=1):
                    self.d.press("back")
                    time.sleep(1)
                else:
                    return True
            except Exception:
                self.d.press("back")
                time.sleep(1)
        return True

    # -----------------------------------------------------------------------
    # CAPTURA DE MENSAGENS E MÍDIA (via XML Hierarchy Parsing)
    # -----------------------------------------------------------------------
    def scrape_visible_messages(self):
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
            
            # 3. Atualizar data via divisores
            for node in root.iter('node'):
                rid = node.get('resource-id', '')
                if rid == IDS["date_divider"]:
                    raw_date = node.get('text', '')
                    if raw_date:
                        parsed = self._parse_date_divider(raw_date)
                        if parsed:
                            self.current_date = parsed
            
            # 4. Processar cada mensagem encontrando blocos de conteúdo
            messages_found += self._parse_messages_from_xml(root)
            
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

    def _parse_messages_from_xml(self, root):
        """
        Percorre o XML e identifica mensagens por seus resource-ids.
        Usa Sliding Window para deduplicar: compara a sequência atual
        com o buffer da tela anterior para encontrar o ponto de sobreposição.
        """
        import xml.etree.ElementTree as ET
        count = 0
        
        # Reinicia o rastreador de timestamp para este scan
        self.last_seen_timestamp = datetime.now().strftime("%H:%M")
        
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
        
        # 1. Coleta TODAS as mensagens da tela atual como uma SEQUÊNCIA ordenada
        current_screen = []
        for row in message_rows:
            result = self._process_row(row)
            if result:
                current_screen.append(result)
        
        if not current_screen:
            return 0
        
        # 2. Encontra o ponto de sobreposição com o buffer anterior
        #    A sobreposição é o maior sufixo do buffer que coincide com um prefixo da tela atual
        overlap_size = self._find_overlap(self._message_buffer, current_screen)
        
        # 3. Salva apenas as mensagens NOVAS (após a sobreposição)
        new_messages = current_screen[overlap_size:]
        
        for msg in new_messages:
            sender, content, timestamp, media_type = msg
            # Calcula o índice sequencial global desta mensagem
            # (quantas vezes essa mesma tupla já apareceu no banco)
            seq_key = f"{self._current_group}|{sender}|{content}|{timestamp}|{media_type}"
            if seq_key not in self._saved_sequences:
                self._saved_sequences = set()  # Limita memória: só rastreia sessão atual
            
            # Conta ocorrências anteriores no DB para este exato conteúdo
            try:
                cursor = self.conn.execute(
                    "SELECT COUNT(*) FROM messages WHERE group_name=? AND sender=? AND content=? AND timestamp_str=? AND media_type=?",
                    (self._current_group, sender, content, timestamp, media_type)
                )
                existing_count = cursor.fetchone()[0]
            except Exception:
                existing_count = 0
            
            seq_index = existing_count  # Próximo índice disponível
            
            if self.save_message(sender, content, timestamp, media_type, seq_index):
                count += 1
                self.log.info(f"[{media_type.upper()}] {sender}: {content[:40]}... ({timestamp})")
        
        # 4. Atualiza o buffer para o próximo scroll
        self._message_buffer = current_screen
        
        return count
    
    def _find_overlap(self, buffer, current):
        """
        Encontra o maior sufixo de 'buffer' que coincide com um prefixo de 'current'.
        Retorna o tamanho da sobreposição.
        """
        if not buffer or not current:
            return 0
        
        max_overlap = min(len(buffer), len(current))
        
        for size in range(max_overlap, 0, -1):
            # Compara o sufixo do buffer (últimos 'size' itens)
            # com o prefixo do current (primeiros 'size' itens)
            suffix = buffer[-size:]
            prefix = current[:size]
            
            if suffix == prefix:
                self.log.debug(f"Overlap de {size} mensagens detectado entre telas.")
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
        
        for el in row_elements:
            rid = el['rid']
            
            if rid == 'message_text':
                has_message_text = el
            elif rid == 'status':
                has_status = True  # Presença de status = mensagem do "Eu"
            elif rid == 'date':
                has_timestamp = el
            elif rid == 'image':
                # Ignora ícones pequenos
                w = el['bounds']['right'] - el['bounds']['left']
                h = el['bounds']['bottom'] - el['bounds']['top']
                if w > 100 and h > 100:
                    has_image = el
            elif rid == 'view_once_media_container_large':
                has_view_once = el
            elif rid == 'main_layout':
                desc = el['desc'].lower()
                if any(kw in desc for kw in ['mensagem de voz', 'áudio', 'audio', 'segundos', 'minutos']):
                    has_voice = el
            elif rid in ('conversation_row_name', 'conversation_row_contact_name'):
                has_sender_name = el
        
        # Determina o remetente
        if has_status:
            sender = "Eu"
        elif has_sender_name and has_sender_name['text']:
            # Limpa lixo Unicode e nomes que vem com prefixos na UI
            raw_name = has_sender_name['text'].replace('\u200e', '').strip()
            # Se vier algo como "Fulano: ", remove o ":"
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
            # Herda o último timestamp visto (evita duplicatas por 'now()')
            timestamp = self.last_seen_timestamp
        
        # Processa o conteúdo baseado no tipo
        if has_message_text and has_message_text['text']:
            return (sender, has_message_text['text'], timestamp, "text")
        elif has_image:
            desc = has_image['desc'] or ''
            content = f"[Foto] {desc}" if desc else "[Foto]"
            return (sender, content, timestamp, "image")
        elif has_view_once:
            desc = has_view_once['desc'] or ''
            desc_lower = desc.lower()
            if 'voz' in desc_lower or 'reprodução' in desc_lower:
                return (sender, f"[Voz Única] {desc}", timestamp, "view_once_voice")
            else:
                return (sender, f"[Foto Única] {desc}", timestamp, "view_once_photo")
        elif has_voice:
            desc = has_voice['desc'] or ''
            # Tenta extrair nome do remetente do desc do áudio
            # Ex: "Th, mensagem de voz, 6 segundos, 14:28"
            if sender == self.last_known_sender or sender == "Desconhecido":
                parts = desc.replace('\u200e', '').split(',')
                if len(parts) > 1:
                    potential_name = parts[0].strip()
                    if potential_name and potential_name.lower() not in ('mensagem de voz', 'áudio'):
                        sender = potential_name
                        self.last_known_sender = sender
            return (sender, f"[Áudio] {desc}", timestamp, "audio")
        
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
    def ingest_legacy(self, scrolls=5, direction="passado"):
        """
        Sobe ou desce a conversa para capturar mensagens antigas (histórico).
        direction: 'passado' (vê o que veio antes) ou 'futuro' (vê o que veio depois)
        """
        self.log.info(f"Ingestão de legado: {scrolls} scrolls na direção '{direction}'.")
        total = 0
        # 'passado' = arrastar para BAIXO (swipe down)
        # 'futuro' = arrastar para CIMA (swipe up)
        swipe_dir = "down" if direction == "passado" else "up"
        
        for i in range(scrolls):
            if not self.running:
                break
            found = self.scrape_visible_messages()
            total += found
            self.log.info(f"Scroll {i+1}/{scrolls} - {found} novas neste scroll, {total} total.")
            
            # Executa o scroll
            self.d.swipe_ext(swipe_dir, scale=0.8)
            time.sleep(1.5)
        
        self.log.info(f"Ingestão de legado concluída: {total} mensagens capturadas.")
        return total

    # -----------------------------------------------------------------------
    # MODO LISTENER (GATILHO AUTOMÁTICO)
    # -----------------------------------------------------------------------
    def listen(self, interval=10):
        """
        Modo Listener: monitora continuamente a lista de conversas e
        captura mensagens novas quando detecta atividade.
        
        Args:
            interval: Segundos entre cada verificação (padrão: 10)
        """
        self.log.info("=" * 60)
        self.log.info(f"MODO LISTENER ATIVADO - Intervalo: {interval}s")
        self.log.info(f"Monitorando {len(self.target_groups)} grupo(s)")
        self.log.info("Pressione Ctrl+C para encerrar.")
        self.log.info("=" * 60)
        
        cycle = 0
        while self.running:
            cycle += 1
            self.log.debug(f"--- Ciclo de monitoramento #{cycle} ---")
            
            try:
                # Garante que o app está aberto
                if not self.ensure_app_open():
                    self.log.error("App não disponível. Aguardando próximo ciclo...")
                    time.sleep(interval * 2)
                    continue
                
                # Para cada grupo alvo
                for group in self.target_groups:
                    if not self.running:
                        break
                    
                    self._current_group = group
                    
                    # Verifica se há mensagens não lidas (badge) ou se é hora de Force Scan
                    has_unread = self._check_unread_badge(group)
                    is_force_scan = (cycle % 5 == 0) # Force scan a cada 5 ciclos (~1.5 min)
                    
                    if has_unread or is_force_scan:
                        if is_force_scan and not has_unread:
                            self.log.info(f"🔍 Executando Force Scan periódico em '{group}'...")
                        else:
                            self.log.info(f"🔔 Mensagens novas detectadas em '{group}'!")
                        
                        if self.navigate_to_group(group):
                            # Loop de captura com scroll para garantir que pegamos tudo
                            total_new_in_session = 0
                            no_new_consecutive = 0
                            
                            while no_new_consecutive < 2 and self.running:
                                found = self.scrape_visible_messages()
                                if found > 0:
                                    total_new_in_session += found
                                    no_new_consecutive = 0
                                    # Desliza o dedo para CIMA para rolar a conversa para BAIXO (mais novas)
                                    self.log.debug("Rolando para baixo para buscar mais mensagens novas...")
                                    self.d.swipe_ext("up", scale=0.7)
                                    time.sleep(1.5)
                                else:
                                    no_new_consecutive += 1
                                    if no_new_consecutive < 2:
                                        # Pequeno scroll adicional para garantir que não parou no meio de um balão
                                        self.d.swipe_ext("up", scale=0.3)
                                        time.sleep(1)
                            
                            if total_new_in_session > 0:
                                self.log.info(f"Sessão de captura em '{group}' finalizada. Total: {total_new_in_session}")
                            self.return_to_chat_list()
                        else:
                            self.log.warning(f"Não foi possível entrar no grupo '{group}'.")
                    else:
                        self.log.debug(f"Sem novidades em '{group}'.")
                
            except Exception as e:
                self.log.error(f"Erro no ciclo #{cycle}: {e}")
            
            # Aguarda próximo ciclo
            for _ in range(interval):
                if not self.running:
                    break
                time.sleep(1)
        
        self.log.info("Listener encerrado de forma limpa.")

    def _check_unread_badge(self, group_name):
        """
        Verifica se há badge de mensagens não lidas no grupo de forma ultra-agressiva.
        """
        try:
            # 1. Busca a entrada do grupo (ignora fuxicos Unicode)
            group_el = self.d(text=group_name)
            if not group_el.exists:
                # Tenta busca parcial se o nome oficial falhar
                group_el = self.d(textContains=group_name)
                if not group_el.exists:
                    return False
            
            # 2. Heurística de proximidade (ID Genérico 'message_count')
            # Procuramos qualquer elemento que contenha 'count' ou 'badge' em layouts pais da linha
            row = group_el.up(className="android.widget.RelativeLayout")
            if not row.exists:
                row = group_el.up(className="android.view.ViewGroup")
            
            if row.exists:
                # Busca qualquer contador numérico no layout da linha da conversa
                indicators = row.child(resourceIdMatches=".*count.*|.*badge.*|.*unread.*")
                if indicators.exists:
                    text = indicators.get_text()
                    if text and text.isdigit():
                        return True
                
                # Check visual secundário: bolinha verde sem ID (TextView com conteúdo pequeno)
                for child in row.child(className="android.widget.TextView"):
                    txt = child.get_text()
                    if txt and txt.isdigit() and len(txt) <= 3:
                        # Verifica se não é o timestamp (0-23, 0-59)
                        if ":" not in txt:
                            return True
            
            return False
            
        except Exception as e:
            self.log.debug(f"Erro ao verificar badge: {e}")
            return False

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
            group_name AS Grupo,
            sender AS Remetente,
            content AS Conteudo,
            media_type AS Tipo_Midia,
            timestamp_str AS Hora,
            message_date AS Data_Mensagem,
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


# ============================================================================
# PONTO DE ENTRADA
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="WhatsApp Business Listener - Captura automática de mensagens"
    )
    parser.add_argument(
        "--grupos", 
        nargs="+", 
        default=["Teste"],
        help="Nome(s) do(s) grupo(s) alvo (padrão: 'Teste')"
    )
    parser.add_argument(
        "--modo", 
        choices=["listener", "legado", "export"],
        default="listener",
        help="Modo de operação: 'listener' (gatilho), 'legado' (histórico), 'export' (CSV)"
    )
    parser.add_argument(
        "--scrolls", 
        type=int, 
        default=5,
        help="Quantidade de scrolls para modo legado (padrão: 5)"
    )
    parser.add_argument(
        "--intervalo", 
        type=int, 
        default=15,
        help="Intervalo de verificação em segundos para modo listener (padrão: 15)"
    )
    parser.add_argument(
        "--serial",
        type=str,
        default=DEVICE_SERIAL,
        help=f"Serial ou IP do dispositivo ADB (padrão: {DEVICE_SERIAL})"
    )
    parser.add_argument(
        "--direcao", 
        choices=["passado", "futuro"],
        default="passado",
        help="Direção do scroll para modo legado: 'passado' (histórico), 'futuro' (novas)"
    )
    
    args = parser.parse_args()
    
    if args.modo == "export":
        export_csv()
    else:
        listener = WhatsAppListener(target_groups=args.grupos, serial=args.serial)
        
        try:
            if args.modo == "legado":
                if listener.ensure_app_open():
                    for grupo in args.grupos:
                        if listener.navigate_to_group(grupo):
                            listener.ingest_legacy(scrolls=args.scrolls, direction=args.direcao)
                            listener.return_to_chat_list()
                        else:
                            listener.log.error(f"Grupo '{grupo}' não encontrado.")
                    listener.log.info("Processo de legado concluído.")
                else:
                    listener.log.critical("Impossível iniciar: app não encontrado.")
            
            elif args.modo == "listener":
                if listener.ensure_app_open():
                    listener.listen(interval=args.intervalo)
                else:
                    listener.log.critical("Impossível iniciar: app não encontrado.")
        finally:
            # Fecha a conexão ANTES de exportar
            listener.close()
            export_csv()
