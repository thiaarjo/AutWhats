# AutWhats - Automacao de Captura de Dados do WhatsApp Business

Sistema de captura automatizada de mensagens de grupos do WhatsApp Business via ADB e uiautomator2. Desenvolvido para ambientes corporativos com foco em integridade de dados, rastreabilidade e tolerancia a falhas.

**Autor:** Thiago Azevedo de Araujo — Ciencias da Computacao

---

## Funcionalidades

- Captura de mensagens de texto com identificacao de remetente
- Deteccao de fotos, audios, mensagens de voz e midias de visualizacao unica
- Identificacao automatica de "Eu" vs outros membros do grupo via analise do elemento `status` da UI
- Extracao de nomes dos membros a partir do header do grupo
- Rastreamento de data absoluta via divisores de conversa (Hoje, Ontem, dd/mm/aaaa)
- Deduplicacao de mensagens via hash MD5
- Banco de dados SQLite com WAL para alta performance
- Exportacao automatica para CSV
- Logging centralizado em arquivo (`scraper.log`)
- Tres modos de operacao: listener, legado, exportacao

## Requisitos

- Python 3.10+
- Dispositivo Android conectado via ADB (USB debugging ativo)
- WhatsApp Business instalado no dispositivo
- Bibliotecas: `uiautomator2`, `pandas`

## Instalacao

```bash
git clone https://github.com/thiaarjo/AutWhats.git
cd AutWhats
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

## Uso

### Modo Listener (Monitoramento Continuo)

Monitora a lista de conversas e captura mensagens novas automaticamente.

```bash
python whatsapp_listener.py --modo listener --grupos "Nome do Grupo" --intervalo 15
```

### Modo Legado (Captura de Historico)

Faz scroll na conversa para capturar mensagens antigas. Ideal para grupos com historico extenso.

```bash
python whatsapp_listener.py --modo legado --grupos "Nome do Grupo" --scrolls 50
```

### Exportacao para CSV

Gera um arquivo CSV a partir dos dados ja capturados no banco.

```bash
python whatsapp_listener.py --modo export
```

### Multiplos Grupos

```bash
python whatsapp_listener.py --modo listener --grupos "Grupo A" "Grupo B" "Grupo C"
```

## Parametros

| Parametro     | Descricao                                          | Padrao     |
|---------------|-----------------------------------------------------|------------|
| `--modo`      | Modo de operacao: `listener`, `legado`, `export`    | `listener` |
| `--grupos`    | Nome(s) do(s) grupo(s) alvo                         | `Teste`    |
| `--scrolls`   | Quantidade de scrolls no modo legado                | `5`        |
| `--intervalo` | Segundos entre verificacoes no modo listener        | `15`       |

## Estrutura do Banco de Dados

Tabela `messages`:

| Coluna         | Tipo      | Descricao                                              |
|----------------|-----------|--------------------------------------------------------|
| `id`           | TEXT (PK) | Hash MD5 para deduplicacao                              |
| `group_name`   | TEXT      | Nome do grupo                                           |
| `sender`       | TEXT      | Remetente (nome, numero ou "Eu")                        |
| `content`      | TEXT      | Conteudo da mensagem                                    |
| `media_type`   | TEXT      | text, image, audio, view_once_photo, view_once_voice    |
| `timestamp_str`| TEXT      | Horario da mensagem (ex: 14:28)                         |
| `message_date` | TEXT      | Data absoluta (ex: 2026-03-24)                          |
| `captured_at`  | TIMESTAMP | Momento da captura pelo sistema                         |

## Arquitetura Tecnica

O sistema opera via ADB, controlando a interface do WhatsApp Business atraves da biblioteca `uiautomator2`. A identificacao de remetentes utiliza uma abordagem baseada em parse do XML da hierarquia de UI:

1. Dump completo do XML da tela em cada varredura
2. Agrupamento de elementos por proximidade vertical (blocos de mensagem)
3. Deteccao de mensagens proprias via presenca do elemento `status` (Lida/Entregue)
4. Extracao de nomes dos demais membros a partir do header `conversation_contact_status`

## Arquivos Gerados

| Arquivo                    | Descricao                                |
|---------------------------|------------------------------------------|
| `whatsapp_capture.db`      | Banco SQLite com todas as mensagens       |
| `mensagens_capturadas.csv` | Exportacao formatada dos dados            |
| `scraper.log`              | Log completo de execucao e erros          |

## Configuracao do Dispositivo

O serial do dispositivo ADB esta definido como constante no arquivo `whatsapp_listener.py` (variavel `DEVICE_SERIAL`). Altere conforme necessario:

```python
DEVICE_SERIAL = "SEU_SERIAL_AQUI"
```

Para descobrir o serial do seu dispositivo:

```bash
adb devices
```

## Licenca

Uso academico e corporativo. Consulte o autor para distribuicao.
