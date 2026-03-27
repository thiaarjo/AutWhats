import uiautomator2 as u2
import xml.etree.ElementTree as ET

def debug():
    d = u2.connect("192.168.0.254:33325")
    print("Conectado ao dispositivo.")
    
    xml_str = d.dump_hierarchy()
    root = ET.fromstring(xml_str)
    
    group_name = "Teste"
    target_node = None
    
    # 1. Encontra o nó do texto
    for node in root.iter('node'):
        if node.get('text') == group_name:
            target_node = node
            break
            
    if target_node is None:
        print(f"ERRO: Nó com texto '{group_name}' não encontrado no XML.")
        # Mostra todos os textos pra debugar
        all_texts = [n.get('text') for n in root.iter('node') if n.get('text')]
        print(f"Textos visíveis: {all_texts[:10]}...")
        return

    print(f"Nó '{group_name}' encontrado. Procurando badge nos ancestrais...")

    # O ET não tem link direto pro pai, precisamos construir o mapa
    parent_map = {c: p for p in root.iter() for c in p}
    
    curr = target_node
    # Sobe até 5 níveis
    for i in range(5):
        parent = parent_map.get(curr)
        if parent is None: break
        
        p_rid = parent.get('resource-id', '')
        p_class = parent.get('class', '')
        print(f"Pai Nível {i+1}: Class={p_class}, ID={p_rid}")
        
        # Procura badge em qualquer descendente deste pai
        for sub in parent.iter('node'):
            rid = sub.get('resource-id', '')
            text = sub.get('text', '')
            if any(k in rid.lower() for k in ['count', 'badge', 'unread']) or (text and text.isdigit() and len(text) <= 3):
                print(f"--- [!] POSSÍVEL BADGE DETECTADO: ID={rid}, Text='{text}', Desc='{sub.get('content-desc')}'")
        
        curr = parent

if __name__ == "__main__":
    debug()
