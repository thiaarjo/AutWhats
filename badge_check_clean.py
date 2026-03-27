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
                txt = node.get('text', '')
                if group_name == txt or group_name == txt.replace('\u200e', ''):
                    target_node = node
                    break
            
            if target_node is None:
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
                        if text and text.isdigit():
                            count = int(text)
                            self.log.info(f"Badge detectado em '{group_name}': {count} nova(s).")
                            return count
                curr = parent

            return 0
            
        except Exception as e:
            self.log.debug(f"Erro ao verificar badge XML de '{group_name}': {e}")
            return 0
