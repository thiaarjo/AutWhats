import uiautomator2 as u2
import json

d = u2.connect('192.168.0.254:33325')
xml = d.dump_hierarchy()
with open('chat_list_dump.xml', 'w', encoding='utf-8') as f:
    f.write(xml)

# Procura por qualquer texto numérico que pareça um badge
for el in d(className='android.widget.TextView'):
    t = el.info.get('text', '')
    if t.isdigit() and len(t) <= 3:
        print(f'Possible Badge: {t} | Bounds: {el.info.get('bounds')} | ID: {el.info.get('resourceName')}')

