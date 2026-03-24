import uiautomator2 as u2
import json

d = u2.connect('192.168.0.254:33091')

# Captura todos os layouts_principais das mensagens
messages = d(resourceId="com.whatsapp.w4b:id/main_layout")

results = []
for msg in messages:
    children = msg.child()
    child_info = []
    for child in children:
        info = child.info
        child_info.append({
            "id": info.get("resourceName", ""),
            "text": info.get("text", ""),
            "desc": info.get("contentDescription", "")
        })
    results.append(child_info)

with open('debug_media.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
    
print("Dump concluded.")
