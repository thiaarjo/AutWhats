import uiautomator2 as u2
d = u2.connect('192.168.0.254:33091')
for el in d(className='android.view.ViewGroup'):
    desc = el.info.get('contentDescription', '') or ''
    if 'voz' in desc.lower() or 'audio' in desc.lower() or 'única' in desc.lower() or 'foto' in desc.lower():
        print(f'MEDIA: {desc}')

