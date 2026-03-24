import json
import gzip

JSON_GZ_FILE = 'analytics_42_20260313_113318.json.gz'
ID_FABIAN = '2296424352'

with gzip.open(JSON_GZ_FILE, 'rt', encoding='utf-8') as f:
    data = json.load(f)

print(f"🔍 BUSCANDO A FABIAN (ID: {ID_FABIAN})")
print("=" * 50)

for lista_nombre in ['faltantes_dd', 'entidades_sin_dd']:
    # CORRECCIÓN: Buscamos directamente en la raíz del JSON
    lista = data.get(lista_nombre, [])

    for entidad in lista:
        if str(entidad.get('id_contraparte')).strip() == ID_FABIAN:
            print(f"\n📦 ENCONTRADO EN LA LISTA: {lista_nombre}")
            print(json.dumps(entidad, indent=4, ensure_ascii=False))