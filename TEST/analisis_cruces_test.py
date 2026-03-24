import pandas as pd
import re

# Pon aquí el nombre exacto de tu archivo extraído de la BD
ARCHIVO = 'datos_hornitos_enero_2026.csv'

def normalize_id(v):
    if pd.isna(v) or v is None: return ""
    return re.sub(r'[^A-Z0-9]', '', str(v).strip().upper()).lstrip('0')

print("🔍 Buscando fusiones por normalización...")
# Cambia a pd.read_excel si es un .xlsx
df = pd.read_csv(ARCHIVO)

# Aplicamos tu misma lógica de limpieza
df['id_normalizado'] = df['id_empleado'].apply(normalize_id)

# Filtramos los que comparten el mismo ID después de limpiarlos
duplicados = df[df.duplicated(subset=['id_normalizado'], keep=False)]

print(f"\n✅ Se encontraron {len(duplicados)} registros que se fusionan.")
print("-" * 60)
print(duplicados[['id_empleado', 'id_normalizado', 'empleado']].sort_values('id_normalizado').to_string(index=False))

print("\n🔍 Buscando registros con IDs vacíos o nulos...")
# Filtramos los que, después de limpiar, quedaron completamente vacíos
vacios = df[df['id_normalizado'] == ""]

print(f"✅ Se encontraron {len(vacios)} registros sin ID válido.")
print("-" * 60)
print(vacios[['tipo_contraparte', 'id_empleado', 'empleado']].to_string(index=False))