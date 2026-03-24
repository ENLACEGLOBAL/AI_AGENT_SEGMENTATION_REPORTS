import pandas as pd
import json
import gzip
import re

# ==========================================
# ⚙️ CONFIGURACIÓN (Ajusta los nombres de tus archivos)
# ==========================================
CSV_FILE = 'datos_hornitos_enero_2026.csv'  # Reemplaza con el nombre exacto de tu CSV
JSON_GZ_FILE = 'analytics_42_20260313_113318.json.gz'


def normalize_id(v):
    """La misma función de tu CrucesAnalyticsService para asegurar igualdad"""
    if pd.isna(v) or v is None:
        return ""
    s = str(v).strip().upper()
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s.lstrip('0')


def rastrear_diferencias():
    print(f"🔄 Cargando datos del CSV: {CSV_FILE}...")

    # 1. Cargar y normalizar IDs del CSV
    try:
        df_sql = pd.read_csv(CSV_FILE)
        # Asumiendo que la columna se llama 'id_empleado' basado en tu query SQL
        df_sql['id_normalizado'] = df_sql['id_empleado'].apply(normalize_id)

        # Filtramos los que queden vacíos después de normalizar
        df_sql_validos = df_sql[df_sql['id_normalizado'] != ""]
        ids_sql_normalizados = set(df_sql_validos['id_normalizado'])

        print(f"   ✅ Total en CSV: {len(df_sql)}")
        print(f"   ✅ Total en CSV (IDs normalizados únicos): {len(ids_sql_normalizados)}")

        # Verificamos si hubo deduplicación natural por la normalización
        if len(df_sql) > len(ids_sql_normalizados):
            print(
                f"   ⚠️ Nota: {len(df_sql) - len(ids_sql_normalizados)} registros se fusionaron al normalizar los IDs.")

    except Exception as e:
        print(f"❌ Error leyendo el CSV: {e}")
        return

    print(f"\n🔄 Cargando datos del JSON comprimido: {JSON_GZ_FILE}...")

    # 2. Cargar y extraer IDs del JSON.GZ
    ids_json = set()
    try:
        with gzip.open(JSON_GZ_FILE, 'rt', encoding='utf-8') as f:
            data = json.load(f)

        payload = data.get('data', {})

        # Buscamos en las dos listas principales donde pueden estar las contrapartes
        listas_a_revisar = []
        if 'tabla_detalles' in payload:
            listas_a_revisar.extend(payload['tabla_detalles'])
        if 'entidades_sin_dd' in payload:
            listas_a_revisar.extend(payload['entidades_sin_dd'])

        for entidad in listas_a_revisar:
            # Validamos que la entidad realmente tenga datos/transacciones como empleado
            empleado_data = entidad.get('empleado', {})
            conteo = empleado_data.get('count', 0) or empleado_data.get('cantidad', 0)

            if conteo > 0:
                id_contraparte = str(entidad.get('id_contraparte', '')).strip()
                if id_contraparte:
                    ids_json.add(id_contraparte)

        print(f"   ✅ Total de Empleados extraídos del JSON: {len(ids_json)}")

    except Exception as e:
        print(f"❌ Error leyendo el archivo JSON.GZ: {e}")
        return

    # 3. Comparación Final
    print("\n" + "=" * 50)
    print("📊 RESULTADOS DE LA COMPARACIÓN")
    print("=" * 50)

    faltantes_en_json = ids_sql_normalizados - ids_json
    extras_en_json = ids_json - ids_sql_normalizados

    if not faltantes_en_json and not extras_en_json:
        print(
            "✅ ¡Los datos coinciden perfectamente! La discrepancia de 4 registros se debió a la deduplicación al normalizar los IDs (ej: '0123' y '123').")
    else:
        if faltantes_en_json:
            print(f"❌ {len(faltantes_en_json)} IDs están en el CSV (SQL) pero NO llegaron al JSON:")
            for emp_id in list(faltantes_en_json)[:20]:  # Mostramos hasta 20
                # Buscar el registro original en el DF para darte más contexto
                info_original = df_sql[df_sql['id_normalizado'] == emp_id].iloc[0]
                print(
                    f"   -> ID Normalizado: {emp_id} | Original: {info_original.get('id_empleado', 'N/A')} | Nombre: {info_original.get('empleado', 'N/A')}")

        if extras_en_json:
            print(f"\n❓ {len(extras_en_json)} IDs están en el JSON pero NO en tu CSV SQL:")
            for emp_id in list(extras_en_json)[:20]:
                print(f"   -> ID JSON: {emp_id}")


if __name__ == "__main__":
    rastrear_diferencias()