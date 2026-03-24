import json
import gzip

# ==========================================
# ⚙️ CONFIGURACIÓN
# ==========================================
JSON_GZ_FILE = 'analytics_42_20260313_113318.json.gz'  # Ajusta si es necesario

# Vamos a buscar a los 3 primeros de tu lista para no saturar la pantalla
IDS_A_BUSCAR = ['2296424352', '2024647576', '1034324524']


def buscar_en_profundidad(obj, target_id, path="root"):
    """Busca recursivamente un valor en cualquier parte del JSON y devuelve la ruta exacta."""
    resultados = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            # Si el valor coincide exactamente
            if str(v).strip() == target_id:
                resultados.append(f"{path} -> Llave: '{k}'")
            # Si es un string y el ID es parte del texto (ej. en transacciones_detalles)
            elif isinstance(v, str) and target_id in v:
                resultados.append(f"{path} -> Llave: '{k}' (Contiene el ID)")
            # Explorar más profundo
            resultados.extend(buscar_en_profundidad(v, target_id, f"{path}.{k}"))

    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if str(v).strip() == target_id:
                resultados.append(f"{path}[{i}]")
            elif isinstance(v, str) and target_id in v:
                resultados.append(f"{path}[{i}] (Contiene el ID)")
            if isinstance(v, (dict, list)):
                resultados.extend(buscar_en_profundidad(v, target_id, f"{path}[{i}]"))

    return resultados


def extraer_bloque_entidad(data, target_id):
    """Busca el bloque completo de la entidad en las tablas principales para inspeccionarlo."""
    listas_principales = ['tabla_detalles', 'entidades_sin_dd', 'transacciones_sin_dd', 'entidades_cruces']

    for nombre_lista in listas_principales:
        if nombre_lista in data.get('data', {}):
            for entidad in data['data'][nombre_lista]:
                # Revisar si el ID está en alguna llave principal de la entidad
                if isinstance(entidad, dict):
                    valores = [str(v).strip() for v in entidad.values()]
                    if target_id in valores:
                        return nombre_lista, entidad
    return None, None


def auditar_ids():
    print(f"🔄 Abriendo radiografía del JSON: {JSON_GZ_FILE}...\n")

    try:
        with gzip.open(JSON_GZ_FILE, 'rt', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error leyendo el archivo JSON.GZ: {e}")
        return

    for emp_id in IDS_A_BUSCAR:
        print("=" * 60)
        print(f"🔍 AUDITANDO ID: {emp_id}")
        print("=" * 60)

        # 1. Búsqueda profunda de rutas
        rutas = buscar_en_profundidad(data, emp_id)
        if rutas:
            print("📍 Dónde aparece este ID en la estructura del JSON:")
            for r in set(rutas):
                print(f"   - {r}")
        else:
            print("   ❌ El ID no aparece en NINGUNA parte del JSON bajo ninguna llave.")

        # 2. Extraer el bloque completo para ver cómo quedó estructurado
        nombre_lista, bloque = extraer_bloque_entidad(data, emp_id)
        if bloque:
            print(f"\n📦 Bloque completo encontrado en la lista '{nombre_lista}':")
            # Extraemos lo más relevante para no imprimir cientos de líneas
            resumen = {
                "id_contraparte": bloque.get("id_contraparte"),
                "empresa": bloque.get("empresa"),
                "conteo_categorias": bloque.get("conteo_categorias"),
                "empleado_count": bloque.get("empleado", {}).get("count") or bloque.get("empleado", {}).get("cantidad"),
                "cliente_count": bloque.get("cliente", {}).get("count") or bloque.get("cliente", {}).get("cantidad"),
                "proveedor_count": bloque.get("proveedor", {}).get("count") or bloque.get("proveedor", {}).get(
                    "cantidad"),
            }
            print(json.dumps(resumen, indent=4, ensure_ascii=False))

            if resumen["empleado_count"] in [0, None]:
                print("\n⚠️ ALERTA: ¡Lo encontramos! Pero el sistema dice que tiene 0 transacciones como empleado.")
                print(
                    "Esto significa que la agrupación (groupby) en Pandas lo procesó, pero no contó sus datos en la sección de empleados.")
        else:
            print(f"\n📦 No se encontró un bloque raíz para {emp_id} en las tablas principales.")


if __name__ == "__main__":
    auditar_ids()