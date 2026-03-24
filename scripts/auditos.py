import os
import sys
import pandas as pd
from sqlalchemy import text

# Apuntamos a la raíz del proyecto
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ruta_raiz)

# Importamos tu conexión real
from src.db.base import SourceSessionLocal


def auditar_fuga_datos(empresa_id):
    print(f"🔍 Iniciando Auditoría Forense para Empresa {empresa_id}...\n")
    db = SourceSessionLocal()
    try:
        tablas = ['clientes', 'proveedores', 'empleados']
        total_sql = 0
        total_pandas = 0

        for tabla in tablas:
            # 1. LA VERDAD ABSOLUTA (SQL Directo)
            query_count = text(f"SELECT COUNT(*) FROM {tabla} WHERE id_empresa = :eid")
            count_sql = db.execute(query_count, {"eid": empresa_id}).scalar()
            total_sql += count_sql

            # 2. LO QUE VE PYTHON (Pandas crudo, sin filtros de tu servicio)
            query_df = text(f"SELECT * FROM {tabla} WHERE id_empresa = :eid")
            df = pd.read_sql(query_df, db.bind, params={"eid": empresa_id})
            count_pandas = len(df)
            total_pandas += count_pandas

            print(f"📂 TABLA: {tabla.upper()}")
            print(f"   ▶ Motor SQL responde: {count_sql:,}".replace(',', '.'))
            print(f"   ▶ Pandas descarga:    {count_pandas:,}".replace(',', '.'))

            if count_sql != count_pandas:
                perdida = count_sql - count_pandas
                print(f"   ⚠️ ALERTA: Fuga de {perdida:,} registros en el driver.\n".replace(',', '.'))
            else:
                print("   ✅ Coinciden perfectamente al descargar.\n")

        print("=" * 45)
        print(f"📊 TOTAL SQL (Tu DBeaver): {total_sql:,}".replace(',', '.'))
        print(f"📊 TOTAL PANDAS (Memoria): {total_pandas:,}".replace(',', '.'))
        print(f"🚨 DIFERENCIA FINAL:       {total_sql - total_pandas:,}".replace(',', '.'))
        print("=" * 45)

    except Exception as e:
        print(f"❌ Error en la auditoría: {e}")
    finally:
        db.close()


if __name__ == '__main__':
    # Pon aquí el ID de la empresa que estás validando (ej. 42)
    auditar_fuga_datos(42)