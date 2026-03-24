import argparse
import json
import gzip
import io
import os
import sys

# Agregamos la raíz del proyecto al path
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ruta_raiz)

from sqlalchemy import text
from src.db.base import SourceSessionLocal
# Importa tu servicio de S3 (ajusta si el método para descargar se llama distinto)
from src.services.s3_service import s3_service
from src.services.pdf_risk_report_service_v2 import pdf_risk_report_service


def obtener_ultimo_json_empresa(empresa_id: int):
    """Consulta la base de datos para obtener la ruta S3 del último JSON de la empresa"""
    db = SourceSessionLocal()
    try:
        # Ajusta el nombre de la tabla y columnas según tu modelo real (ej. analytics_reports, generated_reports)
        query = text("""
            SELECT json_path, data_json 
            FROM cruces_entidades_analytics 
            WHERE empresa_id = :eid 
            ORDER BY created_at DESC LIMIT 1
        """)
        resultado = db.execute(query, {"eid": empresa_id}).fetchone()
        return resultado
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Regenerar PDF de Cruces desde S3")

    # 🟢 AHORA PEDIMOS LA EMPRESA
    parser.add_argument("--empresa", type=int, required=True, help="ID de la empresa")

    parser.add_argument("--fecha_desde", type=str, default="")
    parser.add_argument("--fecha_hasta", type=str, default="")
    parser.add_argument("--monto_min", type=str, default="")
    parser.add_argument('--monto_min_tx', type=float, default=0.0,
                        help='Monto mínimo por transacción individual (filtro en cascada)')
    parser.add_argument("--sin_dd", action="store_true")
    parser.add_argument("--con_cruces", action="store_true")
    parser.add_argument('--email_to', type=str, default=None, help='Correo electrónico para enviar el PDF generado')
    parser.add_argument('--oficial', type=str, default=None, help='Observaciones del Oficial de Cumplimiento')
    args = parser.parse_args()

    print(f"🔍 Buscando el último análisis de la empresa {args.empresa} en la BD...")
    registro = obtener_ultimo_json_empresa(args.empresa)

    if not registro:
        print(f"❌ No se encontró ningún análisis previo para la empresa {args.empresa}.")
        sys.exit(1)

    file_path, data_json_local = registro

    analytics_data = None

    # Si el JSON era pequeño y se guardó directo en la BD
    if data_json_local and data_json_local != "STORED_IN_DB":
        print("📥 Cargando JSON directamente desde la Base de Datos...")
        analytics_data = json.loads(data_json_local)

    # Si es un archivo grande guardado en S3 (.gz)
    elif file_path and file_path.endswith('.gz'):
        print(f"☁️ Descargando JSON comprimido desde S3 ({file_path})...")
        try:
            # 🟢 Asegúrate de que tu s3_service tenga un método para descargar o leer bytes
            # Si se llama distinto, ajústalo aquí. Asumo que devuelve los bytes del archivo.
            file_bytes = s3_service.download_file_bytes(file_path)

            with gzip.GzipFile(fileobj=io.BytesIO(file_bytes), mode='rb') as f:
                analytics_data = json.loads(f.read().decode('utf-8'))
        except Exception as e:
            print(f"❌ Error descargando/descomprimiendo desde S3: {e}")
            sys.exit(1)
    else:
        print("❌ El registro no tiene un JSON válido ni una ruta a S3.")
        sys.exit(1)

    # Extraer payload base
    if isinstance(analytics_data, dict) and 'data' in analytics_data:
        analytics_data = analytics_data['data']

    filtros = {
        "fecha_desde": args.fecha_desde,
        "fecha_hasta": args.fecha_hasta,
        "monto_min": args.monto_min,
        "monto_min_tx": args.monto_min_tx,

        "sin_dd": "true" if args.sin_dd else "false",
        "con_cruces": "true" if args.con_cruces else "false"
    }

    print("\n📊 Aplicando filtros:")
    print(json.dumps(filtros, indent=4))
    print("\n🚀 Generando PDF al vuelo...")

    resultado = pdf_risk_report_service.generate_pdf_report(
        analytics_data=analytics_data,
        tipo_contraparte="Universo General Filtrado",
        filtros_pdf=filtros,
        email_to=args.email_to,
        oficial_conclusion=args.oficial
    )

    if resultado.get("status") == "success":
        print("\n✅ ¡ÉXITO! PDF Generado y subido a S3.")
        print(f"📄 S3 Key: {resultado.get('file')}")
    else:
        print("\n❌ FALLÓ LA GENERACIÓN DEL PDF:")
        print(resultado.get("message"))


if __name__ == "__main__":
    main()
