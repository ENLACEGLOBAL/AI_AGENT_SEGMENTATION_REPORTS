import sys
import os
import argparse
import uvicorn
from src.db.base import TargetSessionLocal
from src.services.report_orchestrator import report_orchestrator

# Add src to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

def run_api(host="0.0.0.0", port=8000):
    print("🚀 Iniciando API de Reportes de Riesgo...")
    print(f"📝 Documentación disponible en: http://{host}:{port}/docs")
    try:
        from src.rta_api.main import app
        uvicorn.run(app, host=host, port=port)
    except ImportError as e:
        print(f"❌ Error importando la aplicación API: {e}")
        print("Asegúrate de que src.rta_api.main existe y las dependencias están instaladas.")
    except Exception as e:
        print(f"❌ Error iniciando el servidor: {e}")

def generate_pdf(empresa_id, tipo_contraparte):
    from datetime import datetime
    import os
    
    # Directorio de salida local
    output_dir = os.path.join("data_provisional", "reports")
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"Reporte_Riesgo_{empresa_id}_{timestamp}.pdf")

    print(f"📄 Generando PDF para empresa {empresa_id} ({tipo_contraparte})...")
    db = TargetSessionLocal()
    try:
        # report_orchestrator.generate_pdf espera: (empresa_id: int, tipo_contraparte: str, db: Session, fecha: str, monto_min: float, output_path: str)
        result = report_orchestrator.generate_pdf(empresa_id, tipo_contraparte, db, output_path=output_path)
        
        # Verificar el resultado
        pdf_res = result.get("pdf", {})
        analytics_res = result.get("analytics", {})
        
        if pdf_res.get("status") == "success":
            print(f"✅ PDF generado exitosamente.")
            print(f"📂 Archivo local: {pdf_res.get('local_file')}")
            if pdf_res.get('file'):
                 print(f"☁️ S3 Key: {pdf_res.get('file')}")
        else:
            print(f"❌ Error generando PDF.")
            if analytics_res.get("status") != "success":
                print(f"   Error en analítica: {analytics_res.get('message')}")
            if pdf_res.get("status") == "error":
                 print(f"   Error en generación PDF: {pdf_res.get('message')}")
            print(f"   Detalles completos: {result}")

    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Herramienta CLI para Reportes de Riesgo")
    subparsers = parser.add_subparsers(dest="command", help="Comando a ejecutar")

    # Command: api
    api_parser = subparsers.add_parser("api", help="Ejecutar servidor API")
    api_parser.add_argument("--host", default="0.0.0.0", help="Host (default: 0.0.0.0)")
    api_parser.add_argument("--port", type=int, default=8000, help="Puerto (default: 8000)")

    # Command: pdf
    pdf_parser = subparsers.add_parser("pdf", help="Generar reporte PDF")
    pdf_parser.add_argument("--empresa-id", type=int, required=True, help="ID de la empresa")
    pdf_parser.add_argument("--tipo", default="cliente", help="Tipo de contraparte (cliente, proveedor)")

    args = parser.parse_args()

    if args.command == "pdf":
        generate_pdf(args.empresa_id, args.tipo)
    elif args.command == "api":
        run_api(args.host, args.port)
    else:
        # Default behavior: Run API if no args provided (backward compatibility)
        if len(sys.argv) == 1:
            run_api()
        else:
            parser.print_help()
