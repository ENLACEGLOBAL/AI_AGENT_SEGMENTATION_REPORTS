# src/analytics_modules/analyzer.py
from datetime import date
from sqlalchemy.orm import Session
from src.db.base import SessionLocal
from src.models.clientes import Cliente
from src.models.proveedores import Proveedor

def analizar_sector_ubicacion(
    id_empresa: int,
    tipo_contraparte: str | None = None,     # "cliente", "proveedor" o None
    fecha_inicio: date | None = None,
    fecha_fin: date | None = None
):
    db: Session = SessionLocal()

    # --------------------------------------
    # FILTRAR CLIENTES
    # --------------------------------------
    query_clientes = db.query(Cliente).filter(Cliente.id_empresa == id_empresa)

    if tipo_contraparte in ("cliente", None):
        if fecha_inicio:
            query_clientes = query_clientes.filter(Cliente.fecha_transaccion >= fecha_inicio)
        if fecha_fin:
            query_clientes = query_clientes.filter(Cliente.fecha_transaccion <= fecha_fin)

        clientes = query_clientes.all()
    else:
        clientes = []

    # --------------------------------------
    # FILTRAR PROVEEDORES
    # --------------------------------------
    query_proveedores = db.query(Proveedor).filter(Proveedor.id_empresa == id_empresa)

    if tipo_contraparte in ("proveedor", None):
        if fecha_inicio:
            query_proveedores = query_proveedores.filter(Proveedor.fecha_transaccion >= fecha_inicio)
        if fecha_fin:
            query_proveedores = query_proveedores.filter(Proveedor.fecha_transaccion <= fecha_fin)

        proveedores = query_proveedores.all()
    else:
        proveedores = []

    # --------------------------------------
    # RESULTADO ANALÍTICO
    # --------------------------------------
    return {
        "empresa_id": id_empresa,
        "total_clientes": len(clientes),
        "total_proveedores": len(proveedores),
        "total_registros_filtrados": len(clientes) + len(proveedores),
    }
