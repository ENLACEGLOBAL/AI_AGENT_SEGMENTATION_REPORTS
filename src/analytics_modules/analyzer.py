# src/analytics_modules/analyzer.py
from datetime import date
from sqlalchemy.orm import Session

from src.db.base import SessionLocal
from src.models.clientes import Cliente
from src.models.proveedores import Proveedor


class Analyzer:

    def __init__(self):
        self.db: Session = SessionLocal()

    # ---------------------------------------------------------
    # Utilidad privada: filtrar registros por empresa + fechas
    # ---------------------------------------------------------
    def _filtrar(self, modelo, id_empresa: int, fecha_inicio, fecha_fin):
        query = self.db.query(modelo).filter(modelo.id_empresa == id_empresa)

        if fecha_inicio:
            query = query.filter(modelo.fecha_transaccion >= fecha_inicio)
        if fecha_fin:
            query = query.filter(modelo.fecha_transaccion <= fecha_fin)

        return query.all()

    # ---------------------------------------------------------
    # Método principal: ANALIZADOR UNIVERSAL
    # ---------------------------------------------------------
    def analizar(self, id_empresa: int,
                 tipo_contraparte: str | None = None,
                 fecha_inicio: date | None = None,
                 fecha_fin: date | None = None):

        clientes = []
        proveedores = []

        if tipo_contraparte in ("cliente", None):
            clientes = self._filtrar(Cliente, id_empresa, fecha_inicio, fecha_fin)

        if tipo_contraparte in ("proveedor", None):
            proveedores = self._filtrar(Proveedor, id_empresa, fecha_inicio, fecha_fin)

        return {
            "empresa_id": id_empresa,
            "filtros": {
                "tipo_contraparte": tipo_contraparte,
                "fecha_inicio": str(fecha_inicio) if fecha_inicio else None,
                "fecha_fin": str(fecha_fin) if fecha_fin else None
            },
            "registros": {
                "clientes": clientes,
                "proveedores": proveedores
            }
        }
