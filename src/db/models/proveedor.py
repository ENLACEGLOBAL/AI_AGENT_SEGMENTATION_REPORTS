from sqlalchemy import Column, Integer, String, Float, Date
from src.db.base import Base

class Proveedor(Base):
    __tablename__ = "proveedores"

    id = Column(Integer, primary_key=True, index=True)
    id_empresa = Column(Integer, index=True)
    no_documento_de_identidad = Column(String(50), index=True)  # id_contraparte
    valor_transaccion = Column(Float)
    orden_clasificacion_del_riesgo = Column(String(50))
    departamento = Column(String(100))
    #lat = Column(Float)
    #lon = Column(Float)
    #ciiu = Column(String(20))
    #actividad = Column(String(200))
    fecha_transaccion = Column(Date)
