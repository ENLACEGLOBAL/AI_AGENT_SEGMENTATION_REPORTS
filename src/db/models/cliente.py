from sqlalchemy import Column, Integer, String, Float, Date
from src.db.base import Base

class Cliente(Base):
    __tablename__ = "clientes"

    id = Column(Integer, primary_key=True, index=True)
    id_empresa = Column(Integer, index=True)
    num_id = Column(String(50), index=True)  # id_contraparte
    valor_transaccion = Column(Float)
    orden_clasificacion_del_riesgo = Column(String(50))
    departamento = Column(String(100))
    lat = Column(Float)
    lon = Column(Float)
    ciiu = Column(String(20))
    actividad = Column(String(200))
    fecha_transaccion = Column(Date)
