from sqlalchemy import Column, Integer, String, Float, Date
from src.db.base import Base

class Empleado(Base):
    __tablename__ = "empleados"

    id = Column(Integer, primary_key=True, index=True)
    id_empresa = Column(Integer, index=True)
    id_empleado = Column(String(50), index=True)  # id_contraparte
    valor = Column(Float)   
    conteo_alto = Column(String(50))
    #departamento = Column(String(100))
    #lat = Column(Float)
    #lon = Column(Float)
    #ciiu = Column(String(20))
    #actividad = Column(String(200))
    fecha_transaccion = Column(Date)
