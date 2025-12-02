from sqlalchemy import Column, Integer, String, Float, Date, DateTime
from src.db.base import Base

class HistoricoPaises(Base):
    __tablename__ = "historico_paises_coop_nocop_par"

    pais = Column(String(100), primary_key=True, index=True)
    clasificacion = Column(String(100))  # COOPERANTE, NO COOPERANTE
    riesgo = Column(String(50))  # Alto, Medio, Bajo
    calificacion = Column(Integer)
    fecha_pais = Column(String(50))  # Changed from fecha_actualizacion

class SegmentacionJurisdicciones(Base):
    __tablename__ = "segmentacion_jurisdicciones"

    id = Column(Integer, primary_key=True, index=True)
    departamento = Column(String(100)) 
    municipio = Column(String(100))
    divipola = Column(String(50))
    categoria = Column(String(50))
    valor_riesgo_jurisdicciones = Column(Float)  # Changed from valor

class AuxiliarCiiu(Base):
    __tablename__ = "auxiliar_ciiu_ajustado"

    ciiu = Column(String(10), primary_key=True, index=True)
    descripcion = Column(String(500))
    riesgo = Column(String(50))  # Changed from categoria
    valor_riesgo = Column(Float)  # Changed from valor_riesgo
