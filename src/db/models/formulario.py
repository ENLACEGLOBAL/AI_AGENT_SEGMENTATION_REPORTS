# src/db/models/formulario.py
"""
Modelo para formularios de debida diligencia
"""
from sqlalchemy import Column, Integer, String, Date
from src.db.base import Base


class Formulario(Base):
    """
    Vista que expone formularios no anulados.
    """
    __tablename__ = "forms_existentes"
    __table_args__ = {"extend_existing": True}

    # PK lógica (proviene de la tabla formularios)
    id_formulario = Column(Integer, primary_key=True)

    id_empresa = Column(Integer, index=True)
    fecha_registro = Column(Date)
    nombre_completo = Column(String(255))
    numero_id = Column(String(50), index=True)
    contraparte = Column(String(100))

    def __repr__(self):
        return (
            f"<Formulario(id={self.id_formulario}, empresa={self.id_empresa}, numero_id={self.numero_id})>"
        )