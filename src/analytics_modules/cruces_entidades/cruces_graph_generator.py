# src/analytics_modules/cruces_entidades/cruces_graph_generator.py
"""
Generador de gráficos para análisis de cruces
"""
import matplotlib.pyplot as plt
import io
import base64
from typing import Dict, Any


class CrucesGraphGenerator:
    """Genera gráficos para visualización de cruces"""
    
    def __init__(self, analytics: 'CrucesAnalytics'):
        self.analytics = analytics
    
    def _fig_to_base64(self, fig) -> str:
        """Convierte figura matplotlib a base64"""
        buffer = io.BytesIO()
        fig.savefig(buffer, format='png', bbox_inches='tight', dpi=150)
        plt.close(fig)
        buffer.seek(0)
        return f"data:image/png;base64,{base64.b64encode(buffer.read()).decode('utf-8')}"
    
    def generate_risk_distribution_chart(self) -> str:
        """Gráfico de distribución de riesgo"""
        dist = self.analytics.get_distribucion_riesgo()
        
        fig, ax = plt.subplots(figsize=(8, 6))
        labels = ['Bajo (1-2)', 'Medio (3)', 'Alto (4-5)']
        values = [dist['bajo'], dist['medio'], dist['alto']]
        colors = ['#2a9d8f', '#f77f00', '#e63946']
        
        ax.bar(labels, values, color=colors, edgecolor='white', linewidth=2)
        ax.set_ylabel('Cantidad de Entidades', fontsize=12)
        ax.set_title('Distribución por Nivel de Riesgo', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
        
        return self._fig_to_base64(fig)
    
    def generate_cross_types_chart(self) -> str:
        """Gráfico de tipos de cruces"""
        tipos = self.analytics.get_tipos_cruces()
        
        fig, ax = plt.subplots(figsize=(8, 6))
        labels = ['Cliente +\nProveedor', 'Proveedor +\nEmpleado', 'Cliente +\nEmpleado', 'Triple\nCruce']
        values = [
            tipos.get('cliente_proveedor', 0),
            tipos.get('proveedor_empleado', 0),
            tipos.get('cliente_empleado', 0),
            tipos.get('triple_cruce', 0)
        ]
        colors = ['#00b4d8', '#f77f00', '#415a77', '#e63946']
        
        ax.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, 
               startangle=90, textprops={'fontsize': 11})
        ax.set_title('Tipos de Cruces Detectados', fontsize=14, fontweight='bold')
        
        return self._fig_to_base64(fig)
    
    def generate_category_distribution_chart(self) -> str:
        """Gráfico de distribución por categorías"""
        dist = self.analytics.get_distribucion_categorias()
        
        fig, ax = plt.subplots(figsize=(8, 6))
        labels = [f'{k} Categorías' for k in sorted(dist.keys())]
        values = [dist[k] for k in sorted(dist.keys())]
        colors = ['#f77f00', '#e63946']
        
        ax.pie(values, labels=labels, autopct='%1.1f%%', colors=colors,
               startangle=90, textprops={'fontsize': 12})
        ax.set_title('Distribución por Número de Categorías', fontsize=14, fontweight='bold')
        
        return self._fig_to_base64(fig)
    
    def generate_top_empresas_chart(self) -> str:
        """Gráfico de top empresas"""
        top = self.analytics.get_top_empresas(top_n=10)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        empresas = [item['empresa'] for item in top]
        cruces = [item['cruces'] for item in top]
        
        ax.barh(empresas, cruces, color='#1b263b')
        ax.set_xlabel('Cantidad de Cruces', fontsize=12)
        ax.set_title('Top 10 Empresas por Cantidad de Cruces', fontsize=14, fontweight='bold')
        ax.invert_yaxis()
        ax.grid(axis='x', alpha=0.3)
        
        return self._fig_to_base64(fig)
    
    def generate_all_charts(self) -> Dict[str, str]:
        """Genera todos los gráficos y devuelve dict con base64"""
        return {
            "risk_distribution": self.generate_risk_distribution_chart(),
            "cross_types": self.generate_cross_types_chart(),
            "category_distribution": self.generate_category_distribution_chart(),
            "top_empresas": self.generate_top_empresas_chart()
        }