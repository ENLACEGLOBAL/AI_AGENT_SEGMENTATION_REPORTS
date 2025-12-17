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
    
    def generate_composite_dashboard_chart(self) -> str:
        dist = self.analytics.get_distribucion_riesgo()
        tipos = self.analytics.get_tipos_cruces()
        cat = self.analytics.get_distribucion_categorias()
        top = self.analytics.get_top_empresas(top_n=10)
        fig, axes = plt.subplots(2, 2, figsize=(12, 8))
        ax = axes[0][0]
        labels = ['Bajo (1-2)', 'Medio (3)', 'Alto (4-5)']
        values = [dist.get('bajo', 0), dist.get('medio', 0), dist.get('alto', 0)]
        colors = ['#2a9d8f', '#f77f00', '#e63946']
        ax.bar(labels, values, color=colors, edgecolor='white', linewidth=2)
        try:
            for container in ax.containers:
                ax.bar_label(container, fmt='%d', padding=3, fontsize=9)
        except Exception:
            pass
        ax.set_ylabel('Cantidad de Entidades', fontsize=12)
        ax.set_title('Distribución por Nivel de Riesgo', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
        ax = axes[0][1]
        labels = ['Cliente+Proveedor', 'Proveedor+Empleado', 'Cliente+Empleado', 'Triple Cruce']
        values = [
            tipos.get('cliente_proveedor', 0),
            tipos.get('proveedor_empleado', 0),
            tipos.get('cliente_empleado', 0),
            tipos.get('triple_cruce', 0)
        ]
        colors = ['#00b4d8', '#f77f00', '#415a77', '#e63946']
        wedges, texts, autotexts = ax.pie(values, labels=None, autopct='%1.1f%%', colors=colors, startangle=90, pctdistance=0.8, textprops={'fontsize': 10})
        ax.legend(wedges, labels, loc='upper right', fontsize=9, frameon=False)
        ax.set_title('Tipos de Cruces Detectados', fontsize=14, fontweight='bold')
        ax = axes[1][0]
        labels = [f'{k} Categorías' for k in sorted(cat.keys())]
        values = [cat[k] for k in sorted(cat.keys())]
        colors = ['#f77f00', '#e63946', '#00b4d8', '#2a9d8f']
        wedges, texts, autotexts = ax.pie(values, labels=None, autopct='%1.1f%%', colors=colors, startangle=90, pctdistance=0.8, textprops={'fontsize': 10})
        ax.legend(wedges, labels, loc='upper right', fontsize=9, frameon=False)
        ax.set_title('Distribución por Número de Categorías', fontsize=14, fontweight='bold')
        ax = axes[1][1]
        empresas = [item['empresa'] for item in top]
        cruces = [item['cruces'] for item in top]
        ax.barh(empresas, cruces, color='#1b263b')
        try:
            for container in ax.containers:
                ax.bar_label(container, fmt='%d', padding=3, fontsize=9)
        except Exception:
            pass
        ax.set_xlabel('Cantidad de Cruces', fontsize=12)
        ax.set_title('Top 10 Empresas por Cantidad de Cruces', fontsize=14, fontweight='bold')
        ax.invert_yaxis()
        ax.grid(axis='x', alpha=0.3)
        fig.tight_layout()
        try:
            fig.text(0.5, 0.01, 'Escala de riesgo: Bajo (1–2), Medio (3), Alto (4–5). Cruce = misma contraparte en múltiples categorías.', ha='center', fontsize=9)
        except Exception:
            pass
        return self._fig_to_base64(fig)
    
    def generate_cruces_heatmap_chart(self) -> str:
        df = getattr(self.analytics, 'df_cruces', None)
        if df is None or df.empty:
            df = self.analytics.procesar_datos()
        if df is None or df.empty:
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.text(0.5, 0.5, "Sin datos de cruces", ha='center', va='center', fontsize=14)
            ax.axis('off')
            return self._fig_to_base64(fig)
        def riesgo_label(row):
            r_cliente = row.get('Mayor_riesgo_clientes', 0)
            r_proveedor = row.get('Mayor_riesgo_proveedores', 0)
            r_empleado = 5 if str(row.get('Mayor_riesgo_empleados', '')).upper() == 'ALTO' else 0
            max_r = max(r_cliente, r_proveedor, r_empleado)
            if max_r >= 4:
                return 'ALTO'
            elif max_r == 3:
                return 'MEDIO'
            else:
                return 'BAJO'
        rows = ['2', '3']
        cols = ['BAJO', 'MEDIO', 'ALTO']
        matrix = [[0 for _ in cols] for _ in rows]
        for _, row in df.iterrows():
            cat = str(row.get('conteo_categorias', ''))
            lab = riesgo_label(row)
            if cat in rows and lab in cols:
                i = rows.index(cat)
                j = cols.index(lab)
                matrix[i][j] += 1
        import numpy as np
        data = np.array(matrix)
        fig, ax = plt.subplots(figsize=(8, 5))
        im = ax.imshow(data, cmap='Blues')
        ax.set_xticks(range(len(cols)))
        ax.set_yticks(range(len(rows)))
        ax.set_xticklabels(cols)
        ax.set_yticklabels([f'{r} Categorías' for r in rows])
        for i in range(data.shape[0]):
            for j in range(data.shape[1]):
                ax.text(j, i, str(data[i, j]), ha='center', va='center', color='black')
        ax.set_title('Mapa de Calor de Cruces (Categorías x Riesgo)', fontsize=14, fontweight='bold')
        fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
        fig.tight_layout()
        return self._fig_to_base64(fig)
    
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
