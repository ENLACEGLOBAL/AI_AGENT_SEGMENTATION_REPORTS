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
        
        # Estilo limpio y profesional (emulando seaborn-whitegrid manualmente para no depender de estilos instalados)
        plt.rcParams.update({
            'axes.facecolor': 'white',
            'axes.edgecolor': '#eaeaea',
            'axes.grid': True,
            'grid.color': '#f0f0f0',
            'grid.alpha': 0.6,
            'font.family': 'sans-serif',
            'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
            'text.color': '#333333',
            'axes.labelcolor': '#555555',
            'xtick.color': '#555555',
            'ytick.color': '#555555'
        })
        
        # Diseño 1x3 horizontal para ocupar menos altura y encajar mejor en el reporte compacto
        fig, axes = plt.subplots(1, 3, figsize=(15, 4))
        
        # 1. Distribución de Riesgo (Barra Simple)
        ax = axes[0]
        labels = ['Bajo', 'Medio', 'Alto']
        values = [dist.get('bajo', 0), dist.get('medio', 0), dist.get('alto', 0)]
        colors = ['#2a9d8f', '#f4a261', '#e76f51'] # Colores pastel/modernos
        
        bars = ax.bar(labels, values, color=colors, width=0.6)
        
        # Etiquetas limpias sobre barras
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height + (max(values)*0.02),
                        f'{int(height)}',
                        ha='center', va='bottom', fontsize=10, fontweight='bold', color='#333333')
                        
        ax.set_title('Nivel de Riesgo', fontsize=12, fontweight='bold', pad=15)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.set_yticks([]) # Quitar eje Y para limpieza
        
        # 2. Tipos de Cruce (Donut Chart moderno)
        ax = axes[1]
        labels_map = {
            'cliente_proveedor': 'Cli+Prov',
            'proveedor_empleado': 'Prov+Emp',
            'cliente_empleado': 'Cli+Emp',
            'triple_cruce': 'Triple'
        }
        labels_pie = [labels_map.get(k, k) for k in tipos.keys()]
        values_pie = list(tipos.values())
        colors_pie = ['#264653', '#2a9d8f', '#e9c46a', '#f4a261']
        
        if sum(values_pie) > 0:
            wedges, texts, autotexts = ax.pie(values_pie, labels=labels_pie, autopct='%1.0f%%', 
                                            colors=colors_pie, startangle=90, pctdistance=0.85,
                                            wedgeprops=dict(width=0.4, edgecolor='white'))
            plt.setp(autotexts, size=9, weight="bold", color="white")
            plt.setp(texts, size=9)
        else:
            ax.text(0.5, 0.5, "Sin datos", ha='center', va='center', color='#999999')
            
        ax.set_title('Tipos de Cruce', fontsize=12, fontweight='bold', pad=15)

        # 3. Distribución Categorías (Donut Chart simple)
        ax = axes[2]
        labels_cat = [f'{k} Cat.' for k in sorted(cat.keys())]
        values_cat = [cat[k] for k in sorted(cat.keys())]
        colors_cat = ['#e9c46a', '#e76f51']
        
        if sum(values_cat) > 0:
            wedges, texts, autotexts = ax.pie(values_cat, labels=labels_cat, autopct='%1.0f%%', 
                                            colors=colors_cat, startangle=90, pctdistance=0.85,
                                            wedgeprops=dict(width=0.4, edgecolor='white'))
            plt.setp(autotexts, size=9, weight="bold", color="white")
            plt.setp(texts, size=9)
        else:
            ax.text(0.5, 0.5, "Sin datos", ha='center', va='center', color='#999999')
            
        ax.set_title('Categorías Involucradas', fontsize=12, fontweight='bold', pad=15)
        
        plt.tight_layout()
        return self._fig_to_base64(fig)
    
    def generate_cruces_heatmap_chart(self) -> str:
        """
        Genera un Dashboard Moderno (Light Mode).
        Incluye:
        1. Indicadores circulares de Riesgo (Bajo, Medio, Alto).
        2. Barras de progreso para Tipos de Cruce.
        """
        import matplotlib.pyplot as plt
        import numpy as np
        from matplotlib.patches import Wedge, FancyBboxPatch

        # 1. Obtener Datos
        dist = self.analytics.get_distribucion_riesgo()
        tipos = self.analytics.get_tipos_cruces()
        
        total_riesgo = sum(dist.values()) if dist else 0
        
        # Colores "Light Mode / Corporate Clean"
        bg_color = '#FFFFFF'
        text_color = '#333333'
        subtext_color = '#666666'
        
        colors_risk = {
            'bajo': '#2a9d8f',   # Teal
            'medio': '#f4a261',  # Orange
            'alto': '#e76f51'    # Red
        }
        
        # Configurar Figura (Ancha y compacta)
        fig = plt.figure(figsize=(12, 5), facecolor=bg_color)
        
        # Grid layout: 2 columnas principales (Riesgo | Tipos)
        gs = fig.add_gridspec(1, 2, width_ratios=[1.2, 1], wspace=0.1)
        
        # --- SECCIÓN 1: RIESGO (3 Circular Gauges) ---
        ax1 = fig.add_subplot(gs[0])
        ax1.set_facecolor(bg_color)
        ax1.axis('off')
        
        # Título Sección
        ax1.text(0.5, 0.9, "DISTRIBUCIÓN DE RIESGO", ha='center', va='center', 
                 color=text_color, fontsize=12, fontweight='bold')
        
        # Dibujar 3 Gauges
        riesgos = ['bajo', 'medio', 'alto']
        labels_riesgo = ['BAJO', 'MEDIO', 'ALTO']
        centers = [0.2, 0.5, 0.8] # Posiciones X relativas
        
        for i, r_key in enumerate(riesgos):
            val = dist.get(r_key, 0)
            pct = (val / total_riesgo * 100) if total_riesgo > 0 else 0
            color = colors_risk[r_key]
            cx = centers[i]
            cy = 0.5
            radius = 0.12
            
            # Fondo del anillo (gris muy claro)
            wedge_bg = Wedge((cx, cy), radius, width=0.02, theta1=0, theta2=360, color='#E0E0E0')
            ax1.add_patch(wedge_bg)
            
            # Anillo de progreso
            theta_end = 360 * (pct / 100)
            wedge_val = Wedge((cx, cy), radius, width=0.02, theta1=90, theta2=90-theta_end, color=color)
            ax1.add_patch(wedge_val)
            
            # Texto Porcentaje (Centro)
            ax1.text(cx, cy, f"{int(pct)}%", ha='center', va='center', 
                     color=text_color, fontsize=14, fontweight='bold')
            
            # Texto Etiqueta (Debajo)
            ax1.text(cx, cy - 0.18, labels_riesgo[i], ha='center', va='center', 
                     color=color, fontsize=9, fontweight='bold')
            
            # Texto Cantidad (Más abajo)
            ax1.text(cx, cy - 0.23, f"{val} Entidades", ha='center', va='center', 
                     color=subtext_color, fontsize=8)

        # --- SECCIÓN 2: TIPOLOGÍA (Progress Bars) ---
        ax2 = fig.add_subplot(gs[1])
        ax2.set_facecolor(bg_color)
        ax2.axis('off')
        
        # Título Sección
        ax2.text(0.5, 0.9, "TIPOLOGÍA DE CRUCES", ha='center', va='center', 
                 color=text_color, fontsize=12, fontweight='bold')
        
        # Datos Tipos
        labels_map = {
            'cliente_proveedor': 'Cliente + Proveedor',
            'proveedor_empleado': 'Proveedor + Empleado',
            'cliente_empleado': 'Cliente + Empleado',
            'triple_cruce': 'Triple Incidencia'
        }
        
        items = []
        for k, label in labels_map.items():
            val = tipos.get(k, 0)
            items.append((label, val))
            
        # Calcular porcentajes para barras
        max_val = max([v for _, v in items]) if items else 1
        if max_val == 0: max_val = 1
        
        y_positions = np.linspace(0.7, 0.2, len(items))
        bar_height = 0.04
        
        for i, (label, val) in enumerate(items):
            y = y_positions[i]
            pct_bar = val / max_val
            
            # Etiqueta (Izquierda)
            ax2.text(
                0.05,
                y + bar_height + 0.02,
                label.upper(),
                ha='left',
                va='bottom',
                color='#000000',
                fontsize=9,
                fontweight='bold',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.9, boxstyle='round,pad=0.2')
            )
            
            # Valor (Derecha)
            ax2.text(
                0.95,
                y + bar_height + 0.02,
                str(val),
                ha='right',
                va='bottom',
                color='#000000',
                fontsize=10,
                fontweight='bold',
                bbox=dict(facecolor='white', edgecolor='none', alpha=0.9, boxstyle='round,pad=0.2')
            )
            
            # Barra Fondo (Gris suave)
            rect_bg = FancyBboxPatch((0.05, y), 0.9, bar_height, 
                                   boxstyle="round,pad=0.01", 
                                   color='#F5F5F5', mutation_scale=10)
            ax2.add_patch(rect_bg)
            
            # Barra Valor (Colores corporativos modernos)
            bar_colors = ['#457b9d', '#e63946', '#f4a261', '#2a9d8f']
            c = bar_colors[i % len(bar_colors)]
            
            if val > 0:
                width = 0.9 * pct_bar
                # Asegurar ancho mínimo para visibilidad
                if width < 0.02: width = 0.02
                
                rect_val = FancyBboxPatch((0.05, y), width, bar_height, 
                                        boxstyle="round,pad=0.01", 
                                        color=c, mutation_scale=10)
                ax2.add_patch(rect_val)

        plt.tight_layout()
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
        """Gráfico de tipos de cruces con efecto relieve y filtrado de ceros"""
        tipos = self.analytics.get_tipos_cruces()
        
        fig, ax = plt.subplots(figsize=(8, 6))
        
        # Datos brutos
        raw_labels = ['Cliente +\nProveedor', 'Proveedor +\nEmpleado', 'Cliente +\nEmpleado', 'Triple\nCruce']
        raw_values = [
            tipos.get('cliente_proveedor', 0),
            tipos.get('proveedor_empleado', 0),
            tipos.get('cliente_empleado', 0),
            tipos.get('triple_cruce', 0)
        ]
        raw_colors = ['#00b4d8', '#f77f00', '#415a77', '#e63946']
        
        # Filtrar valores cero para evitar superposición de etiquetas y sectores vacíos
        filtered_data = [(l, v, c) for l, v, c in zip(raw_labels, raw_values, raw_colors) if v > 0]
        
        if filtered_data:
            labels, values, colors = zip(*filtered_data)
            
            # Efecto "Explode" para separar las rebanadas y dar relieve
            explode = [0.05] * len(values) 
            
            wedges, texts, autotexts = ax.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, 
                                            startangle=140, pctdistance=0.85, labeldistance=1.1,
                                            shadow=True, explode=explode, 
                                            textprops={'fontsize': 11, 'fontweight': 'bold', 'color': '#333333'})
            
            # Mejorar visibilidad del porcentaje (Blanco y negrita)
            plt.setp(autotexts, size=10, weight="bold", color="white", path_effects=[])
            # Agregar borde negro suave al texto blanco para contraste si es necesario (opcional)
            
        else:
            ax.text(0.5, 0.5, "Sin Tipologías Detectadas", ha='center', va='center', fontsize=12, color='#666666')
            
        ax.set_title('Tipos de Cruces Detectados', fontsize=14, fontweight='bold', pad=20)
        
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
