import io
import json
import os
import math
import numpy as np
from datetime import datetime
from typing import Any, Dict, Optional, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch
from matplotlib.patches import Polygon as MplPolygon
import colorsys

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
    Image, KeepTogether, PageBreak
)
from reportlab.platypus.flowables import CondPageBreak, Flowable
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT

from src.db.base import TargetSessionLocal, SourceSessionLocal
from src.db.models.generated_report import GeneratedReport
from src.services.s3_service import s3_service

# ══════════════════════════════════════════════════════════════
# PALETA DE COLORES (Adaptada al Logo Riesgos365)
# ══════════════════════════════════════════════════════════════
C = {
    "header_bg": "#00B5CB",  # Cyan del logo (Swoop y detalles)
    "dark_text": "#4D4D4D",  # Gris oscuro del logo (Texto principal)
    "pink": "#D81A60",  # Fucsia/Rojo del logo (Riesgo Alto / Alertas)
    "orange": "#F29100",  # Naranja del logo (Riesgo Medio)
    "teal": "#00A97E",  # Verde del logo (Riesgo Bajo / Ok)
    "slate": "#4D4D4D",
    "gray": "#64748B",
    "light": "#F8FAFC",
    "white": "#FFFFFF",
    "border": "#E2E8F0",
}


# ── Helper: figura matplotlib → Image de ReportLab ────────────────────────────
def _fig_to_img(fig, w_inch, h_inch, dpi=150):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=dpi,
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=w_inch * inch, height=h_inch * inch)


# ══════════════════════════════════════════════════════════════
# PORTADA  (página 1 — canvas puro)
# ══════════════════════════════════════════════════════════════
def _draw_cover(canvas_obj, doc, empresa_nombre, empresa_id,
                periodo, tipo_contraparte, logo_path):
    W, H = A4

    # Fondo blanco humo
    canvas_obj.setFillColor(colors.HexColor("#F8FAFC"))
    canvas_obj.rect(0, 0, W, H, fill=1, stroke=0)

    # Círculos decorativos tipo red/graph (esquinas)
    def draw_net(cx, cy, r):
        canvas_obj.saveState()
        canvas_obj.setStrokeColor(colors.HexColor("#CBD5E1"))
        canvas_obj.setLineWidth(0.5)
        canvas_obj.circle(cx, cy, r, fill=0, stroke=1)
        canvas_obj.circle(cx, cy, r * 0.6, fill=0, stroke=1)
        n = 10
        for i in range(n):
            angle = 2 * math.pi * i / n
            nx, ny = cx + r * math.cos(angle), cy + r * math.sin(angle)
            canvas_obj.setFillColor(colors.HexColor("#94A3B8"))
            canvas_obj.circle(nx, ny, 3.5, fill=1, stroke=0)
            canvas_obj.setStrokeColor(colors.HexColor("#CBD5E1"))
            canvas_obj.line(cx, cy, nx, ny)
        canvas_obj.restoreState()

    draw_net(W - 55, H - 55, 140)
    draw_net(60, 100, 110)

    # Logo centrado
    if logo_path and os.path.exists(logo_path):
        try:
            ir = ImageReader(logo_path)
            iw, ih = ir.getSize()
            lw = 2.8 * inch
            lh = lw * (ih / float(iw))
            if lh > 1.1 * inch:
                lh = 1.1 * inch
                lw = lh * (iw / float(ih))
            canvas_obj.drawImage(logo_path, (W - lw) / 2, H - lh - 48,
                                 width=lw, height=lh,
                                 preserveAspectRatio=True, mask="auto")
        except Exception:
            pass

    canvas_obj.setFillColor(colors.HexColor(C["gray"]))
    canvas_obj.setFont("Helvetica", 9)
    canvas_obj.drawCentredString(W / 2, H - 168, "Expertos en Auditoría y Cumplimiento")

    # 🟢 MODIFICACIÓN 1: Título y subtítulos actualizados
    canvas_obj.setFillColor(colors.HexColor(C["dark_text"]))
    canvas_obj.setFont("Helvetica-Bold", 40)
    canvas_obj.drawCentredString(W / 2, H * 0.57, "Informe Ejecutivo")

    # Subtítulo (Más pegado al título principal)
    canvas_obj.setFillColor(colors.HexColor(C["slate"]))
    canvas_obj.setFont("Helvetica", 15)
    canvas_obj.drawCentredString(W / 2, H * 0.57 - 40, "Identificación de Multi-vínculos")
    canvas_obj.drawCentredString(W / 2, H * 0.57 - 60, "y contrapartes sin debida diligencia")

    # Badge pill con fecha
    pw, ph = 320, 34
    px = (W - pw) / 2
    py = H * 0.21
    canvas_obj.setStrokeColor(colors.HexColor(C["header_bg"]))
    canvas_obj.setFillColor(colors.white)
    canvas_obj.setLineWidth(1.5)
    canvas_obj.roundRect(px, py, pw, ph, 17, fill=1, stroke=1)
    canvas_obj.setFillColor(colors.HexColor(C["dark_text"]))
    canvas_obj.setFont("Helvetica-Bold", 11)

    # 🟢 MODIFICACIÓN 2: Meses en español de forma manual
    meses_es = ["enero", "febrero", "marzo", "abril", "mayo", "junio",
                "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
    hoy = datetime.now()
    fecha_espanol = f"{hoy.day} de {meses_es[hoy.month - 1]} de {hoy.year}"

    canvas_obj.drawCentredString(
        W / 2, py + 11,
        f"Plataforma Riesgos 365  |  {fecha_espanol}"
    )

    # Barra de datos inferior (Fondo blanco, borde superior cyan)
    canvas_obj.setFillColor(colors.white)
    canvas_obj.rect(0, 0, W, 62, fill=1, stroke=0)
    canvas_obj.setFillColor(colors.HexColor(C["header_bg"]))
    canvas_obj.rect(0, 60, W, 2, fill=1, stroke=0)

    def footer_label(label, value, x, align="left"):
        canvas_obj.setFillColor(colors.HexColor(C["gray"]))
        canvas_obj.setFont("Helvetica", 7)
        if align == "right":
            canvas_obj.drawRightString(x, 46, label)
        else:
            canvas_obj.drawString(x, 46, label)
        canvas_obj.setFillColor(colors.HexColor(C["dark_text"]))
        canvas_obj.setFont("Helvetica-Bold", 10)
        if align == "right":
            canvas_obj.drawRightString(x, 30, str(value)[:50])
        else:
            canvas_obj.drawString(x, 30, str(value)[:50])

    footer_label("EMPRESA", empresa_nombre, 20)
    footer_label("PERÍODO", periodo, 280)

    # 🟢 MODIFICACIÓN 3: Cambio de Alcance
    footer_label("ALCANCE", "Monitoreo Contrapartes", W - 20, "right")


# ══════════════════════════════════════════════════════════════
# HEADER / FOOTER páginas interiores
# ══════════════════════════════════════════════════════════════
def _draw_inner(canvas_obj, doc, logo_path=None):
    W, H = A4
    canvas_obj.setFillColor(colors.white)
    canvas_obj.rect(0, H - 38, W, 38, fill=1, stroke=0)
    canvas_obj.setStrokeColor(colors.HexColor(C["header_bg"]))
    canvas_obj.setLineWidth(2)
    canvas_obj.line(0, H - 38, W, H - 38)

    if logo_path and os.path.exists(logo_path):
        try:
            ir = ImageReader(logo_path)
            iw, ih = ir.getSize()
            lw = 1.1 * inch
            lh = lw * (ih / float(iw))
            if lh > 0.30 * inch:
                lh = 0.30 * inch
                lw = lh * (iw / float(ih))
            canvas_obj.drawImage(logo_path, 14, H - 36, width=lw, height=lh,
                                 preserveAspectRatio=True, mask="auto")
        except Exception:
            pass

    canvas_obj.setFillColor(colors.HexColor(C["dark_text"]))
    canvas_obj.setFont("Helvetica-Bold", 8)
    canvas_obj.drawRightString(W - 14, H - 22,
                               "Informe Ejecutivo de Riesgo — Análisis de Relaciones Cruzadas")

    canvas_obj.setFillColor(colors.HexColor(C["light"]))
    canvas_obj.rect(0, 0, W, 26, fill=1, stroke=0)
    canvas_obj.setStrokeColor(colors.HexColor(C["border"]))
    canvas_obj.setLineWidth(0.5)
    canvas_obj.line(0, 26, W, 26)
    canvas_obj.setFillColor(colors.HexColor(C["gray"]))
    canvas_obj.setFont("Helvetica", 7)
    canvas_obj.drawString(14, 9, "Plataforma Riesgos 365  |  Confidencial")
    canvas_obj.drawRightString(W - 14, 9, f"Página {doc.page}")


# ══════════════════════════════════════════════════════════════
# GRÁFICO 1 – KPI CARDS  (slide 2)
# ══════════════════════════════════════════════════════════════
def _chart_kpi_panel(total_reg, cruces_count, pct_cruces, riesgo_prom,
                     triple_count, sin_dd_total, total_contra):
    fig, axes = plt.subplots(2, 3, figsize=(11, 5.2))
    fig.patch.set_facecolor("#F8FAFC")
    plt.subplots_adjust(hspace=0.40, wspace=0.28)

    def nivel(val, t1, t2):
        if val <= t1:
            return C["teal"], "Bajo"
        elif val <= t2:
            return C["orange"], "Medio"
        return C["pink"], "Alto"

    cards = [
        ("Total muestra analizada",
         f"{total_reg:,}".replace(",", "."), "registros", C["dark_text"], None, None),
        ("% Contrapartes con\nConflictos de Interés",
         f"{pct_cruces:.2f}%".replace(".", ","), "",
         C["dark_text"], *nivel(pct_cruces, 1, 5)),
        ("Contrapartes con\nConflictos de Interés",
         f"{cruces_count:,}".replace(",", "."), "contrapartes",
         C["dark_text"], *nivel(pct_cruces, 1, 5)),
        ("Casos con triple relación\n(Cliente-Proveedor-Empleado)",
         f"{triple_count} casos", "",
         C["dark_text"], *nivel(triple_count, 0, 5)),
        ("Transacciones de contrapartes sin\ndebida diligencia actualizada",
         f"{sin_dd_total:,}".replace(",", "."), "sin DD",
         C["dark_text"], *nivel(sin_dd_total, 0, 10)),
        ("Total contrapartes\nanalizadas",
         f"{total_contra:,}".replace(",", "."), "contrapartes",
         C["dark_text"], None, None),
    ]

    for ax, (label, val, sub, vcol, dcol, nivel_txt) in zip(axes.flat, cards):
        ax.set_facecolor("white")
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis("off")

        rect = FancyBboxPatch((0.03, 0.04), 0.94, 0.92,
                              boxstyle="round,pad=0.02",
                              lw=1.2, edgecolor="#E2E8F0", facecolor="white")
        ax.add_patch(rect)
        # Línea superior cyan
        ax.plot([0.03, 0.97], [0.96, 0.96], lw=3, color=C["header_bg"],
                solid_capstyle="round")

        ax.text(0.09, 0.84, label, fontsize=8.5, color=C["gray"],
                va="top", ha="left", multialignment="left")
        ax.text(0.09, 0.50, val, fontsize=20, fontweight="bold",
                color=vcol, va="center", ha="left")
        if sub:
            ax.text(0.09, 0.24, sub, fontsize=8.5, color=C["gray"], va="center")

        if dcol:
            c = plt.Circle((0.82, 0.42), 0.11, color=dcol, alpha=0.90, zorder=5)
            ax.add_patch(c)
            glow = plt.Circle((0.82, 0.42), 0.15, color=dcol, alpha=0.15, zorder=4)
            ax.add_patch(glow)
            ax.text(0.82, 0.20, nivel_txt, fontsize=7.5, fontweight="bold",
                    color=dcol, ha="center", va="center", multialignment="center")

    return _fig_to_img(fig, 7.0, 3.6)


# ══════════════════════════════════════════════════════════════
# GRÁFICO 2 – BARRAS HORIZONTALES + CARD  (slide 3)
# ══════════════════════════════════════════════════════════════
def _chart_relaciones(counts: dict):
    fig, (ax_bar, ax_card) = plt.subplots(1, 2, figsize=(11, 3.8),
                                          gridspec_kw={"width_ratios": [1.5, 1]})
    fig.patch.set_facecolor("#F8FAFC")

    labels = list(counts.keys())
    values = list(counts.values())
    bar_colors = []
    for lbl in labels:
        if "Triple" in lbl:
            bar_colors.append(C["pink"])
        elif "Cliente" in lbl and "Proveedor" in lbl:
            bar_colors.append(C["orange"])
        elif "Proveedor" in lbl and "Empleado" in lbl:
            bar_colors.append(C["header_bg"])
        else:
            bar_colors.append(C["teal"])

    ax_bar.set_facecolor("white")
    bars = ax_bar.barh(labels, values, color=bar_colors, edgecolor="none", height=0.50)
    max_v = max(values) if any(v > 0 for v in values) else 1
    for bar, v in zip(bars, values):
        ax_bar.text(v + max_v * 0.025, bar.get_y() + bar.get_height() / 2,
                    str(v), va="center", ha="left",
                    fontsize=11, fontweight="bold", color=C["dark_text"])
    ax_bar.set_xlabel("Número de casos", fontsize=9, color=C["gray"])
    ax_bar.tick_params(axis="y", labelsize=10, colors=C["dark_text"])
    ax_bar.tick_params(axis="x", colors="#94A3B8", labelsize=8)
    for sp in ["top", "right", "left"]:
        ax_bar.spines[sp].set_visible(False)
    ax_bar.grid(axis="x", alpha=0.22, color="#CBD5E1")
    ax_bar.set_xlim(0, max_v * 1.25)

    # 🟢 MODIFICACIÓN 4: Se comenta el título interno redundante
    # ax_bar.set_title("Distribución Estratégica de Relaciones Cruzadas",
    #                  fontsize=11, fontweight="bold", color=C["dark_text"], pad=10)

    # Card de análisis
    ax_card.set_facecolor("white")
    ax_card.set_xlim(0, 1)
    ax_card.set_ylim(0, 1)
    ax_card.axis("off")
    rect = FancyBboxPatch((0.04, 0.04), 0.92, 0.92,
                          boxstyle="round,pad=0.02",
                          lw=1.2, edgecolor="#E2E8F0", facecolor="white")
    ax_card.add_patch(rect)

    max_label = max(counts, key=counts.get) if any(counts.values()) else "N/A"
    lines = [
        (0.88, "Análisis Ejecutivo", 10, "bold", C["dark_text"]),
        (0.72, f"Mayor concentración:", 8.5, "bold", C["slate"]),
        (0.63, f"{max_label}", 8.5, "normal", C["header_bg"]),
        (0.50, "Los casos de Triple Relación", 8, "normal", C["gray"]),
        (0.42, "incrementan la exposición por", 8, "normal", C["gray"]),
        (0.34, "conflicto estructural.", 8, "normal", C["gray"]),
        (0.22, "Las relaciones múltiples aumentan", 8, "normal", C["gray"]),
        (0.14, "el riesgo de favorecimiento indebido.", 8, "normal", C["gray"]),
    ]
    for y, txt, fs, fw, fc in lines:
        ax_card.text(0.10, y, txt, fontsize=fs, fontweight=fw,
                     color=fc, va="center", ha="left")

    # Dots decorativos
    for i, (dx, dc) in enumerate(zip([0.12, 0.22, 0.32, 0.42],
                                     [C["teal"], C["pink"], C["orange"], C["header_bg"]])):
        ax_card.add_patch(plt.Circle((dx, 0.05), 0.04, color=dc, zorder=5))

    return _fig_to_img(fig, 7.0, 3.0)


# ══════════════════════════════════════════════════════════════
# GRÁFICO 3 – PIRÁMIDE 3D  (slide 4)
# ══════════════════════════════════════════════════════════════
def _chart_piramide(total_contra, sin_dd_contra, alto_riesgo_sin_form):
    fig, ax = plt.subplots(figsize=(10, 5.2))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 6.5)
    ax.axis("off")

    # 🟢 MODIFICACIÓN 5: Se comenta el título interno redundante
    # ax.set_title("Estado de Debida Diligencia y Riesgo Estratégico",
    #              fontsize=13, fontweight="bold", color=C["dark_text"], pad=12)

    # 🟢 MODIFICACIÓN 5b: Agregamos parámetros de offset X (line_x, text_x) para empujar el texto verde a la derecha
    layers = [
        # (tw, yb, h, col, label, sub1, sub2, line_x, text_x)

        # 🟢 TOP (ROJO): Personas sin DD y de Riesgo Alto
        (0.9, 4.2, 1.0, C["pink"], "Prioridad Crítica (Alto)",
         "Contrapartes de Alto Riesgo SIN Debida Diligencia",
         f"{alto_riesgo_sin_form} contrapartes urgentes", 6.2, 6.38),

        # 🟢 MEDIO (AMARILLO): Personas sin DD
        (2.4, 2.6, 1.4, C["orange"], "Brecha de Cumplimiento (Medio)",
         "Contrapartes SIN Debida Diligencia",
         f"{sin_dd_contra:,} contrapartes".replace(",", "."), 6.2, 6.38),

        # 🟢 BASE (VERDE): Total de personas evaluadas - MOVIDO A LA DERECHA
        (4.2, 0.5, 1.9, C["teal"], "Alcance de la Muestra (Base)",
         "Total Contrapartes Analizadas",
         f"{total_contra:,} contrapartes en la muestra".replace(",", "."), 6.8, 6.98),
    ]

    cx = 3.8
    for (tw, yb, h, col, label, sub1, sub2, line_x, text_x) in layers:
        bw = tw + h * 0.85
        # Cara frontal
        verts_f = [(cx - bw / 2, yb), (cx + bw / 2, yb),
                   (cx + tw / 2, yb + h), (cx - tw / 2, yb + h)]
        ax.add_patch(MplPolygon(verts_f, closed=True, color=col, alpha=0.90, zorder=3))
        # Cara superior (sombra clara)
        d = 0.28
        verts_t = [(cx - tw / 2, yb + h), (cx + tw / 2, yb + h),
                   (cx + tw / 2 + d, yb + h + d * 0.45),
                   (cx - tw / 2 + d, yb + h + d * 0.45)]
        r, g, b = int(col[1:3], 16) / 255, int(col[3:5], 16) / 255, int(col[5:7], 16) / 255
        hh, s, v = colorsys.rgb_to_hsv(r, g, b)
        r2, g2, b2 = colorsys.hsv_to_rgb(hh, s * 0.85, min(v * 1.28, 1.0))
        top_col = f"#{int(r2 * 255):02x}{int(g2 * 255):02x}{int(b2 * 255):02x}"
        ax.add_patch(MplPolygon(verts_t, closed=True, color=top_col, alpha=0.90, zorder=4))

        # Línea conectora y texto usando las nuevas coordenadas X
        mid_y = yb + h / 2
        ax.annotate("", xy=(line_x, mid_y), xytext=(cx + bw / 2 + 0.1, mid_y),
                    arrowprops=dict(arrowstyle="-", color=col, lw=1.2))
        ax.scatter([line_x], [mid_y], s=45, color=col, zorder=5)
        ax.text(text_x, mid_y + 0.22, label, fontsize=9, fontweight="bold",
                color=col, va="bottom", ha="left")
        ax.text(text_x, mid_y - 0.04, sub1, fontsize=7.5, color=C["slate"],
                va="top", ha="left")
        ax.text(text_x, mid_y - 0.30, sub2, fontsize=7.5, color=C["gray"],
                va="top", ha="left")

    return _fig_to_img(fig, 7.2, 3.9)


# ══════════════════════════════════════════════════════════════
# GRÁFICO 4 – TIMELINE PLAN DE ACCIÓN  (slide 6)
# ══════════════════════════════════════════════════════════════
def _chart_plan_accion():
    fig, ax = plt.subplots(figsize=(10, 2.8))
    fig.patch.set_facecolor("#F8FAFC")
    ax.set_facecolor("#F8FAFC")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 3.0)
    ax.axis("off")
    ax.set_title("Plan de Acción y Conclusión Ejecutiva",
                 fontsize=12, fontweight="bold", color=C["dark_text"], pad=10)

    # 🟢 MODIFICACIÓN 6: Textos actualizados del plan de acción
    steps = [
        (1.5, C["pink"], "1", "Intervención\nInmediata",
         "Actualizar DD en\ncasos identificados"),
        (5.0, C["orange"], "2", "Justificación\nde Vínculos",
         "Actualizar D.D con\njustificación de Vínculo"),
        (8.5, C["teal"], "3", "Monitoreo\nActivo",
         "Filtración por\nmontos de D.D"),
    ]
    ax.plot([1.5, 8.5], [2.05, 2.05], color="#CBD5E1", lw=2.5,
            solid_capstyle="round", zorder=1)

    for (x, col, num, label, desc) in steps:
        ax.add_patch(plt.Circle((x, 2.05), 0.40, color=col, zorder=3))
        ax.add_patch(plt.Circle((x, 2.05), 0.54, color=col, alpha=0.18, zorder=2))
        ax.text(x, 2.05, num, fontsize=17, fontweight="bold", color="white",
                ha="center", va="center", zorder=4)
        ax.text(x, 1.45, label, fontsize=9, fontweight="bold", color=col,
                ha="center", va="top", multialignment="center", zorder=4)
        ax.text(x, 0.78, desc, fontsize=7.8, color=C["slate"],
                ha="center", va="top", multialignment="center", zorder=4)

    return _fig_to_img(fig, 7.0, 2.4)


# ══════════════════════════════════════════════════════════════
# TABLA DETALLE  (estilo dark UX — slide 5)
# ══════════════════════════════════════════════════════════════
def _build_detail_table(sin_dd_list, table_cell_style):
    if not sin_dd_list:
        return None

    def fmt(v):
        return "" if v is None else str(v)[:40]

    def fmta(v):
        try:
            return f"$ {float(v):>12,.0f}".replace(",", ".")
        except Exception:
            return fmt(v)

    def parse_monto(val):
        if not val: return 0.0
        if isinstance(val, (int, float)): return float(val)
        s = str(val).replace("$", "").replace(",", "").replace(" ", "").strip()
        try:
            return float(s)
        except ValueError:
            return 0.0

    def is_high_risk(val):
        if val is None: return False
        try:
            if float(val) >= 4.0: return True
        except (ValueError, TypeError):
            pass
        s = str(val).lower()
        return "alto" in s or "critic" in s or "crític" in s

    hdr_style = ParagraphStyle("TH", fontName="Helvetica-Bold", fontSize=8,
                               textColor=colors.white, alignment=TA_CENTER)
    no_dd_style = ParagraphStyle("NoDD", fontName="Helvetica-Bold", fontSize=8,
                                 textColor=colors.HexColor(C["pink"]),
                                 alignment=TA_CENTER)
    yes_dd_style = ParagraphStyle("YesDD", fontName="Helvetica-Bold", fontSize=8,
                                  textColor=colors.HexColor(C["teal"]),
                                  alignment=TA_CENTER)

    multi_line_style = ParagraphStyle("Multi", fontName="Helvetica", fontSize=7.5,
                                      textColor=colors.HexColor(C["slate"]),
                                      alignment=TA_CENTER, leading=10)

    rows = [[
        Paragraph("ID", hdr_style),
        Paragraph("Empresa / Entidad", hdr_style),
        Paragraph("Tipo", hdr_style),
        Paragraph("Monto Riesgo / Transacciones", hdr_style),
        Paragraph("Estado DD", hdr_style),
    ]]

    for r in sin_dd_list[:15]:
        nombre_raw = r.get("nombre") or r.get("empresa") or "N/D"
        nombre_clean = fmt(nombre_raw)

        riesgo_val = r.get("riesgo_maximo", 0)
        es_alto_riesgo = is_high_risk(riesgo_val)

        tiene_dd = r.get("dd", False) or r.get("tiene_formulario", False)

        if es_alto_riesgo and not tiene_dd:
            nombre_final = f"<b>{nombre_clean}</b><br/><font color='{C['pink']}' size='7.5'>⚠️ ALTO RIESGO (Sin DD)</font>"
        elif es_alto_riesgo and tiene_dd:
            nombre_final = f"<b>{nombre_clean}</b><br/><font color='{C['orange']}' size='7.5'>⚠️ ALTO RIESGO (Documentado)</font>"
        else:
            nombre_final = nombre_clean

        id_val = r.get("id_contraparte") or r.get("id") or r.get("nit") or "N/D"

        is_grouped = any(k in r for k in ["cliente", "proveedor", "empleado"]) and isinstance(r.get("cliente", {}),
                                                                                              dict)

        tipos_list = []
        montos_list = []

        if is_grouped:
            c_amt = parse_monto(r.get("cliente", {}).get("amount", 0) or r.get("cliente", {}).get("suma", 0))
            p_amt = parse_monto(r.get("proveedor", {}).get("amount", 0) or r.get("proveedor", {}).get("suma", 0))
            e_amt = parse_monto(r.get("empleado", {}).get("amount", 0) or r.get("empleado", {}).get("suma", 0))

            c_count = int(r.get("cliente", {}).get("count", 0) or r.get("cliente", {}).get("cantidad", 0) or 0)
            p_count = int(r.get("proveedor", {}).get("count", 0) or r.get("proveedor", {}).get("cantidad", 0) or 0)
            e_count = int(r.get("empleado", {}).get("count", 0) or r.get("empleado", {}).get("cantidad", 0) or 0)

            if c_amt > 0 or c_count > 0:
                tipos_list.append("Cliente")
                montos_list.append(f"{fmta(c_amt)} <font size=6.5 color='#64748B'>({c_count} txs)</font>")

            if p_amt > 0 or p_count > 0:
                tipos_list.append("Proveedor")
                montos_list.append(f"{fmta(p_amt)} <font size=6.5 color='#64748B'>({p_count} txs)</font>")

            if e_amt > 0 or e_count > 0:
                tipos_list.append("Empleado")
                montos_list.append(f"{fmta(e_amt)} <font size=6.5 color='#64748B'>({e_count} txs)</font>")

            tipo_str = "<br/>".join(tipos_list) if tipos_list else "N/D"
            monto_str = "<br/>".join(montos_list) if montos_list else "$ 0"

        else:
            t = str(r.get("tipo", "")).capitalize()
            tipo_str = t if t else "N/D"
            m_val = parse_monto(r.get("monto", 0) or r.get("valor", 0))
            t_count = int(r.get("count", 0) or r.get("cantidad", 0) or 1)
            tx_str = f" <font size=6.5 color='#64748B'>({t_count} txs)</font>" if t_count > 0 else ""
            monto_str = fmta(m_val) + tx_str

        estado_dd_p = Paragraph("SÍ", yes_dd_style) if tiene_dd else Paragraph("NO", no_dd_style)

        rows.append([
            Paragraph(fmt(id_val), table_cell_style),
            Paragraph(nombre_final, table_cell_style),
            Paragraph(tipo_str, multi_line_style),
            Paragraph(monto_str, multi_line_style),
            estado_dd_p,
        ])

    t = Table(rows, colWidths=[65, 165, 75, 115, 55])

    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(C["header_bg"])),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor(C["border"])),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F8FAFC")]),
    ]))
    return t


# ══════════════════════════════════════════════════════════════
# SERVICIO PRINCIPAL
# ══════════════════════════════════════════════════════════════
class PDFRiskReportService:

    def _obtener_nombre_empresa(self, empresa_id: int) -> str:
        from sqlalchemy import create_engine, text
        from src.core.config2 import settings as form_settings
        try:
            url = form_settings.TARGET_DATABASE_URL
            if "mysql+mysqlconnector" not in url and url.startswith("mysql://"):
                url = url.replace("mysql://", "mysql+pymysql://")

            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                query = text("SELECT razon_social FROM empresas WHERE id_empresa = :eid LIMIT 1")
                result = conn.execute(query, {"eid": empresa_id}).fetchone()

                if result and result[0]:
                    return str(result[0]).strip().upper()
        except Exception as e:
            print(f"⚠️ Error obteniendo razón social de BD formularios: {e}")

        return f"EMPRESA ID: {empresa_id}"

    def _calculate_period(self, data: Dict[str, Any], filtros: Optional[Dict[str, Any]]) -> str:
        filtros = filtros or {}
        f_desde = filtros.get("fecha_desde")
        f_hasta = filtros.get("fecha_hasta")

        if not f_desde or not f_hasta:
            all_dates = []
            detalles = data.get("tabla_detalles", [])
            if not detalles:
                detalles = data.get("transacciones_sin_dd", [])

            for e in detalles:
                for role in ["cliente", "proveedor", "empleado"]:
                    role_data = e.get(role, {})
                    if isinstance(role_data, dict):
                        txs = role_data.get("transacciones_detalles", [])
                        if isinstance(txs, list) and len(txs) > 0:
                            for tx in txs:
                                d = tx.get("fecha") or tx.get("date")
                                if d: all_dates.append(str(d))
                        else:
                            fechas = role_data.get("fechas_transaccion", [])
                            if isinstance(fechas, list):
                                for d in fechas:
                                    if d: all_dates.append(str(d))

            valid_dates = []
            for d in all_dates:
                d_str = str(d).strip().split(' ')[0].replace("/", "-")
                if d_str and d_str.lower() not in ['n/a', 'nan', 'none', '—', '']:
                    if len(d_str) >= 10:
                        valid_dates.append(d_str[:10])

            if valid_dates:
                valid_dates.sort()
                calc_min = valid_dates[0]
                calc_max = valid_dates[-1]
            else:
                calc_min = calc_max = None

            if not f_desde and calc_min:
                f_desde = calc_min
            if not f_hasta and calc_max:
                f_hasta = calc_max

        if f_desde and f_hasta:
            if f_desde == f_hasta:
                return f_desde
            return f"{f_desde} a {f_hasta}"
        elif f_desde:
            return f"Desde {f_desde}"
        elif f_hasta:
            return f"Hasta {f_hasta}"
        else:
            return "Histórico Completo"

    def generate_pdf_report(
            self,
            analytics_json_path: Optional[str] = None,
            analytics_data: Optional[Dict[str, Any]] = None,
            tipo_contraparte: str = "cliente",
            output_path: Optional[str] = None,
            filtros_pdf: Optional[Dict[str, Any]] = None,
            email_to: Optional[str] = None,
            oficial_conclusion: Optional[str] = None
    ) -> Dict[str, Any]:

        try:
            analytics: Dict[str, Any] = {}
            if analytics_data is not None:
                analytics = analytics_data
            elif analytics_json_path:
                with open(analytics_json_path, "r", encoding="utf-8") as f:
                    analytics = json.load(f)
            else:
                return {"status": "error",
                        "message": "Debe enviar analytics_json_path o analytics_data"}

            empresa_id = analytics.get("empresa_id")
            if not empresa_id:
                return {"status": "error",
                        "message": "empresa_id no encontrado en la analítica"}

            nombre_real = self._obtener_nombre_empresa(int(empresa_id))
            analytics["empresa_nombre"] = nombre_real

            is_filtered = False
            if filtros_pdf:
                is_filtered = any([
                    filtros_pdf.get("fecha_desde"),
                    filtros_pdf.get("fecha_hasta"),
                    float(filtros_pdf.get("monto_min", 0) or 0) > 0,
                    float(filtros_pdf.get("monto_min_tx", 0) or 0) > 0,
                    str(filtros_pdf.get("sin_dd", "")).lower() in ['true', '1', 'yes'],
                    str(filtros_pdf.get("con_cruces", "")).lower() in ['true', '1', 'yes']
                ])

            analytics["is_filtered_flag"] = is_filtered

            if is_filtered:
                analytics = self._apply_pdf_filters(analytics, filtros_pdf)
            else:
                agrupados = analytics.get("entidades_sin_dd", [])

                def get_total_monto(x):
                    def clean(val):
                        if not val: return 0.0
                        if isinstance(val, (int, float)): return float(val)
                        try:
                            return float(str(val).replace("$", "").replace(",", "").replace(" ", "").strip())
                        except:
                            return 0.0

                    c = clean(x.get("cliente", {}).get("amount", 0) or x.get("cliente", {}).get("suma", 0))
                    p = clean(x.get("proveedor", {}).get("amount", 0) or x.get("proveedor", {}).get("suma", 0))
                    e = clean(x.get("empleado", {}).get("amount", 0) or x.get("empleado", {}).get("suma", 0))
                    return abs(c + p + e)

                agrupados.sort(key=get_total_monto, reverse=True)
                analytics["transacciones_sin_dd"] = agrupados

                alto_riesgo_count = 0
                for x in agrupados:
                    r_max = str(x.get("riesgo_maximo", 0)).lower()
                    try:
                        if float(r_max) >= 4.0:
                            alto_riesgo_count += 1
                    except ValueError:
                        if "alto" in r_max or "critic" in r_max or "crític" in r_max:
                            alto_riesgo_count += 1

                if "estadisticas_formularios" not in analytics:
                    analytics["estadisticas_formularios"] = {}
                analytics["estadisticas_formularios"]["alto_riesgo_sin_formulario"] = alto_riesgo_count

            analytics["periodo_calculado"] = self._calculate_period(analytics, filtros_pdf)

            buffer = io.BytesIO()
            self._build_pdf(buffer, empresa_id=int(empresa_id),
                            data=analytics, tipo_contraparte=tipo_contraparte,
                            oficial_conclusion=oficial_conclusion)
            pdf_bytes = buffer.getvalue()
            buffer.close()

            filename = (f"Reporte_Riesgo_{empresa_id}_"
                        f"{datetime.now():%Y%m%d_%H%M%S}.pdf")

            email_sent = False
            if email_to:
                email_sent = self._send_email_with_mailgun(
                    to_email=email_to,
                    pdf_bytes=pdf_bytes,
                    filename=filename,
                    empresa_nombre=nombre_real,
                    periodo=analytics.get("periodo_calculado", "Histórico Completo")
                )

            if is_filtered:
                s3_key = f"temp/{empresa_id}/{filename}"
            else:
                s3_key = f"reports/{filename}"

            s3_url = s3_service.upload_file(pdf_bytes, s3_key)
            virtual_path = s3_key if s3_url else f"DB_STORED:{filename}"

            if not is_filtered:
                self._save_to_db(
                    company_id=int(empresa_id),
                    file_path=virtual_path,
                    pdf_content=None if s3_url else pdf_bytes,
                )

            return {"status": "success", "file": virtual_path,
                    "empresa_id": int(empresa_id), "local_file": None}

        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"status": "error",
                    "message": f"Error generando PDF: {e.__class__.__name__}: {e}"}

    def _apply_pdf_filters(self, data: Dict[str, Any], filters: Dict[str, Any]) -> Dict[str, Any]:
        import copy
        fd = copy.deepcopy(data)

        def clean_monto(val):
            if not val: return 0.0
            if isinstance(val, (int, float)): return float(val)
            s = str(val).replace("$", "").replace(",", "").replace(" ", "").strip()
            try:
                return float(s)
            except ValueError:
                return 0.0

        f_desde = filters.get("fecha_desde")
        f_hasta = filters.get("fecha_hasta")
        m_min_consolidado = clean_monto(filters.get("monto_min", 0))
        m_min_tx = clean_monto(filters.get("monto_min_tx", 0))

        sin_dd_only = str(filters.get("sin_dd", "")).lower() in ['true', '1', 'yes']
        cruces_only = str(filters.get("con_cruces", "")).lower() in ['true', '1', 'yes']

        def is_in_range(d_str):
            if not d_str or str(d_str).strip().lower() in ['n/a', 'nan', '—', 'none', '']: return False
            d = str(d_str).strip().split(' ')[0].replace('/', '-')
            if f_desde and d < f_desde: return False
            if f_hasta and d > f_hasta: return False
            return True

        original_list = fd.get("tabla_detalles", [])
        filtered_list = []

        agg_total_tx = 0
        agg_sin_dd = 0
        agg_triple = 0
        agg_c_p = 0
        agg_p_e = 0
        agg_c_e = 0
        agg_con_cruces = 0
        sum_riesgo = 0
        alto_riesgo_sin_form = 0

        for e in original_list:
            dd = e.get("dd", False) or e.get("tiene_formulario", False)

            c_data = e.get("cliente", {})
            p_data = e.get("proveedor", {})
            emp_data = e.get("empleado", {})

            if f_desde or f_hasta or m_min_tx > 0:
                for role_data in [c_data, p_data, emp_data]:
                    if not role_data: continue
                    txs = role_data.get("transacciones_detalles", [])
                    valid_txs = []

                    for tx in txs:
                        if (f_desde or f_hasta) and not is_in_range(tx.get("fecha", "")):
                            continue
                        if m_min_tx > 0 and abs(clean_monto(tx.get("monto", 0))) < m_min_tx:
                            continue
                        valid_txs.append(tx)

                    role_data["transacciones_detalles"] = valid_txs
                    role_data["count"] = len(valid_txs)
                    role_data["cantidad"] = len(valid_txs)

                    new_amount = sum([clean_monto(tx.get("monto", 0)) for tx in valid_txs])
                    role_data["amount"] = new_amount
                    role_data["suma"] = new_amount

            c_count = int(c_data.get("count", 0) or c_data.get("cantidad", 0))
            p_count = int(p_data.get("count", 0) or p_data.get("cantidad", 0))
            emp_count = int(emp_data.get("count", 0) or emp_data.get("cantidad", 0))
            total_tx_row = c_count + p_count + emp_count

            if (f_desde or f_hasta or m_min_tx > 0) and total_tx_row == 0:
                continue

            cruces_actuales = sum(1 for c in [c_count, p_count, emp_count] if c > 0)
            if cruces_only and cruces_actuales < 2:
                continue

            if sin_dd_only and dd:
                continue

            if m_min_consolidado > 0:
                c_amt = clean_monto(c_data.get("amount", 0) or c_data.get("suma", 0))
                p_amt = clean_monto(p_data.get("amount", 0) or p_data.get("suma", 0))
                emp_amt = clean_monto(emp_data.get("amount", 0) or emp_data.get("suma", 0))

                row_amt = c_amt + p_amt + emp_amt

                if abs(row_amt) < m_min_consolidado:
                    continue

            filtered_list.append(e)
            agg_total_tx += total_tx_row

            if not dd:
                agg_sin_dd += total_tx_row
                r_max = str(e.get("riesgo_maximo", 0)).lower()
                try:
                    if float(r_max) >= 4.0: alto_riesgo_sin_form += 1
                except ValueError:
                    if "alto" in r_max or "critic" in r_max or "crític" in r_max:
                        alto_riesgo_sin_form += 1

            if cruces_actuales > 1:
                agg_con_cruces += 1
            if cruces_actuales == 3:
                agg_triple += 1
            if c_count > 0 and p_count > 0: agg_c_p += 1
            if p_count > 0 and emp_count > 0: agg_p_e += 1
            if c_count > 0 and emp_count > 0: agg_c_e += 1
            try:
                sum_riesgo += float(e.get("riesgo_maximo", 0))
            except (ValueError, TypeError):
                pass

        lista_top = [e for e in filtered_list]

        def get_total_recalculado(x):
            c = clean_monto(x.get("cliente", {}).get("amount", 0) or x.get("cliente", {}).get("suma", 0))
            p = clean_monto(x.get("proveedor", {}).get("amount", 0) or x.get("proveedor", {}).get("suma", 0))
            e_val = clean_monto(x.get("empleado", {}).get("amount", 0) or x.get("empleado", {}).get("suma", 0))
            return abs(c + p + e_val)

        lista_top.sort(key=get_total_recalculado, reverse=True)

        fd["tabla_detalles"] = filtered_list
        fd["transacciones_sin_dd"] = lista_top

        total_entities = len(filtered_list)
        fd["total_transacciones"] = agg_total_tx
        fd["transacciones_sin_dd_total"] = agg_sin_dd

        if "kpis" not in fd: fd["kpis"] = {}
        fd["kpis"]["total_registros"] = total_entities
        fd["kpis"]["entidades_cruces"] = agg_con_cruces
        fd["kpis"]["porcentaje_cruces"] = (agg_con_cruces / total_entities * 100) if total_entities > 0 else 0.0
        fd["kpis"]["riesgo_promedio"] = (sum_riesgo / total_entities) if total_entities > 0 else 0.0

        if "tipos_cruces" not in fd: fd["tipos_cruces"] = {}
        fd["tipos_cruces"]["triple_cruce"] = agg_triple
        fd["tipos_cruces"]["cliente_proveedor"] = agg_c_p
        fd["tipos_cruces"]["proveedor_empleado"] = agg_p_e
        fd["tipos_cruces"]["cliente_empleado"] = agg_c_e

        if "estadisticas_formularios" not in fd: fd["estadisticas_formularios"] = {}
        fd["estadisticas_formularios"]["alto_riesgo_sin_formulario"] = alto_riesgo_sin_form

        return fd

    def _find_logo(self) -> Optional[str]:
        candidates = [
            os.path.join(os.getcwd(), "Logo.png"),
            "/app/Logo.png",
            "Logo.png",
            os.path.join(os.path.dirname(__file__), "Logo.png"),
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        return None

    def _build_pdf(self, output: io.BytesIO, empresa_id: int,
                   data: Dict[str, Any], tipo_contraparte: str,
                   oficial_conclusion: Optional[str] = None) -> None:

        logo_path = self._find_logo()
        styles = getSampleStyleSheet()

        normal_style = ParagraphStyle(
            "N", fontName="Helvetica", fontSize=10,
            textColor=colors.HexColor(C["slate"]), spaceAfter=4, leading=14)
        section_label_style = ParagraphStyle(
            "SL", fontName="Helvetica-Bold", fontSize=10,
            textColor=colors.white, spaceAfter=0)
        table_cell_style = ParagraphStyle(
            "TC", fontName="Helvetica", fontSize=8,
            textColor=colors.HexColor(C["slate"]), leading=10)
        conclusion_style = ParagraphStyle(
            "CO", fontName="Helvetica-Bold", fontSize=10,
            textColor=colors.white, leading=16, spaceAfter=0, alignment=TA_CENTER)

        # ── Datos ─────────────────────────────────────────────────────────
        kpis = data.get("kpis", {}) if isinstance(data.get("kpis", {}), dict) else {}
        tipos = data.get("tipos_cruces", {}) if isinstance(data.get("tipos_cruces", {}), dict) else {}

        total_reg = int(data.get("total_transacciones") or kpis.get("total_registros") or 0)
        cruces_count = int(kpis.get("entidades_cruces") or kpis.get("total_cruces") or 0)
        pct_cruces = float(kpis.get("porcentaje_cruces") or 0.0)
        riesgo_prom = float(kpis.get("riesgo_promedio") or 0.0)
        sin_dd_total = int(data.get("transacciones_sin_dd_total") or 0)
        triple_count = int(tipos.get("triple_relacion") or tipos.get("triple_cruce") or 0)
        total_contra = int(kpis.get("total_registros") or kpis.get("total_contrapartes") or cruces_count or 0)

        counts = {
            "Cliente – Proveedor": int(tipos.get("cliente_proveedor") or 0),
            "Proveedor – Empleado": int(tipos.get("proveedor_empleado") or 0),
            "Cliente – Empleado": int(tipos.get("cliente_empleado") or 0),
            "Triple Relación": triple_count,
        }

        periodo = data.get("periodo_calculado", "Histórico Completo")
        empresa_nombre = (data.get("empresa_nombre") or
                          data.get("company_name") or
                          f"Empresa ID: {empresa_id}")

        sin_dd_list = data.get("transacciones_sin_dd") or []
        stats_dd = data.get("estadisticas_formularios", {}) \
            if isinstance(data.get("estadisticas_formularios", {}), dict) else {}
        dd_pct = stats_dd.get("porcentaje_completado")
        dd_pct_str = f"{float(dd_pct):.1f}%" if dd_pct is not None else "N/D"

        # ── Callbacks ─────────────────────────────────────────────────────
        def on_first_page(cv, doc_obj):
            _draw_cover(cv, doc_obj, empresa_nombre=empresa_nombre,
                        empresa_id=empresa_id, periodo=periodo,
                        tipo_contraparte=tipo_contraparte, logo_path=logo_path)

        def on_later_pages(cv, doc_obj):
            _draw_inner(cv, doc_obj, logo_path=logo_path)

        pdf_title = f"Reporte-{empresa_nombre.replace(' ', '_')}"
        # ── Documento ─────────────────────────────────────────────────────
        doc = SimpleDocTemplate(
            output, pagesize=A4,
            leftMargin=40, rightMargin=40,
            topMargin=50, bottomMargin=38,
            title=pdf_title,
            author="Riesgos 365"
        )
        W_content = A4[0] - 80

        def section_header(num, title, col=C["header_bg"]):
            badge = Table(
                [[Paragraph(f"<b>{num}. {title}</b>", section_label_style)]],
                colWidths=[W_content])
            badge.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(col)),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]))
            return [badge, Spacer(1, 10)]

        story = []
        story.append(PageBreak())  # Página 1 = portada canvas

        # S1: KPI Panel
        story += section_header("1", "Panel de Estado General: Resumen de Indicadores Estratégicos")
        story.append(_chart_kpi_panel(total_reg, cruces_count, pct_cruces, riesgo_prom,
                                      triple_count, sin_dd_total, total_contra))
        story.append(Spacer(1, 16))

        # S2: Distribución relaciones
        story.append(CondPageBreak(240))
        # 🟢 MODIFICACIÓN 4: Título principal de la sección modificado
        story += section_header("2", "Distribución Estratégica de Relaciones Cruzadas - Distribución Multi-vínculos")
        story.append(_chart_relaciones(counts))
        story.append(Spacer(1, 16))

        # S3: Pirámide
        story.append(CondPageBreak(270))
        story += section_header("3", "Estado de Debida Diligencia y Riesgo Estratégico")
        alto_riesgo_sin_form = stats_dd.get("alto_riesgo_sin_formulario") or 0
        sin_dd_contra_count = len(sin_dd_list)
        story.append(_chart_piramide(total_contra, sin_dd_contra_count, alto_riesgo_sin_form))
        story.append(Spacer(1, 16))

        # Tabla de Detalles Top 15 anexa a la Pirámide
        if sin_dd_list:
            is_filtered = data.get("is_filtered_flag", False)
            titulo_tabla = "Detalle de Contrapartes Filtradas (Top 15):" if is_filtered else "Detalle de Casos Críticos sin Debida Diligencia (Top 15):"

            story.append(Paragraph(
                f"<b>{titulo_tabla}</b>",
                ParagraphStyle("Sub2", fontName="Helvetica-Bold", fontSize=9,
                               textColor=colors.HexColor(C["dark_text"]), spaceAfter=6)))
            t_det = _build_detail_table(sin_dd_list, table_cell_style)
            if t_det:
                story.append(t_det)
        story.append(Spacer(1, 16))

        # S4: Plan de acción
        story.append(CondPageBreak(220))
        story += section_header("4", "Plan de Acción y Conclusión Ejecutiva")
        story.append(_chart_plan_accion())
        story.append(Spacer(1, 14))

        # 🟢 CONCLUSIÓN
        concl_text = (
            f"El análisis de vinculaciones revela concentraciones de riesgo en el {pct_cruces:.2f}% de la muestra. "
            "Para dar estricto cumplimiento al marco normativo, es prioritario gestionar la actualización de los "
            f"formatos de Conocimiento de Contraparte (avance: {dd_pct_str}), documentando las justificaciones de "
            "los cruces de roles y aplicando los controles compensatorios definidos en el manual de cumplimiento."
        )
        concl_box = Table([[Paragraph(concl_text, conclusion_style)]], colWidths=[W_content])
        concl_box.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(C["dark_text"])),
            ("LEFTPADDING", (0, 0), (-1, -1), 18),
            ("RIGHTPADDING", (0, 0), (-1, -1), 18),
            ("TOPPADDING", (0, 0), (-1, -1), 16),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 16),
        ]))
        story.append(concl_box)

        # 🟢 Observaciones del Oficial de Cumplimiento
        if oficial_conclusion:
            story.append(Spacer(1, 16))
            oficial_title = Paragraph(
                "<b>Conclusiones del Oficial de Cumplimiento:</b>",
                ParagraphStyle("OficialTitle", fontName="Helvetica-Bold", fontSize=10,
                               textColor=colors.HexColor(C["header_bg"]), spaceAfter=6)
            )
            oficial_body = Paragraph(
                f"<i>«{oficial_conclusion}»</i>",
                ParagraphStyle("OficialBody", fontName="Helvetica", fontSize=9, textColor=colors.HexColor(C["slate"]),
                               leading=14)
            )

            oficial_box = Table([[oficial_title], [oficial_body]], colWidths=[W_content])
            oficial_box.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("LINEBEFORE", (0, 0), (0, -1), 3, colors.HexColor(C["teal"])),  # Borde verde a la izquierda
                ("LEFTPADDING", (0, 0), (-1, -1), 14),
                ("RIGHTPADDING", (0, 0), (-1, -1), 14),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]))
            story.append(oficial_box)

        doc.build(story, onFirstPage=on_first_page, onLaterPages=on_later_pages)

    def _save_to_db(self, company_id: int, file_path: str,
                    pdf_content: Optional[bytes]) -> None:
        db = SourceSessionLocal()
        try:
            db_report = GeneratedReport(
                file_path=file_path,
                company_id=company_id,
                pdf_content=pdf_content,
            )
            db.add(db_report)
            db.commit()
            db.refresh(db_report)
        except Exception:
            db.rollback()
        finally:
            try:
                db.close()
            except Exception:
                pass

    def _send_email_with_mailgun(self, to_email: str, pdf_bytes: bytes, filename: str, empresa_nombre: str,
                                 periodo: str) -> bool:
        """Arma un correo en HTML y envía el PDF adjunto vía Mailgun SMTP."""
        from src.core.config2 import settings

        msg = MIMEMultipart()
        msg['From'] = settings.MAIL_FROM
        msg['To'] = to_email
        msg['Subject'] = f"📊 Informe Ejecutivo de Riesgos - {empresa_nombre}"

        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; color: #334155; line-height: 1.6; background-color: #F8FAFC; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; border: 1px solid #E2E8F0; border-radius: 8px; overflow: hidden;">
                <div style="background-color: {C['dark_text']}; padding: 25px; text-align: center; border-bottom: 4px solid {C['header_bg']};">
                    <h2 style="color: #FFFFFF; margin: 0; font-size: 22px;">Informe de Riesgos y Conflictos de Interés</h2>
                </div>
                <div style="padding: 30px;">
                    <p style="font-size: 16px;">Hola,</p>
                    <p style="font-size: 16px;">Adjunto encontrarás el <strong>Informe Ejecutivo de Riesgos</strong> solicitado para <strong>{empresa_nombre}</strong>.</p>

                    <div style="background-color: #F1F5F9; padding: 15px; border-left: 4px solid {C['header_bg']}; margin: 20px 0;">
                        <p style="margin: 0;"><strong>Período analizado:</strong> {periodo}</p>
                    </div>

                    <p style="font-size: 15px; color: #475569;">Este documento contiene la auditoría de relaciones cruzadas, estatus de debida diligencia y focos de concentración económica de la organización.</p>
                    <br>
                    <p style="font-size: 13px; color: #94A3B8; border-top: 1px solid #E2E8F0; padding-top: 15px;">Este es un mensaje automático y seguro generado por la Plataforma Riesgos 365. Por favor no respondas a este correo.</p>
                </div>
            </div>
        </body>
        </html>
        """
        msg.attach(MIMEText(html_body, 'html'))

        pdf_attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(pdf_attachment)

        try:
            print(f"Enviando correo a {to_email} vía Mailgun SMTP...")
            server = smtplib.SMTP(settings.MAILGUN_SMTP_SERVER, settings.MAILGUN_SMTP_PORT)
            server.starttls()
            server.login(settings.MAILGUN_SMTP_LOGIN, settings.MAILGUN_SMTP_PASSWORD)
            server.send_message(msg)
            server.quit()
            print("✅ ¡Correo enviado exitosamente!")
            return True
        except Exception as e:
            print(f"❌ Error enviando correo vía SMTP: {e}")
            return False


pdf_risk_report_service = PDFRiskReportService()
