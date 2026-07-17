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
from reportlab.graphics.shapes import Drawing, Rect, String, Group

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
# FOOTER (todas las páginas)
# ══════════════════════════════════════════════════════════════
def _draw_footer(canvas_obj, doc):
    W, H = A4
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
# BANNER DE TÍTULO — se aplica en todas las páginas
# ══════════════════════════════════════════════════════════════
def _draw_title_banner(canvas_obj, doc, empresa_nombre, periodo,
                       tipo_text, logo_path, banner_height=78):
    W, H = A4
    y0 = H - banner_height

    canvas_obj.setFillColor(colors.HexColor(C["header_bg"]))
    canvas_obj.rect(0, y0, W, banner_height, fill=1, stroke=0)

    # Logo anclado a la esquina superior izquierda (pequeño, sin invadir
    # el área centrada del título)
    if logo_path and os.path.exists(logo_path):
        try:
            ir = ImageReader(logo_path)
            iw, ih = ir.getSize()
            lh = 20
            lw = lh * (iw / float(ih))
            if lw > 80:
                lw = 80
                lh = lw * (ih / float(iw))
            lx = 14
            ly = H - 14 - lh
            canvas_obj.drawImage(logo_path, lx, ly, width=lw, height=lh,
                                 preserveAspectRatio=True, mask="auto")
        except Exception:
            pass

    # Título y subtítulo centrados en el ancho completo de la página
    canvas_obj.setFillColor(colors.white)
    canvas_obj.setFont("Helvetica-Bold", 17)
    canvas_obj.drawCentredString(W / 2, y0 + banner_height * 0.60,
                                 "INFORME EJECUTIVO DE RIESGOS")

    canvas_obj.setFont("Helvetica", 9.5)
    sub = f"Empresa: {empresa_nombre}   |   Periodo: {periodo}   |   Tipo de contraparte: {tipo_text}"
    canvas_obj.drawCentredString(W / 2, y0 + banner_height * 0.28, sub)

    _draw_footer(canvas_obj, doc)


# ══════════════════════════════════════════════════════════════
# ESTILOS COMPARTIDOS PARA COMPONENTES NATIVOS (look "dashboard web")
# ══════════════════════════════════════════════════════════════
def _bar_drawing(pct, width=170, height=8, color=None):
    """Barra horizontal simple: fondo gris + relleno proporcional."""
    color = color or C["header_bg"]
    pct = max(0.0, min(100.0, pct))
    d = Drawing(width, height)
    d.add(Rect(0, 0, width, height, rx=height / 2, ry=height / 2,
               fillColor=colors.HexColor("#E9EEF3"), strokeColor=None))
    fill_w = max(height, width * (pct / 100.0)) if pct > 0 else 0
    if fill_w > 0:
        d.add(Rect(0, 0, fill_w, height, rx=height / 2, ry=height / 2,
                   fillColor=colors.HexColor(color), strokeColor=None))
    return d


# ══════════════════════════════════════════════════════════════
# BLOQUE 1 – TARJETAS KPI  (estilo "Panel de control general")
# ══════════════════════════════════════════════════════════════
def _kpi_card(label, value, sub, value_color, accent_color):
    label_style = ParagraphStyle("KL", fontName="Helvetica", fontSize=9.5,
                                 textColor=colors.HexColor(C["dark_text"]),
                                 leading=13)
    value_style = ParagraphStyle("KV", fontName="Helvetica-Bold", fontSize=22,
                                 textColor=colors.HexColor(value_color),
                                 leading=26, spaceBefore=6, spaceAfter=4)
    sub_style = ParagraphStyle("KS", fontName="Helvetica", fontSize=8.5,
                               textColor=colors.HexColor(C["gray"]), leading=11)

    cell = [
        [Paragraph(label, label_style)],
        [Paragraph(value, value_style)],
    ]
    if sub:
        cell.append([Paragraph(sub, sub_style)])

    inner = Table(cell, colWidths=[220])
    inner.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
    ]))

    card = Table([[inner]], colWidths=[236])
    card.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor(C["border"])),
        ("LINEABOVE", (0, 0), (-1, 0), 3, colors.HexColor(accent_color)),
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 16),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 16),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
    ]))
    return card


def _kpi_dd_summary_cards(total_contra, sin_dd_total):
    """Par de tarjetas: Muestra analizada / Contrapartes sin DD."""
    pct_sin_dd = (sin_dd_total / total_contra * 100.0) if total_contra else 0.0

    card1 = _kpi_card(
        "Muestra analizada",
        f"{total_contra:,}".replace(",", "."),
        f"{total_contra:,}".replace(",", ".") + " contrapartes",
        C["dark_text"], C["header_bg"])
    card2 = _kpi_card(
        "Contrapartes sin DD",
        f"{sin_dd_total:,}".replace(",", "."),
        f"{pct_sin_dd:.1f}%".replace(".", ",") + " del total",
        C["pink"], C["pink"])

    row = Table([[card1, card2]], colWidths=[248, 248], hAlign="LEFT")
    row.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    return row


def _clasificar_contrapartes_por_rol(tabla_detalles):
    """Clasifica cada contraparte en su combinación exclusiva de roles
    (cliente/proveedor/empleado) y separa con/sin DD dentro de cada una."""

    def count_val(d):
        d = d or {}
        return int(d.get("count", 0) or d.get("cantidad", 0) or 0)

    buckets = {}
    for e in tabla_detalles or []:
        roles = []
        if count_val(e.get("cliente")) > 0:
            roles.append("cliente")
        if count_val(e.get("proveedor")) > 0:
            roles.append("proveedor")
        if count_val(e.get("empleado")) > 0:
            roles.append("empleado")
        if not roles:
            continue

        key = tuple(roles)
        dd = e.get("dd", False) or e.get("tiene_formulario", False)
        b = buckets.setdefault(key, {"con_dd": 0, "sin_dd": 0})
        if dd:
            b["con_dd"] += 1
        else:
            b["sin_dd"] += 1

    etiquetas = [
        (("cliente",), "Clientes"),
        (("proveedor",), "Proveedores"),
        (("empleado",), "Empleados"),
        (("cliente", "proveedor"), "Cliente + Proveedor"),
        (("cliente", "proveedor", "empleado"), "Cliente + Proveedor + Empleado"),
        (("proveedor", "empleado"), "Proveedor + Empleado"),
        (("cliente", "empleado"), "Cliente + Empleado"),
    ]

    filas = []
    for key, label in etiquetas:
        b = buckets.get(key)
        es_triple = key == ("cliente", "proveedor", "empleado")
        if b is None:
            if es_triple:
                filas.append((label, 0, 0, 0))
            continue
        con_dd, sin_dd = b["con_dd"], b["sin_dd"]
        total = con_dd + sin_dd
        if total == 0 and not es_triple:
            continue
        filas.append((label, con_dd, sin_dd, total))
    return filas


def _tabla_contrapartes_sin_dd(filas):
    """Tabla Rol | Con DD | SIN DD | Total, con fila TOTAL al final."""
    title_style = ParagraphStyle("TRD_T", fontName="Helvetica-Bold", fontSize=10.5,
                                 textColor=colors.HexColor(C["dark_text"]), spaceAfter=8)
    hdr_style = ParagraphStyle("TRD_H", fontName="Helvetica-Bold", fontSize=9,
                               textColor=colors.white, alignment=TA_CENTER)
    label_style = ParagraphStyle("TRD_L", fontName="Helvetica-Bold", fontSize=9,
                                 textColor=colors.HexColor(C["dark_text"]))
    val_style = ParagraphStyle("TRD_V", fontName="Helvetica", fontSize=9,
                               textColor=colors.HexColor(C["dark_text"]), alignment=TA_CENTER)
    sin_dd_style = ParagraphStyle("TRD_S", fontName="Helvetica-Bold", fontSize=9,
                                  textColor=colors.HexColor(C["pink"]), alignment=TA_CENTER)
    total_label_style = ParagraphStyle("TRD_TL", fontName="Helvetica-Bold", fontSize=9,
                                       textColor=colors.HexColor(C["dark_text"]))
    total_val_style = ParagraphStyle("TRD_TV", fontName="Helvetica-Bold", fontSize=9,
                                     textColor=colors.HexColor(C["dark_text"]), alignment=TA_CENTER)

    def fnum(n):
        return f"{n:,}".replace(",", ".") if n else "-"

    rows = [[
        Paragraph("Rol", hdr_style),
        Paragraph("Con DD", hdr_style),
        Paragraph("SIN DD", hdr_style),
        Paragraph("Total", hdr_style),
    ]]

    tot_con_dd = tot_sin_dd = tot_total = 0
    for label, con_dd, sin_dd, total in filas:
        tot_con_dd += con_dd
        tot_sin_dd += sin_dd
        tot_total += total
        rows.append([
            Paragraph(label, label_style),
            Paragraph(fnum(con_dd), val_style),
            Paragraph(fnum(sin_dd), sin_dd_style),
            Paragraph(fnum(total), val_style),
        ])

    rows.append([
        Paragraph("TOTAL", total_label_style),
        Paragraph(fnum(tot_con_dd), total_val_style),
        Paragraph(fnum(tot_sin_dd), total_val_style),
        Paragraph(fnum(tot_total), total_val_style),
    ])

    t = Table(rows, colWidths=[190, 90, 90, 90])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(C["header_bg"])),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LINEABOVE", (0, -1), (-1, -1), 1.2, colors.HexColor(C["header_bg"])),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -2), [colors.white, colors.HexColor("#F8FAFC")]),
    ]))

    wrapper = Table([
        [Paragraph("Contrapartes sin Debida Diligencia", title_style)],
        [t],
    ], colWidths=[460])
    wrapper.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return wrapper


# ══════════════════════════════════════════════════════════════
# BLOQUE 2 – DISTRIBUCIÓN DE MULTI-VÍNCULOS (lista con barras)
# ══════════════════════════════════════════════════════════════
def _distribucion_multivinculos(counts: dict):
    title_style = ParagraphStyle("DT", fontName="Helvetica-Bold", fontSize=11,
                                 textColor=colors.HexColor(C["dark_text"]), spaceAfter=2)
    sub_style = ParagraphStyle("DS", fontName="Helvetica", fontSize=8.5,
                               textColor=colors.HexColor(C["gray"]), spaceAfter=8)
    row_val_style = ParagraphStyle("RV", fontName="Helvetica-Bold", fontSize=9.5,
                                   textColor=colors.HexColor(C["dark_text"]),
                                   alignment=TA_LEFT)

    total = sum(counts.values()) or 1

    def color_for(lbl):
        if "Triple" in lbl:
            return C["pink"]
        if "Cliente" in lbl and "Proveedor" in lbl:
            return C["pink"]
        if "Proveedor" in lbl and "Empleado" in lbl:
            return C["header_bg"]
        return C["teal"]

    tbl_rows = [
        [Paragraph("<b>Distribución de multi-vínculos</b>", title_style), "", ""],
        [Paragraph("Concentración por tipo de relación cruzada", sub_style), "", ""],
    ]

    for lbl, v in counts.items():
        pct = (v / total * 100.0) if total else 0.0
        col = color_for(lbl)
        bar = _bar_drawing(pct if v > 0 else 0, width=230, height=6, color=col)
        right_txt = f"<b>{v} casos - {pct:.0f}%</b>" if v > 0 else "<b>0 Casos</b>"
        tbl_rows.append([
            Paragraph(lbl, row_val_style),
            bar,
            Paragraph(right_txt, ParagraphStyle(
                "RT", fontName="Helvetica-Bold", fontSize=9,
                textColor=colors.HexColor(col if v > 0 else C["gray"]),
                alignment=TA_LEFT)),
        ])

    t = Table(tbl_rows, colWidths=[150, 230, 110])
    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("SPAN", (0, 0), (-1, 0)),
        ("SPAN", (0, 1), (-1, 1)),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, 2), (-1, -2), 0.5, colors.HexColor(C["border"])),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


# ══════════════════════════════════════════════════════════════
# BLOQUE 3 – AVANCE DE DEBIDA DILIGENCIA (barra segmentada)
# ══════════════════════════════════════════════════════════════
def _avance_debida_diligencia(pct_cumplido, pct_pendiente, pct_critico,
                              n_cumplido, n_pendiente, n_critico, n_total):
    sub_style = ParagraphStyle("ADD_S", fontName="Helvetica", fontSize=8.5,
                               textColor=colors.HexColor(C["gray"]), spaceAfter=8)

    width, height = 460, 16
    d = Drawing(width, height)
    d.add(Rect(0, 0, width, height, rx=height / 2, ry=height / 2,
               fillColor=colors.HexColor("#E9EEF3"), strokeColor=None))
    x = 0
    for pct, col in [(pct_cumplido, C["teal"]), (pct_pendiente, C["orange"]),
                     (pct_critico, C["pink"])]:
        w = width * (pct / 100.0)
        if w > 0:
            d.add(Rect(x, 0, w, height, fillColor=colors.HexColor(col), strokeColor=None))
            x += w

    legend_num_style = lambda col: ParagraphStyle(
        f"LN_{col}", fontName="Helvetica-Bold", fontSize=15,
        textColor=colors.HexColor(col), alignment=TA_CENTER)
    legend_lbl_style = ParagraphStyle("LL", fontName="Helvetica-Bold", fontSize=9,
                                      textColor=colors.HexColor(C["dark_text"]),
                                      alignment=TA_CENTER)
    legend_sub_style = ParagraphStyle("LSub", fontName="Helvetica", fontSize=8,
                                      textColor=colors.HexColor(C["gray"]),
                                      alignment=TA_CENTER)

    legend_data = [
        ("Cumplido", C["teal"], pct_cumplido, n_cumplido),
        ("Pendiente", C["orange"], pct_pendiente, n_pendiente),
        ("Crítico", C["pink"], pct_critico, n_critico),
    ]
    legend_row = []
    for lbl, col, pct, n in legend_data:
        cell = Table([
            [Paragraph(lbl, legend_lbl_style)],
            [Paragraph(f"{pct:.0f}%", legend_num_style(col))],
            [Paragraph(f"{n} de {n_total}", legend_sub_style)],
        ], colWidths=[150])
        cell.setStyle(TableStyle([
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))
        legend_row.append(cell)

    legend_tbl = Table([legend_row], colWidths=[150, 150, 150])
    legend_tbl.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LINEBEFORE", (1, 0), (1, 0), 0.5, colors.HexColor(C["border"])),
        ("LINEBEFORE", (2, 0), (2, 0), 0.5, colors.HexColor(C["border"])),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
    ]))

    wrapper = Table([
        [Paragraph("Formatos de conocimiento de contraparte", sub_style)],
        [d],
        [Spacer(1, 10)],
        [legend_tbl],
    ], colWidths=[460])
    wrapper.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return wrapper


# ══════════════════════════════════════════════════════════════
# BLOQUE 4 – PLAN DE ACCIÓN DE CONTRAPARTE (3 tarjetas)
# ══════════════════════════════════════════════════════════════
def _plan_accion_cards():
    steps = [
        ("01", C["pink"], "Intervención inmediata",
         "Actualizar DD en las contrapartes identificadas con mayor exposición."),
        ("02", C["orange"], "Justificación de vínculos",
         "Documentar la justificación de los cruces Cliente – Proveedor."),
        ("03", C["teal"], "Monitoreo activo",
         "Configurar alertas por monto y reglas de re-verificación periódica."),
    ]

    num_style = lambda col: ParagraphStyle(
        f"PN_{col}", fontName="Helvetica-Bold", fontSize=13,
        textColor=colors.HexColor(col))
    title_style = lambda col: ParagraphStyle(
        f"PT_{col}", fontName="Helvetica-Bold", fontSize=10.5,
        textColor=colors.HexColor(col), spaceBefore=4, spaceAfter=4)
    desc_style = ParagraphStyle("PD", fontName="Helvetica", fontSize=8.5,
                                textColor=colors.HexColor(C["slate"]), leading=12)

    cells = []
    for num, col, title, desc in steps:
        inner = Table([
            [Paragraph(num, num_style(col))],
            [Paragraph(title, title_style(col))],
            [Paragraph(desc, desc_style)],
        ], colWidths=[150])
        inner.setStyle(TableStyle([
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
            ("TOPPADDING", (0, 0), (-1, -1), 1),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
        ]))
        card = Table([[inner]], colWidths=[156])
        card.setStyle(TableStyle([
            ("LINEABOVE", (0, 0), (-1, 0), 3, colors.HexColor(col)),
            ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor(C["border"])),
            ("TOPPADDING", (0, 0), (-1, -1), 12),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
            ("LEFTPADDING", (0, 0), (-1, -1), 12),
            ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ]))
        cells.append(card)

    row = Table([cells], colWidths=[168, 168, 168])
    row.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    return row


# ══════════════════════════════════════════════════════════════
# TABLA DETALLE  (estilo dark UX — Top 10)
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

    for r in sin_dd_list[:10]:
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
            # 🟢 CORRECCIÓN: Cambiar TARGET por SOURCE para conectar a dbeg365
            url = form_settings.SOURCE_DATABASE_URL
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

            return {
                "status": "success",
                "file": virtual_path,
                "s3_url": s3_url,
                "empresa_id": int(empresa_id),
                "local_file": None
            }

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

        generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")
        is_filtered = data.get("is_filtered_flag", False)
        if not is_filtered:
            tipo_text = "Clientes, Proveedores y Empleados"
        elif tipo_contraparte:
            tipo_text = tipo_contraparte.capitalize()
        else:
            tipo_text = "N/D"

        # ── Callback ──────────────────────────────────────────────────────
        # El mismo banner de título (logo a la izquierda + "Informe Ejecutivo
        # de Riesgos" + Empresa/Periodo/Tipo) se aplica en TODAS las páginas.
        def on_every_page(cv, doc_obj):
            _draw_title_banner(cv, doc_obj, empresa_nombre=empresa_nombre,
                               periodo=periodo, tipo_text=tipo_text,
                               logo_path=logo_path)

        pdf_title = f"Reporte-{empresa_nombre.replace(' ', '_')}"

        BANNER_H = 78
        TOP_MARGIN = BANNER_H + 18   # deja aire debajo del banner cyan

        # ── Documento (una sola plantilla: banner en todas las páginas) ─────
        doc = SimpleDocTemplate(
            output, pagesize=A4,
            leftMargin=40, rightMargin=40,
            topMargin=TOP_MARGIN, bottomMargin=38,
            title=pdf_title,
            author="Riesgos 365"
        )

        W_content = A4[0] - 80

        footer_style = ParagraphStyle(
            "FooterInfo", fontName="Helvetica", fontSize=9,
            textColor=colors.HexColor(C["dark_text"]), leading=12)

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

        # ── S1: Contrapartes sin Debida Diligencia ──────────────────────────
        story += section_header("1", "Contrapartes sin Debida Diligencia")
        sin_dd_contra_count_s1 = len(sin_dd_list)
        story.append(_kpi_dd_summary_cards(total_contra, sin_dd_contra_count_s1))
        story.append(Spacer(1, 10))
        filas_rol_dd = _clasificar_contrapartes_por_rol(data.get("tabla_detalles", []))
        story.append(_tabla_contrapartes_sin_dd(filas_rol_dd))
        story.append(Spacer(1, 10))
        story.append(_distribucion_multivinculos(counts))
        story.append(Spacer(1, 16))

        # ── S2: Avance de debida diligencia ─────────────────────────────────
        story.append(CondPageBreak(320))
        story += section_header("2", "Avance de debida diligencia")

        alto_riesgo_sin_form = stats_dd.get("alto_riesgo_sin_formulario") or 0
        sin_dd_contra_count = len(sin_dd_list)
        n_total_dd = total_contra or sin_dd_contra_count or 1

        n_critico = int(stats_dd.get("formularios_criticos") or alto_riesgo_sin_form or 0)
        n_cumplido = stats_dd.get("formularios_cumplidos")
        if n_cumplido is None:
            n_cumplido = max(n_total_dd - sin_dd_contra_count, 0)
        n_cumplido = int(n_cumplido)
        n_pendiente = max(n_total_dd - n_cumplido - n_critico, 0)

        pct_cumplido = (n_cumplido / n_total_dd * 100.0) if n_total_dd else 0.0
        pct_critico = (n_critico / n_total_dd * 100.0) if n_total_dd else 0.0
        pct_pendiente = max(100.0 - pct_cumplido - pct_critico, 0.0)

        story.append(_avance_debida_diligencia(
            pct_cumplido, pct_pendiente, pct_critico,
            n_cumplido, n_pendiente, n_critico, n_total_dd))
        story.append(Spacer(1, 16))

        # Tabla de Detalles Top 10 (Contrapartes críticas sin DD)
        if sin_dd_list:
            titulo_tabla = "Contrapartes críticas sin DD — Detalle de contrapartes filtradas (Top 10):" \
                if is_filtered else "Contrapartes críticas sin DD — Top 10 por monto de exposición:"

            story.append(Paragraph(
                f"<b>{titulo_tabla}</b>",
                ParagraphStyle("Sub2", fontName="Helvetica-Bold", fontSize=9,
                               textColor=colors.HexColor(C["dark_text"]), spaceAfter=6)))
            t_det = _build_detail_table(sin_dd_list, table_cell_style)
            if t_det:
                story.append(t_det)
        story.append(Spacer(1, 16))

        # ── S3: Plan de acción de contraparte ────────────────────────────────
        story.append(PageBreak())
        story += section_header("3", "Plan de acción de contraparte")
        story.append(Paragraph(
            "Formatos de conocimiento de contraparte",
            ParagraphStyle("PlanSub", fontName="Helvetica", fontSize=8.5,
                           textColor=colors.HexColor(C["gray"]), spaceAfter=8)))
        story.append(_plan_accion_cards())
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

        story.append(Spacer(1, 10))
        footer_box = Table([
            [Paragraph(f"Fecha de generación: <b>{generated_at}</b>", footer_style),
             Paragraph(f"Tipo contraparte: <b>{tipo_text}</b>", footer_style)],
        ], colWidths=[W_content / 2, W_content / 2])
        footer_box.setStyle(TableStyle([
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ]))
        story.append(footer_box)

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

        doc.build(story, onFirstPage=on_every_page, onLaterPages=on_every_page)

    def _save_to_db(self, company_id: int, file_path: str,
                    pdf_content: Optional[bytes]) -> None:
        db = TargetSessionLocal()
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
