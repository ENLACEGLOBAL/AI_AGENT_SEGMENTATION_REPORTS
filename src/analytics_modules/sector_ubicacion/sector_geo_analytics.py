# src/analytics_modules/sector_geo/sector_geo_analytics.py
import pandas as pd
from typing import Dict, Any, List


class SectorGeoAnalytics:
    """
    Analítica geográfica para el HTML.
    Recibe transacciones + información FATF cargada desde la BD.
    """

    def __init__(
        self,
        df_transacciones: pd.DataFrame,
        df_historico: pd.DataFrame,
        df_paises_final: pd.DataFrame,
        df_segmentacion: pd.DataFrame
    ):
        self.df = df_transacciones

        # Unificación FATF
        self.df_fatf = self._unificar_fatf(df_historico, df_paises_final, df_segmentacion)

    # ---------------------------------------------------------
    #    NORMALIZAR ORIGEN FATF
    # ---------------------------------------------------------
    def _unificar_fatf(
        self,
        df_historico: pd.DataFrame,
        df_final: pd.DataFrame,
        df_seg: pd.DataFrame
    ) -> pd.DataFrame:

        frames = []

        if not df_historico.empty:
            frames.append(
                df_historico.rename(columns={
                    "pais": "pais",
                    "estatus": "estatus"
                })
            )

        if not df_final.empty:
            frames.append(
                df_final.rename(columns={
                    "pais": "pais",
                    "estatus": "estatus"
                })
            )

        if not df_seg.empty:
            frames.append(
                df_seg.rename(columns={
                    "pais": "pais",
                    "categoria": "estatus"
                })
            )

        df_all = (
            pd.concat(frames, ignore_index=True)
            .dropna(subset=["pais"])
        )

        # Normalización
        df_all["pais"] = df_all["pais"].astype(str).str.strip().str.upper()
        df_all["estatus"] = df_all["estatus"].astype(str).str.strip().str.upper()

        return df_all

    # ---------------------------------------------------------
    # 1. KPIs
    # ---------------------------------------------------------
    def get_kpis(self) -> Dict[str, Any]:
        alto = self.df[self.df["riesgo"].str.upper() == "ALTO"]

        return {
            "total_transacciones": int(len(alto)),
            "empresas_involucradas": int(alto["empresa"].nunique()),
            "monto_total": float(alto["monto"].sum())
        }

    # ---------------------------------------------------------
    # 2. Mapa Colombia
    # ---------------------------------------------------------
    def get_mapa_colombia(self) -> List[Dict[str, Any]]:
        alto = self.df[self.df["riesgo"].str.upper() == "ALTO"]

        return [
            {
                "departamento": row["departamento"],
                "coords": [row["lat"], row["lon"]],
                "empresa": row["empresa"],
                "ciiu": row["ciiu"],
                "monto": float(row["monto"])
            }
            for _, row in alto.iterrows()
        ]

    # ---------------------------------------------------------
    # 3. Mapa Global FATF
    # ---------------------------------------------------------
    def get_fatf_status(self) -> Dict[str, str]:
        return {
            row["pais"]: row["estatus"]
            for _, row in self.df_fatf.iterrows()
        }
