"""
Módulo para generar gráficos desde datos CSV/Excel
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from typing import Dict, Optional, Any
import hashlib
from datetime import datetime


class ChartGenerator:
    """Genera gráficos interactivos con Plotly"""
    
    def __init__(self, charts_dir: Path):
        self.charts_dir = Path(charts_dir)
        self.data_dir = self.charts_dir.parent / "data"
    
    def _load_data(self, filename: str) -> pd.DataFrame:
        """Carga datos desde CSV o Excel"""
        filepath = self.data_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")
        
        if filename.endswith('.csv'):
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                try:
                    return pd.read_csv(filepath, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            raise ValueError("No se pudo leer el archivo CSV")
        
        elif filename.endswith(('.xlsx', '.xls')):
            return pd.read_excel(filepath)
        
        raise ValueError(f"Formato no soportado: {filename}")
    
    def create_chart(
        self,
        filename: str,
        chart_type: str,
        x_column: str,
        y_column: Optional[str] = None,
        title: str = "Gráfico",
        filters: Dict[str, Any] = None
    ) -> str:
        """
        Crea un gráfico y lo guarda como HTML e imagen PNG
        
        Args:
            filename: Nombre del archivo de datos
            chart_type: Tipo de gráfico (line, bar, pie, scatter, histogram, heatmap)
            x_column: Columna para eje X
            y_column: Columna para eje Y (opcional para algunos tipos)
            title: Título del gráfico
            filters: Diccionario de filtros {columna: valor}
        
        Returns:
            Ruta del archivo HTML generado
        """
        # Cargar datos
        df = self._load_data(filename)
        
        # Aplicar filtros si existen
        if filters:
            for col, val in filters.items():
                if col in df.columns:
                    df = df[df[col] == val]
        
        # Generar nombre único para el gráfico
        chart_id = self._generate_chart_id(filename, chart_type, x_column, y_column, filters)
        html_path = self.charts_dir / f"{chart_id}.html"
        png_path = self.charts_dir / f"{chart_id}.png"
        
        # Crear el gráfico según el tipo
        fig = None
        
        if chart_type == "line":
            if y_column:
                fig = px.line(df, x=x_column, y=y_column, title=title)
            else:
                fig = px.line(df, x=x_column, title=title)
        
        elif chart_type == "bar":
            if y_column:
                # Agrupar datos si es necesario
                if df[x_column].dtype == 'object':
                    grouped = df.groupby(x_column)[y_column].sum().reset_index()
                    fig = px.bar(grouped, x=x_column, y=y_column, title=title)
                else:
                    fig = px.bar(df, x=x_column, y=y_column, title=title)
            else:
                # Gráfico de frecuencias
                value_counts = df[x_column].value_counts().reset_index()
                value_counts.columns = [x_column, 'count']
                fig = px.bar(value_counts, x=x_column, y='count', title=title)
        
        elif chart_type == "pie":
            # Para gráfico de pastel, x es categorías y y es valores
            if y_column:
                grouped = df.groupby(x_column)[y_column].sum().reset_index()
                fig = px.pie(grouped, names=x_column, values=y_column, title=title)
            else:
                value_counts = df[x_column].value_counts().reset_index()
                value_counts.columns = [x_column, 'count']
                fig = px.pie(value_counts, names=x_column, values='count', title=title)
        
        elif chart_type == "scatter":
            if not y_column:
                raise ValueError("El gráfico scatter requiere columna Y")
            fig = px.scatter(df, x=x_column, y=y_column, title=title)
        
        elif chart_type == "histogram":
            fig = px.histogram(df, x=x_column, title=title)
        
        elif chart_type == "heatmap":
            # Crear matriz de correlación o pivotear datos
            if y_column and x_column in df.columns and y_column in df.columns:
                pivot_table = df.pivot_table(
                    values=df.columns[2] if len(df.columns) > 2 else df.columns[0],
                    index=x_column,
                    columns=y_column,
                    aggfunc='sum'
                )
                fig = px.imshow(
                    pivot_table,
                    labels=dict(x=y_column, y=x_column, color="Valor"),
                    title=title
                )
            else:
                # Mapa de calor de correlación
                numeric_cols = df.select_dtypes(include=['number']).columns
                corr_matrix = df[numeric_cols].corr()
                fig = px.imshow(
                    corr_matrix,
                    labels=dict(color="Correlación"),
                    title=title,
                    text_auto=True
                )
        
        else:
            raise ValueError(f"Tipo de gráfico no soportado: {chart_type}")
        
        # Personalizar diseño
        fig.update_layout(
            template="plotly_white",
            font=dict(size=12),
            showlegend=True,
            hovermode='closest'
        )
        
        # Guardar como HTML (interactivo)
        fig.write_html(str(html_path))
        
        # Guardar como PNG (estático)
        try:
            fig.write_image(str(png_path), width=1200, height=600)
        except Exception as e:
            print(f"No se pudo guardar PNG: {e}")
            # PNG requiere kaleido: pip install kaleido
        
        return str(html_path)
    
    def _generate_chart_id(
        self,
        filename: str,
        chart_type: str,
        x_column: str,
        y_column: Optional[str],
        filters: Optional[Dict]
    ) -> str:
        """Genera un ID único para el gráfico basado en sus parámetros"""
        # Crear string con todos los parámetros
        params = f"{filename}_{chart_type}_{x_column}_{y_column}_{filters}"
        
        # Hash MD5 para ID corto
        hash_obj = hashlib.md5(params.encode())
        hash_id = hash_obj.hexdigest()[:8]
        
        # Timestamp para evitar colisiones
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        return f"chart_{chart_type}_{hash_id}_{timestamp}"
    
    def list_charts(self) -> Dict[str, list]:
        """Lista todos los gráficos generados"""
        html_files = list(self.charts_dir.glob("*.html"))
        png_files = list(self.charts_dir.glob("*.png"))
        
        return {
            "html_charts": [f.name for f in html_files],
            "png_charts": [f.name for f in png_files],
            "total": len(html_files)
        }