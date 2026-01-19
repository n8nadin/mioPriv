"""
Módulo para análisis de archivos CSV y Excel
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time


class DataFileHandler(FileSystemEventHandler):
    """Maneja eventos de cambios en archivos"""
    
    def __init__(self, callback):
        self.callback = callback
    
    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(('.csv', '.xlsx', '.xls')):
            self.callback(event.src_path)


class DataAnalyzer:
    """Analiza archivos CSV y Excel"""
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.observer = None
        self.watching = False
        self.last_changes = []
    
    def list_files(self) -> Dict[str, List[str]]:
        """Lista todos los archivos de datos disponibles"""
        csv_files = list(self.data_dir.glob("*.csv"))
        excel_files = list(self.data_dir.glob("*.xlsx")) + list(self.data_dir.glob("*.xls"))
        json_files = list(self.data_dir.glob("*.json"))
        
        return {
            "csv_files": [f.name for f in csv_files],
            "excel_files": [f.name for f in excel_files],
            "json_files": [f.name for f in json_files],
            "total": len(csv_files) + len(excel_files) + len(json_files)
        }
    
    def _load_file(self, filename: str) -> pd.DataFrame:
        """Carga un archivo CSV, Excel o JSON"""
        filepath = self.data_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")
        
        if filename.endswith('.csv'):
            # Intentar diferentes encodings
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                try:
                    return pd.read_csv(filepath, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            raise ValueError(f"No se pudo leer el archivo CSV con los encodings soportados")
        
        elif filename.endswith(('.xlsx', '.xls')):
            return pd.read_excel(filepath)
        
        elif filename.endswith('.json'):
            # Intentar leer JSON de diferentes formas
            try:
                # Primero intentar como tabla directa
                df = pd.read_json(filepath)
                return df
            except:
                # Si falla, intentar como estructura anidada
                import json
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Si es un diccionario con una lista dentro
                if isinstance(data, dict):
                    for key, value in data.items():
                        if isinstance(value, list):
                            return pd.DataFrame(value)
                
                # Si es una lista directa
                if isinstance(data, list):
                    return pd.DataFrame(data)
                
                raise ValueError("Formato JSON no reconocido")
        
        else:
            raise ValueError(f"Formato de archivo no soportado: {filename}")
    
    def analyze_file(self, filename: str, preview_rows: int = 5) -> Dict[str, Any]:
        """Analiza un archivo y retorna estadísticas"""
        df = self._load_file(filename)
        
        # Información básica
        info = {
            "filename": filename,
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024**2,
            "missing_values": df.isnull().sum().to_dict(),
            "preview": df.head(preview_rows).to_dict(orient='records')
        }
        
        # Estadísticas para columnas numéricas
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            stats_df = df[numeric_cols].describe()
            # Convertir a dict limpiando NaN
            info["statistics"] = {}
            for col in stats_df.columns:
                info["statistics"][col] = {
                    stat: (None if pd.isna(val) else float(val))
                    for stat, val in stats_df[col].items()
                }
        
        # Valores únicos para columnas categóricas (solo primeras 50)
        categorical_cols = df.select_dtypes(include=['object']).columns[:50]
        if len(categorical_cols) > 0:
            info["unique_values"] = {
                col: {
                    "count": int(df[col].nunique()),
                    "top_5": df[col].value_counts().head(5).to_dict()
                }
                for col in categorical_cols
            }
        
        return info
    
    def query_data(self, filename: str, query: str) -> Dict[str, Any]:
        """
        Ejecuta consultas en lenguaje natural sobre los datos
        Traduce la consulta a operaciones pandas
        """
        df = self._load_file(filename)
        
        # Parsear consulta simple (puedes expandir esto con NLP más avanzado)
        query_lower = query.lower()
        
        try:
            # Suma por grupo
            if "suma" in query_lower and "por" in query_lower:
                parts = query_lower.split("por")
                value_col = self._extract_column_name(parts[0], df)
                group_col = self._extract_column_name(parts[1], df)
                
                result = df.groupby(group_col)[value_col].sum().to_dict()
                return {
                    "query": query,
                    "result_type": "grouped_sum",
                    "result": result
                }
            
            # Promedio
            elif "promedio" in query_lower or "media" in query_lower:
                parts = query_lower.split("por") if "por" in query_lower else [query_lower]
                value_col = self._extract_column_name(parts[0], df)
                
                if len(parts) > 1:
                    group_col = self._extract_column_name(parts[1], df)
                    result = df.groupby(group_col)[value_col].mean().to_dict()
                else:
                    result = float(df[value_col].mean())
                
                return {
                    "query": query,
                    "result_type": "average",
                    "result": result
                }
            
            # Contar
            elif "contar" in query_lower or "cuenta" in query_lower:
                if "por" in query_lower:
                    parts = query_lower.split("por")
                    group_col = self._extract_column_name(parts[1], df)
                    result = df[group_col].value_counts().to_dict()
                else:
                    result = len(df)
                
                return {
                    "query": query,
                    "result_type": "count",
                    "result": result
                }
            
            # Máximo/Mínimo
            elif "máximo" in query_lower or "maximo" in query_lower or "max" in query_lower:
                col = self._extract_column_name(query_lower, df)
                result = {
                    "value": float(df[col].max()),
                    "row": df[df[col] == df[col].max()].to_dict(orient='records')[0]
                }
                return {
                    "query": query,
                    "result_type": "maximum",
                    "result": result
                }
            
            elif "mínimo" in query_lower or "minimo" in query_lower or "min" in query_lower:
                col = self._extract_column_name(query_lower, df)
                result = {
                    "value": float(df[col].min()),
                    "row": df[df[col] == df[col].min()].to_dict(orient='records')[0]
                }
                return {
                    "query": query,
                    "result_type": "minimum",
                    "result": result
                }
            
            # Filtrar
            elif "filtrar" in query_lower or "donde" in query_lower or "con" in query_lower:
                # Extrae condiciones básicas
                filtered_df = self._apply_filters(df, query_lower)
                return {
                    "query": query,
                    "result_type": "filtered_data",
                    "result": {
                        "count": len(filtered_df),
                        "data": filtered_df.head(10).to_dict(orient='records')
                    }
                }
            
            else:
                return {
                    "query": query,
                    "error": "No se pudo interpretar la consulta. Intenta con: 'suma de X por Y', 'promedio de X', 'contar por Y', etc."
                }
        
        except Exception as e:
            return {
                "query": query,
                "error": f"Error al ejecutar consulta: {str(e)}"
            }
    
    def _extract_column_name(self, text: str, df: pd.DataFrame) -> str:
        """Extrae el nombre de columna del texto"""
        text = text.lower().strip()
        
        # Remover palabras comunes
        for word in ["suma", "promedio", "media", "de", "del", "la", "el", "por", "contar", "cuenta"]:
            text = text.replace(word, "")
        
        text = text.strip()
        
        # Buscar coincidencia exacta o parcial con columnas
        for col in df.columns:
            if text in col.lower() or col.lower() in text:
                return col
        
        # Si no encuentra, usar la primera columna numérica
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            return numeric_cols[0]
        
        return df.columns[0]
    
    def _apply_filters(self, df: pd.DataFrame, query: str) -> pd.DataFrame:
        """Aplica filtros básicos a un DataFrame"""
        # Implementación simple - puedes expandir
        filtered = df.copy()
        
        # Buscar patrones como "categoria = A" o "precio > 100"
        import re
        patterns = re.findall(r'(\w+)\s*([><=]+)\s*(["\']?\w+["\']?)', query)
        
        for col, op, val in patterns:
            if col in df.columns:
                val = val.strip('"').strip("'")
                try:
                    val = float(val)
                except:
                    pass
                
                if op == '==' or op == '=':
                    filtered = filtered[filtered[col] == val]
                elif op == '>':
                    filtered = filtered[filtered[col] > val]
                elif op == '<':
                    filtered = filtered[filtered[col] < val]
                elif op == '>=':
                    filtered = filtered[filtered[col] >= val]
                elif op == '<=':
                    filtered = filtered[filtered[col] <= val]
        
        return filtered
    
    def start_watching(self):
        """Inicia el monitoreo de cambios en archivos"""
        if not self.watching:
            event_handler = DataFileHandler(self._on_file_changed)
            self.observer = Observer()
            self.observer.schedule(event_handler, str(self.data_dir), recursive=False)
            self.observer.start()
            self.watching = True
    
    def stop_watching(self):
        """Detiene el monitoreo de cambios"""
        if self.watching and self.observer:
            self.observer.stop()
            self.observer.join()
            self.watching = False
    
    def _on_file_changed(self, filepath: str):
        """Callback cuando un archivo cambia"""
        self.last_changes.append({
            "filepath": filepath,
            "timestamp": time.time()
        })
    
    def get_watch_status(self) -> Dict[str, Any]:
        """Retorna el estado del monitoreo"""
        return {
            "watching": self.watching,
            "last_changes": self.last_changes[-10:]  # Últimos 10 cambios
        }