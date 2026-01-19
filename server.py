#!/usr/bin/env python3
"""
Servidor MCP para Análisis de Datos e IA Local
Proporciona herramientas para análisis de CSV/Excel, gráficos, búsqueda web y RAG
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Sequence
from mcp.server import Server
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource
from mcp.server.stdio import stdio_server

# Importar herramientas personalizadas
from tools.data_analysis import DataAnalyzer
from tools.charts import ChartGenerator
from tools.web_search import WebSearcher
from tools.rag_incidents import IncidentRAG

# Configuración
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CHARTS_DIR = BASE_DIR / "charts"
RAG_DIR = BASE_DIR / "rag_db"

# Crear directorios si no existen
DATA_DIR.mkdir(exist_ok=True)
CHARTS_DIR.mkdir(exist_ok=True)
RAG_DIR.mkdir(exist_ok=True)

# Inicializar servidor MCP
app = Server("analytics-server")

# Inicializar herramientas
data_analyzer = DataAnalyzer(DATA_DIR)
chart_generator = ChartGenerator(CHARTS_DIR)
web_searcher = WebSearcher()
incident_rag = IncidentRAG(RAG_DIR)


@app.list_tools()
async def list_tools() -> list[Tool]:
    """Lista todas las herramientas disponibles"""
    return [
        Tool(
            name="list_data_files",
            description="Lista todos los archivos CSV y Excel disponibles en la carpeta de datos",
            inputSchema={
                "type": "object",
                "properties": {},
            }
        ),
        Tool(
            name="analyze_data",
            description="Analiza un archivo CSV o Excel y retorna estadísticas descriptivas, columnas, tipos de datos, etc.",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Nombre del archivo (ej: ventas.csv)"
                    },
                    "preview_rows": {
                        "type": "integer",
                        "description": "Número de filas a previsualizar (default: 5)",
                        "default": 5
                    }
                },
                "required": ["filename"]
            }
        ),
        Tool(
            name="query_data",
            description="Ejecuta consultas SQL sobre un archivo CSV/Excel usando pandas",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Nombre del archivo"
                    },
                    "query": {
                        "type": "string",
                        "description": "Consulta en lenguaje natural (ej: 'suma de ventas por categoría')"
                    }
                },
                "required": ["filename", "query"]
            }
        ),
        Tool(
            name="create_chart",
            description="Crea un gráfico (línea, barra, pastel, scatter, histograma) desde datos CSV/Excel",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "Nombre del archivo de datos"
                    },
                    "chart_type": {
                        "type": "string",
                        "enum": ["line", "bar", "pie", "scatter", "histogram", "heatmap"],
                        "description": "Tipo de gráfico"
                    },
                    "x_column": {
                        "type": "string",
                        "description": "Columna para eje X (o categorías para pie)"
                    },
                    "y_column": {
                        "type": "string",
                        "description": "Columna para eje Y (o valores para pie)"
                    },
                    "title": {
                        "type": "string",
                        "description": "Título del gráfico",
                        "default": "Gráfico"
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filtros opcionales (ej: {'categoria': 'A'})",
                        "default": {}
                    }
                },
                "required": ["filename", "chart_type", "x_column"]
            }
        ),
        Tool(
            name="search_web",
            description="Busca información en internet usando DuckDuckGo. Útil para obtener datos actualizados o información que no está en los archivos locales.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Consulta de búsqueda"
                    },
                    "num_results": {
                        "type": "integer",
                        "description": "Número de resultados (default: 5)",
                        "default": 5
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="load_incidents",
            description="Carga incidencias desde una URL o archivo CSV al sistema RAG para búsquedas semánticas",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "URL de la web con incidencias o nombre del archivo CSV local"
                    },
                    "source_type": {
                        "type": "string",
                        "enum": ["url", "file"],
                        "description": "Tipo de fuente",
                        "default": "url"
                    }
                },
                "required": ["source"]
            }
        ),
        Tool(
            name="search_similar_incidents",
            description="Busca incidencias similares a una nueva usando búsqueda semántica (RAG)",
            inputSchema={
                "type": "object",
                "properties": {
                    "incident_description": {
                        "type": "string",
                        "description": "Descripción de la nueva incidencia"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Número de incidencias similares a retornar",
                        "default": 5
                    }
                },
                "required": ["incident_description"]
            }
        ),
        Tool(
            name="watch_data_changes",
            description="Monitorea cambios en archivos de la carpeta data/ y notifica cuando se actualizan",
            inputSchema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["start", "stop", "status"],
                        "description": "Acción a realizar"
                    }
                },
                "required": ["action"]
            }
        ),
        Tool(
            name="get_chart_image",
            description="Obtiene la imagen de un gráfico generado previamente",
            inputSchema={
                "type": "object",
                "properties": {
                    "chart_name": {
                        "type": "string",
                        "description": "Nombre del archivo del gráfico (sin extensión)"
                    }
                },
                "required": ["chart_name"]
            }
        )
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
    """Ejecuta una herramienta"""
    
    try:
        if name == "list_data_files":
            files = data_analyzer.list_files()
            return [TextContent(
                type="text",
                text=json.dumps(files, indent=2, ensure_ascii=False)
            )]
        
        elif name == "analyze_data":
            result = data_analyzer.analyze_file(
                arguments["filename"],
                preview_rows=arguments.get("preview_rows", 5)
            )
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, ensure_ascii=False)
            )]
        
        elif name == "query_data":
            result = data_analyzer.query_data(
                arguments["filename"],
                arguments["query"]
            )
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, ensure_ascii=False)
            )]
        
        elif name == "create_chart":
            chart_path = chart_generator.create_chart(
                filename=arguments["filename"],
                chart_type=arguments["chart_type"],
                x_column=arguments["x_column"],
                y_column=arguments.get("y_column"),
                title=arguments.get("title", "Gráfico"),
                filters=arguments.get("filters", {})
            )
            return [TextContent(
                type="text",
                text=f"Gráfico creado exitosamente: {chart_path}"
            )]
        
        elif name == "search_web":
            results = await web_searcher.search(
                arguments["query"],
                num_results=arguments.get("num_results", 5)
            )
            return [TextContent(
                type="text",
                text=json.dumps(results, indent=2, ensure_ascii=False)
            )]
        
        elif name == "load_incidents":
            result = await incident_rag.load_incidents(
                source=arguments["source"],
                source_type=arguments.get("source_type", "url")
            )
            return [TextContent(
                type="text",
                text=json.dumps(result, indent=2, ensure_ascii=False)
            )]
        
        elif name == "search_similar_incidents":
            results = incident_rag.search_similar(
                arguments["incident_description"],
                top_k=arguments.get("top_k", 5)
            )
            return [TextContent(
                type="text",
                text=json.dumps(results, indent=2, ensure_ascii=False)
            )]
        
        elif name == "watch_data_changes":
            action = arguments["action"]
            if action == "start":
                data_analyzer.start_watching()
                return [TextContent(type="text", text="Monitoreo de cambios iniciado")]
            elif action == "stop":
                data_analyzer.stop_watching()
                return [TextContent(type="text", text="Monitoreo de cambios detenido")]
            else:
                status = data_analyzer.get_watch_status()
                return [TextContent(type="text", text=json.dumps(status, indent=2))]
        
        elif name == "get_chart_image":
            chart_path = CHARTS_DIR / f"{arguments['chart_name']}.png"
            if chart_path.exists():
                with open(chart_path, "rb") as f:
                    image_data = f.read()
                return [ImageContent(
                    type="image",
                    data=image_data,
                    mimeType="image/png"
                )]
            else:
                return [TextContent(
                    type="text",
                    text=f"Gráfico no encontrado: {arguments['chart_name']}"
                )]
        
        else:
            return [TextContent(
                type="text",
                text=f"Herramienta desconocida: {name}"
            )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=f"Error al ejecutar {name}: {str(e)}"
        )]


async def main():
    """Inicia el servidor MCP"""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    print("🚀 Iniciando servidor MCP de Analytics...")
    print(f"📁 Directorio de datos: {DATA_DIR}")
    print(f"📊 Directorio de gráficos: {CHARTS_DIR}")
    print(f"🔍 Base de datos RAG: {RAG_DIR}")
    asyncio.run(main())