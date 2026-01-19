#!/usr/bin/env python3
"""
MCP HTTP Bridge - Convierte servidor MCP en API HTTP REST

Permite que el servidor MCP sea accesible remotamente vía HTTP
en lugar de solo por stdin/stdout (que no es remoto)

Uso:
    python mcp_http_bridge.py
    
Escucha en:
    http://0.0.0.0:9000
    
Los clientes (como web_dashboard_v2.py) pueden conectar a:
    http://[IP_SERVIDOR]:9000
"""

from flask import Flask, request, jsonify
import subprocess
import json
from pathlib import Path
import sys
import asyncio
from typing import Any, Dict

# Importar herramientas localmente (en el servidor)
sys.path.insert(0, str(Path(__file__).parent))
from tools.data_analysis import DataAnalyzer
from tools.charts import ChartGenerator
from tools.web_search import WebSearcher
from tools.rag_incidents import IncidentRAG

app = Flask(__name__)

# Configuración
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CHARTS_DIR = BASE_DIR / "charts"
RAG_DIR = BASE_DIR / "rag_db"

# Crear directorios
DATA_DIR.mkdir(exist_ok=True)
CHARTS_DIR.mkdir(exist_ok=True)
RAG_DIR.mkdir(exist_ok=True)

# Inicializar herramientas (en servidor)
data_analyzer = DataAnalyzer(DATA_DIR)
chart_generator = ChartGenerator(CHARTS_DIR)
web_searcher = WebSearcher()
incident_rag = IncidentRAG(RAG_DIR)

print("""
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║        🌐 MCP HTTP BRIDGE - Servidor de Herramientas    ║
║                                                           ║
║        Escuchando en: 0.0.0.0:9000                       ║
║                                                           ║
║        Los clientes pueden conectar a:                   ║
║        http://[IP_SERVIDOR]:9000                         ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
""")


# =================== RUTAS BÁSICAS ===================

@app.route('/health', methods=['GET'])
def health():
    """Verifica que el servidor está activo"""
    return jsonify({
        'status': 'online',
        'service': 'MCP HTTP Bridge',
        'version': '1.0'
    })


@app.route('/api/tools', methods=['GET'])
def list_tools():
    """Lista todas las herramientas disponibles"""
    return jsonify({
        'success': True,
        'tools': [
            {
                'name': 'list_data_files',
                'description': 'Lista archivos CSV, Excel y JSON disponibles'
            },
            {
                'name': 'analyze_data',
                'description': 'Analiza un archivo y retorna estadísticas'
            },
            {
                'name': 'query_data',
                'description': 'Ejecuta consultas en lenguaje natural sobre datos'
            },
            {
                'name': 'create_chart',
                'description': 'Crea gráficos (línea, barra, pastel, scatter, etc)'
            },
            {
                'name': 'search_web',
                'description': 'Busca información en internet'
            },
            {
                'name': 'load_incidents',
                'description': 'Carga incidencias al sistema RAG'
            },
            {
                'name': 'search_similar_incidents',
                'description': 'Busca incidencias similares usando RAG'
            },
            {
                'name': 'rag_stats',
                'description': 'Obtiene estadísticas de la base de datos RAG'
            }
        ]
    })


# =================== HERRAMIENTAS: DATA ANALYSIS ===================

@app.route('/api/tool/list_data_files', methods=['POST'])
def tool_list_data_files():
    """Lista archivos disponibles"""
    try:
        result = data_analyzer.list_files()
        return jsonify({
            'success': True,
            'result': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


@app.route('/api/tool/analyze_data', methods=['POST'])
def tool_analyze_data():
    """Analiza un archivo"""
    data = request.json or {}
    args = data.get('arguments', {})
    
    try:
        filename = args.get('filename')
        preview_rows = args.get('preview_rows', 5)
        
        if not filename:
            return jsonify({
                'success': False,
                'error': 'filename es requerido'
            }), 400
        
        result = data_analyzer.analyze_file(filename, preview_rows=preview_rows)
        
        # Limpiar NaN
        import math
        def clean_nans(obj):
            if isinstance(obj, dict):
                return {k: clean_nans(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nans(item) for item in obj]
            elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            return obj
        
        result = clean_nans(result)
        
        return jsonify({
            'success': True,
            'result': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


@app.route('/api/tool/query_data', methods=['POST'])
def tool_query_data():
    """Ejecuta consultas sobre datos"""
    data = request.json or {}
    args = data.get('arguments', {})
    
    try:
        filename = args.get('filename')
        query = args.get('query')
        
        if not filename or not query:
            return jsonify({
                'success': False,
                'error': 'filename y query son requeridos'
            }), 400
        
        result = data_analyzer.query_data(filename, query)
        
        return jsonify({
            'success': True,
            'result': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


# =================== HERRAMIENTAS: GRÁFICOS ===================

@app.route('/api/tool/create_chart', methods=['POST'])
def tool_create_chart():
    """Crea un gráfico"""
    data = request.json or {}
    args = data.get('arguments', {})
    
    try:
        result = chart_generator.create_chart(
            filename=args.get('filename'),
            chart_type=args.get('chart_type'),
            x_column=args.get('x_column'),
            y_column=args.get('y_column'),
            title=args.get('title', 'Gráfico'),
            filters=args.get('filters', {})
        )
        
        return jsonify({
            'success': True,
            'result': {
                'chart_path': result,
                'chart_name': Path(result).stem
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


# =================== HERRAMIENTAS: WEB SEARCH ===================

@app.route('/api/tool/search_web', methods=['POST'])
def tool_search_web():
    """Busca en internet"""
    data = request.json or {}
    args = data.get('arguments', {})
    
    try:
        query = args.get('query')
        num_results = args.get('num_results', 5)
        
        if not query:
            return jsonify({
                'success': False,
                'error': 'query es requerido'
            }), 400
        
        # Ejecutar búsqueda async
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(
            web_searcher.search(query, num_results=num_results)
        )
        loop.close()
        
        return jsonify({
            'success': True,
            'result': {
                'query': query,
                'results': results,
                'count': len(results)
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


# =================== HERRAMIENTAS: RAG ===================

@app.route('/api/tool/load_incidents', methods=['POST'])
def tool_load_incidents():
    """Carga incidencias al RAG"""
    data = request.json or {}
    args = data.get('arguments', {})
    
    try:
        source = args.get('source')
        source_type = args.get('source_type', 'file')
        
        if not source:
            return jsonify({
                'success': False,
                'error': 'source es requerido'
            }), 400
        
        # Ejecutar async
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(
            incident_rag.load_incidents(source, source_type)
        )
        loop.close()
        
        return jsonify({
            'success': result.get('success', False),
            'result': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


@app.route('/api/tool/search_similar_incidents', methods=['POST'])
def tool_search_similar_incidents():
    """Busca incidencias similares"""
    data = request.json or {}
    args = data.get('arguments', {})
    
    try:
        description = args.get('incident_description')
        top_k = args.get('top_k', 5)
        
        if not description:
            return jsonify({
                'success': False,
                'error': 'incident_description es requerido'
            }), 400
        
        result = incident_rag.search_similar(description, top_k=top_k)
        
        return jsonify({
            'success': True,
            'result': result
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


@app.route('/api/tool/rag_stats', methods=['POST'])
def tool_rag_stats():
    """Obtiene estadísticas RAG"""
    try:
        stats = incident_rag.get_stats()
        
        return jsonify({
            'success': True,
            'result': stats
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


# =================== RUTAS ADICIONALES ===================

@app.route('/api/chart/<chart_name>', methods=['GET'])
def get_chart(chart_name):
    """Obtiene un gráfico generado"""
    chart_path = CHARTS_DIR / f"{chart_name}.html"
    
    if not chart_path.exists():
        return jsonify({'error': 'Gráfico no encontrado'}), 404
    
    from flask import send_file
    return send_file(chart_path, mimetype='text/html')


@app.route('/api/status', methods=['GET'])
def status():
    """Estado completo del servidor"""
    try:
        files = data_analyzer.list_files()
        rag_stats = incident_rag.get_stats()
        
        return jsonify({
            'success': True,
            'server': {
                'status': 'online',
                'port': 9000
            },
            'data': {
                'csv_files': len(files['csv_files']),
                'excel_files': len(files['excel_files']),
                'json_files': len(files['json_files'])
            },
            'rag': {
                'total_incidents': rag_stats.get('total_incidents', 0),
                'ready': rag_stats.get('rag_ready', False)
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400


# =================== ERROR HANDLERS ===================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Ruta no encontrada',
        'available_routes': [
            'GET /health',
            'GET /api/tools',
            'GET /api/status',
            'POST /api/tool/[tool_name]'
        ]
    }), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({
        'success': False,
        'error': 'Error interno del servidor'
    }), 500


if __name__ == '__main__':
    print(f"""
📁 Directorio de datos: {DATA_DIR}
📊 Directorio de gráficos: {CHARTS_DIR}
🔍 Base de datos RAG: {RAG_DIR}

🚀 Iniciando servidor...
    """)
    
    app.run(
        host='0.0.0.0',
        port=9000,
        debug=False,  # False en producción
        use_reloader=False
    )
