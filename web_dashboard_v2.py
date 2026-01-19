#!/usr/bin/env python3
"""
Web Dashboard v2 - Interfaz ligera conectada a servidor MCP remoto

En lugar de cargar todas las herramientas localmente,
conecta al servidor MCP via HTTP para ejecutar operaciones.

Configuración:
    MCP_SERVER_URL = "http://[IP_SERVIDOR]:9000"
    
Uso:
    python web_dashboard_v2.py
    
Acceso:
    http://localhost:5000
"""

from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import requests
import asyncio
from pathlib import Path
from datetime import datetime

app = Flask(__name__)
CORS(app)

# =================== CONFIGURACIÓN ===================

BASE_DIR = Path(__file__).parent

# 🔴 CAMBIAR ESTO a tu servidor potente
MCP_SERVER_URL = "http://192.168.30.13:9000"  # ← CAMBIAR AQUÍ

# Ollamaa
OLLAMA_URL = "http://192.168.30.13:11434"

# n8n
N8N_BASE = "http://localhost:5678"
N8N_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJjOWU4YzViZS1jZjFjLTQyNGYtOTkyMC02NjU2ZjJhMTgxOTkiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzY1OTgxOTc0fQ.-RLJ1lSDnXAkJEV4DsGhJlH_0o7wDzt1fsAMYQ_SsUs"

# Historial de conversación
conversation_history = []

print(f"""
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║        🏢 CUARTEL GENERAL IA - WEB DASHBOARD v2           ║
║        (Conectado a servidor remoto)                      ║
║                                                           ║
║        Accede a: http://localhost:5000                   ║
║                                                           ║
║        Servidor MCP: {MCP_SERVER_URL}
║        Ollama: {OLLAMA_URL}
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
""")


# =================== RUTAS ===================

@app.route('/')
def index():
    """Página principal"""
    return render_template('dashboard.html')


@app.route('/static/<path:filename>')
def serve_static(filename):
    """Sirve archivos estáticos"""
    static_dir = BASE_DIR / 'static'
    return send_file(static_dir / filename)


# =================== PROXY A SERVIDOR MCP ===================

def call_mcp_tool(tool_name: str, arguments: dict):
    """
    Llama una herramienta en el servidor MCP remoto
    
    Args:
        tool_name: Nombre de la herramienta
        arguments: Argumentos para la herramienta
    
    Returns:
        dict con resultado o error
    """
    try:
        response = requests.post(
            f"{MCP_SERVER_URL}/api/tool/{tool_name}",
            json={"arguments": arguments},
            timeout=120
        )
        
        data = response.json()
        
        if not data.get('success', False):
            return {
                'error': data.get('error', 'Error desconocido'),
                'success': False
            }
        
        return {
            'success': True,
            'result': data.get('result')
        }
    
    except requests.exceptions.ConnectionError:
        return {
            'error': f'No se puede conectar al servidor MCP en {MCP_SERVER_URL}',
            'success': False
        }
    except requests.exceptions.Timeout:
        return {
            'error': 'Timeout: El servidor tardó demasiado en responder',
            'success': False
        }
    except Exception as e:
        return {
            'error': f'Error al llamar herramienta: {str(e)}',
            'success': False
        }


# =================== OLLAMA MODELS ===================

@app.route('/api/ollama/models', methods=['GET'])
def get_ollama_models():
    """Obtiene modelos disponibles en Ollama"""
    try:
        response = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
        if response.status_code == 200:
            models = response.json().get('models', [])
            return jsonify({
                'success': True,
                'models': [m['name'] for m in models]
            })
        return jsonify({'success': False, 'error': 'No se pudo conectar a Ollama'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# =================== CHAT ===================

@app.route('/api/chat', methods=['POST'])
def chat():
    """Endpoint principal del chat"""
    data = request.json
    message = data.get('message', '')
    model = data.get('model', 'llama3.2')
    use_history = data.get('use_history', True)
    
    if not message:
        return jsonify({'error': 'Mensaje vacío'}), 400
    
    try:
        # Detectar si necesita herramientas
        tool_result = detect_and_use_tool(message)
        
        # Preparar contexto
        context = build_context(message, tool_result)
        
        # Preparar mensajes para Ollama
        messages = []
        
        # System prompt
        messages.append({
            'role': 'system',
            'content': get_system_prompt()
        })
        
        # Historial
        if use_history and conversation_history:
            for msg in conversation_history[-6:]:
                messages.append(msg)
        
        # Contexto de herramientas
        if context:
            messages.append({
                'role': 'system',
                'content': f"Información obtenida de herramientas:\n\n{context}"
            })
        
        # Mensaje del usuario
        messages.append({
            'role': 'user',
            'content': message
        })
        
        # Llamar a Ollama
        response = requests.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                'model': model,
                'messages': messages,
                'stream': False
            },
            timeout=120
        )
        
        if response.status_code == 200:
            ollama_response = response.json()
            assistant_message = ollama_response['message']['content']
            
            # Guardar en historial
            if use_history:
                conversation_history.append({
                    'role': 'user',
                    'content': message
                })
                conversation_history.append({
                    'role': 'assistant',
                    'content': assistant_message
                })
                
                # Limitar historial
                if len(conversation_history) > 20:
                    conversation_history.pop(0)
                    conversation_history.pop(0)
            
            return jsonify({
                'success': True,
                'response': assistant_message,
                'model': model,
                'tool_used': tool_result.get('tool') if tool_result else None
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Error de Ollama: {response.status_code}'
            })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/chat/clear', methods=['POST'])
def clear_history():
    """Limpia historial de conversación"""
    global conversation_history
    conversation_history = []
    return jsonify({'success': True})


def get_system_prompt():
    """System prompt para la IA"""
    return """Eres un asistente de análisis de datos interno de una empresa. Ayudas a analizar archivos CSV, Excel, crear visualizaciones y buscar información.

Capacidades:
- Analizar archivos de datos (CSV, Excel)
- Crear gráficos y visualizaciones
- Buscar información en internet
- Buscar incidencias similares en la base de datos RAG
- Responder preguntas sobre los datos

Cuando el usuario pida analizar datos, crear gráficos o buscar información, ya tendrás acceso a los resultados de las herramientas correspondientes.

Sé conciso, profesional y útil. Si te proporcionan datos de herramientas, úsalos para dar respuestas precisas."""


def detect_and_use_tool(message):
    """Detecta qué herramienta usar"""
    message_lower = message.lower()
    
    # Listar archivos
    if any(word in message_lower for word in ['listar archivos', 'qué archivos', 'archivos disponibles']):
        result = call_mcp_tool('list_data_files', {})
        if result.get('success'):
            return {
                'tool': 'list_files',
                'result': result.get('result')
            }
    
    # Analizar archivo
    if any(word in message_lower for word in ['analiza', 'análisis', 'analizar']):
        # Primero obtener lista de archivos
        files_result = call_mcp_tool('list_data_files', {})
        if files_result.get('success'):
            files = files_result.get('result', {})
            all_files = files.get('csv_files', []) + files.get('excel_files', [])
            
            for f in all_files:
                if f.lower() in message_lower or f.replace('.csv', '').replace('.xlsx', '').lower() in message_lower:
                    result = call_mcp_tool('analyze_data', {
                        'filename': f,
                        'preview_rows': 5
                    })
                    if result.get('success'):
                        return {
                            'tool': 'analyze_file',
                            'result': result.get('result')
                        }
    
    # Búsqueda web
    if any(word in message_lower for word in ['busca en internet', 'buscar en internet', 'google', 'busca información']):
        query = message.replace('busca', '').replace('buscar', '').replace('en internet', '').replace('información sobre', '').strip()
        if query:
            result = call_mcp_tool('search_web', {
                'query': query,
                'num_results': 3
            })
            if result.get('success'):
                return {
                    'tool': 'web_search',
                    'query': query,
                    'result': result.get('result')
                }
    
    # RAG - búsqueda de incidencias
    if any(word in message_lower for word in ['incidencia', 'similar', 'parecido', 'incidente']):
        result = call_mcp_tool('search_similar_incidents', {
            'incident_description': message,
            'top_k': 3
        })
        if result.get('success'):
            return {
                'tool': 'rag_search',
                'result': result.get('result')
            }
    
    return None


def build_context(message, tool_result):
    """Construye contexto basado en herramientas usadas"""
    if not tool_result:
        return None
    
    tool = tool_result.get('tool')
    
    if tool == 'list_files':
        result = tool_result.get('result', {})
        return f"""Archivos disponibles:
- CSV: {', '.join(result.get('csv_files', [])) if result.get('csv_files') else 'Ninguno'}
- Excel: {', '.join(result.get('excel_files', [])) if result.get('excel_files') else 'Ninguno'}
Total: {result.get('total', 0)} archivos"""
    
    elif tool == 'analyze_file':
        result = tool_result.get('result', {})
        return f"""Análisis de {result.get('filename', '')}:
- Filas: {result.get('rows', 0):,}
- Columnas: {result.get('columns', 0)}
- Nombres: {', '.join(result.get('column_names', []))}
- Memoria: {result.get('memory_usage_mb', 0):.2f} MB"""
    
    elif tool == 'web_search':
        results = tool_result.get('result', {}).get('results', [])
        if not results:
            return "No se pudieron obtener resultados de búsqueda"
        
        search_text = f"Resultados de búsqueda para '{tool_result.get('query', '')}':\n\n"
        for i, r in enumerate(results[:3], 1):
            search_text += f"{i}. {r.get('title', '')}\n   {r.get('snippet', '')[:200]}\n"
        return search_text
    
    elif tool == 'rag_search':
        result = tool_result.get('result', {})
        incidents = result.get('similar_incidents', [])
        if not incidents:
            return "No se encontraron incidencias similares"
        
        rag_text = "Incidencias similares:\n\n"
        for i, inc in enumerate(incidents[:3], 1):
            rag_text += f"{i}. Similitud: {inc.get('similarity_score', 0):.1%}\n   {inc.get('text', '')[:100]}\n"
        return rag_text
    
    return None


# =================== ARCHIVOS ===================

@app.route('/api/files', methods=['GET'])
def get_files():
    """Lista archivos desde servidor MCP"""
    result = call_mcp_tool('list_data_files', {})
    
    if result.get('success'):
        return jsonify(result.get('result'))
    else:
        return jsonify({'error': result.get('error')}), 400


@app.route('/api/analyze/<filename>', methods=['GET'])
def analyze_file(filename):
    """Analiza archivo en servidor MCP"""
    preview_rows = request.args.get('preview_rows', 5, type=int)
    
    result = call_mcp_tool('analyze_data', {
        'filename': filename,
        'preview_rows': preview_rows
    })
    
    if result.get('success'):
        return jsonify(result.get('result'))
    else:
        return jsonify({'error': result.get('error')}), 400


# =================== GRÁFICOS ===================

@app.route('/api/chart/create', methods=['POST'])
def create_chart():
    """Crea gráfico en servidor MCP"""
    data = request.json
    
    result = call_mcp_tool('create_chart', {
        'filename': data.get('filename'),
        'chart_type': data.get('chart_type'),
        'x_column': data.get('x_column'),
        'y_column': data.get('y_column'),
        'title': data.get('title', 'Gráfico'),
        'filters': data.get('filters', {})
    })
    
    if result.get('success'):
        chart_result = result.get('result', {})
        return jsonify({
            'success': True,
            'chart_path': chart_result.get('chart_path'),
            'chart_name': chart_result.get('chart_name'),
            'html_url': f'/api/chart/{chart_result.get("chart_name")}'
        })
    else:
        return jsonify({'error': result.get('error')}), 400


@app.route('/api/chart/view/<chart_name>')
def view_chart(chart_name):
    """Obtiene gráfico del servidor"""
    try:
        response = requests.get(
            f"{MCP_SERVER_URL}/api/chart/{chart_name}",
            timeout=10
        )
        if response.status_code == 200:
            return response.content, 200, {'Content-Type': 'text/html'}
        else:
            return "Gráfico no encontrado", 404
    except Exception as e:
        return f"Error: {str(e)}", 500


# =================== RAG ===================

@app.route('/api/rag/load', methods=['POST'])
def load_rag():
    """Carga incidencias en servidor MCP"""
    data = request.json
    
    result = call_mcp_tool('load_incidents', {
        'source': data.get('source', 'incidencias.json'),
        'source_type': data.get('source_type', 'file')
    })
    
    if result.get('success'):
        rag_result = result.get('result', {})
        return jsonify({
            'success': rag_result.get('success', False),
            'incidents_loaded': rag_result.get('incidents_loaded', 0),
            'source': rag_result.get('source'),
            'message': f"{rag_result.get('incidents_loaded', 0)} incidencias cargadas"
        })
    else:
        return jsonify({'error': result.get('error')}), 400


@app.route('/api/rag/search', methods=['POST'])
def search_rag():
    """Busca incidencias similares en servidor MCP"""
    data = request.json
    
    result = call_mcp_tool('search_similar_incidents', {
        'incident_description': data.get('description'),
        'top_k': data.get('top_k', 5)
    })
    
    if result.get('success'):
        return jsonify(result.get('result'))
    else:
        return jsonify({'error': result.get('error')}), 400


@app.route('/api/rag/stats', methods=['GET'])
def rag_stats():
    """Estadísticas RAG del servidor MCP"""
    result = call_mcp_tool('rag_stats', {})
    
    if result.get('success'):
        return jsonify(result.get('result'))
    else:
        return jsonify({'error': result.get('error')}), 400


# =================== STATUS ===================

@app.route('/api/status', methods=['GET'])
def status():
    """Estado del servidor MCP"""
    try:
        response = requests.get(f"{MCP_SERVER_URL}/api/status", timeout=5)
        if response.status_code == 200:
            return jsonify(response.json())
    except:
        pass
    
    return jsonify({
        'success': False,
        'error': 'Servidor MCP no disponible'
    }), 500


if __name__ == '__main__':
    print("""
✨ Iniciando servidor web...

Asegúrate de que:
1. El servidor MCP está corriendo en {0}
2. Ollama está disponible en {1}
3. Puedes acceder a http://localhost:5000
    """.format(MCP_SERVER_URL, OLLAMA_URL))
    
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True,
        use_reloader=False
    )
