"""
Módulo RAG para búsqueda semántica de incidencias similares
Con soporte para visualización 3D Galaxy
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer
import aiohttp
from bs4 import BeautifulSoup
import numpy as np
from sklearn.decomposition import PCA
import json


class IncidentRAG:
    """Sistema RAG para buscar incidencias similares"""
    
    def __init__(self, rag_dir: Path, use_ollama_embeddings: bool = True, ollama_url: str = "http://192.168.30.13:11434"):
        self.rag_dir = Path(rag_dir)
        self.rag_dir.mkdir(exist_ok=True)
        
        # Configuración de embeddings
        self.use_ollama = use_ollama_embeddings
        self.ollama_url = ollama_url
        
        # Verificar si ya existe ChromaDB persistente
        chroma_exists = (self.rag_dir / "chroma.sqlite3").exists()
        
        # Inicializar ChromaDB
        self.client = chromadb.PersistentClient(
            path=str(self.rag_dir),
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Crear o obtener colección
        try:
            self.collection = self.client.get_collection("incidents")
            print(f"✅ Colección 'incidents' cargada con {self.collection.count()} incidencias")
        except:
            print("⚠️ Colección 'incidents' no existe, creando nueva...")
            self.collection = self.client.create_collection(
                name="incidents",
                metadata={"description": "Incidencias para búsqueda semántica"}
            )
            print("✅ Colección 'incidents' creada (vacía)")
        
        # Modelo de embeddings
        if self.use_ollama:
            print(f"🔗 Usando Ollama embeddings desde: {self.ollama_url}")
            print("   Modelo: nomic-embed-text")
            self.model = None  # No usaremos sentence-transformers
        else:
            try:
                print("Cargando modelo de embeddings local (Sentence Transformers)...")
                self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                print("✅ Modelo de embeddings cargado")
            except:
                print("⚠️ Descargando modelo de embeddings (primera vez)...")
                self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                print("✅ Modelo descargado")
    
    def _generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Genera embeddings usando Ollama o Sentence Transformers"""
        if self.use_ollama:
            import requests
            
            embeddings = []
            for text in texts:
                try:
                    response = requests.post(
                        f"{self.ollama_url}/api/embeddings",
                        json={
                            "model": "nomic-embed-text",
                            "prompt": text[:2000]  # Limitar tamaño
                        },
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        embeddings.append(data['embedding'])
                    else:
                        print(f"⚠️ Error en embedding para texto: {text[:50]}...")
                        # Usar vector dummy si falla
                        embeddings.append([0.0] * 768)
                
                except Exception as e:
                    print(f"⚠️ Error generando embedding: {e}")
                    embeddings.append([0.0] * 768)
            
            return embeddings
        else:
            # Usar Sentence Transformers local
            return self.model.encode(texts, show_progress_bar=False).tolist()
    
    async def load_incidents(self, source: str, source_type: str = "url") -> Dict[str, Any]:
        """Carga incidencias desde una URL o archivo CSV/JSON"""
        incidents = []
        
        try:
            if source_type == "url":
                incidents = await self._scrape_incidents_from_url(source)
            elif source_type == "file":
                incidents = self._load_incidents_from_file(source)
            else:
                return {"error": f"Tipo de fuente no soportado: {source_type}"}
            
            if not incidents:
                return {"error": "No se encontraron incidencias"}
            
            # Agregar a ChromaDB
            self._add_incidents_to_db(incidents)
            
            return {
                "success": True,
                "incidents_loaded": len(incidents),
                "source": source,
                "source_type": source_type
            }
        
        except Exception as e:
            import traceback
            return {
                "error": f"Error al cargar incidencias: {str(e)}",
                "traceback": traceback.format_exc()
            }
    
    async def _scrape_incidents_from_url(self, url: str) -> List[Dict[str, str]]:
        """Extrae incidencias desde una página web"""
        incidents = []
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=15) as response:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Adaptable según estructura de tu web
                    incident_elements = soup.find_all(['div', 'li', 'tr'], class_=lambda x: x and any(
                        keyword in str(x).lower() for keyword in ['incident', 'incidencia', 'issue', 'ticket']
                    ))
                    
                    for i, elem in enumerate(incident_elements):
                        text = elem.get_text(strip=True)
                        if len(text) > 20:
                            incidents.append({
                                'id': f"web_{i}",
                                'title': text[:100],
                                'description': text,
                                'source': url,
                                'Proyecto': 'Web Scraping'
                            })
        
        except Exception as e:
            print(f"Error scraping: {e}")
        
        return incidents
    
    def _load_incidents_from_file(self, filename: str) -> List[Dict[str, str]]:
        """Carga incidencias desde un archivo CSV o JSON"""
        # Buscar en diferentes ubicaciones
        possible_paths = [
            self.rag_dir.parent / "data" / filename,
            self.rag_dir / filename,
            Path(filename)
        ]
        
        filepath = None
        for path in possible_paths:
            if path.exists():
                filepath = path
                break
        
        if not filepath:
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")
        
        incidents = []
        
        # Si es JSON
        if filename.endswith('.json'):
            print(f"Cargando JSON desde: {filepath}")
            
            # Leer en chunks para archivos grandes
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Manejar diferentes estructuras JSON
                items_to_process = []
                
                if isinstance(data, dict):
                    # Buscar la clave que contiene la lista de incidencias
                    for key in ['incidencias', 'data', 'items', 'incidents', 'records']:
                        if key in data and isinstance(data[key], list):
                            items_to_process = data[key]
                            break
                    
                    # Si no encuentra, buscar cualquier lista
                    if not items_to_process:
                        for value in data.values():
                            if isinstance(value, list):
                                items_to_process = value
                                break
                
                elif isinstance(data, list):
                    items_to_process = data
                
                print(f"Procesando {len(items_to_process)} items del JSON...")
                
                # Procesar items en batches
                batch_size = 100
                for batch_start in range(0, len(items_to_process), batch_size):
                    batch = items_to_process[batch_start:batch_start + batch_size]
                    
                    for idx, item in enumerate(batch):
                        if isinstance(item, dict):
                            # Extraer campos con múltiples variantes
                            incident = {
                                'id': str(item.get('id', item.get('ID', item.get('_id', f'json_{batch_start + idx}')))),
                                'title': str(item.get('title', item.get('titulo', item.get('Proyecto', item.get('nombre', 'Sin título'))))),
                                'description': str(item.get('description', item.get('descripcion', item.get('Descripción', item.get('desc', ''))))),
                                'source': filename,
                                'Proyecto': str(item.get('Proyecto', item.get('proyecto', item.get('project', 'Sin proyecto'))))
                            }
                            
                            # Agregar TODOS los campos adicionales
                            for k, v in item.items():
                                if k not in incident and v is not None:
                                    incident[k] = str(v)[:500]  # Limitar tamaño
                            
                            incidents.append(incident)
                    
                    if batch_start % 500 == 0 and batch_start > 0:
                        print(f"  Procesados {batch_start} incidencias...")
                
                print(f"✅ Total cargado: {len(incidents)} incidencias")
                return incidents
            
            except json.JSONDecodeError as e:
                raise ValueError(f"Error al parsear JSON: {str(e)}")
        
        # Si es CSV
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath)
            
            for idx, row in df.iterrows():
                incident = {
                    'id': str(row.get('id', row.get('ID', f'csv_{idx}'))),
                    'title': str(row.get('title', row.get('titulo', row.get('Proyecto', '')))),
                    'description': str(row.get('description', row.get('descripcion', ''))),
                    'source': filename,
                    'Proyecto': str(row.get('Proyecto', 'Sin proyecto'))
                }
                
                # Agregar columnas adicionales
                for col in df.columns:
                    if col not in incident:
                        incident[col] = str(row[col])
                
                incidents.append(incident)
        
        return incidents
    
    def _add_incidents_to_db(self, incidents: List[Dict[str, str]]):
        """Agrega incidencias a la base de datos vectorial en batches"""
        if not incidents:
            return
        
        print(f"Agregando {len(incidents)} incidencias a ChromaDB...")
        
        # Procesar en batches pequeños para evitar problemas de memoria
        batch_size = 10 if self.use_ollama else 50  # Batches más pequeños con Ollama
        
        for batch_start in range(0, len(incidents), batch_size):
            batch = incidents[batch_start:batch_start + batch_size]
            
            ids = [inc['id'] for inc in batch]
            documents = [
                f"{inc.get('title', '')} {inc.get('description', '')} {inc.get('Proyecto', '')}"
                for inc in batch
            ]
            metadatas = [
                {k: v for k, v in inc.items() if k != 'id'}
                for inc in batch
            ]
            
            # Generar embeddings para este batch
            if batch_start % 100 == 0:
                print(f"  Generando embeddings para batch {batch_start//batch_size + 1}... ({batch_start}/{len(incidents)})")
            
            embeddings = self._generate_embeddings(documents)
            
            # Agregar a ChromaDB
            try:
                self.collection.add(
                    ids=ids,
                    documents=documents,
                    embeddings=embeddings,
                    metadatas=metadatas
                )
            except Exception as e:
                # Si hay duplicados, intentar con upsert
                if batch_start % 100 == 0:
                    print(f"  Usando upsert para batch {batch_start//batch_size + 1}")
                self.collection.upsert(
                    ids=ids,
                    documents=documents,
                    embeddings=embeddings,
                    metadatas=metadatas
                )
            
            if batch_start % 250 == 0 and batch_start > 0:
                print(f"  ✅ Agregados {batch_start} incidencias")
        
        print(f"✅ Completado: {len(incidents)} incidencias en ChromaDB")
    
    def search_similar(self, incident_description: str, top_k: int = 5, filters: Dict = None) -> Dict[str, Any]:
        """
        Busca incidencias similares usando búsqueda semántica en RAG
        Mucho más rápido que buscar en JSON
        """
        try:
            print(f"🔍 Buscando similares para: '{incident_description[:50]}...'")
            
            # Generar embedding de la consulta
            query_embedding = self._generate_embeddings([incident_description])[0]
            
            # Construir where clause si hay filtros
            where_clause = None
            if filters:
                where_clause = {}
                for key, value in filters.items():
                    where_clause[key] = value
            
            # Buscar en ChromaDB (ultra rápido, usa índice FAISS)
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=min(top_k, 50),  # Limitar a 50 máximo para rendimiento
                where=where_clause
            )
            
            # Formatear resultados con TODOS los campos
            similar_incidents = []
            
            if results['ids'] and len(results['ids'][0]) > 0:
                for i in range(len(results['ids'][0])):
                    # Calcular similitud (1 - distancia)
                    similarity = 1 - results['distances'][0][i]
                    
                    # Solo incluir si similitud > 0.3 (30%)
                    if similarity > 0.3:
                        metadata = results['metadatas'][0][i]
                        
                        # Asegurarnos de incluir TODOS los campos comunes
                        incident = {
                            'id': results['ids'][0][i],
                            'similarity_score': float(similarity),
                            'text': results['documents'][0][i][:300],  # Texto corto para preview
                            'full_text': results['documents'][0][i],  # Texto completo
                            'metadata': {
                                # Campos estándar
                                'ID': metadata.get('ID', metadata.get('id', metadata.get('Identificador_incidencia', results['ids'][0][i]))),
                                'Proyecto': metadata.get('Proyecto', metadata.get('proyecto', 'No especificado')),
                                'Fecha': metadata.get('Fecha', metadata.get('fecha', metadata.get('Fecha_envío_incidencia', metadata.get('Fecha del incidente', 'N/A')))),
                                'Descripción': metadata.get('Descripción', metadata.get('descripcion', metadata.get('Descripcion Problema', metadata.get('Descripción_incidencia', results['documents'][0][i][:200])))),
                                'Solución': metadata.get('Solución', metadata.get('solucion', metadata.get('Solucion', 'No registrada'))),
                                'Estado': metadata.get('Estado', metadata.get('estado', metadata.get('status', ''))),
                                'Prioridad': metadata.get('Prioridad', metadata.get('prioridad', metadata.get('priority', ''))),
                                
                                # Incluir TODOS los demás campos
                                **{k: v for k, v in metadata.items() 
                                   if k not in ['ID', 'id', 'Identificador_incidencia', 'Proyecto', 'proyecto', 'Fecha', 'fecha', 
                                               'Descripción', 'descripcion', 'Solución', 'solucion',
                                               'Estado', 'estado', 'Prioridad', 'prioridad', 'source',
                                               'Fecha_envío_incidencia', 'Descripción_incidencia']}
                            }
                        }
                        similar_incidents.append(incident)
            
            print(f"✅ Encontradas {len(similar_incidents)} incidencias similares")
            
            return {
                "query": incident_description,
                "similar_incidents": similar_incidents,
                "total_found": len(similar_incidents),
                "search_time_ms": 0  # ChromaDB es tan rápido que ni se nota
            }
        
        except Exception as e:
            import traceback
            return {
                "query": incident_description,
                "error": f"Error en búsqueda: {str(e)}",
                "traceback": traceback.format_exc(),
                "similar_incidents": []
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estadísticas de la base de datos"""
        try:
            count = self.collection.count()
            
            # Verificar si hay archivos FAISS/PKL
            faiss_exists = False
            pkl_exists = False
            chroma_exists = False
            
            # Buscar archivos index en la carpeta
            for file in self.rag_dir.iterdir():
                if 'faiss' in file.name.lower() or file.suffix == '.faiss':
                    faiss_exists = True
                if file.suffix == '.pkl':
                    pkl_exists = True
                if file.name == 'chroma.sqlite3':
                    chroma_exists = True
            
            return {
                "total_incidents": count,
                "collection_name": self.collection.name,
                "has_data": count > 0,
                "files": {
                    "faiss": faiss_exists,
                    "pkl": pkl_exists,
                    "chroma": chroma_exists
                },
                "rag_ready": count > 0 and (faiss_exists or chroma_exists)
            }
        except Exception as e:
            return {
                "error": str(e), 
                "total_incidents": 0,
                "has_data": False,
                "rag_ready": False
            }
    
    def clear_database(self) -> Dict[str, Any]:
        """Limpia la base de datos de incidencias"""
        try:
            self.client.delete_collection("incidents")
            self.collection = self.client.create_collection(
                name="incidents",
                metadata={"description": "Incidencias para búsqueda semántica"}
            )
            return {"success": True, "message": "Base de datos limpiada"}
        except Exception as e:
            return {"error": str(e)}
    
    def get_galaxy_data(self, use_cache=True) -> Dict[str, Any]:
        """Obtiene datos para visualización 3D Galaxy (con caché)"""
        
        # Archivo de caché
        cache_file = self.rag_dir / "galaxy_cache.json"
        
        # Si existe caché y está activado, usarlo
        if use_cache and cache_file.exists():
            try:
                print("📦 Cargando Galaxy desde caché...")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                
                # Verificar que el caché coincide con el número actual de incidencias
                current_count = self.collection.count()
                if cached_data.get('total_incidents') == current_count:
                    print(f"✅ Caché válido: {current_count} incidencias")
                    return cached_data
                else:
                    print(f"⚠️ Caché desactualizado ({cached_data.get('total_incidents')} vs {current_count})")
            except Exception as e:
                print(f"⚠️ Error leyendo caché: {e}")
        
        try:
            print("🔄 Generando Galaxy data (primera vez puede tardar)...")
            
            # Obtener solo IDs y metadatas (sin embeddings completos)
            results = self.collection.get(
                include=['metadatas', 'documents']
            )
            
            if not results['ids'] or len(results['ids']) == 0:
                return {
                    "success": False,
                    "error": "No hay datos en la base de datos. Carga incidencias primero."
                }
            
            total_incidents = len(results['ids'])
            print(f"📊 Procesando {total_incidents} incidencias...")
            
            # OPTIMIZACIÓN: Solo obtener embeddings de una muestra por proyecto
            # Agrupar primero por proyecto
            projects_temp = {}
            
            for i, metadata in enumerate(results['metadatas']):
                project_name = metadata.get('Proyecto', metadata.get('proyecto', 'Sin proyecto'))
                
                if project_name not in projects_temp:
                    projects_temp[project_name] = []
                
                projects_temp[project_name].append({
                    'id': results['ids'][i],
                    'text': results['documents'][i][:150],  # Truncar para rendimiento
                    'metadata': {k: str(v)[:50] for k, v in metadata.items()}  # Limitar tamaño
                })
            
            # Ahora crear soles con posiciones calculadas eficientemente
            suns = []
            
            for idx, (project_name, incidents) in enumerate(projects_temp.items()):
                # Posición basada en hash del nombre (consistente)
                import hashlib
                hash_val = int(hashlib.md5(project_name.encode()).hexdigest(), 16)
                
                # Usar hash para posición pseudo-aleatoria pero consistente
                angle = (hash_val % 360) * (3.14159 / 180)
                radius = 30 + (hash_val % 50)
                
                x = np.cos(angle) * radius
                y = (hash_val % 20) - 10
                z = np.sin(angle) * radius
                
                # OPTIMIZACIÓN: Limitar incidencias mostradas por proyecto
                max_incidents_to_show = 500
                incidents_to_show = incidents[:max_incidents_to_show]
                
                suns.append({
                    'name': project_name,
                    'x': float(x),
                    'y': float(y),
                    'z': float(z),
                    'size': len(incidents),  # Tamaño real
                    'incident_count': len(incidents),
                    'incidents': incidents_to_show,  # Solo primeras N para visualizar
                    'has_more': len(incidents) > max_incidents_to_show
                })
            
            galaxy_data = {
                "success": True,
                "suns": suns,
                "total_projects": len(suns),
                "total_incidents": total_incidents
            }
            
            # Guardar caché
            try:
                print("💾 Guardando caché...")
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(galaxy_data, f, ensure_ascii=False)
                print("✅ Caché guardado")
            except Exception as e:
                print(f"⚠️ No se pudo guardar caché: {e}")
            
            print(f"✅ Galaxy generada: {len(suns)} proyectos")
            return galaxy_data
        
        except Exception as e:
            import traceback
            return {
                "success": False,
                "error": str(e),
                "traceback": traceback.format_exc()
            }