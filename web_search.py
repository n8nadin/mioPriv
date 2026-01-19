"""
Módulo para búsqueda de información en internet
"""

import aiohttp
import asyncio
from typing import List, Dict, Any
from bs4 import BeautifulSoup
import json


class WebSearcher:
    """Realiza búsquedas en internet usando DuckDuckGo"""
    
    def __init__(self):
        self.base_url = "https://html.duckduckgo.com/html/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    async def search(self, query: str, num_results: int = 5) -> List[Dict[str, str]]:
        """
        Busca en DuckDuckGo y retorna resultados
        
        Args:
            query: Consulta de búsqueda
            num_results: Número de resultados a retornar
        
        Returns:
            Lista de diccionarios con title, url, snippet
        """
        try:
            async with aiohttp.ClientSession() as session:
                # Hacer petición POST a DuckDuckGo
                data = {'q': query}
                async with session.post(
                    self.base_url,
                    data=data,
                    headers=self.headers,
                    timeout=10
                ) as response:
                    html = await response.text()
                    
                    # Parsear resultados
                    soup = BeautifulSoup(html, 'html.parser')
                    results = []
                    
                    # Encontrar elementos de resultados
                    result_divs = soup.find_all('div', class_='result')
                    
                    for div in result_divs[:num_results]:
                        try:
                            # Extraer título y URL
                            title_elem = div.find('a', class_='result__a')
                            if not title_elem:
                                continue
                            
                            title = title_elem.get_text(strip=True)
                            url = title_elem.get('href', '')
                            
                            # Extraer snippet
                            snippet_elem = div.find('a', class_='result__snippet')
                            snippet = snippet_elem.get_text(strip=True) if snippet_elem else ""
                            
                            if title and url:
                                results.append({
                                    'title': title,
                                    'url': url,
                                    'snippet': snippet
                                })
                        except Exception as e:
                            continue
                    
                    return results if results else await self._fallback_search(query, num_results)
        
        except Exception as e:
            return [{
                'error': f'Error en búsqueda: {str(e)}',
                'query': query
            }]
    
    async def _fallback_search(self, query: str, num_results: int) -> List[Dict[str, str]]:
        """
        Método alternativo usando API pública (si la primera falla)
        """
        try:
            # Alternativa: usar la API de Wikipedia para algunas consultas
            if any(word in query.lower() for word in ['qué es', 'quien es', 'definición']):
                return await self._search_wikipedia(query)
            
            return [{
                'message': 'No se pudieron obtener resultados. Intenta reformular la búsqueda.',
                'query': query
            }]
        
        except:
            return []
    
    async def _search_wikipedia(self, query: str) -> List[Dict[str, str]]:
        """Busca en Wikipedia como fallback"""
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://es.wikipedia.org/w/api.php"
                params = {
                    'action': 'query',
                    'format': 'json',
                    'list': 'search',
                    'srsearch': query,
                    'srlimit': 3
                }
                
                async with session.get(url, params=params) as response:
                    data = await response.json()
                    results = []
                    
                    for item in data.get('query', {}).get('search', []):
                        results.append({
                            'title': item['title'],
                            'url': f"https://es.wikipedia.org/wiki/{item['title'].replace(' ', '_')}",
                            'snippet': item['snippet'].replace('<span class="searchmatch">', '').replace('</span>', '')
                        })
                    
                    return results
        except:
            return []
    
    async def fetch_page_content(self, url: str) -> Dict[str, Any]:
        """
        Obtiene el contenido completo de una página web
        
        Args:
            url: URL de la página
        
        Returns:
            Diccionario con title, text, links
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, timeout=15) as response:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Eliminar scripts y estilos
                    for script in soup(["script", "style"]):
                        script.decompose()
                    
                    # Extraer título
                    title = soup.find('title')
                    title_text = title.get_text() if title else "Sin título"
                    
                    # Extraer texto principal
                    text = soup.get_text(separator='\n', strip=True)
                    
                    # Extraer links
                    links = []
                    for link in soup.find_all('a', href=True):
                        links.append({
                            'text': link.get_text(strip=True),
                            'url': link['href']
                        })
                    
                    return {
                        'url': url,
                        'title': title_text,
                        'text': text[:5000],  # Limitar a 5000 caracteres
                        'links': links[:20],  # Primeros 20 links
                        'success': True
                    }
        
        except Exception as e:
            return {
                'url': url,
                'error': str(e),
                'success': False
            }