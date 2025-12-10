from airflow.decorators import task
import requests
import re
import time
from datetime import date, datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../..'))
from config.meta_api_config import get_config


# Extrair hashtags
def extract_hashtags(text):
    if not text:
        return []
    return re.findall(r'#(\w+)', text)


# Extrair menções
def extract_mentions(text):
    if not text:
        return []
    return re.findall(r'@(\w+)', text)


# Extrair shortcode da URL: https://www.instagram.com/p/ABC123/
def extract_shortcode(permalink):
    if not permalink:
        return None
    match = re.search(r'/p/([^/]+)/', permalink) or re.search(r'/reel/([^/]+)/', permalink)
    return match.group(1) if match else None


# Extrair tipo de midia (imagem,video ou carrossel)
def map_media_type(api_type):
    type_map = {
        'IMAGE': 'Image',
        'VIDEO': 'Video',
        'CAROUSEL_ALBUM': 'Sidecar'
    }
    return type_map.get(api_type, api_type)


# Coletar insights
# def get_media_insights(media_id, media_type, access_token, api_version):
#     if media_type in ["IMAGE", "CAROUSEL_ALBUM"]:
#         metrics = "reach,saved,likes,comments,shares,total_interactions"
#     elif media_type in ["VIDEO", "REELS"]:
#         metrics = "views,reach,saved,likes,comments,shares,total_interactions"
#     else:
#         return None
    
#     url = (
#         f"https://graph.facebook.com/{api_version}/{media_id}/insights"
#         f"?metric={metrics}&access_token={access_token}"
#     )
    
#     try:
#         response = requests.get(url, timeout=30)
#         if response.status_code == 200:
#             return response.json()
#         else:
#             print(f"Não foi possível buscar insights para a mídia {media_id}: {response.text}")
#             return None
#     except Exception as e:
#         print(f"Erro ao buscar insights para a mídia {media_id}: {e}")
#         return None


# Extrair valor de insight específico
# def extract_insight_value(insights, metric_name):
#     # Extrai um valor de métrica específico dos dados de insights.
#     if not insights or 'data' not in insights:
#         return None
    
#     for metric in insights['data']:
#         if metric.get('name') == metric_name:
#             values = metric.get('values', [])
#             if values and len(values) > 0:
#                 return values[0].get('value')
#     return None


# Função para realizar requisições com tentativa de repetição (retry) e backoff exponencial.
def fetch_with_retry(url, max_retries=3, initial_delay=2):
    delay = initial_delay
    attempt = 0
    
    while attempt < max_retries:
        try:
            response = requests.get(url, timeout=30)
            
            # Sucesso
            if response.status_code == 200:
                return response
            
            # Limite de taxa (429 ou 403) - Pausa por 1 hora e tenta novamente
            elif response.status_code in [429, 403]:
                print(f"Limite de taxa ou Proibido ({response.status_code}) encontrado!")
                print("Pausando execução por 1/2 hora para verificar se o limite reseta...")
                time.sleep(1800)  # Espera 1/2 hora
                print("Retomando execução após pausa de 1/2 hora...")
                continue
            
            # Erro temporário do servidor - tenta novamente com backoff
            elif response.status_code in [500, 502, 503, 504]:
                print(f"Tentativa {attempt + 1}/{max_retries}: Recebeu {response.status_code}, tentando novamente em {delay}s...")
                print(f"Erro: {response.text}")
                time.sleep(delay)
                delay *= 2
                attempt += 1
                continue
            
            # Outros erros do cliente
            else:
                print(f"Erro do cliente {response.status_code}: {response.text}")
                return response
                
        except requests.exceptions.Timeout:
            print(f"Tentativa {attempt + 1}/{max_retries}: Tempo esgotado (Timeout), tentando novamente em {delay}s...")
            time.sleep(delay)
            delay *= 2
            attempt += 1
        except requests.exceptions.RequestException as e:
            print(f"Tentativa {attempt + 1}/{max_retries}: Requisição falhou - {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            attempt += 1
            
    return None


# Coleta dados do Instagram usando a API Meta com paginação.
@task
def fetch_data_meta_api(username: str, fetch_all_posts=True, max_posts=5000, app_id=1):
    config = get_config(app_id)
    
    print(f"🔧 Usando Meta API App {app_id} para coletar dados de @{username}")
    
    ig_user_id = config['ig_user_id']
    access_token = config['access_token']
    api_version = config['api_version']
    
    # Campos para solicitar da API
    profile_fields = (
        "name,website,biography,followers_count,follows_count,media_count,"
        "profile_picture_url"
    )
    
    media_fields = (
        "id,media_type,timestamp,permalink,caption,"
        "like_count,comments_count"
    )
    
    # Constroi a URL inicial da API para business discovery
    limit = 25
    data = None
    
    while limit > 0:
        url = (
            f"https://graph.facebook.com/{api_version}/{ig_user_id}"
            f"?fields=business_discovery.username({username})"
            f"{{{profile_fields},media.limit({limit}){{{media_fields}}}}}"
            f"&access_token={access_token}"
        )
        
        try:
            response = fetch_with_retry(url, max_retries=3, initial_delay=2)
            
            if response is None:
                break
            
            if response.status_code == 200:
                data = response.json()
                break
                
            if "Please reduce the amount of data" in response.text:
                print(f"⚠️ Erro de volume de dados. Reduzindo limite de {limit} para {int(limit/2)}...")
                limit = int(limit / 2)
                continue
            
            print(f"Erro na requisição inicial: {response.status_code} - {response.text}")
            break
            
        except Exception as e:
            raise Exception(f"Erro ao buscar dados da API Meta: {e}")

    if data is None:
        error_msg = (
            f"Falha ao buscar dados iniciais para @{username}. "
            f"Verifique se a conta é Business/Creator pública e se o token é válido."
        )
        print(error_msg)
        raise Exception(error_msg)
    
    if 'business_discovery' not in data:
        raise Exception(f"Nenhum dado de business_discovery encontrado para o usuário: {username}")
    
    business_data = data['business_discovery']
    media_response = business_data.get('media', {})
    all_media_data = media_response.get('data', [])
    
    # Se fetch_all_posts for True, pagina através de todos os posts
    if fetch_all_posts:
        print(f"Buscando posts para @{username} (Limite: {max_posts})...")
        page_count = 1
        consecutive_failures = 0
        max_consecutive_failures = 4
        
        # Continua buscando enquanto houver um cursor 'after' E não tivermos atingido max_posts
        while 'paging' in media_response and 'cursors' in media_response['paging']:
            if len(all_media_data) >= max_posts:
                print(f"Limite de {max_posts} posts atingido. Parando coleta.")
                break

            cursors = media_response['paging'].get('cursors', {})
            after_cursor = cursors.get('after')
            
            if not after_cursor:
                print("Não há mais páginas para buscar.")
                break
            
            print(f"Buscando página {page_count + 1}...")
            
            # Constroi URL paginada com cursor 'after'
            while limit > 0:
                next_url = (
                    f"https://graph.facebook.com/{api_version}/{ig_user_id}"
                    f"?fields=business_discovery.username({username})"
                    f"{{media.limit({limit}).after({after_cursor}){{{media_fields}}}}}"
                    f"&access_token={access_token}"
                )
                
                # Usa mecanismo de retry
                response = fetch_with_retry(next_url, max_retries=3, initial_delay=2)
                
                if response is not None and "Please reduce the amount of data" in response.text:
                    print(f"Erro de volume de dados na paginação. Reduzindo limite de {limit} para {int(limit/2)}...")
                    limit = int(limit / 2)
                    continue
                
                break
            
            if response is None or response.status_code != 200:
                consecutive_failures += 1
                print(f"Falha ao buscar página {page_count + 1} após tentativas.")
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"Parando paginação após {consecutive_failures} falhas consecutivas.")
                    print(f"Coletados {len(all_media_data)} posts antes de parar.")
                    break
                
                # Tenta continuar com o próximo cursor se disponível
                break
            
            # Reseta contador de falhas no sucesso
            consecutive_failures = 0
            
            try:
                next_data = response.json()
                
                # Extrai os novos itens de mídia do business_discovery
                if 'business_discovery' in next_data:
                    media_response = next_data['business_discovery'].get('media', {})
                    new_media = media_response.get('data', [])
                    
                    if not new_media:
                        print("Não há mais posts para buscar.")
                        break
                    
                    all_media_data.extend(new_media)
                    page_count += 1
                    print(f"Coletados {len(all_media_data)} posts até agora...")
                else:
                    print("Estrutura de resposta inválida para paginação.")
                    break
                
                # Delay para respeitar os limites de taxa, 60 segundos entre as páginas
                time.sleep(60)
                
            except Exception as e:
                print(f"Erro ao processar resposta da paginação: {e}")
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    print(f"Parando após {consecutive_failures} falhas consecutivas de processamento.")
                    break
        
        print(f"Total de posts coletados: {len(all_media_data)}")
    
    # Processa cada item de mídia
    posts = []
    for idx, media in enumerate(all_media_data):
        # Respeita max_posts durante o processamento também, apenas por precaução
        if idx >= max_posts:
            break
            
        media_id = media.get('id')
        media_type = media.get('media_type')
        timestamp = media.get('timestamp')
        permalink = media.get('permalink')
        caption = media.get('caption', '')
        
        # Constroi objeto post no formato necessário
        post = {
            'id': media_id,
            'type': map_media_type(media_type),
            'shortCode': extract_shortcode(permalink),
            'caption': caption,
            'hashtags': extract_hashtags(caption),
            'mentions': extract_mentions(caption),
            'url': permalink,
            'commentsCount': media.get('comments_count', 0),
            'likesCount': media.get('like_count', 0),
            'timestamp': timestamp,
            'ownerUsername': username,
            'isVideo': media_type == 'VIDEO',
        }
        
        posts.append(post)
        
        # Indicador de progresso
        if (idx + 1) % 100 == 0:
            print(f"Processados {idx + 1}/{len(all_media_data)} posts...")
        
        # Pequeno delay para respeitar limites de taxa
        # time.sleep(30)
    
    print(f"Processados com sucesso todos os {len(posts)} posts para @{username}")
    
    return {
        'username': username,
        'timestamp': date.today().isoformat(),
        'profile_data': {
            'name': business_data.get('name'),
            'website': business_data.get('website'),
            'biography': business_data.get('biography'),
            'followers_count': business_data.get('followers_count'),
            'follows_count': business_data.get('follows_count'),
            'media_count': business_data.get('media_count'),
            # 'profile_picture_url': business_data.get('profile_picture_url')
        },
        'posts': posts
    }