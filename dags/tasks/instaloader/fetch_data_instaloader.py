from airflow.decorators import task
import instaloader
import time
import os
from datetime import date

SESSION_DIR = "/opt/airflow/dags/sessions"
USERNAME = "technews24.7"

@task
def fetch_data_instaloader(username: str, limite=2):

    session_file = os.path.join(SESSION_DIR, f"session-{USERNAME}")
    if not os.path.exists(session_file):
        raise FileNotFoundError(f"Arquivo de sessão não encontrado: {session_file}")

    try:
        L = instaloader.Instaloader()
        L.load_session_from_file(USERNAME, session_file)
        L.test_login()
        print("Sessão carregada com sucesso.")

    except Exception as e:
        raise Exception(f"Erro ao carregar sessão: {e}")

    profile = instaloader.Profile.from_username(L.context, username)

    profile_data = {
        'username': profile.username,
        'full_name': profile.full_name,
        'biography': profile.biography,
        'followers': profile.followers,
        'followees': profile.followees,
        'posts_count': profile.mediacount,
        'is_verified': profile.is_verified,
        'is_private': profile.is_private,
        'external_url': profile.external_url,
        'is_business_account': profile.is_business_account,
    }

    posts_data = []
    for i, post in enumerate(profile.get_posts()):
        if i >= limite:
            break

        try:
            if post.mediacount > 1:
                tipo = 'carousel'
            elif post.is_video:
                tipo = 'video'
            else:
                tipo = 'image'

            caption = post.caption or ''

            engagement_total = post.likes + post.comments
            engagement_rate = (engagement_total / profile.followers * 100) if profile.followers > 0 else 0

            posts_data.append({
                'id': i + 1,
                'shortcode': post.shortcode,
                'date': post.date,
                'datetime_str': post.date.strftime('%Y-%m-%d'),
                'year': post.date.year,
                'month': post.date.month,
                'day': post.date.day,
                'weekday': post.date.weekday(),
                'caption': caption,
                'likes': post.likes,
                'comments': post.comments,
                'is_video': post.is_video,
                'video_view_count': post.video_view_count if post.is_video else None,
                'tipo_conteudo': tipo,
                'hashtags': list(post.caption_hashtags),
                'mentions': list(post.caption_mentions),
                'engagement_total': engagement_total,
                'engagement_rate': engagement_rate,
                'post_url': f"https://www.instagram.com/p/{post.shortcode}/"
            })

            time.sleep(2)

        except Exception as e:
            print(f"Erro ao coletar post {i}: {e}")
            break

    return {
        "username": username,
        "timestamp": date.today().isoformat(),
        "profile_data": profile_data,
        "posts_data": posts_data
    }
