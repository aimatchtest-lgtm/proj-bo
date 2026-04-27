import os
import requests
from supabase import create_client
from datetime import datetime, timezone
import time

# 1. Настройки
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

def safe_get(url):
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.json().get("data")
        elif r.status_code == 429:
            print("Лимит! Ждем 60 сек...")
            time.sleep(60)
            return safe_get(url)
        else:
            print(f"Ошибка {r.status_code}: {url}")
            return None
    except Exception as e:
        print(f"Сбой соединения: {e}")
        return None

def main():
    print("🚀 Запуск реального сборщика матчей...")
    
    # НАСТРОЙКА: Какую лигу собираем? 39 = АПЛ (England Premier League)
    LEAGUE_ID = 39
    YEAR = 2024 # Сезон 2024/2025
    
    print(f"Сбор матчей для Лиги ID: {LEAGUE_ID}, Год: {YEAR}")
    
    # 1. Получаем список матчей (лимит 100 за раз)
    games_list = safe_get(f"{BASE}/games/list?leagueid={LEAGUE_ID}&year={YEAR}&limit=100")
    
    if not games_list:
        print("❌ Не удалось получить список матчей.")
        return

    print(f"✅ Найдено {len(games_list)} матчей. Начинаем сохранение...")
    
    saved_count = 0
    for game in games_list:
        gid = game.get("id")
        home_team = game.get("homeTeam", {}).get("name")
        away_team = game.get("awayTeam", {}).get("name")
        date_str = game.get("date")
        status_code = game.get("status")
        
        # Определяем статус для БД
        db_status = "scheduled"
        if status_code in [8, 9, 10, 17, 18]:
            db_status = "finished"
        elif status_code in [3, 4, 5, 6, 7, 11, 18, 19]:
            db_status = "live"
            
        # Парсинг времени
        match_time = None
        if date_str:
            try:
                # Убираем Z если есть, добавляем +00:00 для ISO формата
                clean_date = date_str.replace('Z', '+00:00')
                if '+' not in clean_date and '-' not in clean_date[10:]:
                     clean_date += '+00:00'
                match_time = datetime.fromisoformat(clean_date)
            except:
                pass

        row = {
            "external_id": str(gid),
            "league_name": "Premier League", # Можно динамически, но пока хардкод для теста
            "home_team": home_team,
            "away_team": away_team,
            "match_time": match_time.isoformat() if match_time else None,
            "status": db_status,
            "score_home": game.get("homeFTResult"),
            "score_away": game.get("awayFTResult"),
            "ht_score_home": game.get("homeHTResult"),
            "ht_score_away": game.get("awayHTResult"),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Проверяем, есть ли уже такой матч
            existing = supabase.table("matches").select("id").eq("external_id", str(gid)).execute()
            
            if existing.data:
                # Обновляем, если матч уже есть (например, изменился счет)
                supabase.table("matches").update(row).eq("external_id", str(gid)).execute()
            else:
                # Вставляем новый
                supabase.table("matches").insert(row).execute()
            
            saved_count += 1
            
        except Exception as e:
            print(f"Ошибка сохранения матча {gid}: {e}")
            
        # Пауза, чтобы не превысить лимиты API (если без ключа)
        time.sleep(0.5) 

    print(f"💾 Готово! Сохранено/Обновлено {saved_count} матчей.")

if __name__ == "__main__":
    main()
