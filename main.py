import os
import re
import requests
from supabase import create_client
from datetime import datetime, timezone
from dateutil import parser as dateparser
import time

# 1. Настройки из GitHub Secrets
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

def safe_get(url):
    """Безопасный GET запрос с обработкой лимитов"""
    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            return r.json().get("data")
        elif r.status_code == 429:
            print("⚠️ Лимит API! Ждем 65 сек...")
            time.sleep(65)
            return safe_get(url)
        else:
            print(f"❌ Ошибка {r.status_code}: {url}")
            return None
    except Exception as e:
        print(f"💥 Сбой соединения: {e}")
        return None

def parse_match_time(date_str):
    """Надёжный парсинг даты из API"""
    if not date_str:
        return None
    try:
        return dateparser.isoparse(date_str)
    except:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except:
            print(f"⚠️ Не удалось распарсить дату: {date_str}")
            return None

def parse_line_value(name):
    """Извлекает числовое значение линии из названия (Over 4.5 -> 4.5)"""
    match = re.search(r'([\d.]+)', name)
    return float(match.group(1)) if match else None

def generate_verdicts(gid, statistics, odds_list, referee_name):
    """
    Анализирует данные матча и создает вердикты для базы данных.
    """
    verdicts = []
    
    # --- 1. ВЕРДИКТ ПО ЖЕЛТЫМ КАРТОЧКАМ (ЖК) ---
    ref_avg_yel = 4.5  # Заглушка, пока нет БД судей
    
    home_yel = statistics.get("yellowCardsHome", 0) or 0
    away_yel = statistics.get("yellowCardsAway", 0) or 0
    
    model_pred_yel = (home_yel + away_yel) / 2 + ref_avg_yel / 2
    
    # Ищем линию букмекера на ЖК
    line_yel = None
    for market in odds_list:
        m_name = market.get("marketName", "")
        if "Yellow" in m_name or "ЖК" in m_name:
            for odd in market.get("odds", []):
                name = odd.get("name", "")
                if "Over" in name or "TB" in name or "Б" in name:
                    line_yel = parse_line_value(name)
                    if line_yel:
                        break
            if line_yel:
                break
    
    # Сигнал на ЖК
    if line_yel and model_pred_yel > line_yel:
        verdicts.append({
            "market_type": "YELLOW_CARDS",
            "recommendation": f"TAKE_TB_{line_yel}",
            "confidence": "HIGH",
            "analysis_json": {
                "progruz": "Нет данных",
                "model_pred": round(model_pred_yel, 1),
                "referee_avg": ref_avg_yel,
                "reason": f"Модель ждет {model_pred_yel:.1f} ЖК, линия всего {line_yel}. Судья строгий."
            }
        })
    else:
        verdicts.append({
            "market_type": "YELLOW_CARDS",
            "recommendation": "SKIP",
            "confidence": "LOW",
            "analysis_json": {"reason": "Нет перевеса модели над линией."}
        })

    # --- 2. ВЕРДИКТ ПО ГОЛАМ (xG) ---
    xg_home = statistics.get("calculatedXgHome", 0) or 0
    xg_away = statistics.get("calculatedXgAway", 0) or 0
    total_xg = xg_home + xg_away
    
    line_goals = None
    for market in odds_list:
        m_name = market.get("marketName", "")
        if "Total" in m_name or "Goals" in m_name or "Голы" in m_name:
            for odd in market.get("odds", []):
                name = odd.get("name", "")
                if "Over" in name or "TB" in name or "Б" in name:
                    line_goals = parse_line_value(name)
                    if line_goals:
                        break
            if line_goals:
                break
    
    # Если линия не найдена, ставим дефолт
    if not line_goals:
        line_goals = 2.5
    
    # Сигнал на голы
    if total_xg > line_goals + 0.3:
        verdicts.append({
            "market_type": "GOALS",
            "recommendation": f"TAKE_TB_{line_goals}",
            "confidence": "MEDIUM",
            "analysis_json": {
                "model_pred": round(total_xg, 2),
                "xg_home": round(xg_home, 2),
                "xg_away": round(xg_away, 2),
                "reason": f"Суммарный xG команд ({total_xg:.2f}) выше линии букмекера ({line_goals})."
            }
        })
    else:
        verdicts.append({
            "market_type": "GOALS",
            "recommendation": "SKIP",
            "confidence": "LOW",
            "analysis_json": {"reason": "xG не подтверждает тотал больше."}
        })

    return verdicts

def main():
    print("🚀 Запуск умного сборщика с аналитикой...")
    
    # НАСТРОЙКА: Какую лигу собираем? 39 = АПЛ
    LEAGUE_ID = 39
    YEAR = 2024 
    
    print(f"Сбор матчей для Лиги ID: {LEAGUE_ID}, Год: {YEAR}")
    
    # 1. Получаем список матчей
    games_list = safe_get(f"{BASE}/games/list?leagueid={LEAGUE_ID}&year={YEAR}&limit=5")
    
    if not games_list:
        print("❌ Не удалось получить список матчей.")
        return

    print(f"✅ Найдено {len(games_list)} матчей. Начинаем детальный сбор...")
    
    saved_matches = 0
    saved_verdicts = 0

    for game_summary in games_list:
        gid = game_summary.get("id")
        
        # 2. Получаем ПОЛНЫЕ данные матча
        full_data = safe_get(f"{BASE}/games/{gid}")
        
        if not full_data:
            continue
            
        game = full_data.get("game", {})
        statistics = full_data.get("statistics", {})
        referee_name = full_data.get("refereeName")
        odds_list = game.get("odds", [])
        
        home_team = game.get("homeTeam", {}).get("name")
        away_team = game.get("awayTeam", {}).get("name")
        
        print(f"⚽ Обработка: {home_team} vs {away_team} (ID: {gid})")

        # Парсинг времени
        match_time = parse_match_time(game.get("date"))
        
        # Определение статуса
        game_status = game.get("status")
        if game_status in [8, 9, 10]:
            status = "finished"
        elif game_status in [1, 2, 3]:
            status = "live"
        else:
            status = "scheduled"

        # Подготовка строки для таблицы matches
        row_match = {
            "external_id": str(gid),
            "league_name": game.get("season", {}).get("league", {}).get("name", "Unknown"),
            "home_team": home_team,
            "away_team": away_team,
            "match_time": match_time.isoformat() if match_time else None,
            "status": status,
            "score_home": game.get("homeFTResult"),
            "score_away": game.get("awayFTResult"),
            "ht_score_home": game.get("homeHTResult"),
            "ht_score_away": game.get("awayHTResult"),
            # Статистика
            "stats_yellow_cards_home": statistics.get("yellowCardsHome"),
            "stats_yellow_cards_away": statistics.get("yellowCardsAway"),
            "stats_corners_home": statistics.get("cornerKicksHome"),
            "stats_corners_away": statistics.get("cornerKicksAway"),
            "stats_fouls_home": statistics.get("foulsHome"),
            "stats_xg_home": statistics.get("calculatedXgHome"),
            "stats_xg_away": statistics.get("calculatedXgAway"),
            "referee_name": referee_name,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Сохраняем или обновляем матч
            existing = supabase.table("matches").select("id").eq("external_id", str(gid)).execute()
            if existing.data:
                supabase.table("matches").update(row_match).eq("external_id", str(gid)).execute()
                print(f"  🔄 Матч обновлен: {home_team} vs {away_team}")
            else:
                supabase.table("matches").insert(row_match).execute()
                print(f"  ✅ Матч сохранен: {home_team} vs {away_team}")
            saved_matches += 1
            
            # 3. Генерируем и сохраняем вердикты
            verdicts = generate_verdicts(gid, statistics, odds_list, referee_name)
            
            for v in verdicts:
                row_verdict = {
                    "match_external_id": str(gid),
                    "market_type": v["market_type"],
                    "recommendation": v["recommendation"],
                    "confidence": v["confidence"],
                    "analysis_json": v["analysis_json"]
                }
                supabase.table("match_verdicts").insert(row_verdict).execute()
                saved_verdicts += 1
                
            print(f"  📊 Сохранено вердиктов: {len(verdicts)}")
                
        except Exception as e:
            print(f"💥 Ошибка сохранения: {e}")
            
        # Пауза между матчами
        time.sleep(1.5)

    print(f"🎉 Готово! Матчей: {saved_matches}, Вердиктов: {saved_verdicts}")

if __name__ == "__main__":
    main()
