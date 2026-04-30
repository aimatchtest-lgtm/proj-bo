import os
import re
import requests
from supabase import create_client
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateparser
import time
import statistics as stats_lib

# ══════════════════════════════════════════════════════
# НАСТРОЙКИ
# ══════════════════════════════════════════════════════
SUPABASE_URL = os.environ.get("SUPABASE_URL")
# ИСПРАВЛЕНО: имя переменной должно совпадать с названием секрета в GitHub
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")  # ✅ Теперь совпадает с GitHub Secrets
API_KEY = os.environ.get("SSTATS_API_KEY")

# Проверка на случай, если ключи всё ещё пустые (для отладки)
if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ОШИБКА: Один из ключей Supabase пустой!")
    print(f"   SUPABASE_URL: {'✓' if SUPABASE_URL else '✗'}")
    print(f"   SUPABASE_KEY: {'✓' if SUPABASE_KEY else '✗'}")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

TOP_LEAGUES = {
    39: "Premier League",
    140: "La Liga",
    78: "Bundesliga",
    135: "Serie A",
    61: "Ligue 1",
    88: "Eredivisie",
    94: "Primeira Liga",
    235: "Russian Premier League",
    41: "EFL League One",
    42: "EFL League Two",
    236: "Russian First League",
    307: "Saudi Pro League",
    2: "UEFA Champions League",
    3: "UEFA Europa League"
}

CURRENT_YEAR = 2026
SEASONS_TO_CHECK = [CURRENT_YEAR, CURRENT_YEAR - 1]

# ══════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════

def safe_get(url, retries=2):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json().get("data")
            elif r.status_code == 429:
                time.sleep(10)
            elif r.status_code == 404:
                return None
            else:
                time.sleep(5)
        except Exception as e:
            print(f"  ❌ Ошибка запроса {url[:50]}...: {e}")
            time.sleep(5)
    return None

def parse_match_time(date_str):
    if not date_str: return None
    try:
        return dateparser.isoparse(date_str)
    except:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except:
            return None

def parse_line_value(name):
    match = re.search(r'([\d.]+)', name)
    return float(match.group(1)) if match else None

# ══════════════════════════════════════════════════════
# ЛОГИКА АНАЛИЗА И ВЕРДИКТОВ
# ══════════════════════════════════════════════════════

def get_team_stats_from_db(team_id, year):
    try:
        data = supabase.table('team_stats').select('*').eq('team_id', team_id).eq('year', year).execute()
        return data.data[0] if data.data else {}
    except:
        return {}

def get_referee_stats_from_db(referee_name):
    if not referee_name:
        return {"avg_yellow_cards": 4.2, "avg_fouls": 25}
    try:
        data = supabase.table("referee_stats").select("*").eq("referee_name", referee_name).execute()
        return data.data[0] if data.data else {"avg_yellow_cards": 4.2, "avg_fouls": 25}
    except:
        return {"avg_yellow_cards": 4.2, "avg_fouls": 25}

def generate_verdicts(game, statistics, odds_list, referee_name, home_id, away_id, home_name, away_name, year):
    verdicts = []
    home_stats = get_team_stats_from_db(home_id, year) if home_id else {}
    away_stats = get_team_stats_from_db(away_id, year) if away_id else {}
    ref_stats = get_referee_stats_from_db(referee_name)
    
    # Отладка: печатаем статистику команд
    # print(f"    📊 Stats Home: {home_stats.get('goals_for_avg')}, Away: {away_stats.get('goals_for_avg')}")

    lines = {}
    for market in odds_list:
        m_name = market.get("marketName", "")
        for odd in market.get("odds", []):
            odd_name = odd.get("name", ""); odd_value = odd.get("value", 0)
            
            # ЖК
            if ("Yellow" in m_name or "ЖК" in m_name) and "yellow_cards" not in lines:
                if "Over" in odd_name or "TB" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 1.5 <= val <= 7.5: lines["yellow_cards"] = {"line": val, "odds": odd_value}
            # Голы
            elif ("Total" in m_name or "Goals" in m_name or "Голы" in m_name) and "total_goals" not in lines:
                if "Over" in odd_name or "TB" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 0.5 < val < 8: lines["total_goals"] = {"line": val, "odds": odd_value}

    # --- Генерация вердиктов ---
    
    # 1. ГОЛЫ
    if "total_goals" in lines:
        line = lines["total_goals"]["line"]
        hg = home_stats.get("goals_for_avg", 0); ag = away_stats.get("goals_for_avg", 0)
        pred = hg + ag
        
        diff = pred - line
        conf, rec = "LOW", "SKIP"
        if diff >= 0.8: conf, rec = "HIGH", f"TAKE_TB_{line}"
        elif diff >= 0.3: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
        elif diff <= -0.8: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
        elif diff <= -0.3: conf, rec = "LOW", f"TAKE_TM_{line}"
        
        verdicts.append({
            "market_type": "GOALS",
            "recommendation": rec,
            "confidence": conf,
            "analysis_json": {
                "model_prediction": round(pred, 1),
                "bookmaker_line": line,
                "difference": round(diff, 1)
            }
        })
        # print(f"    ⚽ Goals: Pred={pred}, Line={line}, Diff={diff} -> {rec}")

    # 2. ЖК
    if "yellow_cards" in lines:
        line = lines["yellow_cards"]["line"]
        hy = home_stats.get("avg_yellow_cards_for", 0); ay = away_stats.get("avg_yellow_cards_for", 0)
        pred = (hy + ay) * 0.5 + ref_stats.get("avg_yellow_cards", 4.2) * 0.5
        diff = pred - line
        
        conf, rec = "LOW", "SKIP"
        if diff >= 1.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
        elif diff >= 0.4: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
        elif diff <= -1.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
        elif diff <= -0.4: conf, rec = "LOW", f"TAKE_TM_{line}"
        
        verdicts.append({
            "market_type": "YELLOW_CARDS",
            "recommendation": rec,
            "confidence": conf,
            "analysis_json": {
                "model_prediction": round(pred, 1),
                "bookmaker_line": line,
                "difference": round(diff, 1)
            }
        })
        # print(f"    🟨 YC: Pred={pred}, Line={line}, Diff={diff} -> {rec}")
        
    return verdicts

def update_team_stats_incremental(home_id, home_name, away_id, away_name, stats, year, league_id, league_name):
    """Обновляет статистику команд точечно после одного матча"""
    if not home_id or not away_id: return
    
    for tid, tname, is_home in [(home_id, home_name, True), (away_id, away_name, False)]:
        try:
            current = get_team_stats_from_db(tid, year)
            
            hs = stats.get("homeFTResult") or 0
            aws = stats.get("awayFTResult") or 0
            
            goals_for = hs if is_home else aws
            goals_against = aws if is_home else hs
            
            wins = 1 if (is_home and hs > aws) or (not is_home and aws > hs) else 0
            draws = 1 if hs == aws else 0
            losses = 1 if (is_home and hs < aws) or (not is_home and aws < hs) else 0
            
            matches_played = current.get("matches_played", 0) + 1
            new_goals_for = current.get("goals_for", 0) + goals_for
            new_goals_against = current.get("goals_against", 0) + goals_against
            
            row = {
                "team_id": int(tid),
                "team_name": tname,
                "league_id": league_id,
                "year": year,
                "league_name": league_name,
                "matches_played": matches_played,
                "wins": current.get("wins", 0) + wins,
                "draws": current.get("draws", 0) + draws,
                "losses": current.get("losses", 0) + losses,
                "goals_for": new_goals_for,
                "goals_against": new_goals_against,
                "goals_for_avg": round(new_goals_for / matches_played, 2),
                "goals_against_avg": round(new_goals_against / matches_played, 2),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            existing = supabase.table("team_stats").select("id").eq("team_id", tid).eq("year", year).execute()
            if existing.data:
                supabase.table("team_stats").update(row).eq("team_id", tid).eq("year", year).execute()
            else:
                supabase.table("team_stats").insert(row).execute()
                
        except Exception as e:
            print(f"      ❌ Ошибка обновления статистики команды {tname}: {e}")

def process_league_season(league_id, league_name, year):
    print(f"  📅 Сезон {year-1}/{year}")
    
    games_list = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=100")
    if not games_list:
        print(f"    ⚠️ Нет данных от API")
        return 0, 0

    total_matches_saved = 0
    total_verdicts_saved = 0
    
    existing_ids_res = supabase.table('matches').select('external_id').eq('league_name', league_name).execute()
    existing_ids = set([m['external_id'] for m in existing_ids_res.data]) if existing_ids_res.data else set()

    for gs in games_list:
        gid = str(gs.get("id"))
        if not gid: continue
        
        is_new = gid not in existing_ids
        
        full_data = safe_get(f"{BASE}/games/{gid}")
        if not full_data: continue
        
        game = full_data.get("game", {})
        stats = full_data.get("statistics", {})
        odds = game.get("odds", [])
        ref_name = full_data.get("refereeName")
        
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        home_id = home.get("id")
        away_id = away.get("id")
        
        gs_code = game.get("status")
        status = "finished" if gs_code in [8, 9, 10] else ("live" if gs_code in [1, 2, 3] else "scheduled")
        mt = parse_match_time(game.get("date"))
        
        row = {
            "external_id": gid,
            "league_name": league_name,
            "home_team": home.get("name"),
            "away_team": away.get("name"),
            "match_time": mt.isoformat() if mt else None,
            "status": status,
            "score_home": game.get("homeFTResult"),
            "score_away": game.get("awayFTResult"),
            "stats_xg_home": stats.get("calculatedXgHome"),
            "stats_xg_away": stats.get("calculatedXgAway"),
            "referee_name": ref_name,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        
        try:
            if is_new:
                supabase.table("matches").insert(row).execute()
                existing_ids.add(gid)
            else:
                supabase.table("matches").update(row).eq("external_id", gid).execute()
            total_matches_saved += 1
        except Exception as e:
            print(f"    ❌ Ошибка сохранения матча {gid}: {e}")
            continue

        if status == "finished":
            update_team_stats_incremental(home_id, home.get("name"), away_id, away.get("name"), stats, year, league_id, league_name)

        if status == "scheduled" and mt and mt <= datetime.now(timezone.utc) + timedelta(days=3):
            verdicts = generate_verdicts(game, stats, odds, ref_name, home_id, away_id, home.get("name"), away.get("name"), year)
            
            supabase.table("match_verdicts").delete().eq("match_external_id", gid).execute()
            
            saved_count = 0
            for v in verdicts:
                if v['recommendation'] != 'SKIP':
                    supabase.table("match_verdicts").insert({
                        "match_external_id": gid,
                        "market_type": v["market_type"],
                        "recommendation": v["recommendation"],
                        "confidence": v["confidence"],
                        "analysis_json": v["analysis_json"]
                    }).execute()
                    saved_count += 1
                    total_verdicts_saved += 1
            
            if saved_count > 0:
                print(f"    ✅ {home.get('name')} vs {away.get('name')}: {saved_count} вердиктов")
            elif len(verdicts) > 0:
                print(f"    ⚪ {home.get('name')} vs {away.get('name')}: Все вердикты SKIP")

        time.sleep(0.2)

    return total_matches_saved, total_verdicts_saved

def main():
    print("🚀 ЗАПУСК УМНОГО СБОРЩИКА (Инкрементальный)")
    print("=" * 50)
    
    total_m = 0
    total_v = 0
    
    for league_id, league_name in TOP_LEAGUES.items():
        print(f"\n🏆 {league_name} (ID: {league_id})")
        for year in SEASONS_TO_CHECK:
            m, v = process_league_season(league_id, league_name, year)
            total_m += m
            total_v += v
            
    print(f"\n🎉 ГОТОВО! Обработано матчей: {total_m}, Создано вердиктов: {total_v}")

if __name__ == "__main__":
    main()
