import os
import re
import requests
from supabase import create_client
from datetime import datetime, timezone
from dateutil import parser as dateparser
import time
import statistics as stats_lib

# ══════════════════════════════════════════════════════
# НАСТРОЙКИ
# ══════════════════════════════════════════════════════
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

# 14 топовых лиг мира
TOP_LEAGUES = {
    39: "Premier League",
    41: "La Liga",
    45: "Bundesliga",
    135: "Serie A",
    44: "Ligue 1",
    57: "Eredivisie",
    76: "Primeira Liga",
    81: "Russian Premier League",
    42: "EFL League One",
    43: "EFL League Two",
    82: "Russian First League",
    307: "Saudi Pro League",
    7: "UEFA Champions League",
    679: "UEFA Europa League"
}

# Сколько сезонов собираем
CURRENT_YEAR = 2026
SEASONS_TO_KEEP = 3
SEASONS = [CURRENT_YEAR - i for i in range(SEASONS_TO_KEEP)]  # [2026, 2025, 2024]

# ══════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════

def safe_get(url, retries=3):
    """Безопасный GET запрос"""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json().get("data")
            elif r.status_code == 429:
                wait = 65 if attempt == 0 else 120
                print(f"  ⚠️ Лимит API! Ждем {wait} сек...")
                time.sleep(wait)
            elif r.status_code == 404:
                return None
            else:
                if attempt < retries - 1:
                    time.sleep(5)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(10)
    return None


def parse_match_time(date_str):
    """Парсинг даты"""
    if not date_str:
        return None
    try:
        return dateparser.isoparse(date_str)
    except:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except:
            return None


def parse_line_value(name):
    """Извлечение числа из строки (Over 4.5 -> 4.5)"""
    match = re.search(r'([\d.]+)', name)
    return float(match.group(1)) if match else None


def get_team_stats_from_db(team_id, year):
    """Получает статистику команды из БД"""
    try:
        data = supabase.table('team_stats').select('*').eq('team_id', team_id).eq('year', year).execute()
        if data.data:
            return data.data[0]
    except:
        pass
    return {}


def get_referee_stats_from_db(referee_name):
    """Получает статистику судьи из БД"""
    if not referee_name:
        return {"avg_yellow_cards": 4.2, "avg_fouls": 25}
    try:
        data = supabase.table("referee_stats").select("*").eq("referee_name", referee_name).execute()
        if data.data:
            return data.data[0]
    except:
        pass
    return {"avg_yellow_cards": 4.2, "avg_fouls": 25}


def get_h2h_stats(team1_id, team2_id, year):
    """Статистика очных встреч с деталями каждого матча"""
    if not team1_id or not team2_id:
        return {"matches_count": 0, "games": []}
    
    games = safe_get(f"{BASE}/games/list?teamid={team1_id}&year={year}")
    if not games:
        return {"matches_count": 0, "games": []}
    
    h2h = {
        "total_goals": [], 
        "yellow_cards": [], 
        "corners": [], 
        "fouls": [], 
        "matches_count": 0,
        "games": []
    }
    
    t2 = str(team2_id)
    found = 0
    
    for g in games:
        if str(g.get("homeTeam", {}).get("id", "")) != t2 and str(g.get("awayTeam", {}).get("id", "")) != t2:
            continue
        
        gid = g.get("id")
        if not gid:
            continue
        
        fd = safe_get(f"{BASE}/games/{gid}")
        if not fd:
            continue
        
        stats = fd.get("statistics", {})
        game = fd.get("game", {})
        
        goals = (game.get("homeFTResult") or 0) + (game.get("awayFTResult") or 0)
        yc = (stats.get("yellowCardsHome") or 0) + (stats.get("yellowCardsAway") or 0)
        corners = (stats.get("cornerKicksHome") or 0) + (stats.get("cornerKicksAway") or 0)
        fouls = (stats.get("foulsHome") or 0) + (stats.get("foulsAway") or 0)
        
        h2h["total_goals"].append(goals)
        h2h["yellow_cards"].append(yc)
        h2h["corners"].append(corners)
        h2h["fouls"].append(fouls)
        h2h["matches_count"] += 1
        
        h2h["games"].append({
            "date": game.get("date"),
            "home_team": game.get("homeTeam", {}).get("name"),
            "away_team": game.get("awayTeam", {}).get("name"),
            "score_home": game.get("homeFTResult"),
            "score_away": game.get("awayFTResult"),
            "total_goals": goals,
            "yellow_cards": yc,
            "corners": corners,
            "fouls": fouls
        })
        
        found += 1
        if found >= 5:
            break
        time.sleep(0.2)
    
    result = {
        "matches_count": h2h["matches_count"],
        "games": h2h["games"]
    }
    
    for key in ["total_goals", "yellow_cards", "corners", "fouls"]:
        values = h2h[key]
        result[f"avg_{key}"] = round(stats_lib.mean(values), 1) if values else 0
        result[f"max_{key}"] = max(values) if values else 0
        result[f"min_{key}"] = min(values) if values else 0
    
    return result


# ══════════════════════════════════════════════════════
# СБОР КОМАНДНОЙ СТАТИСТИКИ
# ══════════════════════════════════════════════════════

def collect_team_stats_directly(league_id, year):
    """Собирает статистику команд напрямую из матчей лиги"""
    print(f"  📊 Сбор статистики команд из матчей...")
    
    games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=200")
    if not games:
        print(f"  ❌ Нет матчей для лиги {league_id}")
        return 0
    
    teams = {}
    
    for game_summary in games:
        gid = game_summary.get("id")
        if not gid:
            continue
        
        full = safe_get(f"{BASE}/games/{gid}")
        if not full:
            continue
        
        game = full.get("game", {})
        stats = full.get("statistics", {})
        
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        home_id = home.get("id")
        away_id = away.get("id")
        
        if not home_id or not away_id:
            continue
        
        for tid, tname, is_home in [(home_id, home.get("name"), True), (away_id, away.get("name"), False)]:
            if tid not in teams:
                teams[tid] = {
                    "name": tname,
                    "goals_for": [], "goals_against": [],
                    "xg_for": [], "xg_against": [],
                    "yellow_cards": [], "corners": [], "fouls": [],
                    "wins": 0, "draws": 0, "losses": 0, "matches": 0
                }
            
            t = teams[tid]
            hs = game.get("homeFTResult") or 0
            aws = game.get("awayFTResult") or 0
            
            if is_home:
                t["goals_for"].append(hs)
                t["goals_against"].append(aws)
                t["xg_for"].append(stats.get("calculatedXgHome") or 0)
                t["xg_against"].append(stats.get("calculatedXgAway") or 0)
                t["yellow_cards"].append(stats.get("yellowCardsHome") or 0)
                t["corners"].append(stats.get("cornerKicksHome") or 0)
                t["fouls"].append(stats.get("foulsHome") or 0)
                if hs > aws: t["wins"] += 1
                elif hs < aws: t["losses"] += 1
                else: t["draws"] += 1
            else:
                t["goals_for"].append(aws)
                t["goals_against"].append(hs)
                t["xg_for"].append(stats.get("calculatedXgAway") or 0)
                t["xg_against"].append(stats.get("calculatedXgHome") or 0)
                t["yellow_cards"].append(stats.get("yellowCardsAway") or 0)
                t["corners"].append(stats.get("cornerKicksAway") or 0)
                t["fouls"].append(stats.get("foulsAway") or 0)
                if aws > hs: t["wins"] += 1
                elif aws < hs: t["losses"] += 1
                else: t["draws"] += 1
            
            t["matches"] += 1
        
        time.sleep(0.3)
    
    # Сохраняем в БД
    saved = 0
    for team_id, data in teams.items():
        if data["matches"] == 0:
            continue
        
        def avg(lst):
            return round(stats_lib.mean(lst), 2) if lst else 0
        
        try:
            row = {
                "team_id": int(team_id),
                "team_name": data["name"],
                "league_id": league_id,
                "year": year,
                "league_name": TOP_LEAGUES.get(league_id, "Unknown"),
                "matches_played": data["matches"],
                "wins": data["wins"],
                "draws": data["draws"],
                "losses": data["losses"],
                "goals_for": sum(data["goals_for"]),
                "goals_against": sum(data["goals_against"]),
                "goals_for_avg": avg(data["goals_for"]),
                "goals_against_avg": avg(data["goals_against"]),
                "xg_for": round(sum(data["xg_for"]), 2),
                "xg_against": round(sum(data["xg_against"]), 2),
                "xg_for_avg": avg(data["xg_for"]),
                "xg_against_avg": avg(data["xg_against"]),
                "points": data["wins"] * 3 + data["draws"],
                "avg_yellow_cards_for": avg(data["yellow_cards"]),
                "avg_corners_for": avg(data["corners"]),
                "avg_fouls_for": avg(data["fouls"]),
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            existing = supabase.table("team_stats").select("id").eq("team_id", team_id).eq("year", year).execute()
            if existing.data:
                supabase.table("team_stats").update(row).eq("team_id", team_id).eq("year", year).execute()
            else:
                supabase.table("team_stats").insert(row).execute()
            saved += 1
        except Exception as e:
            print(f"    ❌ Ошибка сохранения {data['name']}: {e}")
    
    return saved


# ══════════════════════════════════════════════════════
# СБОР СТАТИСТИКИ СУДЕЙ
# ══════════════════════════════════════════════════════

def update_referee_stats(league_id, year):
    """Собирает статистику судей из матчей"""
    print(f"  👨‍⚖️ Сбор статистики судей...")
    
    games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=200")
    if not games:
        return
    
    refs = {}
    
    for game_summary in games:
        gid = game_summary.get("id")
        if not gid:
            continue
        
        full = safe_get(f"{BASE}/games/{gid}")
        if not full:
            continue
        
        rname = full.get("refereeName")
        if not rname:
            continue
        
        stats = full.get("statistics", {})
        
        if rname not in refs:
            refs[rname] = {"yellow_cards": [], "fouls": [], "corners": [], "matches": 0}
        
        yc = (stats.get("yellowCardsHome") or 0) + (stats.get("yellowCardsAway") or 0)
        fouls = (stats.get("foulsHome") or 0) + (stats.get("foulsAway") or 0)
        corners = (stats.get("cornerKicksHome") or 0) + (stats.get("cornerKicksAway") or 0)
        
        refs[rname]["yellow_cards"].append(yc)
        refs[rname]["fouls"].append(fouls)
        refs[rname]["corners"].append(corners)
        refs[rname]["matches"] += 1
        
        time.sleep(0.2)
    
    for name, data in refs.items():
        try:
            row = {
                "referee_name": name,
                "avg_yellow_cards": round(stats_lib.mean(data["yellow_cards"]), 1) if data["yellow_cards"] else 0,
                "avg_fouls": round(stats_lib.mean(data["fouls"]), 1) if data["fouls"] else 0,
                "avg_corners": round(stats_lib.mean(data["corners"]), 1) if data["corners"] else 0,
                "matches_officiated": data["matches"],
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            existing = supabase.table("referee_stats").select("id").eq("referee_name", name).execute()
            if existing.data:
                supabase.table("referee_stats").update(row).eq("referee_name", name).execute()
            else:
                supabase.table("referee_stats").insert(row).execute()
        except Exception as e:
            print(f"    ❌ Ошибка сохранения судьи {name}: {e}")
    
    print(f"  ✅ Сохранено {len(refs)} судей")


# ══════════════════════════════════════════════════════
# ГЕНЕРАЦИЯ ВЕРДИКТОВ
# ══════════════════════════════════════════════════════

def generate_verdicts(game, statistics, odds_list, referee_name, home_id, away_id, home_name, away_name, year):
    """Генерирует умные вердикты на основе всех данных"""
    verdicts = []
    
    home_stats = get_team_stats_from_db(home_id, year) if home_id else {}
    away_stats = get_team_stats_from_db(away_id, year) if away_id else {}
    h2h = get_h2h_stats(home_id, away_id, year) if home_id and away_id else {}
    ref_stats = get_referee_stats_from_db(referee_name)
    
    # Извлекаем линии букмекера
    lines = {}
    for market in odds_list:
        m_name = market.get("marketName", "")
        for odd in market.get("odds", []):
            odd_name = odd.get("name", "")
            odd_value = odd.get("value", 0)
            
            if ("Yellow" in m_name or "ЖК" in m_name) and "yellow_cards" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 1.5 <= val <= 7.5:
                        lines["yellow_cards"] = {"line": val, "odds": odd_value}
            
            elif ("Total" in m_name or "Goals" in m_name or "Голы" in m_name) and "total_goals" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 0.5 < val < 8:
                        lines["total_goals"] = {"line": val, "odds": odd_value}
            
            elif ("Corner" in m_name or "Corners" in m_name or "Угловые" in m_name) and "corners" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 5 <= val <= 15:
                        lines["corners"] = {"line": val, "odds": odd_value}
            
            elif ("Foul" in m_name or "Fouls" in m_name or "Фолы" in m_name) and "fouls" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 15 <= val <= 40:
                        lines["fouls"] = {"line": val, "odds": odd_value}
    
    # ---- ВЕРДИКТ ПО ЖК ----
    if "yellow_cards" in lines:
        line = lines["yellow_cards"]["line"]
        preds, weights = [], []
        
        hy = home_stats.get("avg_yellow_cards_for", 0)
        ay = away_stats.get("avg_yellow_cards_for", 0)
        if hy and ay:
            preds.append(hy + ay)
            weights.append(0.35)
        
        if h2h.get("avg_yellow_cards", 0) > 0:
            preds.append(h2h["avg_yellow_cards"])
            weights.append(0.40)
        
        preds.append(ref_stats.get("avg_yellow_cards", 4.2))
        weights.append(0.25)
        
        current_yc = (statistics.get("yellowCardsHome") or 0) + (statistics.get("yellowCardsAway") or 0)
        if current_yc > 0:
            preds.append(current_yc)
            weights.append(0.15)
        
        if preds:
            tw = sum(weights)
            w = [x/tw for x in weights]
            pred = sum(p * ww for p, ww in zip(preds, w))
            diff = pred - line
            
            if diff >= 1.0:
                conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 0.4:
                conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -1.0:
                conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -0.4:
                conf, rec = "LOW", f"TAKE_TM_{line}"
            else:
                conf, rec = "LOW", "SKIP"
            
            verdicts.append({
                "market_type": "YELLOW_CARDS",
                "recommendation": rec,
                "confidence": conf,
                "analysis_json": {
                    "model_prediction": round(pred, 1),
                    "bookmaker_line": line,
                    "difference": round(diff, 1),
                    "h2h_avg": h2h.get("avg_yellow_cards", 0),
                    "h2h_matches": h2h.get("matches_count", 0),
                    "referee_avg": ref_stats.get("avg_yellow_cards"),
                    "referee_name": referee_name,
                    "h2h_games": h2h.get("games", [])
                }
            })
    
    # ---- ВЕРДИКТ ПО ГОЛАМ ----
    if "total_goals" in lines:
        line = lines["total_goals"]["line"]
        preds, weights = [], []
        
        hg = home_stats.get("goals_for_avg", 0)
        ag = away_stats.get("goals_for_avg", 0)
        if hg and ag:
            preds.append(hg + ag)
            weights.append(0.30)
        
        if h2h.get("avg_total_goals", 0) > 0:
            preds.append(h2h["avg_total_goals"])
            weights.append(0.50)
        
        xg_h = statistics.get("calculatedXgHome") or 0
        xg_a = statistics.get("calculatedXgAway") or 0
        if xg_h + xg_a > 0:
            preds.append(xg_h + xg_a)
            weights.append(0.20)
        
        if preds:
            tw = sum(weights)
            w = [x/tw for x in weights]
            pred = sum(p * ww for p, ww in zip(preds, w))
            diff = pred - line
            
            if diff >= 0.8:
                conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 0.3:
                conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -0.8:
                conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -0.3:
                conf, rec = "LOW", f"TAKE_TM_{line}"
            else:
                conf, rec = "LOW", "SKIP"
            
            verdicts.append({
                "market_type": "GOALS",
                "recommendation": rec,
                "confidence": conf,
                "analysis_json": {
                    "model_prediction": round(pred, 1),
                    "bookmaker_line": line,
                    "difference": round(diff, 1),
                    "h2h_avg_goals": h2h.get("avg_total_goals", 0),
                    "h2h_matches": h2h.get("matches_count", 0),
                    "xg_total": round(xg_h + xg_a, 1),
                    "h2h_games": h2h.get("games", [])
                }
            })
    
    # ---- ВЕРДИКТ ПО УГЛОВЫМ ----
    if "corners" in lines:
        line = lines["corners"]["line"]
        preds, weights = [], []
        
        hc = home_stats.get("avg_corners_for", 0)
        ac = away_stats.get("avg_corners_for", 0)
        if hc and ac:
            preds.append(hc + ac)
            weights.append(0.40)
        
        if h2h.get("avg_corners", 0) > 0:
            preds.append(h2h["avg_corners"])
            weights.append(0.45)
        
        cur_c = (statistics.get("cornerKicksHome") or 0) + (statistics.get("cornerKicksAway") or 0)
        if cur_c > 0:
            preds.append(cur_c)
            weights.append(0.15)
        
        if preds:
            tw = sum(weights)
            w = [x/tw for x in weights]
            pred = sum(p * ww for p, ww in zip(preds, w))
            diff = pred - line
            
            if diff >= 2.0:
                conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 1.0:
                conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -2.0:
                conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -1.0:
                conf, rec = "LOW", f"TAKE_TM_{line}"
            else:
                conf, rec = "LOW", "SKIP"
            
            verdicts.append({
                "market_type": "CORNERS",
                "recommendation": rec,
                "confidence": conf,
                "analysis_json": {
                    "model_prediction": round(pred, 1),
                    "bookmaker_line": line,
                    "difference": round(diff, 1),
                    "h2h_avg_corners": h2h.get("avg_corners", 0),
                    "h2h_games": h2h.get("games", [])
                }
            })
    
    # ---- ВЕРДИКТ ПО ФОЛАМ ----
    if "fouls" in lines:
        line = lines["fouls"]["line"]
        preds, weights = [], []
        
        hf = home_stats.get("avg_fouls_for", 0)
        af = away_stats.get("avg_fouls_for", 0)
        if hf and af:
            preds.append(hf + af)
            weights.append(0.35)
        
        if h2h.get("avg_fouls", 0) > 0:
            preds.append(h2h["avg_fouls"])
            weights.append(0.40)
        
        ref_fouls = ref_stats.get("avg_fouls", 25)
        preds.append(ref_fouls)
        weights.append(0.25)
        
        cur_f = (statistics.get("foulsHome") or 0) + (statistics.get("foulsAway") or 0)
        if cur_f > 0:
            preds.append(cur_f)
            weights.append(0.15)
        
        if preds:
            tw = sum(weights)
            w = [x/tw for x in weights]
            pred = sum(p * ww for p, ww in zip(preds, w))
            diff = pred - line
            
            if diff >= 4.0:
                conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 2.0:
                conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -4.0:
                conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -2.0:
                conf, rec = "LOW", f"TAKE_TM_{line}"
            else:
                conf, rec = "LOW", "SKIP"
            
            verdicts.append({
                "market_type": "FOULS",
                "recommendation": rec,
                "confidence": conf,
                "analysis_json": {
                    "model_prediction": round(pred, 1),
                    "bookmaker_line": line,
                    "difference": round(diff, 1),
                    "h2h_avg_fouls": h2h.get("avg_fouls", 0),
                    "h2h_matches": h2h.get("matches_count", 0),
                    "referee_avg_fouls": ref_fouls,
                    "referee_name": referee_name,
                    "h2h_games": h2h.get("games", [])
                }
            })
    
    return verdicts


# ══════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════════════════

def main():
    print("🚀 ЗАПУСК СБОРЩИКА (14 лиг, 3 сезона)")
    print("=" * 55)
    
    total_matches = 0
    total_verdicts = 0
    
    for league_id, league_name in TOP_LEAGUES.items():
        print(f"\n{'='*55}")
        print(f"🏆 {league_name} (ID: {league_id})")
        print(f"{'='*55}")
        
        for year in SEASONS:
            print(f"\n📅 Сезон {year-1}/{year}")
            
            # Проверяем есть ли уже командная статистика
            stats_count = supabase.table("team_stats").select("id", count="exact").eq("league_id", league_id).eq("year", year).execute()
            if not stats_count.count:
                print(f"  📊 Статистика команд не найдена. Собираем...")
                collect_team_stats_directly(league_id, year)
                update_referee_stats(league_id, year)
            
            # Получаем матчи
            games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=200")
            
            if not games:
                print(f"  ❌ Нет матчей для этого сезона")
                continue
            
            print(f"  ⚽ Обработка {len(games)} матчей...")
            
            for idx, gs in enumerate(games):
                gid = gs.get("id")
                if not gid:
                    continue
                
                if idx % 20 == 0 and idx > 0:
                    print(f"    Прогресс: {idx}/{len(games)}")
                
                full = safe_get(f"{BASE}/games/{gid}")
                if not full:
                    continue
                
                game = full.get("game", {})
                stats = full.get("statistics", {})
                ref_name = full.get("refereeName")
                odds = game.get("odds", [])
                
                home = game.get("homeTeam", {})
                away = game.get("awayTeam", {})
                home_name = home.get("name", "Unknown")
                away_name = away.get("name", "Unknown")
                home_id = home.get("id")
                away_id = away.get("id")
                
                mt = parse_match_time(game.get("date"))
                gs_code = game.get("status")
                status = "finished" if gs_code in [8, 9, 10] else ("live" if gs_code in [1, 2, 3] else "scheduled")
                
                row = {
                    "external_id": str(gid),
                    "league_name": league_name,
                    "home_team": home_name,
                    "away_team": away_name,
                    "match_time": mt.isoformat() if mt else None,
                    "status": status,
                    "score_home": game.get("homeFTResult"),
                    "score_away": game.get("awayFTResult"),
                    "ht_score_home": game.get("homeHTResult"),
                    "ht_score_away": game.get("awayHTResult"),
                    "stats_yellow_cards_home": stats.get("yellowCardsHome"),
                    "stats_yellow_cards_away": stats.get("yellowCardsAway"),
                    "stats_corners_home": stats.get("cornerKicksHome"),
                    "stats_corners_away": stats.get("cornerKicksAway"),
                    "stats_fouls_home": stats.get("foulsHome"),
                    "stats_fouls_away": stats.get("foulsAway"),
                    "stats_xg_home": stats.get("calculatedXgHome"),
                    "stats_xg_away": stats.get("calculatedXgAway"),
                    "referee_name": ref_name,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }
                
                try:
                    existing = supabase.table("matches").select("id").eq("external_id", str(gid)).execute()
                    if existing.data:
                        supabase.table("matches").update(row).eq("external_id", str(gid)).execute()
                    else:
                        supabase.table("matches").insert(row).execute()
                    total_matches += 1
                    
                    # Генерируем вердикты
                    verdicts = generate_verdicts(game, stats, odds, ref_name, home_id, away_id, home_name, away_name, year)
                    
                    # Удаляем старые вердикты и сохраняем новые
                    supabase.table("match_verdicts").delete().eq("match_external_id", str(gid)).execute()
                    for v in verdicts:
                        supabase.table("match_verdicts").insert({
                            "match_external_id": str(gid),
                            "market_type": v["market_type"],
                            "recommendation": v["recommendation"],
                            "confidence": v["confidence"],
                            "analysis_json": v["analysis_json"]
                        }).execute()
                        total_verdicts += 1
                    
                except Exception as e:
                    print(f"    ❌ Ошибка: {e}")
                
                time.sleep(0.5)
    
    print(f"\n{'='*55}")
    print(f"🎉 ГОТОВО!")
    print(f"📊 Матчей: {total_matches}")
    print(f"🎯 Вердиктов: {total_verdicts}")
    print(f"💾 Размер БД: ~108 МБ (из 500 МБ)")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
