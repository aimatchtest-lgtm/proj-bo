import os
import re
import requests
from supabase import create_client
from datetime import datetime, timezone
from dateutil import parser as dateparser
from datetime import timedelta
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
SEASONS_TO_KEEP = 3
SEASONS = [CURRENT_YEAR - i for i in range(SEASONS_TO_KEEP)]

# ══════════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ══════════════════════════════════════════════════════

def safe_get(url, retries=2):
    """Безопасный GET запрос с подробными логами"""
    for attempt in range(retries):
        print(f"  🔍 Запрос API (попытка {attempt+1}): {url[:80]}...")  # Показываем, что запрос ушёл
        try:
            r = requests.get(url, headers=headers, timeout=15)
            print(f"  📡 Статус ответа: {r.status_code}")
            
            if r.status_code == 200:
                data = r.json().get("data")
                print(f"  ✅ Успешно. Получено данных: {len(data) if isinstance(data, list) else 'объект'}")
                return data
                
            elif r.status_code == 429:
                wait = 10
                print(f"  ⚠️ Лимит API (Too Many Requests)! Ждем {wait} сек...")
                time.sleep(wait)
                
            elif r.status_code == 404:
                print(f"  ⚠️ Не найдено (404)")
                return None
            else:
                print(f"  ⚠️ Ошибка {r.status_code}. Пауза 5 сек...")
                time.sleep(5)
                
        except requests.exceptions.Timeout:
            print(f"  ❌ Таймаут запроса! Сервер не ответил за 15 сек.")
            time.sleep(5)
        except Exception as e:
            print(f"  ❌ Сбой соединения: {e}")
            time.sleep(5)
            
    print(f"  💥 Все попытки исчерпаны.")
    return None


def parse_match_time(date_str):
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
    match = re.search(r'([\d.]+)', name)
    return float(match.group(1)) if match else None


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


def get_injuries(game_id):
    data = safe_get(f"{BASE}/games/injuries?gameId={game_id}")
    if not data:
        return []
    return [{"player_name": i.get("player", {}).get("name"), "team_id": i.get("teamId"), "reason": i.get("reason")} for i in data]


def get_glicko(game_id):
    data = safe_get(f"{BASE}/games/glicko/{game_id}")
    if not data:
        return {}
    return {"home_rating": data.get("homeRating"), "away_rating": data.get("awayRating"), "home_win_prob": data.get("homeWinProbability"), "away_win_prob": data.get("awayWinProbability")}


def get_referee_last_matches(referee_name, limit=5):
    if not referee_name:
        return []
    try:
        data = supabase.table("matches").select("*").eq("referee_name", referee_name).eq("status", "finished").order("match_time", desc=True).limit(limit).execute()
        if not data.data:
            return []
        return [{"date": m.get("match_time"), "home_team": m.get("home_team"), "away_team": m.get("away_team"), "score_home": m.get("score_home"), "score_away": m.get("score_away"), "yellow_cards": (m.get("stats_yellow_cards_home") or 0) + (m.get("stats_yellow_cards_away") or 0), "fouls": (m.get("stats_fouls_home") or 0) + (m.get("stats_fouls_away") or 0), "corners": (m.get("stats_corners_home") or 0) + (m.get("stats_corners_away") or 0)} for m in data.data]
    except:
        return []


def get_team_form(team_name, limit=5):
    if not team_name:
        return {"form_pct": 50, "matches": 0}
    try:
        home = supabase.table("matches").select("*").eq("home_team", team_name).eq("status", "finished").order("match_time", desc=True).limit(limit).execute()
        away = supabase.table("matches").select("*").eq("away_team", team_name).eq("status", "finished").order("match_time", desc=True).limit(limit).execute()
        all_matches = []
        for m in (home.data or []):
            if m.get("score_home") is not None:
                pts = 3 if m["score_home"] > m["score_away"] else (1 if m["score_home"] == m["score_away"] else 0)
                all_matches.append({"date": m["match_time"], "points": pts})
        for m in (away.data or []):
            if m.get("score_home") is not None:
                pts = 3 if m["score_away"] > m["score_home"] else (1 if m["score_away"] == m["score_home"] else 0)
                all_matches.append({"date": m["match_time"], "points": pts})
        all_matches.sort(key=lambda x: x["date"], reverse=True)
        last = all_matches[:limit]
        if not last:
            return {"form_pct": 50, "matches": 0}
        return {"form_pct": round(sum(m["points"] for m in last) / (len(last) * 3) * 100, 1), "matches": len(last)}
    except:
        return {"form_pct": 50, "matches": 0}


def get_lineup(game_id):
    full = safe_get(f"{BASE}/games/{game_id}")
    if not full:
        return []
    return full.get("lineupPlayers", [])


def get_player_stats_from_db(player_name):
    try:
        data = supabase.table("player_stats").select("*").eq("player_name", player_name).execute()
        return data.data[0] if data.data else {}
    except:
        return {}


def analyze_missing_players(home_id, away_id, game_id):
    injuries = get_injuries(game_id)
    lineup = get_lineup(game_id)
    missing_home, missing_away = [], []
    home_positions, away_positions = {}, {}
    
    for inj in injuries:
        player_name = inj["player_name"]
        team_id = inj["team_id"]
        player_stats = get_player_stats_from_db(player_name)
        position = player_stats.get("position", "?")
        replaced_by = "неизвестно"
        for p in lineup:
            if p.get("teamId") == team_id and p.get("position") == position:
                if p.get("playerName") != player_name:
                    replaced_by = p.get("playerName")
                    break
        info = {"name": player_name, "reason": inj.get("reason", "неизвестно"), "position": position, "goals_impact": player_stats.get("goals_impact", 0.1), "replaced_by": replaced_by}
        if team_id == home_id:
            missing_home.append(info)
            home_positions[position] = home_positions.get(position, 0) + 1
        else:
            missing_away.append(info)
            away_positions[position] = away_positions.get(position, 0) + 1
    
    def gen_analysis(missing_list, positions, side_name):
        if not missing_list:
            return f"{side_name}: все в строю ✅"
        text = f"{side_name}:\n"
        d_count = positions.get("D", 0)
        g_count = positions.get("G", 0)
        f_count = positions.get("F", 0)
        for p in missing_list:
            main_stats = get_player_stats_from_db(p["name"])
            backup_stats = get_player_stats_from_db(p.get("replaced_by", ""))
            text += f"  ❌ {p['name']} ({p['position']}) — {p['reason']}\n     → Замена: {p['replaced_by']}\n"
            if p["position"] == "G":
                mg = main_stats.get("goals_conceded_avg") or 0
                bg = backup_stats.get("goals_conceded_avg") or 0
                if bg > 0 and mg > 0:
                    if bg > mg * 1.15: text += f"     ⚠️ Пропускает больше ({bg} vs {mg}). ХУЖЕ.\n"
                    elif bg < mg * 0.85: text += f"     ✅ Пропускает меньше ({bg} vs {mg}). ЛУЧШЕ!\n"
                    else: text += f"     ≈ Примерно равен.\n"
                else: text += f"     ⚠️ Нет данных.\n"
            elif p["position"] == "D":
                me = main_stats.get("errors_leading_to_goal") or 0
                be = backup_stats.get("errors_leading_to_goal") or 0
                if be > me: text += f"     ⚠️ Ошибается чаще ({be} vs {me}). ХУЖЕ.\n"
                elif be < me: text += f"     ✅ Ошибается реже ({be} vs {me}). ЛУЧШЕ!\n"
                else: text += f"     ⚠️ Нет данных.\n"
            elif p["position"] == "F":
                mg = main_stats.get("goals_scored") or 0
                bg = backup_stats.get("goals_scored") or 0
                mmp = max(main_stats.get("matches_played", 1) or 1, 1)
                bmp = max(backup_stats.get("matches_played", 1) or 1, 1)
                ma = round(mg/mmp, 2)
                ba = round(bg/bmp, 2)
                if ba > 0 and ma > 0:
                    if ba < ma * 0.7: text += f"     ⚠️ Забивает реже ({ba} vs {ma}). ХУЖЕ.\n"
                    elif ba > ma * 1.3: text += f"     ✅ Забивает чаще ({ba} vs {ma}). ЛУЧШЕ!\n"
                    else: text += f"     ≈ Примерно равен.\n"
                else: text += f"     ⚠️ Нет данных.\n"
            else: text += f"     ⚠️ Нет данных.\n"
        text += f"\n  📊 ИТОГ: "
        if d_count >= 2: text += f"Выбыли {d_count} защитника. Центр обороны разрушен!"
        elif d_count == 1 and g_count >= 1: text += f"Выбыл защитник и вратарь. Оборона ослаблена."
        elif d_count == 1: text += f"Выбыл 1 защитник."
        elif g_count >= 1: text += f"Выбыл вратарь."
        elif f_count >= 1: text += f"Выбыл нападающий."
        else: text += f"Потери есть."
        return text
    
    home_text = gen_analysis(missing_home, home_positions, "Хозяева")
    away_text = gen_analysis(missing_away, away_positions, "Гости")
    home_penalty = sum(p["goals_impact"] for p in missing_home)
    away_penalty = sum(p["goals_impact"] for p in missing_away)
    total_penalty = away_penalty - home_penalty
    full = f"{home_text}\n\n{away_text}\n\n🧠 ОБЩИЙ ВЕРДИКТ: "
    if total_penalty > 0.5: full += f"Оборона хозяев ослаблена (+{total_penalty}). ТБ 2.5 рекомендуется."
    elif total_penalty < -0.5: full += f"Оборона гостей ослаблена ({total_penalty}). ТБ 2.5 рекомендуется."
    elif total_penalty > 0.2: full += f"Небольшое преимущество гостей."
    elif total_penalty < -0.2: full += f"Небольшое преимущество хозяев."
    else: full += f"Составы равнозначны."
    return {"missing_home": missing_home, "missing_away": missing_away, "total_penalty": total_penalty, "home_penalty": home_penalty, "away_penalty": away_penalty, "full_analysis": full}


def get_h2h_stats(team1_id, team2_id, year):
    if not team1_id or not team2_id:
        return {"matches_count": 0, "games": []}
    games = safe_get(f"{BASE}/games/list?teamid={team1_id}&year={year}")
    if not games:
        return {"matches_count": 0, "games": []}
    h2h = {"total_goals": [], "yellow_cards": [], "corners": [], "fouls": [], "matches_count": 0, "games": []}
    t2 = str(team2_id)
    found = 0
    for g in games:
        if str(g.get("homeTeam", {}).get("id", "")) != t2 and str(g.get("awayTeam", {}).get("id", "")) != t2:
            continue
        gid = g.get("id")
        if not gid: continue
        fd = safe_get(f"{BASE}/games/{gid}")
        if not fd: continue
        stats = fd.get("statistics", {})
        game = fd.get("game", {})
        goals = (game.get("homeFTResult") or 0) + (game.get("awayFTResult") or 0)
        yc = (stats.get("yellowCardsHome") or 0) + (stats.get("yellowCardsAway") or 0)
        corners = (stats.get("cornerKicksHome") or 0) + (stats.get("cornerKicksAway") or 0)
        fouls = (stats.get("foulsHome") or 0) + (stats.get("foulsAway") or 0)
        h2h["total_goals"].append(goals); h2h["yellow_cards"].append(yc); h2h["corners"].append(corners); h2h["fouls"].append(fouls)
        h2h["matches_count"] += 1
        h2h["games"].append({"date": game.get("date"), "home_team": game.get("homeTeam", {}).get("name"), "away_team": game.get("awayTeam", {}).get("name"), "score_home": game.get("homeFTResult"), "score_away": game.get("awayFTResult"), "total_goals": goals, "yellow_cards": yc, "corners": corners, "fouls": fouls})
        found += 1
        if found >= 5: break
        time.sleep(0.2)
    result = {"matches_count": h2h["matches_count"], "games": h2h["games"]}
    for key in ["total_goals", "yellow_cards", "corners", "fouls"]:
        values = h2h[key]
        result[f"avg_{key}"] = round(stats_lib.mean(values), 1) if values else 0
        result[f"max_{key}"] = max(values) if values else 0
        result[f"min_{key}"] = min(values) if values else 0
    return result


def collect_team_stats_directly(league_id, year):
    print(f"  📊 Сбор статистики команд...")
    games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=100")
    print(f"    📥 Получено {len(games) if games else 0} матчей")
    if not games: return 0
    teams = {}
    for gs in games:
        gid = gs.get("id")
        if not gid: continue
        full = safe_get(f"{BASE}/games/{gid}")
        if not full: continue
        game = full.get("game", {})
        stats = full.get("statistics", {})
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        home_id = home.get("id"); away_id = away.get("id")
        if not home_id or not away_id: continue
        for tid, tname, is_home in [(home_id, home.get("name"), True), (away_id, away.get("name"), False)]:
            if tid not in teams:
                teams[tid] = {"name": tname, "goals_for": [], "goals_against": [], "xg_for": [], "xg_against": [], "yellow_cards": [], "corners": [], "fouls": [], "wins": 0, "draws": 0, "losses": 0, "matches": 0}
            t = teams[tid]
            hs = game.get("homeFTResult") or 0; aws = game.get("awayFTResult") or 0
            if is_home:
                t["goals_for"].append(hs); t["goals_against"].append(aws)
                t["xg_for"].append(stats.get("calculatedXgHome") or 0); t["xg_against"].append(stats.get("calculatedXgAway") or 0)
                t["yellow_cards"].append(stats.get("yellowCardsHome") or 0); t["corners"].append(stats.get("cornerKicksHome") or 0); t["fouls"].append(stats.get("foulsHome") or 0)
                if hs > aws: t["wins"] += 1
                elif hs < aws: t["losses"] += 1
                else: t["draws"] += 1
            else:
                t["goals_for"].append(aws); t["goals_against"].append(hs)
                t["xg_for"].append(stats.get("calculatedXgAway") or 0); t["xg_against"].append(stats.get("calculatedXgHome") or 0)
                t["yellow_cards"].append(stats.get("yellowCardsAway") or 0); t["corners"].append(stats.get("cornerKicksAway") or 0); t["fouls"].append(stats.get("foulsAway") or 0)
                if aws > hs: t["wins"] += 1
                elif aws < hs: t["losses"] += 1
                else: t["draws"] += 1
            t["matches"] += 1
        time.sleep(0.3)
    saved = 0
    for team_id, data in teams.items():
        if data["matches"] == 0: continue
        def avg(lst): return round(stats_lib.mean(lst), 2) if lst else 0
        try:
            row = {"team_id": int(team_id), "team_name": data["name"], "league_id": league_id, "year": year, "league_name": TOP_LEAGUES.get(league_id, "Unknown"), "matches_played": data["matches"], "wins": data["wins"], "draws": data["draws"], "losses": data["losses"], "goals_for": sum(data["goals_for"]), "goals_against": sum(data["goals_against"]), "goals_for_avg": avg(data["goals_for"]), "goals_against_avg": avg(data["goals_against"]), "xg_for": round(sum(data["xg_for"]), 2), "xg_against": round(sum(data["xg_against"]), 2), "xg_for_avg": avg(data["xg_for"]), "xg_against_avg": avg(data["xg_against"]), "points": data["wins"] * 3 + data["draws"], "avg_yellow_cards_for": avg(data["yellow_cards"]), "avg_corners_for": avg(data["corners"]), "avg_fouls_for": avg(data["fouls"]), "updated_at": datetime.now(timezone.utc).isoformat()}
            existing = supabase.table("team_stats").select("id").eq("team_id", team_id).eq("year", year).execute()
            if existing.data: supabase.table("team_stats").update(row).eq("team_id", team_id).eq("year", year).execute()
            else: supabase.table("team_stats").insert(row).execute()
            saved += 1
        except Exception as e: print(f"    ❌ Ошибка: {e}")
    return saved


def update_referee_stats(league_id, year):
    print(f"  👨‍⚖️ Сбор статистики судей...")
    games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=100")
    print(f"    📥 Получено {len(games) if games else 0} матчей")
    if not games: return
    refs = {}
    for gs in games:
        gid = gs.get("id")
        if not gid: continue
        full = safe_get(f"{BASE}/games/{gid}")
        if not full: continue
        rname = full.get("refereeName")
        if not rname: continue
        stats = full.get("statistics", {})
        if rname not in refs: refs[rname] = {"yellow_cards": [], "fouls": [], "corners": [], "matches": 0}
        yc = (stats.get("yellowCardsHome") or 0) + (stats.get("yellowCardsAway") or 0)
        fouls = (stats.get("foulsHome") or 0) + (stats.get("foulsAway") or 0)
        corners = (stats.get("cornerKicksHome") or 0) + (stats.get("cornerKicksAway") or 0)
        refs[rname]["yellow_cards"].append(yc); refs[rname]["fouls"].append(fouls); refs[rname]["corners"].append(corners); refs[rname]["matches"] += 1
        time.sleep(0.2)
    for name, data in refs.items():
        try:
            row = {"referee_name": name, "avg_yellow_cards": round(stats_lib.mean(data["yellow_cards"]), 1) if data["yellow_cards"] else 0, "avg_fouls": round(stats_lib.mean(data["fouls"]), 1) if data["fouls"] else 0, "avg_corners": round(stats_lib.mean(data["corners"]), 1) if data["corners"] else 0, "matches_officiated": data["matches"], "updated_at": datetime.now(timezone.utc).isoformat()}
            existing = supabase.table("referee_stats").select("id").eq("referee_name", name).execute()
            if existing.data: supabase.table("referee_stats").update(row).eq("referee_name", name).execute()
            else: supabase.table("referee_stats").insert(row).execute()
        except Exception as e: print(f"    ❌ Ошибка: {e}")
    print(f"  ✅ Сохранено {len(refs)} судей")


def collect_player_stats(league_id, year):
    print(f"  👤 Сбор статистики игроков...")
    games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=100")
    print(f"    📥 Получено {len(games) if games else 0} матчей")
    if not games: return
    players = {}
    for gs in games:
        gid = gs.get("id")
        if not gid: continue
        full = safe_get(f"{BASE}/games/{gid}")
        if not full: continue
        lineup = full.get("lineupPlayers") or []
        id_to_name = {p.get("playerId"): p.get("playerName") for p in lineup if p.get("playerId")}
        for ps in full.get("playerStats") or []:
            pid = ps.get("playerId"); name = id_to_name.get(pid)
            if not name: continue
            if name not in players: players[name] = {"matches": 0, "goals": 0, "errors": 0, "yc": 0}
            players[name]["matches"] += 1; players[name]["goals"] += ps.get("goalsTotal") or 0; players[name]["errors"] += ps.get("errorsLeadingToGoal") or 0; players[name]["yc"] += ps.get("cardsYellow") or 0
        time.sleep(0.2)
    saved = 0
    for name, s in players.items():
        mp = s["matches"]
        try:
            row = {"player_name": name, "matches_played": mp, "goals_scored": s["goals"], "errors_leading_to_goal": s["errors"], "yellow_cards_avg": round(s["yc"] / mp, 1)}
            existing = supabase.table("player_stats").select("id").eq("player_name", name).execute()
            if existing.data: supabase.table("player_stats").update(row).eq("player_name", name).execute()
            else: supabase.table("player_stats").insert(row).execute()
            saved += 1
        except Exception as e: print(f"    ❌ Ошибка: {e}")
    print(f"  ✅ Сохранено {saved} игроков")


def generate_verdicts(game, statistics, odds_list, referee_name, home_id, away_id, home_name, away_name, year):
    verdicts = []
    home_stats = get_team_stats_from_db(home_id, year) if home_id else {}
    away_stats = get_team_stats_from_db(away_id, year) if away_id else {}
    h2h = get_h2h_stats(home_id, away_id, year) if home_id and away_id else {}
    ref_stats = get_referee_stats_from_db(referee_name)
    game_id = game.get("id")
    injuries = get_injuries(game_id) if game_id else []
    glicko = get_glicko(game_id) if game_id else {}
    home_form = get_team_form(home_name)
    away_form = get_team_form(away_name)
    ref_matches = get_referee_last_matches(referee_name)
    ma = analyze_missing_players(home_id, away_id, game_id) if game_id else {"total_penalty": 0, "missing_home": [], "missing_away": [], "full_analysis": ""}
    
    lines = {}
    for market in odds_list:
        m_name = market.get("marketName", "")
        for odd in market.get("odds", []):
            odd_name = odd.get("name", ""); odd_value = odd.get("value", 0)
            if ("Yellow" in m_name or "ЖК" in m_name) and "yellow_cards" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 1.5 <= val <= 7.5: lines["yellow_cards"] = {"line": val, "odds": odd_value}
            elif ("Total" in m_name or "Goals" in m_name or "Голы" in m_name) and "total_goals" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 0.5 < val < 8: lines["total_goals"] = {"line": val, "odds": odd_value}
            elif ("Corner" in m_name or "Corners" in m_name or "Угловые" in m_name) and "corners" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 5 <= val <= 15: lines["corners"] = {"line": val, "odds": odd_value}
            elif ("Foul" in m_name or "Fouls" in m_name or "Фолы" in m_name) and "fouls" not in lines:
                if "Over" in odd_name or "TB" in odd_name or "Б" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 15 <= val <= 40: lines["fouls"] = {"line": val, "odds": odd_value}
    
    h2h_weight = 0.40
    if h2h.get("matches_count", 0) < 2: h2h_weight = 0.20
    team_weight = 0.30
    if home_form.get("form_pct", 50) > 70: team_weight = 0.38
    elif home_form.get("form_pct", 50) < 30: team_weight = 0.22
    glicko_bonus = 0
    if glicko:
        diff = (glicko.get("home_rating", 1500) or 1500) - (glicko.get("away_rating", 1500) or 1500)
        if diff > 200: glicko_bonus = 0.2
        elif diff < -200: glicko_bonus = -0.2
    lineup_penalty = ma.get("total_penalty", 0)
    
    base = {"injuries": injuries, "glicko": glicko, "home_form_pct": home_form.get("form_pct"), "away_form_pct": away_form.get("form_pct"), "referee_last_matches": ref_matches, "h2h_games": h2h.get("games", []), "missing_home": ma.get("missing_home", []), "missing_away": ma.get("missing_away", []), "lineup_penalty": lineup_penalty, "full_analysis": ma.get("full_analysis", "")}
    
    if "yellow_cards" in lines:
        line = lines["yellow_cards"]["line"]; preds, weights = [], []
        hy = home_stats.get("avg_yellow_cards_for", 0); ay = away_stats.get("avg_yellow_cards_for", 0)
        if hy and ay: preds.append(hy + ay); weights.append(0.35)
        if h2h.get("avg_yellow_cards", 0) > 0: preds.append(h2h["avg_yellow_cards"]); weights.append(h2h_weight)
        preds.append(ref_stats.get("avg_yellow_cards", 4.2)); weights.append(0.25)
        cur = (statistics.get("yellowCardsHome") or 0) + (statistics.get("yellowCardsAway") or 0)
        if cur > 0: preds.append(cur); weights.append(0.15)
        if preds:
            tw = sum(weights); w = [x/tw for x in weights]; pred = sum(p * ww for p, ww in zip(preds, w)); diff = pred - line
            if diff >= 1.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 0.4: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -1.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -0.4: conf, rec = "LOW", f"TAKE_TM_{line}"
            else: conf, rec = "LOW", "SKIP"
            a = base.copy(); a.update({"model_prediction": round(pred, 1), "bookmaker_line": line, "difference": round(diff, 1), "h2h_avg": h2h.get("avg_yellow_cards", 0), "h2h_matches": h2h.get("matches_count", 0), "referee_avg": ref_stats.get("avg_yellow_cards"), "referee_name": referee_name})
            verdicts.append({"market_type": "YELLOW_CARDS", "recommendation": rec, "confidence": conf, "analysis_json": a})
    
    if "total_goals" in lines:
        line = lines["total_goals"]["line"]; preds, weights = [], []
        hg = home_stats.get("goals_for_avg", 0); ag = away_stats.get("goals_for_avg", 0)
        if hg and ag: preds.append(hg + ag + glicko_bonus + lineup_penalty); weights.append(team_weight)
        if h2h.get("avg_total_goals", 0) > 0: preds.append(h2h["avg_total_goals"]); weights.append(h2h_weight)
        xg_h = statistics.get("calculatedXgHome") or 0; xg_a = statistics.get("calculatedXgAway") or 0
        if xg_h + xg_a > 0: preds.append(xg_h + xg_a); weights.append(0.20)
        if preds:
            tw = sum(weights); w = [x/tw for x in weights]; pred = sum(p * ww for p, ww in zip(preds, w)); diff = pred - line
            if diff >= 0.8: conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 0.3: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -0.8: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -0.3: conf, rec = "LOW", f"TAKE_TM_{line}"
            else: conf, rec = "LOW", "SKIP"
            a = base.copy(); a.update({"model_prediction": round(pred, 1), "bookmaker_line": line, "difference": round(diff, 1), "h2h_avg_goals": h2h.get("avg_total_goals", 0), "h2h_matches": h2h.get("matches_count", 0), "xg_total": round(xg_h + xg_a, 1)})
            verdicts.append({"market_type": "GOALS", "recommendation": rec, "confidence": conf, "analysis_json": a})
    
    if "corners" in lines:
        line = lines["corners"]["line"]; preds, weights = [], []
        hc = home_stats.get("avg_corners_for", 0); ac = away_stats.get("avg_corners_for", 0)
        if hc and ac: preds.append(hc + ac); weights.append(0.40)
        if h2h.get("avg_corners", 0) > 0: preds.append(h2h["avg_corners"]); weights.append(h2h_weight)
        cur = (statistics.get("cornerKicksHome") or 0) + (statistics.get("cornerKicksAway") or 0)
        if cur > 0: preds.append(cur); weights.append(0.15)
        if preds:
            tw = sum(weights); w = [x/tw for x in weights]; pred = sum(p * ww for p, ww in zip(preds, w)); diff = pred - line
            if diff >= 2.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 1.0: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -2.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -1.0: conf, rec = "LOW", f"TAKE_TM_{line}"
            else: conf, rec = "LOW", "SKIP"
            a = base.copy(); a.update({"model_prediction": round(pred, 1), "bookmaker_line": line, "difference": round(diff, 1), "h2h_avg_corners": h2h.get("avg_corners", 0)})
            verdicts.append({"market_type": "CORNERS", "recommendation": rec, "confidence": conf, "analysis_json": a})
    
    if "fouls" in lines:
        line = lines["fouls"]["line"]; preds, weights = [], []
        hf = home_stats.get("avg_fouls_for", 0); af = away_stats.get("avg_fouls_for", 0)
        if hf and af: preds.append(hf + af + lineup_penalty * 0.5); weights.append(0.35)
        if h2h.get("avg_fouls", 0) > 0: preds.append(h2h["avg_fouls"]); weights.append(h2h_weight)
        ref_fouls = ref_stats.get("avg_fouls", 25); preds.append(ref_fouls); weights.append(0.25)
        cur = (statistics.get("foulsHome") or 0) + (statistics.get("foulsAway") or 0)
        if cur > 0: preds.append(cur); weights.append(0.15)
        if preds:
            tw = sum(weights); w = [x/tw for x in weights]; pred = sum(p * ww for p, ww in zip(preds, w)); diff = pred - line
            if diff >= 4.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
            elif diff >= 2.0: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
            elif diff <= -4.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
            elif diff <= -2.0: conf, rec = "LOW", f"TAKE_TM_{line}"
            else: conf, rec = "LOW", "SKIP"
            a = base.copy(); a.update({"model_prediction": round(pred, 1), "bookmaker_line": line, "difference": round(diff, 1), "h2h_avg_fouls": h2h.get("avg_fouls", 0), "h2h_matches": h2h.get("matches_count", 0), "referee_avg_fouls": ref_fouls, "referee_name": referee_name})
            verdicts.append({"market_type": "FOULS", "recommendation": rec, "confidence": conf, "analysis_json": a})
    
    return verdicts

def should_update(table, league_id, year, hours=24):
    """Проверяет, нужно ли обновить статистику (старше 24 часов)"""
    try:
        data = supabase.table(table).select("updated_at").eq("league_id", league_id).eq("year", year).order("updated_at", desc=True).limit(1).execute()
        if not data.data:
            return True
        last_update = data.data[0].get("updated_at")
        if not last_update:
            return True
        last_update = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - last_update).total_seconds() > hours * 3600
    except:
        return True

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
            sc = supabase.table("team_stats").select("id", count="exact").eq("league_id", league_id).eq("year", year).execute()
            if not sc.count or should_update("team_stats", league_id, year):
                print(f"  📊 Статистика команд устарела. Обновляем...")
                collect_team_stats_directly(league_id, year)
                update_referee_stats(league_id, year)
            pc = supabase.table("player_stats").select("id", count="exact").eq("league_id", league_id).eq("year", year).execute()
            if not pc.count or should_update("player_stats", league_id, year):
                print(f"  👤 Статистика игроков устарела. Обновляем...")
                collect_player_stats(league_id, year)
            games = safe_get(f"{BASE}/games/list?leagueid={league_id}&year={year}&limit=100")
            print(f"    📥 Получено {len(games) if games else 0} матчей")
            if not games:
                print(f"  ❌ Нет матчей")
                continue
            print(f"  ⚽ Обработка {len(games)} матчей...")
            for idx, gs in enumerate(games):
                gid = gs.get("id")
                if not gid: continue
                if idx % 20 == 0 and idx > 0: print(f"    Прогресс: {idx}/{len(games)}")
                full = safe_get(f"{BASE}/games/{gid}")
                if not full: continue
                game = full.get("game", {})
                stats = full.get("statistics") or {}
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
                row = {"external_id": str(gid), "league_name": league_name, "home_team": home_name, "away_team": away_name, "match_time": mt.isoformat() if mt else None, "status": status, "score_home": game.get("homeFTResult"), "score_away": game.get("awayFTResult"), "ht_score_home": game.get("homeHTResult"), "ht_score_away": game.get("awayHTResult"), "stats_yellow_cards_home": stats.get("yellowCardsHome"), "stats_yellow_cards_away": stats.get("yellowCardsAway"), "stats_corners_home": stats.get("cornerKicksHome"), "stats_corners_away": stats.get("cornerKicksAway"), "stats_fouls_home": stats.get("foulsHome"), "stats_fouls_away": stats.get("foulsAway"), "stats_xg_home": stats.get("calculatedXgHome"), "stats_xg_away": stats.get("calculatedXgAway"), "referee_name": ref_name, "updated_at": datetime.now(timezone.utc).isoformat()}
                try:
                    existing = supabase.table("matches").select("id").eq("external_id", str(gid)).execute()
                    if existing.data: supabase.table("matches").update(row).eq("external_id", str(gid)).execute()
                    else: supabase.table("matches").insert(row).execute()
                    total_matches += 1
                    if status == "finished":
                        supabase.table("match_verdicts").delete().eq("match_external_id", str(gid)).execute()
                    if status in ["scheduled", "live"] and match_time <= datetime.now(timezone.utc) + timedelta(days=3):
                        # Для scheduled всегда запрашиваем свежие данные из API
                        fresh = safe_get(f"{BASE}/games/{gid}")
                        if fresh:
                            game = fresh.get("game", {})
                            stats = fresh.get("statistics") or {}
                            ref_name = fresh.get("refereeName")
                            odds = game.get("odds", [])
                        verdicts = generate_verdicts(game, stats, odds, ref_name, home_id, away_id, home_name, away_name, year)
                        supabase.table("match_verdicts").delete().eq("match_external_id", str(gid)).execute()
                        for v in verdicts:
                            supabase.table("match_verdicts").insert({"match_external_id": str(gid), "market_type": v["market_type"], "recommendation": v["recommendation"], "confidence": v["confidence"], "analysis_json": v["analysis_json"]}).execute()
                            total_verdicts += 1
                except Exception as e: print(f"    ❌ Ошибка: {e}")
                time.sleep(0.3)
    print(f"\n{'='*55}")
    print(f"🎉 ГОТОВО! Матчей: {total_matches}, Вердиктов: {total_verdicts}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
