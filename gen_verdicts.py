import os
import re
import requests
from supabase import create_client
from datetime import datetime, timezone, timedelta
import time
import statistics as stats_lib

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"

def safe_get(url, retries=2):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json().get("data")
            elif r.status_code == 429:
                time.sleep(10)
            else:
                time.sleep(5)
        except:
            time.sleep(5)
    return None

def parse_line_value(name):
    match = re.search(r'([\d.]+)', name)
    return float(match.group(1)) if match else None

def get_team_stats(team_id, year):
    try:
        data = supabase.table('team_stats').select('*').eq('team_id', team_id).eq('year', year).execute()
        return data.data[0] if data.data else {}
    except:
        return {}

def get_referee_stats(ref_name):
    try:
        data = supabase.table("referee_stats").select("*").eq("referee_name", ref_name).execute()
        return data.data[0] if data.data else {"avg_yellow_cards": 4.2, "avg_fouls": 25}
    except:
        return {"avg_yellow_cards": 4.2, "avg_fouls": 25}

def get_team_form(team_name, limit=5):
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
            return 50
        return round(sum(m["points"] for m in last) / (len(last) * 3) * 100, 1)
    except:
        return 50

def generate_verdicts(game_id, home_id, away_id, home_name, away_name, year):
    full = safe_get(f"{BASE}/games/{game_id}")
    if not full:
        return []
    
    game = full.get("game", {})
    ref_name = full.get("refereeName")
    odds = game.get("odds", [])
    
    home_stats = get_team_stats(home_id, year)
    away_stats = get_team_stats(away_id, year)
    ref_stats = get_referee_stats(ref_name)
    
    lines = {}
    for market in odds:
        m_name = market.get("marketName", "")
        for odd in market.get("odds", []):
            odd_name = odd.get("name", "")
            odd_value = odd.get("value", 0)
            
            if ("Yellow" in m_name or "ЖК" in m_name) and "yellow_cards" not in lines:
                if "Over" in odd_name or "TB" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 1.5 <= val <= 7.5:
                        lines["yellow_cards"] = {"line": val, "odds": odd_value}
            
            elif ("Total" in m_name or "Goals" in m_name or "Голы" in m_name) and "total_goals" not in lines:
                if "Over" in odd_name or "TB" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 0.5 < val < 8:
                        lines["total_goals"] = {"line": val, "odds": odd_value}
            
            elif ("Corner" in m_name or "Corners" in m_name or "Угловые" in m_name) and "corners" not in lines:
                if "Over" in odd_name or "TB" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 5 <= val <= 15:
                        lines["corners"] = {"line": val, "odds": odd_value}
            
            elif ("Foul" in m_name or "Fouls" in m_name or "Фолы" in m_name) and "fouls" not in lines:
                if "Over" in odd_name or "TB" in odd_name:
                    val = parse_line_value(odd_name)
                    if val and 15 <= val <= 40:
                        lines["fouls"] = {"line": val, "odds": odd_value}
    
    verdicts = []
    base = {"referee_name": ref_name}
    
    if "yellow_cards" in lines:
        line = lines["yellow_cards"]["line"]
        hy = home_stats.get("avg_yellow_cards_for", 0)
        ay = away_stats.get("avg_yellow_cards_for", 0)
        pred = (hy + ay) * 0.35 + ref_stats.get("avg_yellow_cards", 4.2) * 0.25
        if pred == 0:
            pred = ref_stats.get("avg_yellow_cards", 4.2)
        diff = pred - line
        
        if diff >= 1.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
        elif diff >= 0.4: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
        elif diff <= -1.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
        elif diff <= -0.4: conf, rec = "LOW", f"TAKE_TM_{line}"
        else: conf, rec = "LOW", "SKIP"
        
        verdicts.append({"market_type": "YELLOW_CARDS", "recommendation": rec, "confidence": conf, "analysis_json": {**base, "model_prediction": round(pred,1), "bookmaker_line": line, "difference": round(diff,1)}})
    
    if "total_goals" in lines:
        line = lines["total_goals"]["line"]
        hg = home_stats.get("goals_for_avg", 0)
        ag = away_stats.get("goals_for_avg", 0)
        pred = hg + ag
        if pred == 0:
            pred = 2.5
        diff = pred - line
        
        if diff >= 0.8: conf, rec = "HIGH", f"TAKE_TB_{line}"
        elif diff >= 0.3: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
        elif diff <= -0.8: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
        elif diff <= -0.3: conf, rec = "LOW", f"TAKE_TM_{line}"
        else: conf, rec = "LOW", "SKIP"
        
        verdicts.append({"market_type": "GOALS", "recommendation": rec, "confidence": conf, "analysis_json": {**base, "model_prediction": round(pred,1), "bookmaker_line": line, "difference": round(diff,1)}})
    
    if "corners" in lines:
        line = lines["corners"]["line"]
        hc = home_stats.get("avg_corners_for", 0)
        ac = away_stats.get("avg_corners_for", 0)
        pred = hc + ac
        if pred == 0:
            pred = 10
        diff = pred - line
        
        if diff >= 2.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
        elif diff >= 1.0: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
        elif diff <= -2.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
        elif diff <= -1.0: conf, rec = "LOW", f"TAKE_TM_{line}"
        else: conf, rec = "LOW", "SKIP"
        
        verdicts.append({"market_type": "CORNERS", "recommendation": rec, "confidence": conf, "analysis_json": {**base, "model_prediction": round(pred,1), "bookmaker_line": line, "difference": round(diff,1)}})
    
    if "fouls" in lines:
        line = lines["fouls"]["line"]
        hf = home_stats.get("avg_fouls_for", 0)
        af = away_stats.get("avg_fouls_for", 0)
        pred = hf + af + ref_stats.get("avg_fouls", 25) * 0.25
        if pred == 0:
            pred = 25
        diff = pred - line
        
        if diff >= 4.0: conf, rec = "HIGH", f"TAKE_TB_{line}"
        elif diff >= 2.0: conf, rec = "MEDIUM", f"TAKE_TB_{line}"
        elif diff <= -4.0: conf, rec = "MEDIUM", f"TAKE_TM_{line}"
        elif diff <= -2.0: conf, rec = "LOW", f"TAKE_TM_{line}"
        else: conf, rec = "LOW", "SKIP"
        
        verdicts.append({"market_type": "FOULS", "recommendation": rec, "confidence": conf, "analysis_json": {**base, "model_prediction": round(pred,1), "bookmaker_line": line, "difference": round(diff,1)}})
    
    return verdicts

def main():
    print("🎯 Генерация вердиктов...")
    cutoff = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    
    matches = supabase.table("matches").select("*").eq("status", "scheduled").lte("match_time", cutoff).execute()
    
    if not matches.data:
        print("✅ Нет scheduled матчей")
        return
    
    print(f"📋 Матчей: {len(matches.data)}")
    total = 0
    
    for m in matches.data:
        gid = m["external_id"]
        full = safe_get(f"{BASE}/games/{gid}")
        if not full:
            continue
        
        game = full.get("game", {})
        home = game.get("homeTeam", {})
        away = game.get("awayTeam", {})
        home_id = home.get("id")
        away_id = away.get("id")
        home_name = m["home_team"]
        away_name = m["away_team"]
        year = game.get("season", {}).get("year", 2025)
        
        verdicts = generate_verdicts(gid, home_id, away_id, home_name, away_name, year)
        
        supabase.table("match_verdicts").delete().eq("match_external_id", gid).execute()
        for v in verdicts:
            supabase.table("match_verdicts").insert({"match_external_id": gid, "market_type": v["market_type"], "recommendation": v["recommendation"], "confidence": v["confidence"], "analysis_json": v["analysis_json"]}).execute()
            total += 1
        
        print(f"  ✅ {home_name} vs {away_name}: {len(verdicts)} вердиктов")
        time.sleep(0.3)
    
    print(f"\n🎉 Готово! {total} вердиктов")

if __name__ == "__main__":
    main()
