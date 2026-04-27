import os
import re
import requests
from supabase import create_client
from datetime import datetime, timezone
import time

# Настройки
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
API_KEY = os.environ.get("SSTATS_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
headers = {"apikey": API_KEY} if API_KEY else {}
BASE = "https://api.sstats.net"


def safe_get(url):
    """Безопасный GET запрос"""
    try:
        r = requests.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            return r.json().get("data")
        elif r.status_code == 429:
            time.sleep(65)
            return safe_get(url)
    except:
        pass
    return None


def parse_line_value(name):
    """Over 4.5 -> 4.5"""
    match = re.search(r'([\d.]+)', name)
    return float(match.group(1)) if match else None


def collect_odds():
    """Собирает коэффициенты для будущих матчей"""
    print(f"💰 Сбор коэффициентов... {datetime.now(timezone.utc).strftime('%H:%M')}")
    
    future = supabase.table("matches").select("external_id").eq("status", "scheduled").execute()
    
    if not future.data:
        print("  ✅ Нет будущих матчей")
        return
    
    print(f"  📋 Будущих матчей: {len(future.data)}")
    saved = 0
    
    for match in future.data:
        gid = match["external_id"]
        
        full = safe_get(f"{BASE}/games/{gid}")
        if not full:
            continue
        
        game = full.get("game", {})
        odds_list = game.get("odds", [])
        
        for market in odds_list:
            m_name = market.get("marketName", "")
            
            market_type = None
            if "Yellow" in m_name or "ЖК" in m_name:
                market_type = "YELLOW_CARDS"
            elif "Total" in m_name or "Goals" in m_name or "Голы" in m_name:
                market_type = "GOALS"
            elif "Corner" in m_name or "Corners" in m_name or "Угловые" in m_name:
                market_type = "CORNERS"
            elif "Foul" in m_name or "Fouls" in m_name or "Фолы" in m_name:
                market_type = "FOULS"
            
            if not market_type:
                continue
            
            for odd in market.get("odds", []):
                odd_name = odd.get("name", "")
                odd_value = odd.get("value", 0)
                
                if not odd_value:
                    continue
                
                line_value = parse_line_value(odd_name)
                if not line_value:
                    continue
                
                existing = supabase.table("odds_movements_compact").select("id,odd_current,odd_start,odd_min,odd_max,sharp_move_count").eq("match_external_id", gid).eq("market_type", market_type).eq("selection", odd_name).execute()
                
                if existing.data:
                    prev = existing.data[0]
                    prev_odd = float(prev["odd_current"])
                    change_pct = ((prev_odd - odd_value) / prev_odd) * 100
                    
                    if abs(change_pct) >= 5.0:
                        row = {
                            "odd_current": odd_value,
                            "odd_min": min(float(prev["odd_min"]), odd_value),
                            "odd_max": max(float(prev["odd_max"]), odd_value),
                            "total_change_pct": round(((float(prev["odd_start"]) - odd_value) / float(prev["odd_start"])) * 100, 1),
                            "sharp_move_count": int(prev["sharp_move_count"]) + 1,
                            "progruz_detected": ((float(prev["odd_start"]) - odd_value) / float(prev["odd_start"])) * 100 <= -10,
                            "last_updated": datetime.now(timezone.utc).isoformat()
                        }
                        supabase.table("odds_movements_compact").update(row).eq("id", prev["id"]).execute()
                        saved += 1
                        direction = "🔻" if change_pct > 0 else "🔺"
                        print(f"  {direction} {market_type} {odd_name}: {prev_odd}→{odd_value} ({change_pct:+.1f}%)")
                else:
                    row = {
                        "match_external_id": gid,
                        "market_type": market_type,
                        "selection": odd_name,
                        "line_value": line_value,
                        "odd_start": odd_value,
                        "odd_current": odd_value,
                        "odd_min": odd_value,
                        "odd_max": odd_value,
                        "total_change_pct": 0,
                        "sharp_move_count": 0,
                        "progruz_detected": False,
                        "last_updated": datetime.now(timezone.utc).isoformat()
                    }
                    supabase.table("odds_movements_compact").insert(row).execute()
                    saved += 1
        
        time.sleep(1)
    
    # Удаляем коэффициенты завершённых матчей
    finished = supabase.table("matches").select("external_id").eq("status", "finished").execute()
    if finished.data:
        for m in finished.data:
            supabase.table("odds_movements_compact").delete().eq("match_external_id", m["external_id"]).execute()
    
    print(f"  💾 Сохранено/обновлено: {saved} записей")


if __name__ == "__main__":
    collect_odds()
