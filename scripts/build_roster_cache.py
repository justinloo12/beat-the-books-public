#!/usr/bin/env python3
"""
Build roster cache files for each MLB team playing on 2026-04-21.
Matches player names from the old snapshot to MLB player IDs from statcast data.
"""

import json
import os
import glob
import unicodedata
import pandas as pd

# Team name -> team ID mapping
TEAM_IDS = {
    "Seattle Mariners": 136,
    "Athletics": 133,
    "Washington Nationals": 120,
    "Atlanta Braves": 144,
    "Kansas City Royals": 118,
    "Baltimore Orioles": 110,
    "Arizona Diamondbacks": 109,
    "Chicago White Sox": 145,
    "Tampa Bay Rays": 139,
    "Cincinnati Reds": 113,
    "Cleveland Guardians": 114,
    "Houston Astros": 117,
    "San Francisco Giants": 137,
    "Los Angeles Dodgers": 119,
    "Detroit Tigers": 116,
    "Milwaukee Brewers": 158,
    "New York Mets": 121,
    "Minnesota Twins": 142,
    "Boston Red Sox": 111,
    "New York Yankees": 147,
    "Chicago Cubs": 112,
    "Philadelphia Phillies": 143,
    "Colorado Rockies": 115,
    "San Diego Padres": 135,
    "Miami Marlins": 146,
    "St. Louis Cardinals": 138,
    "Los Angeles Angels": 108,
    "Toronto Blue Jays": 141,
}


def normalize_name(name: str) -> str:
    """Normalize a player name: strip accents, lowercase, strip whitespace."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_str.lower().strip()


def statcast_to_first_last(name: str) -> str:
    """Convert 'Last, First' statcast format to 'First Last'."""
    if "," in name:
        parts = name.split(",", 1)
        last = parts[0].strip()
        first = parts[1].strip()
        return f"{first} {last}"
    return name.strip()


def load_player_id_map(statcast_files: list[str]) -> dict[str, int]:
    """
    Load all statcast files and build a map from normalized 'First Last' name -> batter ID.
    When there are multiple IDs for the same name, prefer the most recent one.
    """
    frames = []
    for fpath in statcast_files:
        try:
            df = pd.read_csv(fpath, usecols=["batter", "player_name", "game_date"], low_memory=False)
            df = df[["batter", "player_name", "game_date"]].dropna(subset=["batter", "player_name"])
            frames.append(df)
        except Exception as e:
            print(f"  WARNING: Could not read {fpath}: {e}")

    if not frames:
        return {}

    combined = pd.concat(frames, ignore_index=True)
    combined["game_date"] = pd.to_datetime(combined["game_date"], errors="coerce")
    # Sort by date descending so most-recent entry wins in drop_duplicates
    combined = combined.sort_values("game_date", ascending=False)

    # Convert statcast 'Last, First' to 'First Last'
    combined["first_last"] = combined["player_name"].apply(statcast_to_first_last)
    combined["norm_name"] = combined["first_last"].apply(normalize_name)

    # Drop duplicate normalized names, keeping most recent
    deduped = combined.drop_duplicates(subset=["norm_name"], keep="first")
    id_map = dict(zip(deduped["norm_name"], deduped["batter"].astype(int)))

    return id_map


def is_position_slot(slot: str) -> bool:
    """Return True if slot is a fielding position (not a batting order number)."""
    batting_order = {"1", "2", "3", "4", "5", "6", "7", "8", "9"}
    return slot not in batting_order


def main():
    snapshot_path = "/home/user/beat-the-books/docs/data/latest.json"
    savant_dir = "/home/user/beat-the-books/data/baseball_savant"
    output_dir = "/home/user/beat-the-books/data/roster_cache"

    os.makedirs(output_dir, exist_ok=True)

    # Load snapshot
    print("Loading snapshot...")
    with open(snapshot_path) as f:
        snapshot = json.load(f)
    lineup_cards = snapshot["daily"]["lineup_cards"]

    # Load statcast player ID map from 2025-2026 files
    print("Loading statcast data (2025-2026)...")
    statcast_files = sorted(
        glob.glob(f"{savant_dir}/statcast_2025*.csv") +
        glob.glob(f"{savant_dir}/statcast_2026*.csv")
    )
    print(f"  Found {len(statcast_files)} files")
    id_map = load_player_id_map(statcast_files)
    print(f"  Loaded {len(id_map)} unique player name -> ID mappings")

    # Build team -> players list from lineup cards
    team_players: dict[str, list[dict]] = {}
    for card in lineup_cards:
        for side in ["home_lineup", "away_lineup"]:
            lineup = card[side]
            team_name = lineup["team"]
            if team_name not in team_players:
                team_players[team_name] = []
            for player in lineup["players"]:
                entry = {
                    "name": player["name"],
                    "slot": player.get("slot", "?"),
                }
                # Avoid exact duplicates (same name+slot)
                if entry not in team_players[team_name]:
                    team_players[team_name].append(entry)

    # Match and write cache files
    print("\nBuilding roster cache files...")
    results = []
    for team_name, players in sorted(team_players.items()):
        team_id = TEAM_IDS.get(team_name)
        if team_id is None:
            print(f"  WARNING: No team ID for '{team_name}', skipping")
            continue

        roster = []
        matched = 0
        unmatched_names = []

        for player in players:
            name = player["name"]
            slot = player["slot"]
            position = slot if is_position_slot(slot) else "?"

            norm = normalize_name(name)
            player_id = id_map.get(norm)

            if player_id is None:
                unmatched_names.append(name)
            else:
                matched += 1

            roster.append({
                "id": player_id,
                "fullName": name,
                "position": position,
            })

        total = len(players)
        match_pct = (matched / total * 100) if total > 0 else 0

        out_path = os.path.join(output_dir, f"roster_{team_id}.json")
        with open(out_path, "w") as f:
            json.dump(roster, f, indent=2)

        results.append({
            "team": team_name,
            "team_id": team_id,
            "total": total,
            "matched": matched,
            "match_pct": match_pct,
            "unmatched": unmatched_names,
        })

        status = "OK" if match_pct >= 50 else "POOR"
        print(f"  [{status}] {team_name} (ID={team_id}): {matched}/{total} matched ({match_pct:.0f}%) -> {out_path}")
        if unmatched_names:
            print(f"         Unmatched: {unmatched_names}")

    # Summary
    print("\n=== SUMMARY ===")
    poor = [r for r in results if r["match_pct"] < 50]
    if poor:
        print(f"Teams with poor match rate (<50%): {len(poor)}")
        for r in poor:
            print(f"  - {r['team']}: {r['matched']}/{r['total']} ({r['match_pct']:.0f}%)")
            print(f"    Unmatched: {r['unmatched']}")
    else:
        print("All teams have >= 50% match rate.")

    print(f"\nTotal teams processed: {len(results)}")
    print(f"Roster cache files written to: {output_dir}")


if __name__ == "__main__":
    main()
