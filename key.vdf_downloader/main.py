import os
import json
import aiohttp
import asyncio
import aiofiles
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn

GAMES_JSON = "28.05.2025-steam_games.json"
REPOS_JSON = "repositories.json"
OUTPUT_DIR = "Games"
SEMAPHORE_LIMIT = 100
CHUNK_SIZE = 10000

semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

async def download_file(session, url, save_path):
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    async with aiofiles.open(save_path, "wb") as f:
                        await f.write(await resp.read())
                    return True
        except Exception:
            return False
    return False

async def download_game(session, repo, appid, game_name, task_id, progress):
    folder = f"[{appid}] {game_name}"
    path = os.path.join(OUTPUT_DIR, folder, repo, "key.vdf")
    url = f"https://raw.githubusercontent.com/{repo}/{appid}/key.vdf"
    await download_file(session, url, path)
    progress.advance(task_id)

async def run_in_chunks(tasks, chunk_size):
    for i in range(0, len(tasks), chunk_size):
        await asyncio.gather(*tasks[i:i+chunk_size])

async def main():
    with open(GAMES_JSON, "r", encoding="utf-8") as f:
        games = json.load(f)

    if not os.path.exists(REPOS_JSON):
        print("repositories.json bulunamadı!")
        return

    with open(REPOS_JSON, "r", encoding="utf-8") as f:
        repos = json.load(f)

    all_tasks_data = [
        (repo, str(game.get("appid")), game.get("name", f"Game_{game.get('appid')}"))
        for game in games for repo in repos
    ]

    async with aiohttp.ClientSession() as session:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as progress:

            task_id = progress.add_task("[cyan]İndiriliyor...", total=len(all_tasks_data))

            tasks = [
                download_game(session, repo, appid, name, task_id, progress)
                for repo, appid, name in all_tasks_data
            ]
            await run_in_chunks(tasks, CHUNK_SIZE)

if __name__ == "__main__":
    asyncio.run(main())
