import asyncio
import os
import time
import random
import asyncpg
from decimal import Decimal
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
import pytz

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_ID = 7123672535

CHANNEL_ID = -1003019603636
CHANNEL_URL = "https://t.me/testnasponsora"

# Московское время
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN environment variable is not set")

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

BOT_USERNAME = None
db_pool = None

user_states = {}
used_buttons = {}
user_sessions = {}
pending_referrals = {}

async def init_db_pool():
    global db_pool
    max_retries = 10
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            print(f"[DB] Attempting connection {attempt + 1}/{max_retries}...")
            db_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=5,
                max_size=10,
                command_timeout=60,
                ssl=True
            )
            print("[DB] Connection pool created successfully")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[DB] Connection attempt {attempt + 1} failed: {e}")
                print(f"[DB] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print(f"[DB] Failed to connect after {max_retries} attempts: {e}")
                raise

    # Создаём все необходимые таблицы
    async with db_pool.acquire() as conn:
        try:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    username TEXT,
                    balance DECIMAL(10, 2) DEFAULT 0,
                    refs INTEGER DEFAULT 0,
                    last_bonus BIGINT DEFAULT 0,
                    used_promos TEXT[] DEFAULT ARRAY[]::TEXT[]
                )
            ''')

            # Таблица состояний пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_states (
                    user_id BIGINT PRIMARY KEY,
                    state_data TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Таблица использованных кнопок
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS used_buttons (
                    user_id BIGINT,
                    button_id TEXT,
                    used_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, button_id)
                )
            ''')

            # Таблица ожидающих рефералов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS pending_referrals (
                    user_id BIGINT PRIMARY KEY,
                    referrer_id BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Таблица сессий пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id BIGINT PRIMARY KEY,
                    session_count INTEGER DEFAULT 0,
                    last_activity TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Таблица промокодов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS promos (
                    code TEXT PRIMARY KEY,
                    reward DECIMAL(10, 2) NOT NULL,
                    uses INTEGER DEFAULT 0
                )
            ''')

            # Таблица турниров
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tournaments (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    start_time BIGINT NOT NULL,
                    end_time BIGINT NOT NULL,
                    duration_days INTEGER NOT NULL,
                    prize_places INTEGER NOT NULL,
                    prizes JSONB NOT NULL,
                    trophy_file_ids JSONB NOT NULL,
                    status TEXT DEFAULT 'active',
                    start_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Таблица участников турнира
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS tournament_participants (
                    tournament_id INTEGER REFERENCES tournaments(id) ON DELETE CASCADE,
                    user_id BIGINT NOT NULL,
                    refs_count INTEGER DEFAULT 0,
                    PRIMARY KEY (tournament_id, user_id)
                )
            ''')

            # Таблица наград пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS user_trophies (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    tournament_id INTEGER REFERENCES tournaments(id),
                    tournament_name TEXT NOT NULL,
                    place INTEGER NOT NULL,
                    trophy_file_id TEXT NOT NULL,
                    prize_stars DECIMAL(10, 2) NOT NULL,
                    date_received BIGINT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            # Таблица состояния создания турнира (для админа)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admin_tournament_creation (
                    admin_id BIGINT PRIMARY KEY,
                    step TEXT NOT NULL,
                    data TEXT DEFAULT '{}',
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            ''')

            print("[DB] All tables initialized successfully")
            
            # Миграция: добавляем колонку start_message если её нет
            try:
                await conn.execute('''
                    ALTER TABLE tournaments 
                    ADD COLUMN IF NOT EXISTS start_message TEXT
                ''')
                print("[DB] Migration: start_message column ensured")
            except Exception as migration_error:
                print(f"[DB] Migration note: {migration_error}")
                
        except Exception as e:
            # If tables already exist, this is fine - just log and continue
            print(f"[DB] Table initialization note: {e}")
            print("[DB] Continuing with existing tables")

async def close_db_pool():
    global db_pool
    if db_pool:
        await db_pool.close()
        print("[DB] Connection pool closed")

async def get_user_state(user_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT state_data FROM user_states WHERE user_id = $1',
            user_id
        )
        return row['state_data'] if row else None

async def set_user_state(user_id: int, state_data):
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO user_states (user_id, state_data, updated_at) 
               VALUES ($1, $2, NOW())
               ON CONFLICT (user_id) 
               DO UPDATE SET state_data = $2, updated_at = NOW()''',
            user_id, state_data
        )

async def delete_user_state(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            'DELETE FROM user_states WHERE user_id = $1',
            user_id
        )

async def is_button_used(user_id: int, button_id: str) -> bool:
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(
            'SELECT EXISTS(SELECT 1 FROM used_buttons WHERE user_id = $1 AND button_id = $2)',
            user_id, button_id
        )
        return result

async def mark_button_used(user_id: int, button_id: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO used_buttons (user_id, button_id, used_at) 
               VALUES ($1, $2, NOW())
               ON CONFLICT (user_id, button_id) DO NOTHING''',
            user_id, button_id
        )

async def get_pending_referral(user_id: int):
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(
            'SELECT referrer_id FROM pending_referrals WHERE user_id = $1',
            user_id
        )
        return result

async def set_pending_referral(user_id: int, referrer_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO pending_referrals (user_id, referrer_id, created_at) 
               VALUES ($1, $2, NOW())
               ON CONFLICT (user_id) 
               DO UPDATE SET referrer_id = $2, created_at = NOW()''',
            user_id, referrer_id
        )

async def delete_pending_referral(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            'DELETE FROM pending_referrals WHERE user_id = $1',
            user_id
        )

async def get_user_session(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(
            'SELECT session_count FROM user_sessions WHERE user_id = $1',
            user_id
        )
        return result if result is not None else 0

async def increment_user_session(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        result = await conn.fetchval(
            '''INSERT INTO user_sessions (user_id, session_count, last_activity) 
               VALUES ($1, 1, NOW())
               ON CONFLICT (user_id) 
               DO UPDATE SET session_count = user_sessions.session_count + 1, last_activity = NOW()
               RETURNING session_count''',
            user_id
        )
        return result

async def cleanup_old_records():
    async with db_pool.acquire() as conn:
        deleted_buttons = await conn.execute(
            "DELETE FROM used_buttons WHERE used_at < NOW() - INTERVAL '24 hours'"
        )
        deleted_states = await conn.execute(
            "DELETE FROM user_states WHERE updated_at < NOW() - INTERVAL '24 hours'"
        )
        deleted_refs = await conn.execute(
            "DELETE FROM pending_referrals WHERE created_at < NOW() - INTERVAL '24 hours'"
        )
        print(f"[CLEANUP] Deleted old records: buttons={deleted_buttons}, states={deleted_states}, referrals={deleted_refs}")

async def get_user(user_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT user_id, name, username, balance, refs, last_bonus, used_promos FROM users WHERE user_id = $1',
            user_id
        )
        if row:
            return {
                'user_id': row['user_id'],
                'name': row['name'],
                'username': row['username'],
                'balance': float(row['balance']),
                'refs': row['refs'],
                'last_bonus': row['last_bonus'],
                'used_promos': row['used_promos'] or []
            }
        return None

async def create_user(user_id: int, name: str, username: str = ''):
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO users (user_id, name, username, balance, refs, last_bonus, used_promos) 
               VALUES ($1, $2, $3, 0, 0, 0, ARRAY[]::TEXT[])
               ON CONFLICT (user_id) DO NOTHING''',
            user_id, name, username
        )
        print(f"[USER] Created new user {user_id}: {name}")

async def update_user_balance(user_id: int, delta: float):
    async with db_pool.acquire() as conn:
        await conn.execute(
            'UPDATE users SET balance = balance + $1 WHERE user_id = $2',
            Decimal(str(delta)), user_id
        )

async def get_user_balance(user_id: int) -> float:
    async with db_pool.acquire() as conn:
        balance = await conn.fetchval(
            'SELECT balance FROM users WHERE user_id = $1',
            user_id
        )
        return float(balance) if balance is not None else 0

async def update_daily_bonus(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                'SELECT last_bonus FROM users WHERE user_id = $1 FOR UPDATE',
                user_id
            )
            if not row:
                return False

            now = time.time()
            if now - row['last_bonus'] >= 86400:
                await conn.execute(
                    'UPDATE users SET balance = balance + 0.2, last_bonus = $1 WHERE user_id = $2',
                    now, user_id
                )
                return True
            return False

async def process_referral_db(user_id: int, ref_id: int, user_name: str):
    try:
        print(f"[REFERRAL] Processing referral: user {user_id} referred by {ref_id}")

        async with db_pool.acquire() as conn:
            async with conn.transaction():
                referrer = await conn.fetchrow(
                    'SELECT user_id, balance, refs FROM users WHERE user_id = $1 FOR UPDATE',
                    ref_id
                )

                if not referrer:
                    print(f"[REFERRAL] ERROR: Referrer {ref_id} not found in users")
                    return

                await conn.execute(
                    'UPDATE users SET balance = balance + 2, refs = refs + 1 WHERE user_id = $1',
                    ref_id
                )
                print(f"[REFERRAL] Added 2 stars to referrer {ref_id}")

        # Проверяем активный турнир и увеличиваем счетчик
        active_tournament = await get_active_tournament()
        if active_tournament:
            await increment_tournament_refs(active_tournament['id'], ref_id)
            print(f"[TOURNAMENT] Added 1 ref to user {ref_id} in tournament {active_tournament['id']}")

        try:
            await bot.send_message(
                ref_id,
                f"👥 {user_name or 'Новый пользователь'} зарегистрировался по вашей ссылке!\n🎉 Ты заработал 2 ⭐️"
            )
            print(f"[REFERRAL] Notification sent to referrer {ref_id}")
        except Exception as e:
            print(f"[REFERRAL] ERROR: Failed to send notification to {ref_id}: {e}")

    except Exception as e:
        print(f"[REFERRAL] ERROR: Failed to process referral: {e}")

async def get_promo(code: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT code, reward, uses FROM promos WHERE code = $1',
            code
        )
        if row:
            return {
                'code': row['code'],
                'reward': float(row['reward']),
                'uses': row['uses']
            }
        return None

async def use_promo(user_id: int, code: str):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            user = await conn.fetchrow(
                'SELECT used_promos FROM users WHERE user_id = $1 FOR UPDATE',
                user_id
            )
            if not user:
                return {'success': False, 'message': '❌ Пользователь не найден'}

            if code in (user['used_promos'] or []):
                return {'success': False, 'message': '❌ Вы уже активировали этот промокод'}

            promo = await conn.fetchrow(
                'SELECT reward, uses FROM promos WHERE code = $1 FOR UPDATE',
                code
            )

            if not promo:
                return {'success': False, 'message': '❌ Неверный промокод'}

            if promo['uses'] <= 0:
                return {'success': False, 'message': '❌ Промокод исчерпан'}

            reward = float(promo['reward'])

            await conn.execute(
                '''UPDATE users 
                   SET balance = balance + $1, 
                       used_promos = array_append(used_promos, $2)
                   WHERE user_id = $3''',
                Decimal(str(reward)), code, user_id
            )

            await conn.execute(
                'UPDATE promos SET uses = uses - 1 WHERE code = $1',
                code
            )

            return {
                'success': True,
                'message': f'✅ Промокод {code} активирован — +{reward} ⭐️'
            }

async def get_top_users(limit: int = 3):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT user_id, name, balance FROM users ORDER BY balance DESC LIMIT $1',
            limit
        )
        return [{'name': row['name'], 'balance': float(row['balance'])} for row in rows]

async def withdraw_balance(user_id: int, amount: float):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            balance = await conn.fetchval(
                'SELECT balance FROM users WHERE user_id = $1 FOR UPDATE',
                user_id
            )
            if not balance or float(balance) < amount:
                return False

            await conn.execute(
                'UPDATE users SET balance = balance - $1 WHERE user_id = $2',
                Decimal(str(amount)), user_id
            )
            return True

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

# ===== TOURNAMENT FUNCTIONS =====

async def create_tournament(name: str, start_time: int, duration_days: int, 
                           prize_places: int, prizes: dict, trophy_file_ids: dict, start_message: str = None):
    """Создает новый турнир"""
    async with db_pool.acquire() as conn:
        end_time = start_time + (duration_days * 86400)

        # Конвертируем словари в JSONB совместимый формат
        import json
        prizes_json = json.dumps(prizes)
        trophy_file_ids_json = json.dumps(trophy_file_ids)

        tournament_id = await conn.fetchval(
            '''INSERT INTO tournaments 
               (name, start_time, end_time, duration_days, prize_places, prizes, trophy_file_ids, status, start_message)
               VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, 'active', $8)
               RETURNING id''',
            name, start_time, end_time, duration_days, prize_places, 
            prizes_json, trophy_file_ids_json, start_message
        )
        return tournament_id

async def get_active_tournament():
    """Получает активный турнир"""
    import json
    async with db_pool.acquire() as conn:
        now = int(time.time())
        row = await conn.fetchrow(
            '''SELECT id, name, start_time, end_time, duration_days, prize_places, prizes, trophy_file_ids, status
               FROM tournaments 
               WHERE status = 'active' AND start_time <= $1 AND end_time > $1
               ORDER BY id DESC LIMIT 1''',
            now
        )
        if row:
            # Парсим JSON поля если они строки
            prizes = row['prizes']
            if isinstance(prizes, str):
                prizes = json.loads(prizes)

            trophy_file_ids = row['trophy_file_ids']
            if isinstance(trophy_file_ids, str):
                trophy_file_ids = json.loads(trophy_file_ids)

            return {
                'id': row['id'],
                'name': row['name'],
                'start_time': row['start_time'],
                'end_time': row['end_time'],
                'duration_days': row['duration_days'],
                'prize_places': row['prize_places'],
                'prizes': prizes,
                'trophy_file_ids': trophy_file_ids,
                'status': row['status']
            }
        return None

async def add_tournament_participant(tournament_id: int, user_id: int):
    """Добавляет участника в турнир"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO tournament_participants (tournament_id, user_id, refs_count)
               VALUES ($1, $2, 0)
               ON CONFLICT (tournament_id, user_id) DO NOTHING''',
            tournament_id, user_id
        )

async def increment_tournament_refs(tournament_id: int, user_id: int):
    """Увеличивает счетчик рефералов участника в турнире"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO tournament_participants (tournament_id, user_id, refs_count)
               VALUES ($1, $2, 1)
               ON CONFLICT (tournament_id, user_id) 
               DO UPDATE SET refs_count = tournament_participants.refs_count + 1''',
            tournament_id, user_id
        )

async def get_tournament_leaderboard(tournament_id: int, limit: int = 10):
    """Получает таблицу лидеров турнира"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT tp.user_id, u.name, u.username, tp.refs_count
               FROM tournament_participants tp
               JOIN users u ON tp.user_id = u.user_id
               WHERE tp.tournament_id = $1
               ORDER BY tp.refs_count DESC
               LIMIT $2''',
            tournament_id, limit
        )
        return [{'user_id': row['user_id'], 'name': row['name'], 
                 'username': row['username'], 'refs_count': row['refs_count']} 
                for row in rows]

async def get_user_tournament_position(tournament_id: int, user_id: int):
    """Получает позицию пользователя в турнире"""
    async with db_pool.acquire() as conn:
        position = await conn.fetchval(
            '''SELECT COUNT(*) + 1
               FROM tournament_participants tp1
               WHERE tp1.tournament_id = $1
               AND tp1.refs_count > (
                   SELECT COALESCE(tp2.refs_count, 0)
                   FROM tournament_participants tp2
                   WHERE tp2.tournament_id = $1 AND tp2.user_id = $2
               )''',
            tournament_id, user_id
        )
        refs_count = await conn.fetchval(
            'SELECT COALESCE(refs_count, 0) FROM tournament_participants WHERE tournament_id = $1 AND user_id = $2',
            tournament_id, user_id
        )
        return {'position': position, 'refs_count': refs_count or 0}

async def finish_tournament(tournament_id: int):
    """Завершает турнир и выдает награды"""
    async with db_pool.acquire() as conn:
        # Получаем данные турнира
        tournament = await conn.fetchrow(
            'SELECT name, prize_places, prizes, trophy_file_ids FROM tournaments WHERE id = $1',
            tournament_id
        )

        if not tournament:
            return False

        prizes = tournament['prizes']
        trophy_file_ids = tournament['trophy_file_ids']

        # Получаем топ участников
        winners = await conn.fetch(
            '''SELECT user_id, refs_count, 
               ROW_NUMBER() OVER (ORDER BY refs_count DESC) as place
               FROM tournament_participants
               WHERE tournament_id = $1
               ORDER BY refs_count DESC
               LIMIT $2''',
            tournament_id, tournament['prize_places']
        )

        # Выдаем награды
        now = int(time.time())
        for winner in winners:
            place = winner['place']
            user_id = winner['user_id']

            if str(place) in prizes:
                prize_stars = float(prizes[str(place)])
                trophy_file_id = trophy_file_ids.get(str(place), trophy_file_ids.get('default', ''))

                # Добавляем награду в таблицу
                await conn.execute(
                    '''INSERT INTO user_trophies 
                       (user_id, tournament_id, tournament_name, place, trophy_file_id, prize_stars, date_received)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)''',
                    user_id, tournament_id, tournament['name'], place, 
                    trophy_file_id, Decimal(str(prize_stars)), now
                )

                # Добавляем звезды на баланс
                await conn.execute(
                    'UPDATE users SET balance = balance + $1 WHERE user_id = $2',
                    Decimal(str(prize_stars)), user_id
                )

        # Закрываем турнир
        await conn.execute(
            'UPDATE tournaments SET status = $1 WHERE id = $2',
            'finished', tournament_id
        )

        return winners

async def get_user_trophies(user_id: int):
    """Получает все награды пользователя"""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            '''SELECT id, tournament_name, place, trophy_file_id, prize_stars, date_received
               FROM user_trophies
               WHERE user_id = $1
               ORDER BY date_received DESC''',
            user_id
        )
        return [{'id': row['id'], 'tournament_name': row['tournament_name'],
                 'place': row['place'], 'trophy_file_id': row['trophy_file_id'],
                 'prize_stars': float(row['prize_stars']), 'date_received': row['date_received']}
                for row in rows]

async def get_admin_tournament_creation_state(admin_id: int):
    """Получает состояние создания турнира админом"""
    import json
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT step, data FROM admin_tournament_creation WHERE admin_id = $1',
            admin_id
        )
        if row:
            return {'step': row['step'], 'data': json.loads(row['data'])}
        return None

async def set_admin_tournament_creation_state(admin_id: int, step: str, data: dict):
    """Устанавливает состояние создания турнира админом"""
    import json
    async with db_pool.acquire() as conn:
        await conn.execute(
            '''INSERT INTO admin_tournament_creation (admin_id, step, data, updated_at)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (admin_id)
               DO UPDATE SET step = $2, data = $3, updated_at = NOW()''',
            admin_id, step, json.dumps(data)
        )

async def delete_admin_tournament_creation_state(admin_id: int):
    """Удаляет состояние создания турнира админом"""
    async with db_pool.acquire() as conn:
        await conn.execute(
            'DELETE FROM admin_tournament_creation WHERE admin_id = $1',
            admin_id
        )

async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except:
        return False

async def send_subscription_message(chat_id: int):
    markup = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="📢 Подписаться на канал", url=CHANNEL_URL)],
        [types.InlineKeyboardButton(text="✅ Проверить подписку", callback_data='check_subscription')]
    ])

    await bot.send_message(
        chat_id,
        "🔒 <b>Для использования бота необходимо подписаться на канал!</b>\n\n"
        "📢 Подпишитесь на наш канал и получите доступ ко всем функциям бота:\n"
        "• 🎮 Мини-игры\n"
        "• 💰 Заработок звёзд\n"
        "• 🎁 Ежедневные бонусы\n"
        "• 👥 Реферальная система\n\n"
        "После подписки нажмите кнопку \"Проверить подписку\"",
        reply_markup=markup,
        parse_mode='HTML'
    )

images = {
    'menu': 'https://i.postimg.cc/Ls8RWPJb/47465488-A1-F0-48-A3-9228-FE04635-E50-FD.jpg',
    'profile': 'https://i.postimg.cc/ZRyQsPSZ/AFE2564-F-20-AA-470-A-993-C-6-D9-BE7-DF67-BB.jpg',
    'games': 'https://i.postimg.cc/SRFvBXS9/AFE2564-F-20-AA-470-A-993-C-6-D9-BE7-DF67-BB.jpg',
    'promo': 'https://i.postimg.cc/tCbcDVmQ/AFE2564-F-20-AA-470-A-993-C-6-D9-BE7-DF67-BB.jpg',
    'referral': 'https://i.postimg.cc/sxZx6Nnm/E42-C5-F7-F-B707-43-D2-94-DB-F525-FBF86-BBC.jpg',
    'withdraw': 'https://i.postimg.cc/NLqMWkSc/65-AB094-D-3-A97-4-A25-8-E2-D-4-C6-CFB2-DD983.jpg',
    'bonus': 'https://i.postimg.cc/rF9bx6dx/IMG-8378.jpg',
    'support': 'https://i.postimg.cc/6pjmbdfQ/IMG-8377.jpg',
    'casino': 'https://i.postimg.cc/3rLWd3DP/96-AE246-D-A9-A9-411-B-A840-CB3382-FD3-D4-F.jpg',
    'dice': 'https://i.postimg.cc/c1wM2sFy/96-AE246-D-A9-A9-411-B-A840-CB3382-FD3-D4-F.jpg',
    'knb': 'https://i.postimg.cc/HnD0nKsh/96-AE246-D-A9-A9-411-B-A840-CB3382-FD3-D4-F.jpg',
    'basket': 'https://i.postimg.cc/6QQTVhm5/E8-D76117-CC3-C-440-E-85-FF-80-ECA05-A9654.jpg',
    'bowling': 'https://i.postimg.cc/KvFQvrB9/96-AE246-D-A9-A9-411-B-A840-CB3382-FD3-D4-F.jpg'
}

class UserStates(StatesGroup):
    awaiting_promo = State()
    awaiting_support = State() 
    awaiting_withdraw = State()
    awaiting_knb_bet = State()
    awaiting_knb_choice = State()
    awaiting_casino_bet = State()
    awaiting_dice_bet = State()
    awaiting_basket_bet = State()
    awaiting_bowling_bet = State()

async def show_menu(chat_id: int, user_id: str = None):
    if user_id:
        await increment_user_session(int(user_id))

    # Проверяем наличие активного турнира
    active_tournament = await get_active_tournament()

    buttons = [
        [types.InlineKeyboardButton(text="👤 Профиль", callback_data='profile'),
         types.InlineKeyboardButton(text="🕹 Игры", callback_data='games')],
        [types.InlineKeyboardButton(text="🔗 Получить ссылку", callback_data='referral'),
         types.InlineKeyboardButton(text="🏆 Топ", callback_data='top')],
        [types.InlineKeyboardButton(text="💰 Вывод", callback_data='withdraw'),
         types.InlineKeyboardButton(text="🎁 Ежедневная награда", callback_data='daily')],
        [types.InlineKeyboardButton(text="🎯 Турниры", callback_data='tournaments'),
         types.InlineKeyboardButton(text="🏅 Мои награды", callback_data='trophies')],
        [types.InlineKeyboardButton(text="📩 Поддержка", callback_data='support')]
    ]

    markup = types.InlineKeyboardMarkup(row_width=2, inline_keyboard=buttons)

    await bot.send_photo(
        chat_id, 
        images['menu'],
        caption="⭐️ Добро пожаловать в меню ⭐️\n\nСейчас бот находится в тест версии, вывод звезд ещё не доступен\n\n<b>Как вывести звезды?</b>\n🔹Получай ежедневные награды, ищи промокоды и зарабатывай звезды\n🔹Приглашай друзей и выполняй задания\n🔹Играй в мини-игры\n🔹Вывод доступен от 50 звезд",
        reply_markup=markup, 
        parse_mode='HTML'
    )

# ===== ADMIN COMMANDS =====

@dp.message(Command("create_tournament"))
async def create_tournament_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет доступа к этой команде")
        return

    await message.reply(
        "🎯 <b>Создание нового турнира</b>\n\n"
        "Напишите название турнира:",
        parse_mode='HTML'
    )
    await set_admin_tournament_creation_state(
        message.from_user.id, 
        'awaiting_name', 
        {}
    )

@dp.message(Command("active_tournament"))
async def active_tournament_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет доступа к этой команде")
        return

    tournament = await get_active_tournament()
    if not tournament:
        await message.reply("ℹ️ Нет активных турниров")
        return

    import datetime
    start_dt = datetime.datetime.fromtimestamp(tournament['start_time'], MOSCOW_TZ)
    end_dt = datetime.datetime.fromtimestamp(tournament['end_time'], MOSCOW_TZ)

    leaderboard = await get_tournament_leaderboard(tournament['id'], 10)

    text = (
        f"🎯 <b>{tournament['name']}</b>\n\n"
        f"📅 Начало: {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
        f"⏰ Конец: {end_dt.strftime('%d.%m.%Y %H:%M')}\n"
        f"🏆 Призовых мест: {tournament['prize_places']}\n\n"
        f"<b>Таблица лидеров:</b>\n"
    )

    for idx, leader in enumerate(leaderboard, 1):
        text += f"{idx}. {leader['name']} - {leader['refs_count']} рефералов\n"

    await message.reply(text, parse_mode='HTML')

@dp.message(Command("end_tournament"))
async def end_tournament_handler(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.reply("❌ У вас нет доступа к этой команде")
        return

    # Получаем название турнира из команды
    command_parts = message.text.split(maxsplit=1)
    
    if len(command_parts) < 2:
        await message.reply(
            "❌ Укажите название турнира\n\n"
            "Пример: /end_tournament Название турнира"
        )
        return
    
    tournament_name = command_parts[1].strip()
    
    # Ищем турнир по названию
    async with db_pool.acquire() as conn:
        import json
        tournament_row = await conn.fetchrow(
            '''SELECT id, name, prize_places, prizes, trophy_file_ids 
               FROM tournaments 
               WHERE name = $1 AND status = 'active' 
               ORDER BY id DESC LIMIT 1''',
            tournament_name
        )
    
    if not tournament_row:
        await message.reply(f"❌ Активный турнир с названием '{tournament_name}' не найден")
        return
    
    # Преобразуем в словарь для совместимости
    tournament = {
        'id': tournament_row['id'],
        'name': tournament_row['name'],
        'prize_places': tournament_row['prize_places'],
        'prizes': tournament_row['prizes'] if isinstance(tournament_row['prizes'], dict) else json.loads(tournament_row['prizes']),
        'trophy_file_ids': tournament_row['trophy_file_ids']
    }

    winners = await finish_tournament(tournament['id'])

    text = f"✅ Турнир <b>{tournament['name']}</b> завершен!\n\n<b>Победители:</b>\n"

    for winner in winners:
        user = await get_user(winner['user_id'])
        place = winner['place']
        prize = tournament['prizes'].get(str(place), 0)
        text += f"{place}. {user['name']} - {winner['refs_count']} рефералов (награда: {prize}⭐️)\n"

        # Отправляем уведомление победителю
        try:
            await bot.send_message(
                winner['user_id'],
                f"🎉 <b>Поздравляем!</b>\n\n"
                f"Ты занял {place} место в турнире <b>{tournament['name']}</b>!\n"
                f"🏆 Твоя награда: {prize}⭐️\n\n"
                f"Проверь раздел 'Мои награды' 🏅",
                parse_mode='HTML'
            )
        except:
            pass

    await message.reply(text, parse_mode='HTML')

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    uid = message.from_user.id

    args = message.text.split()
    ref_id = None
    if len(args) > 1:
        ref_id = args[1]
        print(f"[REFERRAL] User {uid} came with ref_id: {ref_id}")

    if not await check_subscription(message.from_user.id):
        if ref_id and str(ref_id) != str(uid):
            try:
                await set_pending_referral(uid, int(ref_id))
                print(f"[REFERRAL] Saved pending referral for {uid} from {ref_id}")
            except ValueError:
                print(f"[REFERRAL] ERROR: Invalid ref_id format: {ref_id}")
        await send_subscription_message(message.chat.id)
        return

    await increment_user_session(uid)
    await delete_user_state(uid)

    user = await get_user(uid)
    is_new_user = user is None

    if is_new_user:
        await create_user(uid, message.from_user.first_name, message.from_user.username or '')

        if ref_id and str(ref_id) != str(uid):
            try:
                ref_id_int = int(ref_id)
                ref_user = await get_user(ref_id_int)
                if ref_user:
                    await process_referral_db(uid, ref_id_int, message.from_user.first_name)
            except ValueError:
                print(f"[REFERRAL] ERROR: Invalid ref_id format: {ref_id}")

    await show_menu(message.chat.id, str(uid))

@dp.callback_query()
async def handle_query(call: types.CallbackQuery):
    user_id = str(call.from_user.id)
    user_id_int = call.from_user.id
    chat_id = call.message.chat.id
    msg_id = call.message.message_id

    if call.data == 'check_subscription':
        if await check_subscription(call.from_user.id):
            try:
                await call.message.delete()
            except:
                pass

            ref_id = await get_pending_referral(user_id_int)
            if ref_id:
                print(f"[REFERRAL] Processing pending referral: {user_id} from {ref_id}")

                user = await get_user(user_id_int)
                is_new_user = user is None

                if is_new_user:
                    await create_user(user_id_int, call.from_user.first_name, call.from_user.username or '')

                    ref_user = await get_user(ref_id)
                    if ref_user and ref_id != user_id_int:
                        await process_referral_db(user_id_int, ref_id, call.from_user.first_name)

                await delete_pending_referral(user_id_int)

            await show_menu(chat_id, user_id)
            await call.answer("✅ Подписка подтверждена! Добро пожаловать!")
        else:
            await call.answer("❌ Вы ещё не подписались на канал!", show_alert=True)
        return

    if not await check_subscription(call.from_user.id):
        try:
            await call.message.delete()
        except:
            pass
        await send_subscription_message(chat_id)
        await call.answer()
        return

    session = await get_user_session(user_id_int)
    key = f"{user_id}:{msg_id}:{session}"

    if await is_button_used(user_id_int, key):
        await call.answer()
        return
    else:
        await mark_button_used(user_id_int, key)

    user = await get_user(user_id_int)
    if not user:
        await create_user(user_id_int, call.from_user.first_name or 'Пользователь', call.from_user.username or '')
        user = await get_user(user_id_int)

    data = call.data
    back_markup = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="◀️ Вернуться в меню", callback_data='menu')]
    ])

    # Не удаляем сообщение для tournaments и tournament - они отправят новое
    if (not (call.data and call.data.startswith('knb_choice_'))
        and call.data != 'knb_repeat_bet'
        and call.data != 'dice_repeat_bet'
        and call.data != 'basket_repeat_bet'
        and call.data != 'casino_repeat_bet'
        and call.data != 'bowling_repeat_bet'
        and call.data != 'tournaments'
        and call.data != 'tournament'):
        try:
            if call.message:
                await call.message.delete()
        except:
            pass

    if data == 'menu':
        await show_menu(chat_id, user_id)

    elif data == 'profile':
        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🎟 Промокод", callback_data='promo')],
            [types.InlineKeyboardButton(text="◀️ Вернуться в меню", callback_data='menu')]
        ])
        await bot.send_photo(
            chat_id, images['profile'],
            caption=(
                f"✨ <b>Профиль</b>\n──────────────\n"
                f"👤 Имя: {user['name']}\n"
                f"🆔 ID: {call.from_user.id}\n──────────────\n"
                f"💰 Баланс: {user['balance']} ⭐️\n"
                f"👥 Рефералов: {user['refs']}"
            ),
            reply_markup=markup,
            parse_mode='HTML'
        )

    elif data == 'promo':
        await bot.send_photo(
            chat_id, images['promo'],
            caption="🎟 Введите промокод ниже:",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_promo')

    elif data == 'referral':
        global BOT_USERNAME
        if BOT_USERNAME is None:
            try:
                bot_info = await bot.get_me()
                BOT_USERNAME = bot_info.username
            except:
                BOT_USERNAME = "unknown_bot"

        link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
        await bot.send_photo(
            chat_id, images['referral'],
            caption=(
                f"⭐️ Зарабатывай звезды приглашая друзей!⭐️\n\n"
                f"👋 Где искать рефералов?\n"
                f"🔸Приглашай в приложение своих друзей\n"
                f"🔸Оставь свою ссылку в своём канале\n"
                f"🔸Отправляй её в разные чаты\n\n"
                f"🚀 За каждого реферала ты получаешь по 2 ⭐️\n\n"
                f"🔗 Твоя реф ссылка:\n{link}"
            ),
            reply_markup=back_markup,
            parse_mode='HTML'
        )

    elif data == 'top':
        top_users = await get_top_users(3)
        text = "🏆 <b>ТОП 3</b>\n"
        medals = ['🥇', '🥈', '🥉']
        for i, user_data in enumerate(top_users):
            text += f"{medals[i]} {user_data['name']} | {user_data['balance']} ⭐️\n"
        await bot.send_message(chat_id, text, reply_markup=back_markup, parse_mode='HTML')

    elif data == 'withdraw':
        await bot.send_photo(
            chat_id, images['withdraw'],
            caption=f"💸 Введите сумму вывода:\nВаш баланс: {user['balance']} ⭐️",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_withdraw')

    elif data == 'daily':
        if await update_daily_bonus(user_id_int):
            await bot.send_photo(
                chat_id, images['bonus'],
                caption="✅ Ты получил 0.2 ⭐️! Возвращайся завтра!",
                reply_markup=back_markup
            )
        else:
            await bot.send_photo(
                chat_id, images['bonus'],
                caption="⏱ Бонус уже получен сегодня. Возвращайся завтра!",
                reply_markup=back_markup
            )

    elif data == 'support':
        await bot.send_photo(
            chat_id, images['support'],
            caption="📩 Напиши свой вопрос, и мы скоро ответим.",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_support')

    elif data == 'trophies':
        trophies = await get_user_trophies(user_id_int)

        if not trophies:
            await bot.send_message(
                chat_id,
                "🏅 <b>МОИ НАГРАДЫ</b>\n\n"
                "📭 У тебя пока нет наград\n\n"
                "Участвуй в турнирах, чтобы получить кубки!",
                reply_markup=back_markup,
                parse_mode='HTML'
            )
        else:
            # Группируем кубки по типу
            trophy_groups = {}
            for trophy in trophies:
                key = (trophy['trophy_file_id'], trophy['place'])
                if key not in trophy_groups:
                    trophy_groups[key] = []
                trophy_groups[key].append(trophy)

            text = f"🏅 <b>МОИ НАГРАДЫ</b>\n\n📊 Всего кубков: {len(trophies)}\n───────────────\n\n"

            # Показываем кубки
            for (file_id, place), group in trophy_groups.items():
                place_emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(place, "🏅")
                text += f"{place_emoji} <b>{place} место</b> x{len(group)}\n"
                for trophy in group[:3]:  # Показываем максимум 3
                    import datetime
                    date = datetime.datetime.fromtimestamp(trophy['date_received'], MOSCOW_TZ).strftime('%d.%m.%Y')
                    text += f"└ {trophy['tournament_name']} ({date})\n"
                if len(group) > 3:
                    text += f"└ и еще {len(group) - 3}...\n"
                text += "\n"

            text += "───────────────\n💪 Продолжай участвовать в турнирах!"

            # Отправляем первый кубок как фото
            first_trophy = trophies[0]
            await bot.send_photo(
                chat_id,
                first_trophy['trophy_file_id'],
                caption=text,
                reply_markup=back_markup,
                parse_mode='HTML'
            )

    elif data == 'tournaments' or data.startswith('tournament_page_'):
        try:
            # Удаляем старое сообщение
            try:
                await call.message.delete()
            except:
                pass

            # Получаем номер страницы
            page = 0
            if data.startswith('tournament_page_'):
                page = int(data.split('_')[-1])

            # Получаем только активные турниры (идущие в данный момент)
            import json
            async with db_pool.acquire() as conn:
                now = int(time.time())
                all_tournaments = await conn.fetch(
                    '''SELECT id, name, start_time, end_time, status, prize_places, prizes
                       FROM tournaments
                       WHERE status = 'active' AND start_time <= $1 AND end_time > $1
                       ORDER BY start_time ASC''',
                    now
                )

            if not all_tournaments:
                await bot.send_message(
                    chat_id,
                    "ℹ️ Сейчас нет активных турниров",
                    reply_markup=back_markup
                )
            else:
                import datetime
                now = int(time.time())

                # Показываем только один турнир на странице
                if page >= len(all_tournaments):
                    page = 0

                t = all_tournaments[page]
                start_dt = datetime.datetime.fromtimestamp(t['start_time'], MOSCOW_TZ)
                end_dt = datetime.datetime.fromtimestamp(t['end_time'], MOSCOW_TZ)

                # Парсим prizes если это строка
                prizes = t['prizes']
                if isinstance(prizes, str):
                    prizes = json.loads(prizes)

                # Определяем статус
                if t['start_time'] > now:
                    status_emoji = "🔜"
                    status_text = "Скоро начнется"
                    time_info = f"⏰ Начало: {start_dt.strftime('%d.%m.%Y %H:%M')}"
                else:
                    status_emoji = "🔥"
                    status_text = "Активен"
                    time_left = t['end_time'] - now
                    days_left = time_left // 86400
                    hours_left = (time_left % 86400) // 3600
                    time_info = f"⏰ Осталось: {days_left}д {hours_left}ч"

                # Призы
                max_prize = max([float(v) for v in prizes.values()])
                prizes_text = "\n".join([
                    f"{'🥇' if int(p) == 1 else '🥈' if int(p) == 2 else '🥉' if int(p) == 3 else '🏅'} {p} место: {v}⭐️"
                    for p, v in prizes.items()
                ])

                text = (
                    f"{status_emoji} <b>{t['name']}</b>\n\n"
                    f"📊 Статус: {status_text}\n"
                    f"{time_info}\n"
                    f"📅 Конец: {end_dt.strftime('%d.%m.%Y %H:%M')}\n"
                    f"🏆 Призовых мест: {t['prize_places']}\n\n"
                    f"<b>💰 Призы:</b>\n{prizes_text}\n\n"
                    f"💡 Приглашай друзей, чтобы выиграть!"
                )

                # Создаем кнопки навигации
                buttons = []

                # Если турниров больше одного, добавляем навигацию
                if len(all_tournaments) > 1:
                    nav_row = []
                    if page > 0:
                        nav_row.append(types.InlineKeyboardButton(text="◀️ Предыдущий", callback_data=f'tournament_page_{page-1}'))
                    if page < len(all_tournaments) - 1:
                        nav_row.append(types.InlineKeyboardButton(text="Следующий ▶️", callback_data=f'tournament_page_{page+1}'))
                    if nav_row:
                        buttons.append(nav_row)

                    # Индикатор страницы
                    buttons.append([types.InlineKeyboardButton(text=f"📄 {page + 1} из {len(all_tournaments)}", callback_data='noop')])

                buttons.append([types.InlineKeyboardButton(text="◀️ Вернуться в меню", callback_data='menu')])

                markup = types.InlineKeyboardMarkup(inline_keyboard=buttons)

                await bot.send_message(
                    chat_id,
                    text,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
        except Exception as e:
            print(f"[ERROR] Tournaments handler failed: {e}")
            await bot.send_message(
                chat_id,
                "❌ Произошла ошибка при загрузке турниров",
                reply_markup=back_markup
            )

    elif data == 'tournament':
        # Удаляем старое сообщение
        try:
            await call.message.delete()
        except:
            pass

        tournament = await get_active_tournament()

        if not tournament:
            await bot.send_message(
                chat_id,
                "ℹ️ Сейчас нет активных турниров",
                reply_markup=back_markup
            )
        else:
            import datetime
            end_dt = datetime.datetime.fromtimestamp(tournament['end_time'], MOSCOW_TZ)
            time_left = tournament['end_time'] - int(time.time())
            days_left = time_left // 86400
            hours_left = (time_left % 86400) // 3600

            # Добавляем пользователя в турнир (если еще не участвует)
            await add_tournament_participant(tournament['id'], user_id_int)

            # Получаем позицию пользователя
            user_pos = await get_user_tournament_position(tournament['id'], user_id_int)

            # Получаем таблицу лидеров
            leaderboard = await get_tournament_leaderboard(tournament['id'], 10)

            text = (
                f"🎯 <b>{tournament['name']}</b>\n\n"
                f"⏰ Осталось: {days_left}д {hours_left}ч\n"
                f"📅 Конец: {end_dt.strftime('%d.%m.%Y %H:%M')}\n"
                f"🏆 Призовых мест: {tournament['prize_places']}\n\n"
                f"<b>Твоя позиция: #{user_pos['position']}</b>\n"
                f"👥 Рефералов: {user_pos['refs_count']}\n\n"
                f"<b>💰 Призы:</b>\n"
            )

            for place, prize in tournament['prizes'].items():
                place_emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(int(place), "🏅")
                text += f"{place_emoji} {place} место: {prize}⭐️\n"

            text += "\n<b>🏆 Топ участников:</b>\n"

            for idx, leader in enumerate(leaderboard, 1):
                emoji = {1: "🥇", 2: "🥈", 3: "🥉"}.get(idx, "▫️")
                text += f"{emoji} {leader['name']} - {leader['refs_count']} реф.\n"

            text += "\n💡 Приглашай друзей, чтобы подняться в рейтинге!"

            await bot.send_message(
                chat_id,
                text,
                reply_markup=back_markup,
                parse_mode='HTML'
            )

    elif data == 'games':
        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="✊ Цуефа (КНБ)", callback_data='game_knb')],
            [types.InlineKeyboardButton(text="🎰 Казино", callback_data='game_casino')],
            [types.InlineKeyboardButton(text="🎲 Кубики", callback_data='game_dice')],
            [types.InlineKeyboardButton(text="🏀 Баскетбол", callback_data='game_basket')],
            [types.InlineKeyboardButton(text="🎳 Боулинг", callback_data='game_bowling')],
            [types.InlineKeyboardButton(text="◀️ Вернуться в меню", callback_data='menu')]
        ])

        await bot.send_photo(
            chat_id, images['games'],
            caption=(
                "Привет! Ты попал в мини-игры 🎯\n"
                "Тут ты можешь повеселиться и заработать звезды!\n\n"
                "Выбери игру ниже:"
            ),
            reply_markup=markup,
            parse_mode='HTML'
        )

    elif data == 'game_knb':
        back_markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ К мини-играм", callback_data='games')]
        ])
        await bot.send_photo(
            chat_id, images['knb'],
            caption="🎮 <b>Добро пожаловать в игру Цуефа (Камень-Ножницы-Бумага)!</b>\n\n"
                    "🔹 <b>Как играть:</b>\n"
                    "1. Введи ставку (от 1 до 50 ⭐️)\n"
                    "2. Выбери ✊ / ✌️ / 🖐\n\n"
                    "📊 <b>Правила выигрыша:</b>\n"
                    "🥇 Победа — ×1.9 от ставки\n🤝 Ничья — ставка возвращается\n💥 Поражение — ставка сгорает\n\n"
                    "💰 Напиши свою ставку:",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_knb_bet')

    elif data and data.startswith('knb_choice_'):
        user_choice = data.split('_')[-1]

        user_state = await get_user_state(user_id_int)
        if not user_state or (isinstance(user_state, dict) and 'bet' not in user_state):
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Ставка не найдена. Начни игру заново.", reply_markup=markup)
            return

        bet = user_state['bet']
        balance = await get_user_balance(user_id_int)

        if bet > balance:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Недостаточно ⭐️ для этой ставки.", reply_markup=markup)
            return

        bot_choice = random.choice(['rock', 'paper', 'scissors'])

        choices_emoji = {
            'rock': '✊',
            'scissors': '✌️',
            'paper': '🖐'
        }

        choices_label = {
            'rock': 'Камень',
            'scissors': 'Ножницы',
            'paper': 'Бумага'
        }

        win_map = {
            'rock': 'scissors',
            'scissors': 'paper',
            'paper': 'rock'
        }

        await bot.send_message(chat_id, "🧍‍♂️ <b>Ты выбрал:</b>", parse_mode='HTML')
        await asyncio.sleep(0.8)
        await bot.send_message(chat_id, choices_emoji[user_choice])
        await asyncio.sleep(1.2)

        await bot.send_message(chat_id, "🤖 <b>Бот выбрал:</b>", parse_mode='HTML')
        await asyncio.sleep(0.8)
        await bot.send_message(chat_id, choices_emoji[bot_choice])
        await asyncio.sleep(1.5)

        if user_choice == bot_choice:
            result_text = "🤝 <b>Ничья!</b> Твоя ставка возвращается."
            delta = 0
        elif win_map[user_choice] == bot_choice:
            delta = round(bet * 0.9, 2)
            result_text = f"🎉 <b>Ты победил!</b>\nТы заработал <b>+{delta} ⭐️</b>!"
        else:
            delta = -bet
            result_text = f"💥 <b>Ты проиграл...</b>\nПроиграно <b>{bet} ⭐️</b>"

        await update_user_balance(user_id_int, delta)
        new_balance = await get_user_balance(user_id_int)

        final_message = (
            "🧠 <b>Результат игры</b>\n"
            "─────────────────\n"
            f"🔹 Ты выбрал: {choices_emoji[user_choice]} {choices_label[user_choice]}\n"
            f"🔸 Бот выбрал: {choices_emoji[bot_choice]} {choices_label[bot_choice]}\n\n"
            f"{result_text}\n"
            "─────────────────\n"
            f"💰 Текущий баланс: {new_balance} ⭐️"
        )

        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔁 Ещё раз (та же ставка)", callback_data='knb_repeat_bet')],
            [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
            [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
        ])

        await bot.send_message(chat_id, final_message, parse_mode='HTML', reply_markup=markup)

        await set_user_state(user_id_int, {
            'last_knb_bet': bet,
            'bet': bet
        })

    elif data == 'knb_repeat_bet':
        last_state = await get_user_state(user_id_int) or {}
        bet = last_state.get('last_knb_bet') if isinstance(last_state, dict) else None

        if not bet:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Ставка не найдена. Начни игру заново.", reply_markup=markup)
            return

        balance = await get_user_balance(user_id_int)
        if bet > balance:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Недостаточно ⭐️ для повторной ставки.", reply_markup=markup)
            return

        markup = types.InlineKeyboardMarkup(row_width=3, inline_keyboard=[
            [types.InlineKeyboardButton(text="✊ Камень", callback_data="knb_choice_rock"),
             types.InlineKeyboardButton(text="✌️ Ножницы", callback_data="knb_choice_scissors"),
             types.InlineKeyboardButton(text="🖐 Бумага", callback_data="knb_choice_paper")]
        ])
        await bot.send_message(chat_id, f"Выбери снова:", reply_markup=markup)

    elif data == 'game_casino':
        back_markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ К мини-играм", callback_data='games')]
        ])
        await bot.send_photo(
            chat_id, images['casino'],
            caption="🎰 <b>Добро пожаловать в Казино Бота!</b>\n\n"
                    "💵 Введи сумму ставки от 1 до 50 ⭐️, чтобы запустить барабаны.\n\n"
                    "🎲 <b>Возможные выигрыши:</b>\n"
                    "• 7️⃣7️⃣7️⃣ — <b>×20</b>\n"
                    "<b>• 🍫 BARы</b> — <b>x15</b>\n"
                    "• 🍋🍋🍋 — <b>×5</b>\n"
                    "• 🍇🍇🍇 — <b>×5</b>\n\n"
                    "Удачи, звёздный игрок! 🌟",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_casino_bet')

    elif data == 'casino_repeat_bet':
        last_state = await get_user_state(user_id_int) or {}
        bet = last_state.get('last_casino_bet') if isinstance(last_state, dict) else None
        if not bet:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Ставка не найдена. Начни игру заново.", reply_markup=markup)
            return

        balance = await get_user_balance(user_id_int)
        if bet > balance:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Недостаточно ⭐️ для повторной ставки.", reply_markup=markup)
            return

        await update_user_balance(user_id_int, -bet)

        msg = await bot.send_dice(chat_id, emoji='🎰')
        value = msg.dice.value if msg.dice else 0
        await asyncio.sleep(2)

        win = 0
        result_text = ""

        if value == 64:
            win = round(bet * 20, 2)
            result_text = f"🎉 <b>ДЖЕКПОТ!</b> 🎰 Выпали 7️⃣7️⃣7️⃣!\n\nТы срываешь куш и получаешь <b>{win}</b> ⭐️!\n\n🔥 Поздравляем, удача на твоей стороне!"
        elif value == 1:
            win = round(bet * 15, 2)
            result_text = f"🎰Три BAR на барабанах!🎰\n\nТы выигрываешь <b>{win}</b> ⭐️ — Отличный результат! 💎"
        elif value == 43:
            win = round(bet * 5, 2)
            result_text = f"🍋Три одинаковых фрукта на барабанах!🍇\n\nТы выигрываешь {win} ⭐️ — неплохо для быстрого захода 😉"
        elif value == 22:
            win = round(bet * 5, 2)
            result_text = f"🍋Три одинаковых фрукта на барабанах!🍇\n\nТы выигрываешь <b>{win}</b> ⭐️ — неплохо для быстрого захода 😉"
        else:
            result_text = f"😓 Увы, звёзды не сошлись...\nТы проиграл {bet} ⭐️."

        await update_user_balance(user_id_int, win)
        new_balance = await get_user_balance(user_id_int)

        final_message = (
            f"🧠 <b>Результат игры</b>\n"
            f"{result_text}\n\n"
            f"💰 <b>Баланс:</b> {new_balance} ⭐️"
        )

        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='casino_repeat_bet'),
             types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
            [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
        ])

        await bot.send_message(chat_id, final_message, parse_mode='HTML', reply_markup=markup)
        await set_user_state(user_id_int, {'last_casino_bet': bet})

    elif data == 'game_dice':
        back_markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ К мини-играм", callback_data='games')]
        ])
        await bot.send_photo(
            chat_id, images['dice'],
            caption="🎲 <b>Игра «Кубики»</b>\n\n"
                    "🔹 Введи ставку (от 1 до 50 ⭐️)\n"
                    "🔹 Бросаем два кубика: сначала бот, затем ты\n"
                    "🔹 Побеждает большее число\n\n"
                    "📊 <b>Правила выигрыша:</b>\n"
                    "🥇 Победа — ×1.9 от ставки\n🤝 Ничья — ставка возвращается\n💥 Поражение — ставка сгорает\n\n"
                    "💰 Напиши свою ставку:",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_dice_bet')

    elif data == 'dice_repeat_bet':
        last_state = await get_user_state(user_id_int) or {}
        bet = last_state.get('last_dice_bet') if isinstance(last_state, dict) else None

        if not bet:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Ставка не найдена. Начни игру заново.", reply_markup=markup)
            return

        balance = await get_user_balance(user_id_int)
        if bet > balance:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Недостаточно ⭐️ для повторной ставки.", reply_markup=markup)
            return

        await update_user_balance(user_id_int, -bet)

        await bot.send_message(chat_id, "🎲 <b>Твой бросок:</b>", parse_mode="HTML")
        user_dice_msg = await bot.send_dice(chat_id, emoji="🎲")
        user_value = user_dice_msg.dice.value if user_dice_msg.dice else 1
        await asyncio.sleep(3)

        await bot.send_message(chat_id, "🤖 <b>Бросок соперника:</b>", parse_mode="HTML")
        bot_dice_msg = await bot.send_dice(chat_id, emoji="🎲")
        bot_value = bot_dice_msg.dice.value if bot_dice_msg.dice else 1
        await asyncio.sleep(3)

        delta = 0
        if user_value > bot_value:
            delta = round(bet * 1.9, 2)
            result_text = f"🎉 <b>Победа!</b> Ты выиграл <b>+{delta} ⭐️</b>"
        elif user_value == bot_value:
            delta = bet
            result_text = f"🤝 <b>Ничья!</b> Ставка <b>{bet}</b> ⭐️ возвращается."
        else:
            result_text = f"💥 <b>Поражение!</b> Ты потерял <b>{bet} ⭐️</b>"

        await update_user_balance(user_id_int, delta)
        new_balance = await get_user_balance(user_id_int)

        final_message = (
            "🧠 <b>Результат игры</b>\n"
            "─────────────────\n"
            f"🔹 Тебе выпало: <b>{user_value}</b>\n"
            f"🔸 Боту выпало: <b>{bot_value}</b>\n\n"
            f"{result_text}\n"
            "─────────────────\n"
            f"💰 Текущий баланс: {new_balance} ⭐️"
        )

        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='dice_repeat_bet')],
            [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
            [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
        ])

        await bot.send_message(chat_id, final_message, parse_mode='HTML', reply_markup=markup)
        await set_user_state(user_id_int, {'last_dice_bet': bet})

    elif data == 'game_basket':
        back_markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ К мини-играм", callback_data='games')]
        ])
        await bot.send_photo(
            chat_id, images['basket'],
            caption="🏀 <b>Игра «Баскетбол»</b>\n\n"
                    "🔹 Введи ставку (от 1 до 50 ⭐️)\n"
                    "🔹 Делаем один бросок мячом 🏀\n"
                    "🔹 Попадание — победа\n\n"
                    "📊 <b>Выплаты:</b>\n"
                    "🥇 Победа — ×2 от ставки\n💥 Промах — ставка сгорает\n\n"
                    "💰 Напиши свою ставку:",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        await set_user_state(user_id_int, 'awaiting_basket_bet')

    elif data == 'basket_repeat_bet':
        last_state = await get_user_state(user_id_int) or {}
        bet = last_state.get('last_basket_bet') if isinstance(last_state, dict) else None

        if not bet:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Ставка не найдена. Начни игру заново.", reply_markup=markup)
            return

        balance = await get_user_balance(user_id_int)
        if bet > balance:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Главное меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Недостаточно ⭐️ для повторной ставки.", reply_markup=markup)
            return

        await update_user_balance(user_id_int, -bet)

        throw_msg = await bot.send_dice(chat_id, emoji="🏀")
        value = throw_msg.dice.value
        await asyncio.sleep(3)

        if value in (4, 5):
            win = round(bet * 2)
            result_text = f"🎉 <b>Попадание!</b>\n\n Ты выигрываешь <b>{win}</b> ⭐️"
        else:
            win = 0
            result_text = f"💥 <b> Мимо!</b>\n\n Ты проиграл <b>{bet}</b> ⭐️"

        await update_user_balance(user_id_int, win)
        new_balance = await get_user_balance(user_id_int)

        final_message = (
            "🧠 <b>Результат игры</b>\n"
            "─────────────────\n"
            f"{result_text}\n"
            "─────────────────\n"
            f"💰 Баланс: {new_balance} ⭐️"
        )

        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='basket_repeat_bet')],
            [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
            [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
        ])

        await bot.send_message(chat_id, final_message, parse_mode='HTML', reply_markup=markup)
        user_states[user_id] = {'last_basket_bet': bet}

    elif data == 'game_bowling':
        back_markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="◀️ К мини-играм", callback_data='games')]
        ])
        await bot.send_photo(
            chat_id, images['bowling'],
            caption="🎳 <b>Игра «Боулинг»</b>\n\n"
                    "🔹 Введи ставку (от 1 до 50 ⭐️)\n"
                    "🔹 Делаем бросок шаром 🎳\n"
                    "🔹 Сбиваем кегли и выигрываем!\n\n"
                    "📊 <b>Выплаты:</b>\n"
                    "🥇 Страйк (6 кеглей) — ×3\n✨ Почти страйк (5 кеглей) — ×2\n💥 Промах — ставка сгорает\n\n"
                    "💰 Напиши свою ставку:",
            reply_markup=back_markup,
            parse_mode='HTML'
        )
        user_states[user_id] = 'awaiting_bowling_bet'

    elif data == 'bowling_repeat_bet':
        last_state = user_states.get(user_id, {})
        bet = last_state.get('last_bowling_bet')

        if not bet:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Вернуться в меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Ставка не найдена. Начни игру заново.", reply_markup=markup)
            return

        balance = await get_user_balance(user_id_int)
        if bet > balance:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Вернуться в меню", callback_data='menu')]
            ])
            await bot.send_message(chat_id, "❌ Недостаточно ⭐️ для ставки", reply_markup=markup)
            return

        await update_user_balance(user_id_int, -bet)

        throw_msg = await bot.send_dice(chat_id, emoji="🎳")
        value = throw_msg.dice.value
        await asyncio.sleep(3)

        if value == 6:
            win = round(bet * 3, 2)
            result_text = f"🎉 <b>СТРАЙК!</b> Все кегли сбиты!\nТы получаешь <b>{win} ⭐️</b>!"
        elif value == 5:
            win = round(bet * 2, 2)
            result_text = f"✨ <b>Отличный бросок!</b> Почти все кегли сбиты.\nТы выигрываешь <b>{win} ⭐️</b>!"
        else:
            win = 0
            result_text = f"💥 <b>Ты промазал...</b> Кегли устояли.\n\n<b>Проиграно {bet} ⭐️</b>"

        await update_user_balance(user_id_int, win)
        new_balance = await get_user_balance(user_id_int)

        final_message = (
            "🧠 <b>Результат игры</b>\n"
            "─────────────────\n"
            f"{result_text}\n"
            "─────────────────\n"
            f"💰 Баланс: {new_balance} ⭐️"
        )

        markup = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='bowling_repeat_bet')],
            [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
            [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
        ])

        await bot.send_message(chat_id, final_message, parse_mode='HTML', reply_markup=markup)
        user_states[user_id] = {'last_bowling_bet': bet}

    # Обработчик для кнопки-индикатора (не делает ничего)
    if data == 'noop':
        await call.answer()
        return

    await call.answer()

# Обработчик для админа - создание турнира
@dp.message(F.photo)
async def handle_photo(message: types.Message):
    if not is_admin(message.from_user.id):
        return

    admin_state = await get_admin_tournament_creation_state(message.from_user.id)
    if not admin_state:
        return

    step = admin_state['step']
    data = admin_state['data']

    # Получаем file_id фотографии
    photo_file_id = message.photo[-1].file_id

    if step.startswith('awaiting_photo_'):
        place = int(step.split('_')[-1])
        if 'trophy_photos' not in data:
            data['trophy_photos'] = {}
        data['trophy_photos'][str(place)] = photo_file_id

        prize_places = data['prize_places']

        # Если это последнее фото
        if place == prize_places:
            # Создаем турнир
            import datetime

            try:
                # Парсим дату начала (в московском времени)
                date_str, time_str = data['start_date'].split()
                day, month, year = map(int, date_str.split('.'))
                hour, minute = map(int, time_str.split(':'))
                start_dt = MOSCOW_TZ.localize(datetime.datetime(year, month, day, hour, minute))
                start_time = int(start_dt.timestamp())

                # Создаем турнир
                tournament_id = await create_tournament(
                    name=data['name'],
                    start_time=start_time,
                    duration_days=data['duration_days'],
                    prize_places=prize_places,
                    prizes=data['prizes'],
                    trophy_file_ids=data['trophy_photos'],
                    start_message=data.get('start_message')
                )

                await message.reply(
                    f"✅ Турнир <b>{data['name']}</b> успешно создан!\n\n"
                    f"ID: {tournament_id}\n"
                    f"Начало: {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
                    f"Длительность: {data['duration_days']} дней\n"
                    f"Призовых мест: {prize_places}\n\n"
                    f"💬 Стартовое сообщение будет отправлено пользователям автоматически в момент начала турнира.",
                    parse_mode='HTML'
                )

                await delete_admin_tournament_creation_state(message.from_user.id)

            except Exception as e:
                await message.reply(f"❌ Ошибка при создании турнира: {e}")
                await delete_admin_tournament_creation_state(message.from_user.id)
        else:
            # Запрашиваем следующее фото
            next_place = place + 1
            await message.reply(
                f"✅ Фото для {place} места сохранено!\n\n"
                f"Теперь отправьте фото кубка для {next_place} места:"
            )
            await set_admin_tournament_creation_state(
                message.from_user.id,
                f'awaiting_photo_{next_place}',
                data
            )

@dp.message(F.text)
async def handle_admin_tournament_creation(message: types.Message):
    if not is_admin(message.from_user.id):
        return await handle_user_input(message)

    admin_state = await get_admin_tournament_creation_state(message.from_user.id)
    if not admin_state:
        return await handle_user_input(message)

    step = admin_state['step']
    data = admin_state['data']

    if step == 'awaiting_name':
        data['name'] = message.text
        await message.reply("📅 Введите дату и время начала (формат: ДД.ММ.ГГГГ ЧЧ:ММ)\nПример: 25.11.2025 12:00")
        await set_admin_tournament_creation_state(message.from_user.id, 'awaiting_start_date', data)

    elif step == 'awaiting_start_date':
        data['start_date'] = message.text
        await message.reply("⏳ Введите длительность турнира в днях (например: 7):")
        await set_admin_tournament_creation_state(message.from_user.id, 'awaiting_duration', data)

    elif step == 'awaiting_duration':
        try:
            data['duration_days'] = int(message.text)
            await message.reply("🏆 Введите количество призовых мест (например: 3):")
            await set_admin_tournament_creation_state(message.from_user.id, 'awaiting_prize_places', data)
        except:
            await message.reply("❌ Введите число!")

    elif step == 'awaiting_prize_places':
        try:
            prize_places = int(message.text)
            data['prize_places'] = prize_places
            data['prizes'] = {}
            await message.reply(f"💰 Введите награду в звездах для 1 места:")
            await set_admin_tournament_creation_state(message.from_user.id, 'awaiting_prize_1', data)
        except:
            await message.reply("❌ Введите число!")

    elif step.startswith('awaiting_prize_'):
        try:
            place = int(step.split('_')[-1])
            prize = float(message.text)
            data['prizes'][str(place)] = prize

            if place < data['prize_places']:
                next_place = place + 1
                await message.reply(f"💰 Введите награду в звездах для {next_place} места:")
                await set_admin_tournament_creation_state(message.from_user.id, f'awaiting_prize_{next_place}', data)
            else:
                # Все призы введены, запрашиваем стартовое сообщение
                await message.reply(
                    "💬 Введите сообщение, которое будет отправлено всем пользователям при начале турнира:\n\n"
                    "💡 Это сообщение будет отправлено автоматически в момент старта турнира"
                )
                await set_admin_tournament_creation_state(message.from_user.id, 'awaiting_start_message', data)
        except:
            await message.reply("❌ Введите число!")

    elif step == 'awaiting_start_message':
        data['start_message'] = message.text
        # После получения стартового сообщения запрашиваем фото
        await message.reply(
            "📸 Отлично! Теперь отправьте фото кубка для 1 места:\n\n"
            "💡 Можно отправить уникальные кубки для каждого места"
        )
        await set_admin_tournament_creation_state(message.from_user.id, 'awaiting_photo_1', data)

    else:
        return await handle_user_input(message)

@dp.message()
async def handle_user_input(message: types.Message):
    uid = str(message.from_user.id)
    uid_int = message.from_user.id

    if not await check_subscription(message.from_user.id):
        await send_subscription_message(message.chat.id)
        return

    state = user_states.get(uid)

    if state == 'awaiting_promo':
        code = message.text.strip().upper()

        result = await use_promo(uid_int, code)
        await message.reply(result['message'])
        user_states[uid] = None

    elif state == 'awaiting_support':
        txt = f"Вопрос от @{message.from_user.username or 'нет username'} (ID {message.from_user.id}):\n\n{message.text}"
        await bot.send_message(ADMIN_ID, txt)
        await message.reply("✅ Вопрос отправлен, ожидайте ответ")
        user_states[uid] = None

    elif state == 'awaiting_withdraw':
        try:
            amt = float(message.text)
            if amt >= 50:
                if await withdraw_balance(uid_int, amt):
                    user = await get_user(uid_int)
                    await message.reply(f"✅ Заявка на вывод {amt} ⭐️ принята")
                    await bot.send_message(ADMIN_ID, f"Заявка от @{user['username']} на {amt}⭐️")
                else:
                    await message.reply("❌ Недостаточно баланса")
            else:
                await message.reply("❌ Минимальная сумма вывода: 50 ⭐️")
        except:
            await message.reply("❌ Введите число")
        user_states[uid] = None

    elif state == 'awaiting_knb_bet':
        try:
            bet = int(message.text)

            if bet < 1 or bet > 50:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Ставка должна быть от 1 до 50 ⭐️", reply_markup=markup)
                user_states[uid] = None
                return

            balance = await get_user_balance(uid_int)
            if bet > balance:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Недостаточно ⭐️ для ставки", reply_markup=markup)
                user_states[uid] = None
                return

            user_states[uid] = {"state": "awaiting_knb_choice", "bet": bet}

            markup = types.InlineKeyboardMarkup(row_width=3, inline_keyboard=[
                [types.InlineKeyboardButton(text="✊ Камень", callback_data="knb_choice_rock"),
                 types.InlineKeyboardButton(text="✌️ Ножницы", callback_data="knb_choice_scissors"),
                 types.InlineKeyboardButton(text="🖐 Бумага", callback_data="knb_choice_paper")]
            ])

            await bot.send_message(
                message.chat.id,
                "Выбирай предмет:",
                parse_mode="HTML",
                reply_markup=markup,
            )

        except ValueError:
            await bot.send_message(message.chat.id, "❌ Введи число!")
            user_states[uid] = None

    elif state == 'awaiting_casino_bet':
        try:
            bet = int(message.text)

            if bet < 1 or bet > 50:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Ставка должна быть от 1 до 50 ⭐️", reply_markup=markup)
                user_states[uid] = None
                return

            balance = await get_user_balance(uid_int)
            if bet > balance:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Недостаточно ⭐️ для ставки", reply_markup=markup)
                user_states[uid] = None
                return

            await update_user_balance(uid_int, -bet)

            await bot.send_message(message.chat.id, "🎰 <b>Твой спин:</b>", parse_mode="HTML")
            slot_msg = await bot.send_dice(message.chat.id, emoji="🎰")
            value = slot_msg.dice.value
            await asyncio.sleep(2)

            win = 0
            result_text = ""

            if value == 64:
                win = round(bet * 20, 2)
                result_text = f"🎉 ДЖЕКПОТ! 🎰 Выпали 7️⃣7️⃣7️⃣!\n\nТы срываешь куш и получаешь {win} ⭐️!\n\n🔥 Поздравляем, удача на твоей стороне!"
            elif value == 1:
                win = round(bet * 15, 2)
                result_text = f"🎰Три BAR на барабанах!🎰\n\nТы выигрываешь {win} ⭐️ — Отличный результат! 💎"
            elif value == 43:
                win = round(bet * 5, 2)
                result_text = f"🍋Три одинаковых фрукта на барабанах!🍇\n\nТы выигрываешь {win} ⭐️ — неплохо для быстрого захода 😉"
            elif value == 22:
                win = round(bet * 5, 2)
                result_text = f"🍋Три одинаковых фрукта на барабанах!🍇\n\nТы выигрываешь {win} ⭐️ — неплохо для быстрого захода 😉"
            else:
                result_text = (
                    f"😓 Увы, звёзды не сошлись...\n"
                    f"Ты проиграл {bet} ⭐️"
                )

            await update_user_balance(uid_int, win)
            new_balance = await get_user_balance(uid_int)

            final_message = (
                f"🧠 <b>Результат игры</b>\n"
                f"{result_text}\n\n"
                f"💰 Баланс: {new_balance} ⭐️"
            )

            markup = types.InlineKeyboardMarkup(row_width=2, inline_keyboard=[
                [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='casino_repeat_bet'),
                 types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
                [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
            ])

            await bot.send_message(message.chat.id, final_message, parse_mode='HTML', reply_markup=markup)
            user_states[uid] = {'last_casino_bet': bet}

        except ValueError:
            await bot.send_message(message.chat.id, "❌ Введи число!")
            user_states[uid] = None

    elif state == 'awaiting_dice_bet':
        try:
            bet = int(message.text)

            if bet < 1 or bet > 50:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Ставка должна быть от 1 до 50 ⭐️", reply_markup=markup)
                user_states[uid] = None
                return

            balance = await get_user_balance(uid_int)
            if bet > balance:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Недостаточно ⭐️ для ставки", reply_markup=markup)
                user_states[uid] = None
                return

            await update_user_balance(uid_int, -bet)

            await bot.send_message(message.chat.id, "🎲 <b>Твой бросок:</b>", parse_mode="HTML")
            user_dice = (await bot.send_dice(message.chat.id, emoji="🎲")).dice.value
            await asyncio.sleep(3)
            await bot.send_message(message.chat.id, "🤖 <b>Бросок соперника:</b>", parse_mode="HTML")
            bot_dice = (await bot.send_dice(message.chat.id, emoji="🎲")).dice.value
            await asyncio.sleep(3)

            if user_dice > bot_dice:
                win = round(bet * 1.9, 2)
                await update_user_balance(uid_int, win)
                result_text = f"🎉 Ты выиграл <b>{win}</b> ⭐️"
            elif user_dice < bot_dice:
                result_text = f"💥 Ты потерял <b>{bet}</b> ⭐️"
            else:
                await update_user_balance(uid_int, bet)
                result_text = f"🤝 <b>Ничья!</b> Ставка <b>{bet}</b> ⭐️\n возвращается"

            new_balance = await get_user_balance(uid_int)

            final_message = (
                "🧠 <b>Результат игры</b>\n"
                "─────────────────\n"
                f"🔹 Тебе выпало: <b>{user_dice}</b>\n"
                f"🔸 Боту выпало: <b>{bot_dice}</b>\n\n"
                f"{result_text}\n"
                "─────────────────\n"
                f"💰 Текущий баланс: {new_balance} ⭐️"
            )

            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='dice_repeat_bet')],
                [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
                [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
            ])

            await bot.send_message(message.chat.id, final_message, parse_mode='HTML', reply_markup=markup)
            user_states[uid] = {'last_dice_bet': bet}

        except ValueError:
            await bot.send_message(message.chat.id, "❌ Введи число!")
            user_states[uid] = None

    elif state == 'awaiting_basket_bet':
        try:
            bet = int(message.text)

            if bet < 1 or bet > 50:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Ставка должна быть от 1 до 50 ⭐️", reply_markup=markup)
                user_states[uid] = None
                return

            balance = await get_user_balance(uid_int)
            if bet > balance:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Недостаточно ⭐️ для ставки", reply_markup=markup)
                user_states[uid] = None
                return

            await update_user_balance(uid_int, -bet)

            throw_msg = await bot.send_dice(message.chat.id, emoji="🏀")
            value = throw_msg.dice.value
            await asyncio.sleep(3)

            if value in (4, 5):
                win = round(bet * 2)
                await update_user_balance(uid_int, win)
                result_text = f"🎉 <b>Попадание!</b>\n\n Ты выигрываешь <b>{win}</b> ⭐️"
            else:
                result_text = f"💥 <b> Мимо!</b>\n\n Ты проиграл <b>{bet}</b> ⭐️"

            new_balance = await get_user_balance(uid_int)

            final_message = (
                "🧠 <b>Результат игры</b>\n"
                "─────────────────\n"
                f"{result_text}\n"
                "─────────────────\n"
                f"💰 Баланс: {new_balance} ⭐️"
            )

            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='basket_repeat_bet')],
                [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
                [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
            ])

            await bot.send_message(message.chat.id, final_message, parse_mode='HTML', reply_markup=markup)
            user_states[uid] = {'last_basket_bet': bet}

        except ValueError:
            await bot.send_message(message.chat.id, "❌ Введи число!")
            user_states[uid] = None

    elif state == 'awaiting_bowling_bet':
        try:
            bet = int(message.text)
            if bet < 1 or bet > 50:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 Вернуться в меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Ставка должна быть от 1 до 50 ⭐️", reply_markup=markup)
                user_states[uid] = None
                return

            balance = await get_user_balance(uid_int)
            if bet > balance:
                markup = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="🏠 Вернуться в меню", callback_data='menu')]
                ])
                await bot.send_message(message.chat.id, "❌ Недостаточно ⭐️ для ставки", reply_markup=markup)
                user_states[uid] = None
                return

            await update_user_balance(uid_int, -bet)

            throw_msg = await bot.send_dice(message.chat.id, emoji="🎳")
            value = throw_msg.dice.value
            await asyncio.sleep(3)

            if value == 6:
                win = round(bet * 3, 2)
                result_text = f"🎉 <b>СТРАЙК!</b> Все кегли сбиты!\nТы получаешь <b>{win} ⭐️</b>!"
            elif value == 5:
                win = round(bet * 2, 2)
                result_text = f"✨ <b>Отличный бросок!</b> Почти все кегли сбиты.\nТы выигрываешь <b>{win} ⭐️</b>!"
            else:
                win = 0
                result_text = f"💥 <b>Ты промазал...</b> Кегли устояли.\n\n<b>Проиграно {bet} ⭐️</b>"

            await update_user_balance(uid_int, win)
            new_balance = await get_user_balance(uid_int)

            final_message = (
                "🧠 <b>Результат игры</b>\n"
                "─────────────────\n"
                f"{result_text}\n"
                "─────────────────\n"
                f"💰 Баланс: {new_balance} ⭐️"
            )

            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🔁 Ещё раз", callback_data='bowling_repeat_bet')],
                [types.InlineKeyboardButton(text="🎯 К мини-играм", callback_data='games')],
                [types.InlineKeyboardButton(text="🏠 В меню", callback_data='menu')]
            ])

            await bot.send_message(message.chat.id, final_message, parse_mode='HTML', reply_markup=markup)
            user_states[uid] = {'last_bowling_bet': bet}

        except ValueError:
            markup = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="🏠 Вернуться в меню", callback_data='menu')]
            ])
            await bot.send_message(message.chat.id, "❌ Нужно ввести число!", reply_markup=markup)
            user_states[uid] = None

# ===== BACKGROUND TASKS =====

async def daily_bonus_notifications():
    """Отправляет уведомления пользователям о доступной ежедневной награде"""
    while True:
        try:
            await asyncio.sleep(3600)  # Проверяем каждый час

            if not db_pool:
                continue

            async with db_pool.acquire() as conn:
                now = time.time()
                # Находим пользователей, которые не забирали награду более 24 часов
                users_to_notify = await conn.fetch(
                    '''SELECT user_id, name FROM users 
                       WHERE last_bonus < $1 AND last_bonus > 0
                       LIMIT 100''',
                    now - 86400  # 24 часа назад
                )

                for user_row in users_to_notify:
                    try:
                        days_ago = int((now - user_row['last_bonus']) / 86400)
                        if days_ago >= 1:
                            await bot.send_message(
                                user_row['user_id'],
                                f"🎁 <b>Твоя ежедневная награда ждет тебя!</b>\n\n"
                                f"💎 Ты не забирал награду уже {days_ago} дней\n"
                                f"⭐️ Получи 0.2 звезды прямо сейчас!",
                                parse_mode='HTML'
                            )
                            print(f"[NOTIFICATION] Sent daily bonus reminder to {user_row['user_id']}")
                    except Exception as e:
                        print(f"[NOTIFICATION] Failed to notify user {user_row['user_id']}: {e}")

        except Exception as e:
            print(f"[NOTIFICATION] Error in daily bonus notifications: {e}")
            await asyncio.sleep(60)

async def tournament_auto_finish():
    """Автоматически завершает турниры, когда время истекло"""
    while True:
        try:
            await asyncio.sleep(300)  # Проверяем каждые 5 минут

            if not db_pool:
                continue

            async with db_pool.acquire() as conn:
                now = int(time.time())
                # Находим турниры, которые закончились, но еще активны
                expired_tournaments = await conn.fetch(
                    '''SELECT id, name FROM tournaments 
                       WHERE status = 'active' AND end_time <= $1''',
                    now
                )

                for tournament in expired_tournaments:
                    try:
                        print(f"[TOURNAMENT] Auto-finishing tournament {tournament['id']}: {tournament['name']}")
                        await finish_tournament(tournament['id'])
                        print(f"[TOURNAMENT] Tournament {tournament['id']} finished successfully")
                    except Exception as e:
                        print(f"[TOURNAMENT] Failed to finish tournament {tournament['id']}: {e}")

        except Exception as e:
            print(f"[TOURNAMENT] Error in auto-finish: {e}")
            await asyncio.sleep(60)

async def cleanup_task():
    """Периодически очищает старые записи"""
    while True:
        try:
            await asyncio.sleep(21600)  # Каждые 6 часов

            if not db_pool:
                continue

            await cleanup_old_records()
            print("[CLEANUP] Old records cleaned successfully")

        except Exception as e:
            print(f"[CLEANUP] Error in cleanup task: {e}")
            await asyncio.sleep(600)

async def tournament_start_notifications():
    """Отправляет стартовые сообщения при начале турниров"""
    notified_tournaments = set()  # Для отслеживания уже отправленных уведомлений
    
    while True:
        try:
            await asyncio.sleep(60)  # Проверяем каждую минуту

            if not db_pool:
                continue

            async with db_pool.acquire() as conn:
                now = int(time.time())
                # Находим турниры, которые начались в последние 2 минуты и еще не завершены
                starting_tournaments = await conn.fetch(
                    '''SELECT id, name, start_message FROM tournaments 
                       WHERE status = 'active' 
                       AND start_time <= $1 
                       AND start_time > $2
                       AND start_message IS NOT NULL''',
                    now, now - 120
                )

                for tournament in starting_tournaments:
                    # Проверяем, не отправляли ли уже уведомление для этого турнира
                    if tournament['id'] in notified_tournaments:
                        continue
                    
                    try:
                        # Получаем всех пользователей
                        all_users = await conn.fetch('SELECT user_id FROM users')
                        
                        sent_count = 0
                        for user_row in all_users:
                            try:
                                await bot.send_message(
                                    user_row['user_id'],
                                    tournament['start_message'],
                                    parse_mode='HTML'
                                )
                                sent_count += 1
                                await asyncio.sleep(0.05)  # Задержка чтобы не словить лимит
                            except Exception as e:
                                print(f"[TOURNAMENT_START] Failed to notify user {user_row['user_id']}: {e}")
                        
                        notified_tournaments.add(tournament['id'])
                        print(f"[TOURNAMENT_START] Sent start notifications for tournament {tournament['id']} to {sent_count} users")
                    except Exception as e:
                        print(f"[TOURNAMENT_START] Failed to send notifications for tournament {tournament['id']}: {e}")

        except Exception as e:
            print(f"[TOURNAMENT_START] Error in start notifications: {e}")
            await asyncio.sleep(60)

async def main():
    global BOT_USERNAME
    print("Бот запускается...")

    try:
        await init_db_pool()

        bot_info = await bot.get_me()
        BOT_USERNAME = bot_info.username
        print(f"[BOT] Bot username cached: {BOT_USERNAME}")

        # Запускаем фоновые задачи
        asyncio.create_task(daily_bonus_notifications())
        asyncio.create_task(tournament_auto_finish())
        asyncio.create_task(tournament_start_notifications())
        asyncio.create_task(cleanup_task())
        print("[BOT] Background tasks started")

        await dp.start_polling(bot)
    except Exception as e:
        print(f"Ошибка при запуске бота: {e}")
    finally:
        await close_db_pool()
        await bot.session.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "webhook":
        # Режим вебхука для Railway
        from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
        from aiohttp import web
        
        async def on_startup(dispatcher: Dispatcher, bot: Bot):
            await bot.set_webhook(f"{os.getenv('RAILWAY_STATIC_URL', 'https://your-domain.up.railway.app')}/webhook")
        
        async def main_webhook():
            await dp.startup.register(on_startup)
            
            app = web.Application()
            webhook_requests_handler = SimpleRequestHandler(
                dispatcher=dp,
                bot=bot,
            )
            webhook_requests_handler.register(app, path="/webhook")
            
            port = int(os.getenv("PORT", 8080))
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, host="0.0.0.0", port=port)
            await site.start()
            
            print(f"Bot started on port {port} with webhook")
            await asyncio.Event().wait()  # Бесконечное ожидание
        
        asyncio.run(main_webhook())
    else:
        # Старый режим polling для локальной разработки
        asyncio.run(main())
