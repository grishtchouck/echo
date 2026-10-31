// «Эхо» — серверная часть. Vercel Function + Upstash Redis (REST).
// Переменные окружения: KV_REST_API_URL / KV_REST_API_TOKEN
// (или UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN) — Vercel добавит их сам
// при подключении Upstash Redis к проекту.
//
// Состояние партии не хранится целиком: оно каждый раз выводится из записей
// игроков. Поэтому одновременные ответы не конфликтуют.

const PEOPLE = require('./_echo-people.js');

const MIN_PLAYERS = 3;
const MAX_PLAYERS = 12;
const MAX_ROUNDS = 4;
const MAX_WORDS = 5;
const TTL = 60 * 60 * 12;

/* ---------- Redis ---------- */
let redisImpl = null;
async function upstash(cmds){
  const env = process.env;
  const find = suffixes => { for (const k of Object.keys(env)) if (suffixes.some(s => k === s || k.endsWith('_' + s)) && env[k]) return env[k]; return null; };
  const url = find(['KV_REST_API_URL', 'UPSTASH_REDIS_REST_URL']);
  const token = find(['KV_REST_API_TOKEN', 'UPSTASH_REDIS_REST_TOKEN']);
  if (!url || !token) throw new HttpError(500, 'Хранилище не подключено: добавьте Upstash Redis в проект на Vercel.');
  const r = await fetch(url.replace(/\/$/, '') + '/pipeline', {
    method: 'POST',
    headers: {Authorization: 'Bearer ' + token, 'Content-Type': 'application/json'},
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new HttpError(502, 'Хранилище не отвечает (' + r.status + ').');
  const j = await r.json();
  return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
function redis(cmds){ return (redisImpl || upstash)(cmds); }
function hash(flat){
  const o = {};
  if (Array.isArray(flat)) for (let i = 0; i < flat.length; i += 2) o[flat[i]] = flat[i + 1];
  else if (flat && typeof flat === 'object') Object.assign(o, flat);
  return o;
}

class HttpError extends Error { constructor(status, msg){ super(msg); this.status = status; } }
const fail = (msg, status) => { throw new HttpError(status || 400, msg); };

/* ---------- утилиты ---------- */
function rnd(n){ return require('crypto').randomInt(n); }
function shuffle(a){ for (let i = a.length - 1; i > 0; i--){ const j = rnd(i + 1); [a[i], a[j]] = [a[j], a[i]]; } return a; }
const K = code => ({host: `e:${code}:host`, players: `e:${code}:p`, game: `e:${code}:g`});
const wKey = (code, n) => `e:${code}:w${n}`;
const qKey = (code, n) => `e:${code}:q${n}`;
const rKey = (code, n) => `e:${code}:r${n}`;   // замены карточек: pid → новая легенда
const vKey = (code, n) => `e:${code}:v${n}`;   // шаг раскрытия
const sKey = code => `e:${code}:sc`;            // очки: номер партии → {pid: очки}

function cleanName(s){
  s = String(s || '').replace(/\s+/g, ' ').trim().slice(0, 20);
  if (!s) fail('Введите имя.');
  return s;
}
function words(s){ return String(s || '').trim().split(/\s+/).filter(w => /[\p{L}\p{N}]/u.test(w)); }
function cleanText(s){
  s = String(s || '').replace(/\s+/g, ' ').trim();
  const w = words(s);
  if (!w.length) fail('Напишите ассоциацию.');
  if (w.length > MAX_WORDS) fail('Не больше ' + MAX_WORDS + ' слов.');
  if (s.length > 80) fail('Слишком длинно.');
  return s;
}
const norm = s => s.toLowerCase().replace(/ё/g, 'е').replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
function namesPerson(text, person){
  const t = norm(text).split(' ');
  const STOP = ['первый','первая','вторая','второй','великий','король','королева','принцесса','магистр','граф','барон','старик','маленький','человек','железный','снежная','красная','собака','страна','чудес','почтальон','бессмертный'];
  const parts = norm(person).split(' ').filter(p => p.length >= 4 && STOP.indexOf(p) < 0);
  return parts.some(p => t.some(w => w.length >= 4 && (w.startsWith(p.slice(0, Math.max(4, p.length - 2))) || p.startsWith(w.slice(0, Math.max(4, w.length - 2))))));
}

/* ---------- чтение комнаты ---------- */
async function load(code, withRound){
  const k = K(code);
  const [host, players, gameRaw] = await redis([['GET', k.host], ['HGETALL', k.players], ['GET', k.game]]);
  if (!host) fail('Комната не найдена. Проверьте код.', 404);
  const pl = hash(players);
  const list = Object.keys(pl).map(id => ({id, ...JSON.parse(pl[id])})).sort((a, b) => a.t - b.t);
  const game = gameRaw ? JSON.parse(gameRaw) : null;
  let W = {}, Q = {}, R = {}, V = 0, SC = {};
  if (game && withRound !== false){
    const [w, q, r, v, sc] = await redis([['HGETALL', wKey(code, game.n)], ['HGETALL', qKey(code, game.n)], ['HGETALL', rKey(code, game.n)], ['GET', vKey(code, game.n)], ['HGETALL', sKey(code)]]);
    W = hash(w); Q = hash(q); R = hash(r); V = +v || 0; SC = hash(sc);
  } else if (withRound !== false){
    SC = hash((await redis([['HGETALL', sKey(code)]]))[0]);
  }
  return {code, host, players: list, game, W, Q, R, V, SC};
}

function phase(room){
  const g = room.game;
  if (!g) return {name: 'lobby'};
  const N = g.order.length;
  for (let r = 0; r < g.rounds; r++){
    const done = g.order.filter(id => room.W[r + ':' + id] != null);
    if (done.length < N) return {name: 'write', round: r, done};
  }
  const done = g.order.filter(id => room.Q[id] != null);
  if (done.length < N) return {name: 'guess', done};
  return {name: 'reveal'};
}

/* Легенды с учётом замен: карточку c в первом раунде держит order[c]. */
function persOf(room){ const g = room.game; return g.pers.map((p, c) => room.R[g.order[c]] || p); }
function listOf(room){
  const g = room.game, eff = persOf(room);
  return g.list.map(name => { const c = g.pers.indexOf(name); return c >= 0 ? eff[c] : name; });
}
function pointsOf(room){
  const g = room.game, N = g.order.length, eff = persOf(room), list = listOf(room), pts = {};
  g.order.forEach(id => { pts[id] = 0; });
  eff.forEach((person, c) => {
    const gid = g.order[(c + g.rounds) % N];
    if (list[+room.Q[gid]] === person){
      pts[gid]++;
      for (let r = 0; r < g.rounds; r++) pts[g.order[(c + r) % N]]++;
    }
  });
  return pts;
}
function board(room){
  const tot = {}, last = {};
  Object.keys(room.SC).forEach(n => { const o = JSON.parse(room.SC[n]); Object.keys(o).forEach(id => { tot[id] = (tot[id] || 0) + o[id]; }); });
  if (room.game && room.SC[room.game.n]) Object.assign(last, JSON.parse(room.SC[room.game.n]));
  return room.players.filter(p => tot[p.id] != null).map(p => ({id: p.id, name: p.name, total: tot[p.id], last: last[p.id] || 0}))
    .sort((a, b) => b.total - a.total || b.last - a.last);
}
const pName = (room, id) => (room.players.find(p => p.id === id) || {name: '—'}).name;
const cardAt = (g, p, r) => ((p - r) % g.order.length + g.order.length) % g.order.length;
function textOn(room, c, r){ const g = room.game; return room.W[r + ':' + g.order[(c + r) % g.order.length]]; }

function view(room, pid){
  const ph = phase(room);
  const me = room.players.find(p => p.id === pid);
  const base = {
    code: room.code,
    me: me ? {id: me.id, name: me.name, host: room.host === pid} : null,
    players: room.players.map(p => ({id: p.id, name: p.name, host: room.host === p.id})),
    min: MIN_PLAYERS, max: MAX_PLAYERS, maxWords: MAX_WORDS,
    phase: ph.name,
    board: board(room)
  };
  const g = room.game;
  if (!g) return base;
  const N = g.order.length, p = g.order.indexOf(pid);
  base.game = {n: g.n, rounds: g.rounds, size: N, inGame: p >= 0};
  base.order = g.order.map(id => ({id, name: pName(room, id)}));
  if (ph.name === 'write'){
    base.round = ph.round;
    base.done = ph.done;
    if (p >= 0){
      const c = cardAt(g, p, ph.round);
      base.prompt = ph.round === 0 ? {kind: 'person', text: persOf(room)[c]} : {kind: 'assoc', text: textOn(room, c, ph.round - 1)};
      base.mine = room.W[ph.round + ':' + pid] || null;
      base.canReroll = ph.round === 0 && !base.mine && !room.R[pid];
    }
  } else if (ph.name === 'guess'){
    base.done = ph.done;
    base.list = listOf(room);
    if (p >= 0){
      const c = cardAt(g, p, g.rounds);
      base.prompt = {kind: 'assoc', text: textOn(room, c, g.rounds - 1)};
      base.mine = room.Q[pid] != null ? +room.Q[pid] : null;
    }
  } else if (ph.name === 'reveal'){
    const eff = persOf(room), list = listOf(room);
    const per = g.rounds + 2, total = N * per, step = Math.min(Math.max(room.V, 1), total);
    let score = 0;
    const all = eff.map((person, c) => {
      const chain = [];
      for (let r = 0; r < g.rounds; r++){ const id = g.order[(c + r) % N]; chain.push({id, name: pName(room, id), text: room.W[r + ':' + id]}); }
      const gid = g.order[(c + g.rounds) % N];
      const guess = list[+room.Q[gid]];
      const ok = guess === person;
      if (ok) score++;
      return {person, chain, guesser: {id: gid, name: pName(room, gid)}, guess, ok};
    });
    // Отдаём только раскрытую часть, чтобы никто не подсмотрел вперёд.
    base.reveal = {step, total, per, done: step >= total};
    base.cards = all.map((card, c) => {
      const shown = step - c * per;
      if (shown <= 0) return null;
      if (shown >= per) return card;
      const part = {person: card.person, chain: card.chain.slice(0, shown - 1), guesser: card.guesser, partial: true};
      return part;
    }).filter(Boolean);
    if (base.reveal.done){ base.score = score; base.points = pointsOf(room); }
  }
  return base;
}

/* ---------- действия ---------- */
async function touch(code, n){
  const k = K(code), cmds = [['EXPIRE', k.host, TTL], ['EXPIRE', k.players, TTL], ['EXPIRE', k.game, TTL]];
  cmds.push(['EXPIRE', sKey(code), TTL]);
  if (n != null) cmds.push(['EXPIRE', wKey(code, n), TTL], ['EXPIRE', qKey(code, n), TTL], ['EXPIRE', rKey(code, n), TTL], ['EXPIRE', vKey(code, n), TTL]);
  await redis(cmds);
}

function newGame(room){
  const ids = room.players.map(p => p.id);
  if (ids.length < MIN_PLAYERS) fail('Нужно хотя бы ' + MIN_PLAYERS + ' игрока.');
  if (ids.length > MAX_PLAYERS) fail('Не больше ' + MAX_PLAYERS + ' игроков.');
  const N = ids.length;
  const order = shuffle(ids.slice());
  const pool = shuffle(PEOPLE.slice());
  const pers = pool.slice(0, N);
  const decoys = pool.slice(N, N * 2);
  return {n: room.game ? room.game.n + 1 : 1, order, pers, list: shuffle(pers.concat(decoys)), rounds: Math.min(N - 1, MAX_ROUNDS)};
}

async function handle(body){
  const a = body.a, pid = String(body.pid || '');
  if (!/^[A-Za-z0-9_-]{8,40}$/.test(pid)) fail('Нет идентификатора игрока.');

  if (a === 'create'){
    const name = cleanName(body.name);
    for (let i = 0; i < 25; i++){
      const code = String(1000 + rnd(9000));
      const k = K(code);
      const [ok] = await redis([['SET', k.host, pid, 'NX', 'EX', TTL]]);
      if (ok === 'OK'){
        await redis([['DEL', k.players, k.game], ['HSET', k.players, pid, JSON.stringify({name, t: Date.now()})], ['EXPIRE', k.players, TTL]]);
        return view(await load(code), pid);
      }
    }
    fail('Не удалось создать комнату, попробуйте ещё раз.', 503);
  }

  const code = String(body.code || '');
  if (!/^\d{4}$/.test(code)) fail('Код — четыре цифры.');

  if (a === 'state') return view(await load(code), pid);

  if (a === 'join'){
    const name = cleanName(body.name);
    const room = await load(code);
    const ph = phase(room);
    const already = room.players.some(p => p.id === pid);
    if (!already && ph.name !== 'lobby' && ph.name !== 'reveal') fail('Игра уже идёт. Дождитесь конца раунда.', 409);
    if (!already && room.players.length >= MAX_PLAYERS) fail('Комната заполнена.', 409);
    const t = already ? room.players.find(p => p.id === pid).t : Date.now();
    await redis([['HSET', K(code).players, pid, JSON.stringify({name, t})]]);
    await touch(code);
    return view(await load(code), pid);
  }

  const room = await load(code);
  const isPlayer = room.players.some(p => p.id === pid);
  if (!isPlayer) fail('Вы не в этой комнате.', 403);
  const ph = phase(room);

  if (a === 'kick' || a === 'leave'){
    const target = a === 'leave' ? pid : String(body.target || '');
    if (a === 'kick' && room.host !== pid) fail('Убирать игроков может только создатель комнаты.', 403);
    if (target === room.host) fail('Создатель комнаты не может выйти.');
    if (ph.name !== 'lobby' && ph.name !== 'reveal') fail('Во время партии игроков не убрать.');
    await redis([['HDEL', K(code).players, target]]);
    return view(await load(code), pid);
  }

  if (a === 'start'){
    if (room.host !== pid) fail('Начать может только создатель комнаты.', 403);
    if (ph.name !== 'lobby' && ph.name !== 'reveal') return view(room, pid);
    const g = newGame(room);
    await redis([['SET', K(code).game, JSON.stringify(g), 'EX', TTL]]);
    await touch(code, g.n);
    return view(await load(code), pid);
  }

  if (a === 'write'){
    if (ph.name !== 'write') fail('Сейчас не время писать.', 409);
    if (+body.round !== ph.round) return view(room, pid);
    const p = room.game.order.indexOf(pid);
    if (p < 0) fail('Вы не участвуете в этой партии.', 403);
    const text = cleanText(body.text);
    if (ph.round === 0 && namesPerson(text, persOf(room)[cardAt(room.game, p, 0)])) fail('Нельзя писать само имя.');
    if (ph.round > 0){
      const prev = norm(textOn(room, cardAt(room.game, p, ph.round), ph.round - 1) || '').split(' ').filter(w => w.length >= 4);
      const mine = norm(text).split(' ');
      const same = prev.find(w => mine.some(m => m.length >= 4 && m.slice(0, Math.max(4, Math.min(m.length, w.length) - 2)) === w.slice(0, Math.max(4, Math.min(m.length, w.length) - 2))));
      if (same) fail('Нельзя повторять слова из полученной ассоциации («' + same + '»).');
    }
    await redis([['HSETNX', wKey(code, room.game.n), ph.round + ':' + pid, text], ['EXPIRE', wKey(code, room.game.n), TTL]]);
    return view(await load(code), pid);
  }

  if (a === 'reroll'){
    if (ph.name !== 'write' || ph.round !== 0) fail('Заменить легенду можно только в первом раунде.', 409);
    if (!room.game.order.includes(pid)) fail('Вы не участвуете в этой партии.', 403);
    if (room.W['0:' + pid] != null) fail('Ты уже написал ассоциацию.');
    if (room.R[pid]) fail('Замена уже использована.');
    const taken = new Set(room.game.pers.concat(room.game.list, Object.values(room.R)));
    const free = PEOPLE.filter(x => !taken.has(x));
    if (!free.length) fail('Легенды закончились.');
    await redis([['HSETNX', rKey(code, room.game.n), pid, free[rnd(free.length)]], ['EXPIRE', rKey(code, room.game.n), TTL]]);
    return view(await load(code), pid);
  }

  if (a === 'next' || a === 'all'){
    if (room.host !== pid) fail('Листает создатель комнаты.', 403);
    if (ph.name !== 'reveal') return view(room, pid);
    const g = room.game, total = g.order.length * (g.rounds + 2), cur = Math.max(room.V, 1);
    const to = a === 'all' ? total : Math.min(total, cur + 1);
    const cmds = [['SET', vKey(code, g.n), String(to), 'EX', TTL]];
    if (to >= total) cmds.push(['HSETNX', sKey(code), String(g.n), JSON.stringify(pointsOf(room))], ['EXPIRE', sKey(code), TTL]);
    await redis(cmds);
    return view(await load(code), pid);
  }

  if (a === 'guess'){
    if (ph.name !== 'guess') fail('Сейчас не время угадывать.', 409);
    if (!room.game.order.includes(pid)) fail('Вы не участвуете в этой партии.', 403);
    const i = +body.choice;
    if (!(i >= 0 && i < room.game.list.length)) fail('Выберите личность.');
    await redis([['HSETNX', qKey(code, room.game.n), pid, String(i)], ['EXPIRE', qKey(code, room.game.n), TTL]]);
    return view(await load(code), pid);
  }

  fail('Неизвестное действие.');
}

module.exports = async function(req, res){
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST'){ res.statusCode = 405; return res.end(JSON.stringify({error: 'POST only'})); }
  try {
    let body = req.body;
    if (typeof body === 'string') body = JSON.parse(body || '{}');
    const out = await handle(body || {});
    res.statusCode = 200;
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.end(JSON.stringify(out));
  } catch (e){
    res.statusCode = e.status || 500;
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.end(JSON.stringify({error: e.status ? e.message : 'Ошибка сервера: ' + e.message}));
  }
};
module.exports._setRedis = fn => { redisImpl = fn; };
module.exports._internal = {phase, view, cardAt, namesPerson, words, persOf, listOf, pointsOf};
