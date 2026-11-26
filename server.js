const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
let OAuth2Client = null; try { ({ OAuth2Client } = require('google-auth-library')); } catch (e) {}
let webpush = null; try { webpush = require('web-push'); } catch (e) {}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e7, cors: { origin: '*' } });

// ================= STORAGE (MongoDB Atlas, falls back to db.json) =================
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, 'db.json');
let db = {
  totalVisits: 0, visitors: [], users: [], friends: {}, requests: {}, global: [], dms: {}, files: [],
  profiles: {}, blocks: {}, accounts: {}, sessions: {}, push: {}, bans: [], strikes: {}, ips: {}, cids: {}, badWords: [], reports: [], groups: {}
};
let M = null;
const logErr = e => console.error('Storage error:', e.message);
const metaOf = () => ({
  totalVisits: db.totalVisits, visitors: db.visitors, users: db.users, friends: db.friends, requests: db.requests,
  profiles: db.profiles, blocks: db.blocks, accounts: db.accounts, sessions: db.sessions, push: db.push,
  bans: db.bans, strikes: db.strikes, ips: db.ips, cids: db.cids, badWords: db.badWords, reports: db.reports, groups: db.groups
});

async function initStorage() {
  if (process.env.MONGODB_URI) {
    try {
      const { MongoClient } = require('mongodb');
      const client = new MongoClient(process.env.MONGODB_URI);
      for (let i = 1; ; i++) {
        try { await client.connect(); break; }
        catch (e) { if (i >= 3) throw e; console.log('MongoDB retry ' + i + '...'); await new Promise(r => setTimeout(r, 3000)); }
      }
      const d = client.db(process.env.MONGODB_DB || 'geminios');
      M = { meta: d.collection('meta'), msgs: d.collection('messages'), files: d.collection('files'), aichats: d.collection('aichats') };
      const meta = await M.meta.findOne({ _id: 'main' });
      if (meta) { delete meta._id; Object.assign(db, meta); }
      (await M.msgs.find().sort({ t: 1 }).toArray()).forEach(m => {
        if (m.chat === 'global') db.global.push(m);
        else (db.dms[m.chat] = db.dms[m.chat] || []).push(m);
      });
      db.files = await M.files.find().sort({ t: 1 }).toArray();
      console.log('Storage: MongoDB Atlas');
      return;
    } catch (e) {
      console.error('!!! MongoDB FAILED: accounts will be LOST on restart. Check MONGODB_URI and Atlas Network Access (0.0.0.0/0). Reason:', e.message);
      M = null;
    }
  } else console.error('!!! MONGODB_URI is not set: accounts will be LOST on restart.');
  try { Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) {}
  console.log('Storage: local file (temporary on free Render)');
}
async function flushMeta() {
  try {
    if (M) await M.meta.replaceOne({ _id: 'main' }, { _id: 'main', ...metaOf() }, { upsert: true });
    else fs.writeFileSync(DB_FILE, JSON.stringify(db));
  } catch (e) { logErr(e); }
}
let metaTimer;
const pMeta = () => { clearTimeout(metaTimer); metaTimer = setTimeout(flushMeta, 1000); };
const pMsg = m => { if (M) M.msgs.replaceOne({ _id: m.id }, { ...m, _id: m.id }, { upsert: true }).catch(logErr); else pMeta(); };
const pMsgDel = id => { if (M) M.msgs.deleteOne({ _id: id }).catch(logErr); else pMeta(); };
const pFile = f => { if (M) M.files.replaceOne({ _id: f.id }, { ...f, _id: f.id }, { upsert: true }).catch(logErr); else pMeta(); };
const pFileDel = id => { if (M) M.files.deleteOne({ _id: id }).catch(logErr); else pMeta(); };
async function fullSave() {
  if (!M) return flushMeta();
  await M.msgs.deleteMany({}); await M.files.deleteMany({});
  const all = [...db.global, ...Object.values(db.dms).flat()];
  if (all.length) await M.msgs.insertMany(all.map(m => ({ ...m, _id: m.id })));
  if (db.files.length) await M.files.insertMany(db.files.map(f => ({ ...f, _id: f.id })));
  await flushMeta();
}
process.on('SIGTERM', async () => { clearTimeout(metaTimer); await flushMeta(); process.exit(0); });

// ================= HELPERS =================
const online = {};
const recording = new Set();
// ---- Doom rooms ----
const doomRooms = {}, doomOf = {};
function doomLeave(name) { const c = doomOf[name]; if (!c) return; delete doomOf[name]; const s = doomRooms[c]; if (!s) return; s.delete(name); if (!s.size) delete doomRooms[c]; else s.forEach(n => to(n, 'doom_players', [...s])); }
// ---- Arena multiplayer rooms (relay only; clients simulate) ----
const arenaRooms = {}, arenaOf = {};
function arenaLeave(name) { const c = arenaOf[name]; if (!c) return; delete arenaOf[name]; const set = arenaRooms[c]; if (!set) return; set.delete(name); if (!set.size) delete arenaRooms[c]; else set.forEach(n => to(n, 'arena_players', [...set])); }
const live = new Set(); // people streaming their screen LIVE (nothing is saved)
const liveViews = {}; // streamer -> Set of viewers
const MAX_VIEWERS = 6;
function stopLive(name) { if (live.delete(name)) { (liveViews[name] || new Set()).forEach(v => to(v, 'live_ended', { name })); delete liveViews[name]; A().emit('live_list', [...live]); } }
function stopWatching(v) { Object.keys(liveViews).forEach(st => { if (liveViews[st].delete(v)) to(st, 'live_viewer_left', { name: v }); }); }
const MAX_BYTES = 14e6;
const A = () => io.to('authed');
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const arr = (o, k) => (hasOwn(o, k) ? o[k] : (o[k] = []));
const prof = n => {
  const p = hasOwn(db.profiles, n) ? db.profiles[n] : (db.profiles[n] = { lastSeen: 0, visits: 0, status: '', emoji: '😀', wins: 0 });
  if (p.coins === undefined) Object.assign(p, { coins: 0, bio: '', avatar: '', ach: [], best: {}, played: {} }); // fills old profiles too
  if (!Array.isArray(p.own)) Object.assign(p, { own: [], rank: '', color: '', badge: '' }); // NEW: shop items (owned list + what is equipped)
  return p;
};
const directory = () => db.users.map(n => { const { avatar, own, ...p } = prof(n); return { name: n, ...p, av: avatar ? (p.avV || 1) : 0, role: roleOf(n) }; });
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const isBlocked = (a, b) => arr(db.blocks, a).includes(b) || arr(db.blocks, b).includes(a);

// ---- coins, achievements, leaderboard ----
const ACH = { // id: [icon, title, test]
  first_win: ['🥇', 'First win', p => p.wins >= 1],
  wins_10: ['🏆', '10 wins', p => p.wins >= 10],
  coins_100: ['🪙', '100 coins', p => p.coins >= 100],
  coins_1000: ['💰', '1000 coins', p => p.coins >= 1000],
  dash_done: ['🔷', 'Finished a Dash level', p => (p.best.gd || 0) >= 100],
  all_games: ['🎮', 'Played all 5 games', p => ['xo', 'arena', 'doom', 'gd', 'draw'].every(g => p.played[g])]
};
// ---- quiz (AI writes the questions; a built-in bank is the fallback) and group chats ----
const QZ_BANK = [
  { q: 'What is the capital of India?', a: ['Mumbai', 'New Delhi', 'Kolkata', 'Chennai'], c: 1 },
  { q: 'Which planet is called the Red Planet?', a: ['Venus', 'Jupiter', 'Mars', 'Saturn'], c: 2 },
  { q: 'Which is the largest ocean?', a: ['Atlantic', 'Indian', 'Arctic', 'Pacific'], c: 3 },
  { q: 'What is the chemical formula of water?', a: ['CO2', 'H2O', 'O2', 'NaCl'], c: 1 },
  { q: 'Who wrote the Indian national anthem?', a: ['Rabindranath Tagore', 'Bankim Chandra', 'Mahatma Gandhi', 'Sarojini Naidu'], c: 0 },
  { q: 'How many days are in a leap year?', a: ['365', '364', '366', '367'], c: 2 },
  { q: 'Which is the fastest land animal?', a: ['Lion', 'Cheetah', 'Horse', 'Leopard'], c: 1 },
  { q: 'Which is the largest planet in our solar system?', a: ['Earth', 'Saturn', 'Neptune', 'Jupiter'], c: 3 },
  { q: 'What is the currency of Japan?', a: ['Yen', 'Won', 'Yuan', 'Dollar'], c: 0 },
  { q: 'What is the national animal of India?', a: ['Lion', 'Elephant', 'Tiger', 'Peacock'], c: 2 },
  { q: 'What is the chemical symbol for gold?', a: ['Ag', 'Au', 'Gd', 'Go'], c: 1 },
  { q: 'How many continents are there?', a: ['5', '6', '8', '7'], c: 3 },
  { q: 'Which gas do plants absorb from the air?', a: ['Oxygen', 'Carbon dioxide', 'Nitrogen', 'Helium'], c: 1 },
  { q: 'Who is known as the Father of the Nation in India?', a: ['Jawaharlal Nehru', 'Sardar Patel', 'Mahatma Gandhi', 'Subhas Chandra Bose'], c: 2 },
  { q: 'How many players are on the field for one cricket team?', a: ['9', '10', '11', '12'], c: 2 },
  { q: 'What does CPU stand for?', a: ['Central Processing Unit', 'Computer Personal Unit', 'Central Program Utility', 'Core Power Unit'], c: 0 },
  { q: 'Which is the longest river in India?', a: ['Yamuna', 'Ganga', 'Godavari', 'Brahmaputra'], c: 1 },
  { q: 'Which planet is closest to the Sun?', a: ['Venus', 'Mercury', 'Earth', 'Mars'], c: 1 },
  { q: 'What is the largest mammal?', a: ['Elephant', 'Blue whale', 'Giraffe', 'Hippopotamus'], c: 1 },
  { q: 'In which year did India gain independence?', a: ['1945', '1947', '1950', '1952'], c: 1 },
  { q: 'How many sides does a hexagon have?', a: ['5', '6', '7', '8'], c: 1 },
  { q: 'What is the smallest prime number?', a: ['0', '1', '2', '3'], c: 2 },
  { q: 'What is the hardest natural substance?', a: ['Gold', 'Iron', 'Diamond', 'Quartz'], c: 2 },
  { q: 'Which festival is called the festival of lights?', a: ['Holi', 'Diwali', 'Eid', 'Pongal'], c: 1 },
  { q: 'What is 12 x 12?', a: ['124', '132', '144', '154'], c: 2 },
  { q: 'Which organ pumps blood in the human body?', a: ['Liver', 'Heart', 'Lungs', 'Kidney'], c: 1 },
  { q: 'What is the capital of Maharashtra?', a: ['Pune', 'Nagpur', 'Mumbai', 'Nashik'], c: 2 },
  { q: 'Which language is used to style web pages?', a: ['HTML', 'CSS', 'Python', 'SQL'], c: 1 }
];
const quizOf = Object.create(null), lastQuiz = Object.create(null), lastGM = Object.create(null), QUIZ_DAILY = 60;
function parseQuiz(t) {
  const m = t && String(t).match(/\[[\s\S]*\]/); if (!m) return null;
  let a; try { a = JSON.parse(m[0]); } catch (e) { return null; }
  if (!Array.isArray(a)) return null;
  const qs = a.map(x => ({ q: String((x && x.q) || '').slice(0, 200), a: Array.isArray(x && x.a) ? x.a.slice(0, 4).map(s => String(s).slice(0, 80)) : [], c: Math.floor(+(x && x.c)) }))
    .filter(x => x.q && x.a.length === 4 && x.c >= 0 && x.c < 4);
  return qs.length >= 3 ? qs.slice(0, 5) : null;
}
const grp = () => db.groups || (db.groups = {});
const myGroups = n => Object.values(grp()).filter(g => g.members.includes(n));
const gInfo = g => ({ id: g.id, name: g.name, owner: g.owner, members: g.members, last: g.msgs.length ? g.msgs[g.msgs.length - 1] : null });
const gRefresh = (g, extra) => { [...new Set(g.members.concat(extra || []))].forEach(n => to(n, 'groups', myGroups(n).map(gInfo))); };
// ---- daily login reward + streak, daily quests (a "day" changes at 00:00 UTC = 05:30 AM India time) ----
const reps = () => db.reports || (db.reports = []);
const dayStr = (off = 0) => new Date(Date.now() + off * 864e5).toISOString().slice(0, 10);
const QUESTS = [
  { id: 'msg', t: 'Send 5 chat messages', goal: 5, coins: 10 },
  { id: 'play', t: 'Finish 3 games', goal: 3, coins: 15 },
  { id: 'win', t: 'Win 1 game', goal: 1, coins: 20 }
];
function questState(name) {
  const p = prof(name);
  if (!p.q || p.q.day !== dayStr()) p.q = { day: dayStr(), list: QUESTS.map(q => ({ id: q.id, n: 0, done: 0 })) };
  return { streak: (p.loginDay === dayStr() || p.loginDay === dayStr(-1)) ? (p.streak || 0) : 0, list: QUESTS.map((q, i) => ({ ...q, n: p.q.list[i].n, done: p.q.list[i].done })) };
}
function questProg(name, id, n = 1) {
  const p = prof(name); questState(name);
  const q = p.q.list.find(x => x.id === id), def = QUESTS.find(x => x.id === id);
  if (!q || q.done) return;
  q.n = Math.min(def.goal, q.n + n);
  if (q.n >= def.goal) {
    q.done = 1; p.coins += def.coins;
    to(name, 'coins', { coins: p.coins, gained: def.coins });
    to(name, 'toast', '✅ Quest done: ' + def.t + ' (+' + def.coins + ' coins)'); stats();
  }
  pMeta(); to(name, 'quests', questState(name));
}
function dailyLogin(name) {
  const p = prof(name), today = dayStr();
  if (p.loginDay !== today) {
    p.streak = p.loginDay === dayStr(-1) ? (p.streak || 0) + 1 : 1; p.loginDay = today;
    const c = Math.min(5 + (p.streak - 1) * 2, 25) + (p.streak % 7 === 0 ? 50 : 0);
    p.coins += c; to(name, 'coins', { coins: p.coins, gained: c });
    to(name, 'toast', '🎁 Daily reward +' + c + ' coins · 🔥 ' + p.streak + '-day streak' + (p.streak % 7 === 0 ? ' (7-day bonus!)' : ''));
    pMeta(); stats();
  }
  to(name, 'quests', questState(name));
}
function reward(name, game, coins, win, score) {
  questProg(name, 'play', 1); if (win) questProg(name, 'win', 1);
  const p = prof(name);
  p.coins += coins; if (win) p.wins = (p.wins || 0) + 1;
  p.played[game] = (p.played[game] || 0) + 1;
  if (score > (p.best[game] || 0)) p.best[game] = score;
  to(name, 'coins', { coins: p.coins, gained: coins });
  Object.keys(ACH).forEach(id => {
    if (!p.ach.includes(id) && ACH[id][2](p)) { p.ach.push(id); to(name, 'toast', ACH[id][0] + ' Achievement: ' + ACH[id][1]); }
  });
  pMeta(); stats();
}

// ---- NEW: shop (spend coins). Owner and Admin can NOT be bought: they only come from the account. ----
const SHOP = {
  rank_vip:    { cat: 'rank',  name: 'VIP',    icon: '⭐', price: 300 },
  rank_pro:    { cat: 'rank',  name: 'PRO',    icon: '💎', price: 800 },
  rank_elite:  { cat: 'rank',  name: 'ELITE',  icon: '🔥', price: 2000 },
  rank_legend: { cat: 'rank',  name: 'LEGEND', icon: '🐉', price: 5000 },
  col_red:     { cat: 'color', name: 'Red',     icon: '🎨', css: '#ff5252', price: 150 },
  col_green:   { cat: 'color', name: 'Green',   icon: '🎨', css: '#66bb6a', price: 150 },
  col_cyan:    { cat: 'color', name: 'Cyan',    icon: '🎨', css: '#26c6da', price: 150 },
  col_pink:    { cat: 'color', name: 'Pink',    icon: '🎨', css: '#f48fb1', price: 150 },
  col_gold:    { cat: 'color', name: 'Gold',    icon: '🎨', css: '#ffd54f', price: 300 },
  col_rainbow: { cat: 'color', name: 'Rainbow', icon: '🌈', css: 'rainbow', price: 700 },
  bdg_ghost:   { cat: 'badge', name: 'Ghost',  icon: '👻', price: 100 },
  bdg_rocket:  { cat: 'badge', name: 'Rocket', icon: '🚀', price: 200 },
  bdg_alien:   { cat: 'badge', name: 'Alien',  icon: '👽', price: 200 },
  bdg_skull:   { cat: 'badge', name: 'Skull',  icon: '💀', price: 300 },
  bdg_diamond: { cat: 'badge', name: 'Diamond', icon: '💠', price: 500 },
  bdg_dragon:  { cat: 'badge', name: 'Dragon', icon: '🐲', price: 1000 },
  rank_bronze:   { cat: 'rank', name: 'BRONZE',   icon: '🥉', price: 100,   fg: '#cd7f32' },
  rank_silver:   { cat: 'rank', name: 'SILVER',   icon: '🥈', price: 200,   fg: '#c0c0c0' },
  rank_gold:     { cat: 'rank', name: 'GOLD',     icon: '🥇', price: 500,   fg: '#ffc107' },
  rank_platinum: { cat: 'rank', name: 'PLATINUM', icon: '💠', price: 1200,  fg: '#80deea' },
  rank_master:   { cat: 'rank', name: 'MASTER',   icon: '🎖️', price: 3000,  fg: '#ef5350' },
  rank_mythic:   { cat: 'rank', name: 'MYTHIC',   icon: '🔱', price: 8000,  fg: '#ba68c8' },
  rank_god:      { cat: 'rank', name: 'GOD',      icon: '⚡', price: 20000, fg: '#ffee58' },
  col_orange:    { cat: 'color', name: 'Orange',  icon: '🟠', css: '#ffa726', price: 150 },
  col_purple:    { cat: 'color', name: 'Purple',  icon: '🟣', css: '#ba68c8', price: 150 },
  col_blue:      { cat: 'color', name: 'Blue',    icon: '🔵', css: '#42a5f5', price: 150 },
  col_lime:      { cat: 'color', name: 'Lime',    icon: '🟢', css: '#c6ff00', price: 200 },
  col_teal:      { cat: 'color', name: 'Teal',    icon: '🩵', css: '#1de9b6', price: 200 },
  col_neon:      { cat: 'color', name: 'Neon Green', icon: '✨', css: '#39ff14', glow: 1, price: 400 },
  col_neonpink:  { cat: 'color', name: 'Neon Pink',  icon: '✨', css: '#ff4081', glow: 1, price: 400 },
  col_fire:      { cat: 'color', name: 'Fire',    icon: '🔥', css: '#ff9100', grad: 'linear-gradient(90deg,#ff1744,#ff9100,#ffea00)', price: 800 },
  col_ocean:     { cat: 'color', name: 'Ocean',   icon: '🌊', css: '#2979ff', grad: 'linear-gradient(90deg,#00e5ff,#2979ff,#7c4dff)', price: 800 },
  col_sunset:    { cat: 'color', name: 'Sunset',  icon: '🌇', css: '#ff5f6d', grad: 'linear-gradient(90deg,#ff5f6d,#ffc371)', price: 900 },
  col_galaxy:    { cat: 'color', name: 'Galaxy',  icon: '🌌', css: '#e040fb', grad: 'linear-gradient(90deg,#7c4dff,#e040fb,#40c4ff)', price: 1500 },
  bdg_heart:     { cat: 'badge', name: 'Heart',   icon: '❤️', price: 120 },
  bdg_star:      { cat: 'badge', name: 'Star',    icon: '⭐', price: 150 },
  bdg_moon:      { cat: 'badge', name: 'Moon',    icon: '🌙', price: 150 },
  bdg_sun:       { cat: 'badge', name: 'Sun',     icon: '☀️', price: 150 },
  bdg_fire:      { cat: 'badge', name: 'Fire',    icon: '🔥', price: 250 },
  bdg_robot:     { cat: 'badge', name: 'Robot',   icon: '🤖', price: 250 },
  bdg_ufo:       { cat: 'badge', name: 'UFO',     icon: '🛸', price: 300 },
  bdg_bolt:      { cat: 'badge', name: 'Bolt',    icon: '⚡', price: 350 },
  bdg_ninja:     { cat: 'badge', name: 'Ninja',   icon: '🥷', price: 400 },
  bdg_wolf:      { cat: 'badge', name: 'Wolf',    icon: '🐺', price: 450 },
  bdg_unicorn:   { cat: 'badge', name: 'Unicorn', icon: '🦄', price: 600 },
  bdg_eagle:     { cat: 'badge', name: 'Eagle',   icon: '🦅', price: 700 },
  bdg_trophy:    { cat: 'badge', name: 'Trophy',  icon: '🏆', price: 800 },
  bdg_crown:     { cat: 'badge', name: 'Crown',   icon: '👑', price: 3000 }
};
// ---- many more shop items (ranks = tags, name colours, badges) ----
[['noob','NOOB','🐣',30,'#b0bec5'],['rookie','ROOKIE','🌱',80,'#8bc34a'],['skilled','SKILLED','🎯',150,'#4db6ac'],['hero','HERO','🦸',400,'#42a5f5'],['knight','KNIGHT','🛡️',700,'#90a4ae'],['samurai','SAMURAI','⚔️',1000,'#ef5350'],['wizard','WIZARD','🧙',1500,'#ab47bc'],['sniper','SNIPER','🎯',1800,'#ff7043'],['titan','TITAN','🗿',2500,'#8d6e63'],['phoenix','PHOENIX','🔥',4000,'#ff9800'],['emperor','EMPEROR','🏯',6000,'#ffca28'],['cosmic','COSMIC','🌌',10000,'#7c4dff'],['immortal','IMMORTAL','♾️',15000,'#00e5ff'],['hp_legend','HP LEGEND','🏆',30000,'#ff4081'],['hp_king','HP KING','👑',50000,'#ffd740']]
  .forEach(([k, n, i, p, c]) => { SHOP['rank_' + k] = { cat: 'rank', name: n, icon: i, price: p, fg: c }; });
[['mint','Mint','#69f0ae',200],['coral','Coral','#ff8a80',200],['sky','Sky','#80d8ff',200],['amber','Amber','#ffc400',250],['violet','Violet','#b388ff',250],['rose','Rose','#ff80ab',250]]
  .forEach(([k, n, c, p]) => { SHOP['col_' + k] = { cat: 'color', name: n, icon: '🎨', css: c, price: p }; });
[['aurora','Aurora','linear-gradient(90deg,#00e676,#00b0ff,#d500f9)','#00e676',1200],['lava','Lava','linear-gradient(90deg,#ff1744,#ff6d00,#ffd600)','#ff6d00',1200],['candy','Candy','linear-gradient(90deg,#ff80ab,#b388ff,#80d8ff)','#ff80ab',1000],['matrix','Matrix','linear-gradient(90deg,#00e676,#1b5e20)','#00e676',1500]]
  .forEach(([k, n, g, c, p]) => { SHOP['col_' + k] = { cat: 'color', name: n, icon: '🌈', css: c, grad: g, price: p }; });
[['cat','Cat','🐱',100],['dog','Dog','🐶',100],['panda','Panda','🐼',150],['fox','Fox','🦊',200],['tiger','Tiger','🐯',300],['shark','Shark','🦈',350],['octopus','Octopus','🐙',300],['butterfly','Butterfly','🦋',250],['cherry','Cherry Blossom','🌸',200],['cactus','Cactus','🌵',150],['gamepad','Gamepad','🎮',250],['music','Music','🎵',150],['rocket2','Space','🪐',400],['comet','Comet','☄️',500],['gem','Gem','💎',900],['medal','Medal','🥇',600],['cyclone','Cyclone','🌀',450],['hp','HP Logo','⬢',2000]]
  .forEach(([k, n, i, p]) => { SHOP['bdg_' + k] = { cat: 'badge', name: n, icon: i, price: p }; });
const SHOP_SLOTS = ['rank', 'color', 'badge'];

// ---- push notifications ----
const pushOn = !!(webpush && process.env.VAPID_PUBLIC && process.env.VAPID_PRIVATE);
if (pushOn) webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'mailto:admin@example.com', process.env.VAPID_PUBLIC, process.env.VAPID_PRIVATE);
function sendPush(name, payload) {
  if (!pushOn) return;
  arr(db.push, name).slice().forEach(s =>
    webpush.sendNotification(s, JSON.stringify(payload), { TTL: payload.type === 'call' ? 45 : 3600, urgency: 'high' })
      .catch(e => { if (e.statusCode === 404 || e.statusCode === 410) { db.push[name] = arr(db.push, name).filter(x => x.endpoint !== s.endpoint); pMeta(); } }));
}

// ---- accounts ----
const gClient = (OAuth2Client && process.env.GOOGLE_CLIENT_ID) ? new OAuth2Client(process.env.GOOGLE_CLIENT_ID) : null;
const akey = n => n.toLowerCase();
const getAcc = k => (hasOwn(db.accounts, k) ? db.accounts[k] : null);
const cleanName = n => String(n || '').replace(/[|<>.$]/g, '').replace(/\s+/g, ' ').trim().slice(0, 24);
const badName = n => n.length < 2 || ['global', '__proto__'].includes(n.toLowerCase());
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const nameOfToken = t => (hasOwn(db.sessions, sha(String(t || ''))) ? db.sessions[sha(String(t))] : null);
const newSession = name => {
  Object.keys(db.sessions).forEach(h => { if (db.sessions[h] === name) delete db.sessions[h]; });
  const t = crypto.randomBytes(32).toString('hex');
  db.sessions[sha(t)] = name; pMeta(); return t;
};
const tvCodes = Object.create(null); // TV sign-in codes: code -> { sid, exp }
const fails = {};
const tooMany = k => fails[k] && fails[k].n >= 5 && Date.now() - fails[k].t < 300000;
const addFail = k => { const f = fails[k]; if (!f || Date.now() - f.t > 300000) fails[k] = { n: 1, t: Date.now() }; else { f.n++; f.t = Date.now(); } };
// ---- phone sign-in with OTP: Twilio Verify (set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_VERIFY_SID on Render) ----
// For testing without SMS set OTP_DEV=1: the code is then printed in the Render logs instead of being sent.
const TW = { sid: process.env.TWILIO_ACCOUNT_SID, tok: process.env.TWILIO_AUTH_TOKEN, ver: process.env.TWILIO_VERIFY_SID };
const TW_ON = !!(TW.sid && TW.tok && TW.ver), PHONE_ON = TW_ON || process.env.OTP_DEV === '1';
const otpDev = Object.create(null), phoneOk = Object.create(null), otpSent = Object.create(null);
const normPhone = s => { s = String(s || '').replace(/[\s\-().]/g, ''); if (/^\d{10}$/.test(s)) s = '+91' + s; return /^\+[1-9]\d{7,14}$/.test(s) ? s : ''; };
const twHead = () => ({ Authorization: 'Basic ' + Buffer.from(TW.sid + ':' + TW.tok).toString('base64'), 'content-type': 'application/x-www-form-urlencoded' });
async function otpSend(phone) {
  if (!TW_ON) { const c = String(crypto.randomInt(100000, 1000000)); otpDev[phone] = { c, exp: Date.now() + 600000, n: 0 }; console.log('[OTP_DEV] code for ' + phone + ': ' + c); return; }
  const r = await fetch('https://verify.twilio.com/v2/Services/' + TW.ver + '/Verifications', { method: 'POST', headers: twHead(), body: new URLSearchParams({ To: phone, Channel: 'sms' }) });
  if (!r.ok) { let m = ''; try { m = (await r.json()).message; } catch (e) {} throw new Error(m || 'SMS service error ' + r.status); }
}
async function otpCheck(phone, code) {
  if (!TW_ON) { const o = otpDev[phone]; if (!o || o.exp < Date.now() || ++o.n > 5) { delete otpDev[phone]; return false; } if (o.c !== code) return false; delete otpDev[phone]; return true; }
  const r = await fetch('https://verify.twilio.com/v2/Services/' + TW.ver + '/VerificationCheck', { method: 'POST', headers: twHead(), body: new URLSearchParams({ To: phone, Code: code }) });
  const j = await r.json().catch(() => ({})); return j.status === 'approved';
}
const OWNER_NAME = 'AllTimeLaugher';
const isReal = n => !!(n && String(n).toLowerCase() === OWNER_NAME.toLowerCase() && getAcc(akey(n))); // the REAL owner: lives in the code, can never be banned or demoted
const isOwner = n => !!(n && (isReal(n) || (getAcc(akey(n)) && getAcc(akey(n)).coOwner))); // real owner + owners made by the real owner
const isAdmin = n => !!(n && getAcc(akey(n)) && (getAcc(akey(n)).admin || getAcc(akey(n)).coAdmin || isOwner(n)));
const roleOf = n => { if (isReal(n)) return 'owner'; if (isOwner(n)) return 'coowner'; const a = n && getAcc(akey(n)); return a && a.admin ? 'admin' : a && a.coAdmin ? 'coadmin' : ''; };

// ---- bans & auto-moderation ----
const ipOf = s => String(s.handshake.headers['x-forwarded-for'] || s.handshake.address || '').split(',')[0].trim();
// A ban matches the account name, the device id (browser), and - only if the admin chose it - an IP address.
const banFor = (name, ip, cid) => {
  db.bans = db.bans.filter(b => !b.until || b.until > Date.now());
  return db.bans.find(b =>
    (ip && ((b.ips || []).includes(ip) || b.ip === ip)) ||
    (cid && (b.cids || []).includes(cid)) ||
    (name && b.name && b.name.toLowerCase() === String(name).toLowerCase()));
};
function banUser(name, ips, cids, seconds, reason, by) {
  const b = { id: uid(), name, ips: ips || [], cids: cids || [], reason, by, at: Date.now(), until: seconds ? Date.now() + seconds * 1000 : 0 };
  db.bans.push(b); pMeta();
  [...io.sockets.sockets.values()].filter(s =>
    (name && s.data.me === name) || b.ips.includes(s.data.ip) || (s.data.cid && b.cids.includes(s.data.cid))
  ).forEach(s => {
    if (isReal(s.data.me)) return;
    s.emit('banned', b); s.disconnect(true);
  });
  return b;
}

// ---- abusive-word list: English, Hindi (Hinglish + Devanagari) and other languages ----
// A leading "=" means "match only as a whole word" (avoids false alarms inside innocent words).
// The owner/admins can add more words from the Admin panel; or set BAD_WORDS="a,b,c" on Render.
const WORDS = [
  // English
  'fuck', 'fucker', 'fucking', 'motherfucker', 'mofo', 'shit', 'bullshit', 'bitch', 'asshole', 'bastard', 'cunt', 'dick', 'dickhead',
  'slut', 'whore', 'pussy', 'cock', 'wanker', 'twat', 'prick', 'nigger', 'nigga', 'faggot', 'retard', '=bollocks',
  // Hindi written in English letters (Hinglish)
  'madarchod', 'maderchod', 'madrchod', 'behenchod', 'bhenchod', 'benchod', 'behnchod', 'bhosdike', 'bhosdi', 'bhosda', 'bhosadi', 'bhosad',
  'chutiya', 'chutia', 'chutiye', 'chut', 'gandu', 'gaandu', '=gaand', 'lund', 'lavda', 'lawda', 'loda', '=randi', '=raand', 'harami',
  'haramzada', 'haramkhor', 'kamina', 'kameena', 'kamine', 'bhadwa', 'bhadwe', 'bsdk', 'mkc', 'bkl', 'maa ki chut', 'behen ki chut',
  // Hindi (Devanagari)
  'मादरचोद', 'बहनचोद', 'भोसड़ीके', 'भोसड़ी', 'चूतिया', 'चुतिया', 'गांडू', 'गाण्डू', '=लंड', 'लौड़ा', 'रंडी', 'हरामी', 'हरामज़ादा', 'कमीना', 'भड़वा', '=चूत', '=गांड',
  // Spanish, French, German, Portuguese, Italian
  'puta', 'puto', 'mierda', 'cabron', 'pendejo', '=joder', 'gilipollas', 'hijueputa', '=coño', '=merde', 'putain', 'connard', 'salope', 'encule', 'enculé',
  'scheisse', 'scheiße', 'arschloch', 'fotze', 'hurensohn', 'caralho', '=porra', 'filhadaputa', 'cazzo', 'stronzo', 'vaffanculo',
  // Russian, Turkish, Indonesian, Arabic, Chinese
  'блядь', 'сука', 'пиздец', 'хуй', 'ебать', 'пидор', 'orospu', 'amk', 'anjing', 'bangsat', 'kontol', 'شرموطة', 'كسمك', '操你妈', '傻逼', '他妈的'
];
const EXTRA_WORDS = process.env.BAD_WORDS ? process.env.BAD_WORDS.split(',') : [];
const UCLS = '\\p{L}\\p{N}\\p{M}';
const prep = t => String(t || '').normalize('NFKC').toLowerCase().replace(/[\u200b-\u200f\u2060\ufeff]/g, '')
  .replace(/[0134578@$!]/g, c => ({ 0: 'o', 1: 'i', 3: 'e', 4: 'a', 5: 's', 7: 't', 8: 'b', '@': 'a', '$': 's', '!': 'i' }[c]))
  .replace(/(.)\1+/gu, '$1');
function badList() {
  return [...WORDS, ...EXTRA_WORDS, ...(db.badWords || [])].map(w => {
    w = String(w).trim().toLowerCase(); const tokOnly = w[0] === '='; if (tokOnly) w = w.slice(1);
    const p = prep(w); return { p, c: p.replace(new RegExp('[^' + UCLS + ']+', 'gu'), ''), tokOnly };
  }).filter(x => x.c.length >= 2);
}
function isAbusive(text) {
  const t = prep(text);
  const tokens = t.split(new RegExp('[^' + UCLS + ']+', 'u')).filter(Boolean);
  const compact = tokens.join('');
  return badList().some(w => {
    if (!/\s/.test(w.p) && tokens.includes(w.p)) return true;
    if (w.tokOnly) return false;
    const ascii = /^[a-z]+$/.test(w.c);
    return w.c.length >= (ascii ? 5 : 3) && compact.includes(w.c);
  });
}
const STRIKE_SEC = [300, 600, 1800]; // 1st offence 5 min, 2nd 10 min, 3rd and later 30 min
function autoBan(name, cid) { // bans ONLY the person who swore: their account + their own browser. Never an IP, never the other person.
  const n = db.strikes[name] = (db.strikes[name] || 0) + 1;
  return banUser(name, [], cid ? [cid] : [], STRIKE_SEC[Math.min(n, 3) - 1], 'Auto-ban: abusive language (offence ' + n + ')', 'AutoMod');
}

// ---- the owner account lives in the code: it is created if missing, and your changed password is kept ----
const OWNER_DEFAULT_PASS = process.env.OWNER_PASS || 'Harsh@8118198!';
function seedAdmin() {
  const k = akey(OWNER_NAME), old = getAcc(k);
  if (!old || process.env.OWNER_RESET === '1') {
    const salt = crypto.randomBytes(16).toString('hex');
    db.accounts[k] = { ...(old || {}), name: OWNER_NAME, salt, hash: hashPw(OWNER_DEFAULT_PASS, salt), admin: true, owner: true, created: (old && old.created) || Date.now() };
    delete db.accounts[k].google;
    console.log(old ? 'Owner password RESET to the default. Remove OWNER_RESET from Render now.' : 'Owner account created: ' + OWNER_NAME);
  } else { old.admin = true; old.owner = true; console.log('Owner account ready: ' + OWNER_NAME); }
  if (!db.users.includes(OWNER_NAME)) db.users.push(OWNER_NAME);
  pMeta();
}

let statT = null;
function stats() { // throttled: many events in a row cause only one broadcast
  if (statT) return;
  statT = setTimeout(() => {
    statT = null;
    A().emit('stats', { online: Object.keys(online).length, totalVisits: db.totalVisits, uniqueVisitors: db.visitors.length });
    A().emit('user_list', Object.keys(online));
    A().emit('directory', directory());
    A().emit('rec_list', [...recording]);
    A().emit('live_list', [...live]);
  }, 700);
}
function social(name) { to(name, 'social', { friends: arr(db.friends, name), requests: arr(db.requests, name) }); }
function dmsFor(name) {
  const out = {};
  Object.keys(db.dms).forEach(k => {
    const [a, b] = k.split('|');
    if (a === name) out[b] = db.dms[k]; else if (b === name) out[a] = db.dms[k];
  });
  return out;
}
const filesFor = n => db.files.filter(f => f.scope !== 'private' || f.uploader === n);
// Performance: photos, voice notes and files are NOT sent in bulk. The app loads them when tapped.
const slim = m => (m.image || m.audio) ? { ...m, image: m.image ? '1' : undefined, audio: m.audio ? '1' : undefined, media: true } : m;
const slimF = f => ({ ...f, url: undefined, big: true });
const lastN = (a, n) => a.slice(-n).map(slim);

// ================= GROUP CALLS (mesh rooms, max 8 people) =================
const rooms = {};
const busy = n => Object.values(rooms).some(r => r.members.has(n));
function missedNote(from, target, video) {
  const m = { user: from, id: uid(), t: Date.now(), timestamp: '', to: target, chat: key(from, target), text: '📞 Missed ' + (video ? 'video ' : '') + 'call from ' + from };
  arr(db.dms, m.chat).push(m); pMsg(m);
  to(from, 'receive_message', m); to(target, 'receive_message', m);
}
function checkRoom(r, reason) {
  if (r.members.size <= 1 && r.pending.size === 0) {
    r.members.forEach(n => to(n, 'call_ended', { room: r.id, reason: reason || 'Everyone else left the call' }));
    delete rooms[r.id];
  }
}
function invite(r, name, from) {
  if (r.members.has(name) || r.pending.has(name)) return;
  if (r.members.size + r.pending.size >= 8) return to(from, 'toast', 'Max 8 people in a call');
  if (!online[name] && !arr(db.push, name).length) { missedNote(from, name, r.video); return to(from, 'toast', name + ' is offline (missed-call note left)'); }
  if (busy(name)) return r.members.forEach(m => to(m, 'call_declined', { room: r.id, name, busy: true }));
  to(name, 'call_invite', { room: r.id, from, video: r.video, members: [...r.members] });
  sendPush(name, { type: 'call', room: r.id, from, video: r.video });
  r.pending.set(name, setTimeout(() => {
    r.pending.delete(name);
    to(name, 'call_cancelled', { room: r.id });
    missedNote(from, name, r.video);
    r.members.forEach(m => to(m, 'call_missed', { room: r.id, name }));
    checkRoom(r, 'No answer');
  }, 45000));
}
function leaveRoom(name, id) {
  const r = rooms[id]; if (!r) return;
  if (r.pending.has(name)) {
    clearTimeout(r.pending.get(name)); r.pending.delete(name);
    r.members.forEach(m => to(m, 'call_declined', { room: id, name }));
  }
  if (r.members.delete(name)) r.members.forEach(m => to(m, 'peer_left', { room: id, name }));
  if (r.members.size === 0) {
    r.pending.forEach((t, n) => { clearTimeout(t); to(n, 'call_cancelled', { room: id }); });
    delete rooms[id]; return;
  }
  checkRoom(r);
}
function leaveAllRooms(name) {
  Object.keys(rooms).forEach(id => { const r = rooms[id]; if (r && (r.members.has(name) || r.pending.has(name))) leaveRoom(name, id); });
}

// ================= TIC-TAC-TOE (online) =================
const ttt = {};
const TL = [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]];
const tttResult = b => {
  for (const l of TL) if (b[l[0]] && b[l[0]] === b[l[1]] && b[l[0]] === b[l[2]]) return { w: b[l[0]], line: l };
  return b.every(Boolean) ? { w: 'draw', line: [] } : null;
};
const emitG = g => {
  const s = { id: g.id, x: g.x, o: g.o, board: g.board, turn: g.turn, over: !!g.over, result: g.result || null };
  to(g.x, 'ttt_state', s); to(g.o, 'ttt_state', s);
};
function tttLeaveAll(name) {
  Object.keys(ttt).forEach(id => {
    const g = ttt[id];
    if (g.x === name || g.o === name) { to(g.x === name ? g.o : g.x, 'ttt_closed', { id }); delete ttt[id]; }
  });
}

// ================= CHESS (online relay) =================
const chess = {};
function chessLeaveAll(name) {
  Object.keys(chess).forEach(id => {
    const g = chess[id];
    if (g.w === name || g.b === name) { to(g.w === name ? g.b : g.w, 'chess_closed', { id }); delete chess[id]; }
  });
}

// ================= ROUTES =================
app.get('/tv', (req, res) => res.redirect('/?tvmode=1'));
app.get('/', (req, res) => { res.set('Cache-Control', 'no-store'); res.sendFile(path.join(__dirname, 'index.html')); });
// ---- app icon + manifest (red Gemini sparkle) so "Install app" / "Save as app" shows the logo, not a letter ----
const ICON = { 192: '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', 512: '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', 180: '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nvvWW2/F02l354drvSillBCN0oRlVYXDH1q5MmFZWaevSPClsg/LPY8KLt8BgoyFGDs9NPT11tafdnWNch7NTTUJKSOa1hgKKW5B7qyVAIzSpdGo5amoMQUgoilEU1mZRink9tO7zy9r7dx/ew9UOj6tRkhzVVWSczadPjlQMySsafFU6lsnTnzu2WcPXbkSCQQigYBr1cCzzFF1BHza1x0Lf2qC5ZjkDDMookZISzx+qLe3ZWSEeqxF3ruFlFzKgDp1mHczCAEAU4jp3wO1KIDH5Z40M7ynsaUMMzaUTuuM6cXYJSqkZISUGUbSsv50//4ft7R8Yu3aTffcYzuZanDCzTANLfze8IUcamAwl+Ac4Olr104NDSGA4Xiw471dOB8b8pghZTZPOz0oZxu8V5kSMyhAwrJ+tanpwcWL91+58tLly92JRETXp7/nSOllRohGyJvXrh27du3BpUv/6MEHNUfxZDvjjytLpA/IU3ACIMzY6319R/v6QpQahEgpFdnHvpchDqTTx/v6goyJMdWYlLiVhd44KQgpDUoPd3T0JJO6WlmmxAz3u8ZYuWE8vm7d/9q37zP3348AKdtWe38Kf86JAVwhouuxQODHly49e+ZMzDBErvGQjkotLnzRHNIZQncoEYALoSNKpTqd8Sr8diktIXJuhssMdeemXWRYSKkR0pVIHL5xI8SYYmoeMxARpORSCgAupVrsMr+6cUkPP2wph9LpSCDw2w888Afve19Q05KWpfgx5b5KKdVAcSGklAFEWwh3YMGTjfIDfpWadAMaeceOJ96tcZlR1LFQW4rGtq96a2haZShUHghUGEa5YVQYRoVhlAcClaGQclwza5+rDQkRUnYnElsWLvzWo49uXrhw0DSLWKlSqpP+ziKobAYWd0Q88EVzuELJm1GbZBMOozx3DlzOFXE0xllNpJQBTTt969aZW7cCzmYzZUuUu35/Tc39tbVpx31FTzsaYopzg7H/tGfP1w4fPt7RESxiPtkTsQW198U3+FbB2Evo3AV74i14RwGUqnWGpmjdHEdncIAgY2du3fqnixcrALjnLRRgAODzANsaG1OcQy4zVAsUwBIiwNhXd+z47HPPjVqWQUhxfAovmxEVV3yyHcVnngpzoRMDyDJjklMHxzDDbW2yTU3kSmMVqAQIUFqBWB4IlOt69sswKggJKqXialXI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', 48: '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' };
const sendIcon = (k) => (req, res) => { res.set({ 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=86400' }); res.send(Buffer.from(ICON[k], 'base64')); };
app.get('/icon-192.png', sendIcon(192));
app.get('/icon-512.png', sendIcon(512));
app.get('/apple-touch-icon.png', sendIcon(180));
app.get('/favicon.png', sendIcon(48));
app.get('/favicon.ico', sendIcon(48));
app.get('/manifest.webmanifest', (req, res) => {
  res.set('Content-Type', 'application/manifest+json');
  res.json({
    id: '/', name: 'HP OS', short_name: 'HP OS', description: 'Chat, calls, games and live screen sharing',
    start_url: '/', scope: '/', display: 'standalone', orientation: 'any',
    background_color: '#0c0202', theme_color: '#0c0202',
    icons: [
      { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
      { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'maskable' },
      { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }
    ]
  });
});
const SW_FALLBACK = `self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(clients.claim()));
self.addEventListener('push',e=>{ let d={}; try{ d=e.data.json(); }catch(x){} const call=d.type==='call';
  const base={renotify:true,silent:false,icon:'/icon-192.png',badge:'/icon-192.png',timestamp:Date.now(),data:d};
  const opts=call?Object.assign(base,{body:d.video?'📹 Incoming video call':'📞 Incoming voice call',tag:'call-'+d.room,requireInteraction:true,vibrate:[500,250,500,250,500,250,500],actions:[{action:'answer',title:'✅ Accept'},{action:'decline',title:'❌ Decline'}]})
    :Object.assign(base,{body:d.body||'',tag:'msg-'+(d.from||''),vibrate:[200,100,200]});
  e.waitUntil(self.registration.showNotification(call?(d.from||'Someone'):(d.title||'HP OS'),opts)); });
self.addEventListener('notificationclick',e=>{ e.notification.close(); const d=e.notification.data||{}, act=e.action; if(d.type==='test') return;
  e.waitUntil((async()=>{ const cs=await clients.matchAll({type:'window',includeUncontrolled:true});
    if(d.type==='call'){ if(act==='decline'){ cs.forEach(c=>c.postMessage({type:'decline',room:d.room})); return; }
      if(cs.length){ cs[0].focus(); cs[0].postMessage({type:'answer',room:d.room,from:d.from,video:d.video}); } else await clients.openWindow('/?answer='+encodeURIComponent(d.room)+'&video='+(d.video?1:0)+'&from='+encodeURIComponent(d.from||'')); return; }
    if(cs.length){ cs[0].focus(); cs[0].postMessage({type:'chat',from:d.from}); } else await clients.openWindow('/?chat='+encodeURIComponent(d.from||'')); })()); });`;
app.get('/sw.js', (req, res) => { res.set('Content-Type', 'application/javascript'); res.set('Cache-Control', 'no-store'); const f = path.join(__dirname, 'sw.js'); if (process.env.CUSTOM_SW === '1' && fs.existsSync(f)) res.sendFile(f); else res.send(SW_FALLBACK); });
app.get('/vapid', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ key: pushOn ? process.env.VAPID_PUBLIC : '' }); });
app.get('/config', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ googleClientId: process.env.GOOGLE_CLIENT_ID || '', persistent: !!M, phone: PHONE_ON }); });
// ---- In-app YouTube + Google search through the official APIs (needs keys on Render) ----
const searchHits = {};
const decodeHtml = s => String(s || '').replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
app.get('/api/search', async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const name = nameOfToken((req.headers.authorization || '').replace(/^Bearer /, ''));
  if (!name) return res.status(401).json({ error: 'Please log in first' });
  const now = Date.now(), h = (searchHits[name] = (searchHits[name] || []).filter(t => now - t < 60000));
  if (h.length >= 20) return res.status(429).json({ error: 'Too many searches. Wait a minute.' });
  h.push(now);
  const q = String(req.query.q || '').slice(0, 200).trim();
  if (!q) return res.json({ items: [] });
  try {
    if (req.query.type === 'yt' || req.query.type === 'shorts') {
      if (!process.env.YOUTUBE_API_KEY) return res.json({ error: 'YouTube search is not set up yet (owner: add YOUTUBE_API_KEY on Render).' });
      const r = await fetch('https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=12&safeSearch=moderate' + (req.query.type === 'shorts' ? '&videoDuration=short' : '') + '&q=' + encodeURIComponent(q + (req.query.type === 'shorts' ? ' #shorts' : '')) + '&key=' + process.env.YOUTUBE_API_KEY);
      const j = await r.json();
      if (j.error) return res.json({ error: j.error.message || 'YouTube API error' });
      return res.json({ items: (j.items || []).filter(i => i.id && i.id.videoId).map(i => ({ id: i.id.videoId, title: decodeHtml(i.snippet.title), by: decodeHtml(i.snippet.channelTitle), img: (i.snippet.thumbnails.medium || {}).url })) });
    }
    if (!process.env.GOOGLE_CSE_KEY || !process.env.GOOGLE_CSE_ID) return res.json({ error: 'Google search is not set up yet (owner: add GOOGLE_CSE_KEY and GOOGLE_CSE_ID on Render).' });
    const r = await fetch('https://www.googleapis.com/customsearch/v1?num=10&key=' + process.env.GOOGLE_CSE_KEY + '&cx=' + process.env.GOOGLE_CSE_ID + '&q=' + encodeURIComponent(q));
    const j = await r.json();
    if (j.error) return res.json({ error: j.error.message || 'Google API error' });
    res.json({ items: (j.items || []).map(i => ({ title: decodeHtml(i.title), link: i.link, snippet: decodeHtml(i.snippet) })) });
  } catch (e) { res.json({ error: 'Search failed. Try again.' }); }
});

app.get('/avatar/:name', (req, res) => {
  const n = findUser(req.params.name), a = n && hasOwn(db.profiles, n) && db.profiles[n].avatar;
  const m = a && /^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=]+)$/.exec(a);
  if (!m) return res.status(404).end();
  res.set({ 'Content-Type': m[1], 'X-Content-Type-Options': 'nosniff', 'Cache-Control': 'public, max-age=31536000, immutable' }); // the URL has ?v=version
  res.send(Buffer.from(m[2], 'base64'));
});
app.get('/backup', (req, res) => {
  if (!process.env.ADMIN_KEY || req.query.key !== process.env.ADMIN_KEY) return res.status(403).send('forbidden');
  res.setHeader('Content-Disposition', 'attachment; filename="db-backup.json"');
  res.json(db);
});
app.post('/restore', express.json({ limit: '200mb' }), async (req, res) => {
  if (!process.env.ADMIN_KEY || req.query.key !== process.env.ADMIN_KEY) return res.status(403).send('forbidden');
  if (!req.body || typeof req.body !== 'object') return res.status(400).send('bad data');
  db = { ...db, ...req.body };
  await fullSave();
  seedAdmin();
  res.send('restored');
});

// ================= SOCKETS =================
// ---- Draw Battle (AI judge) (added) ----
const DB_PROMPTS = [
  'cat', 'dog', 'house', 'pizza', 'rocket', 'tree', 'robot', 'guitar', 'sun', 'car',
  'fish', 'castle', 'phone', 'dragon', 'bridge', 'cake', 'planet', 'bicycle', 'ghost', 'clock',
  'umbrella', 'elephant', 'airplane', 'icecream', 'mountain', 'butterfly', 'snowman', 'crown', 'lighthouse', 'owl',
  'spaceship', 'sword', 'flower', 'penguin', 'pirate ship', 'volcano', 'teapot', 'dinosaur', 'rainbow', 'cactus',
  'hot air balloon', 'apple', 'banana', 'boat', 'train', 'bus', 'helicopter', 'moon', 'star', 'cloud',
  'lightning', 'snail', 'spider', 'bee', 'turtle', 'giraffe', 'lion', 'monkey', 'rabbit', 'horse',
  'cow', 'pig', 'duck', 'frog', 'shark', 'whale', 'octopus', 'crab', 'jellyfish', 'parrot',
  'tent', 'campfire', 'key', 'lamp', 'chair', 'table', 'bed', 'door', 'window', 'bell',
  'balloon', 'heart', 'diamond', 'hat', 'glasses', 'shoe', 'backpack', 'book', 'pencil', 'scissors',
  'hammer', 'ladder', 'anchor', 'trophy', 'drum', 'piano', 'burger', 'donut', 'pineapple', 'watermelon'
]; // 100 things to draw
const battles = Object.create(null), battleOf = Object.create(null);
const b64 = d => String(d).replace(/^data:image\/jpeg;base64,/, '');
// AI for the drawing judge and the quiz. Order: your own local AI (LOCAL_AI_URL, any OpenAI-compatible server such as Ollama), then Groq (GROQ_API_KEY), else the built-in basics.
const AI_ON = () => !!(process.env.GEMINI_API_KEY || process.env.LOCAL_AI_URL || process.env.GROQ_API_KEY || process.env.OPENROUTER_API_KEY || process.env.CEREBRAS_API_KEY);
function aiProviders() {
  const tries = [];
  if (process.env.GEMINI_API_KEY) tries.push({ url: 'https://generativelanguage.googleapis.com/v1beta/openai/chat/completions', key: process.env.GEMINI_API_KEY, models: (process.env.GEMINI_MODEL || 'gemini-2.5-flash').split(',').map(x => x.trim()).filter(Boolean) });
  if (process.env.LOCAL_AI_URL) tries.push({ url: process.env.LOCAL_AI_URL.replace(/\/+$/, '') + '/chat/completions', key: process.env.LOCAL_AI_KEY || '', models: (process.env.LOCAL_AI_MODEL || 'llama3.2').split(',').map(x => x.trim()).filter(Boolean) });
  if (process.env.CEREBRAS_API_KEY) tries.push({ url: 'https://api.cerebras.ai/v1/chat/completions', key: process.env.CEREBRAS_API_KEY, models: [process.env.CEREBRAS_MODEL || 'gpt-oss-120b'] });
  if (process.env.OPENROUTER_API_KEY) tries.push({ url: 'https://openrouter.ai/api/v1/chat/completions', key: process.env.OPENROUTER_API_KEY, models: [process.env.OPENROUTER_MODEL || 'openai/gpt-oss-120b:free', 'openrouter/free'] });
  if (process.env.GROQ_API_KEY) tries.push({ url: 'https://api.groq.com/openai/v1/chat/completions', key: process.env.GROQ_API_KEY, models: [process.env.GROQ_MODEL, 'meta-llama/llama-4-scout-17b-16e-instruct', 'meta-llama/llama-4-maverick-17b-128e-instruct'].filter(Boolean) });
  return tries;
}
async function callAI(text, imgs) {
  const tries = aiProviders();
  for (const t of tries) for (const model of t.models) {
    try {
      const content = [{ type: 'text', text }].concat((imgs || []).map(i => ({ type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + b64(i) } })));
      const r = await fetch(t.url, { method: 'POST', signal: AbortSignal.timeout(60000), headers: { 'content-type': 'application/json', ...(t.key ? { Authorization: 'Bearer ' + t.key } : {}) }, body: JSON.stringify({ model, max_tokens: 1500, temperature: 0.3, messages: [{ role: 'user', content: (imgs && imgs.length) ? content : text }] }) });
      const raw = await r.text(); let j; try { j = JSON.parse(raw); } catch (e) { console.error('AI bad reply from', t.url, 'status', r.status, JSON.stringify(raw.slice(0, 150))); continue; }
      const out = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
      if (out) return out;
      console.error('AI:', t.url.slice(0, 40), model, JSON.stringify(j).slice(0, 200));
    } catch (e) { console.error('AI error:', e.message); }
  }
  return null;
}
// Claude chat for the "Claude AI" app: uses ANTHROPIC_API_KEY if set, else the same Groq / local AI as the games
async function callChat(hist) {
  if (process.env.ANTHROPIC_API_KEY) {
    try {
      const r = await fetch('https://api.anthropic.com/v1/messages', { method: 'POST', signal: AbortSignal.timeout(60000), headers: { 'content-type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' }, body: JSON.stringify({ model: process.env.CLAUDE_MODEL || 'claude-sonnet-5-5', max_tokens: 800, system: 'You are Claude, a helpful AI assistant inside HP OS. Keep replies friendly and concise.', messages: hist }) });
      const j = await r.json(), t = j && j.content && j.content.map(c => c.text || '').join('');
      if (t) return t; console.error('Claude:', JSON.stringify(j).slice(0, 200));
    } catch (e) { console.error('Claude error:', e.message); }
  }
  const flat = hist.map(m => (m.role === 'user' ? 'User: ' : 'Assistant: ') + m.content).join('\n');
  return callAI('You are a helpful assistant called Claude. Continue this chat with one concise reply.\n' + flat + '\nAssistant:');
}
// ================= Gemini chat: many saved chats, files/images in, images out =================
const aiUse = Object.create(null);
const GM_SYS = 'You are Gemini, a friendly and capable AI assistant inside HP OS. Reply in the language the user writes in. Use Markdown (headings, lists, tables, code blocks) when it helps. Give complete, detailed, well-organised answers when the question needs depth (essays, explanations, code and plans can be long). Answer simple questions briefly. If the user asks you to make it short, shorter, brief, or a summary, reply briefly and stay brief until they ask for more. Use the whole conversation as context, including the text of any attached files. When asked to create a PDF or a document, just write the full content in your reply: the app turns your reply into a PDF file automatically, so never say you cannot create PDFs.';
const AIC = {
  async list(u) {
    if (M) return (await M.aichats.find({ u }, { projection: { msgs: 0 } }).sort({ t: -1 }).limit(3000).toArray()).map(c => ({ id: c._id, title: c.title, t: c.t }));
    return Object.values(db.aichats || {}).filter(c => c.u === u).sort((a, b) => b.t - a.t).map(c => ({ id: c.id, title: c.title, t: c.t }));
  },
  async get(u, id) {
    if (M) { const c = await M.aichats.findOne({ _id: id, u }); return c && { id: c._id, u: c.u, title: c.title, t: c.t, msgs: c.msgs || [] }; }
    const c = (db.aichats || {})[id]; return c && c.u === u ? c : null;
  },
  async put(c) {
    c.t = Date.now();
    if (M) await M.aichats.replaceOne({ _id: c.id }, { ...c, _id: c.id }, { upsert: true });
    else { (db.aichats = db.aichats || {})[c.id] = c; pMeta(); }
  },
  async del(u, id) {
    if (M) await M.aichats.deleteOne({ _id: id, u });
    else { const c = (db.aichats || {})[id]; if (c && c.u === u) { delete db.aichats[id]; pMeta(); } }
  }
};
async function callLLM(system, hist, imgs, maxTokens) {
  const retried = {};
  for (const t of aiProviders()) for (const model of t.models) {
    try {
      const msgs = [{ role: 'system', content: system }].concat(hist.map((m, i) => (i === hist.length - 1 && imgs.length)
        ? { role: 'user', content: [{ type: 'text', text: m.content || 'Look at the attached image(s).' }].concat(imgs.map(u => ({ type: 'image_url', image_url: { url: u } }))) } : m));
      const r = await fetch(t.url, { method: 'POST', signal: AbortSignal.timeout(120000), headers: { 'content-type': 'application/json', ...(t.key ? { Authorization: 'Bearer ' + t.key } : {}) }, body: JSON.stringify({ model, max_tokens: maxTokens, temperature: 0.7, messages: msgs }) });
      const raw = await r.text(); let j; try { j = JSON.parse(raw); } catch (e) { console.error('AI bad reply from', t.url, 'status', r.status, JSON.stringify(raw.slice(0, 150))); continue; }
      const out = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
      if (out) return out;
      console.error('AI:', t.url.slice(0, 40), model, JSON.stringify(j).slice(0, 250));
      if ((r.status === 503 || r.status === 429) && !retried[model]) { retried[model] = 1; await new Promise(z => setTimeout(z, 2500)); try { const r2 = await fetch(t.url, { method: 'POST', signal: AbortSignal.timeout(120000), headers: { 'content-type': 'application/json', ...(t.key ? { Authorization: 'Bearer ' + t.key } : {}) }, body: JSON.stringify({ model, max_tokens: maxTokens, temperature: 0.7, messages: msgs }) }); const j2 = await r2.json(); const o2 = j2 && j2.choices && j2.choices[0] && j2.choices[0].message && j2.choices[0].message.content; if (o2) return o2; } catch (e) { console.error('AI retry error:', e.message); } }
    } catch (e) { console.error('AI error:', e.message); }
  }
  return null;
}
// Nano Banana (Gemini image models) through your Gemini key; falls back to a free image service
async function genImage(prompt, ref) {
  const key = process.env.GEMINI_API_KEY || (/generativelanguage/.test(process.env.LOCAL_AI_URL || '') ? process.env.LOCAL_AI_KEY : '');
  if (key) {
    const model = process.env.GEMINI_IMAGE_MODEL || 'gemini-3.1-flash-lite-image';
    try {
      const parts = [{ text: prompt }];
      const m = ref && /^data:(image\/\w+);base64,(.+)$/.exec(ref); if (m) parts.push({ inline_data: { mime_type: m[1], data: m[2] } });
      const r = await fetch('https://generativelanguage.googleapis.com/v1beta/models/' + model + ':generateContent', { method: 'POST', signal: AbortSignal.timeout(120000), headers: { 'content-type': 'application/json', 'x-goog-api-key': key }, body: JSON.stringify({ contents: [{ parts }], generationConfig: { responseModalities: ['IMAGE', 'TEXT'] } }) });
      const j = await r.json();
      const ps = (j && j.candidates && j.candidates[0] && j.candidates[0].content && j.candidates[0].content.parts) || [];
      const im = ps.find(p => p.inlineData || p.inline_data);
      if (im) { const d = im.inlineData || im.inline_data; return { img: 'data:' + (d.mimeType || d.mime_type || 'image/png') + ';base64,' + d.data, note: ps.filter(p => p.text).map(p => p.text).join('') }; }
      console.error('IMG:', model, JSON.stringify(j).slice(0, 300));
    } catch (e) { console.error('IMG error:', e.message); }
  }
  if (ref) return null;
  if (process.env.CF_ACCOUNT_ID && process.env.CF_API_TOKEN) {
    try {
      const r = await fetch('https://api.cloudflare.com/client/v4/accounts/' + process.env.CF_ACCOUNT_ID + '/ai/run/@cf/black-forest-labs/flux-1-schnell', { method: 'POST', signal: AbortSignal.timeout(90000), headers: { 'content-type': 'application/json', Authorization: 'Bearer ' + process.env.CF_API_TOKEN }, body: JSON.stringify({ prompt: prompt.slice(0, 2000), steps: 6 }) });
      const j = await r.json();
      const b = j && j.result && j.result.image;
      if (b) return { img: 'data:image/jpeg;base64,' + b, note: '' };
      console.error('IMG Cloudflare:', r.status, JSON.stringify(j).slice(0, 250));
    } catch (e) { console.error('IMG Cloudflare error:', e.message); }
  }
  try {
    const r = await fetch('https://image.pollinations.ai/prompt/' + encodeURIComponent(prompt.slice(0, 500)) + '?width=1024&height=1024&nologo=true', { signal: AbortSignal.timeout(90000) });
    const ct = r.headers.get('content-type') || '';
    if (r.ok && ct.startsWith('image/')) return { img: 'data:' + ct.split(';')[0] + ';base64,' + Buffer.from(await r.arrayBuffer()).toString('base64'), note: '' };
    console.error('IMG fallback status', r.status);
  } catch (e) { console.error('IMG fallback error:', e.message); }
  return null;
}
const DRAW_DAILY = 100; // painting game: max coins per player per day, then it stops paying
const recentPrompts = Object.create(null);
// picks one of the 100 words; avoids the last 15 words each player already got
function pickPrompt(...names) {
  const avoid = new Set(); names.forEach(n => n && (recentPrompts[n] || []).forEach(w => avoid.add(w)));
  let pool = DB_PROMPTS.filter(w => !avoid.has(w)); if (!pool.length) pool = DB_PROMPTS;
  const w = pool[Math.floor(Math.random() * pool.length)];
  names.forEach(n => { if (!n) return; const l = recentPrompts[n] = (recentPrompts[n] || []).concat([w]); if (l.length > 15) l.shift(); });
  return w;
}
const basicScore = d => !d.img ? 0 : Math.round(Math.min(1, (+d.cov || 0) / .08) * 40 + Math.min(1, (+d.strokes || 0) / 25) * 20);
// SOLO: one drawing, the AI rates it 0..100
async function judgeSolo(prompt, D) {
  const t = await callAI('A player had 60 seconds to draw "' + prompt + '" using a finger or mouse. The image is their drawing. Score it from 0 to 100 for how recognisable it is as "' + prompt + '", plus creativity and effort. A blank page or random scribble must score under 10. Be fair and use the full range. Reply with ONLY this JSON: {"score":number,"comment":"one short friendly sentence"}', [D.img]);
  const m = t && t.match(/\{[\s\S]*\}/);
  if (m) {
    try {
      const j = JSON.parse(m[0]);
      return { score: Math.max(0, Math.min(100, Math.round(+j.score) || 0)), comment: String(j.comment || '').replace(/[<>]/g, '').slice(0, 160), ai: true };
    } catch (e) {}
  }
  return { score: basicScore(D), comment: 'Basic score (the AI judge is not set up yet).', ai: false };
}
// returns { a, b, comment, ai } with a and b = 0..100
async function judgeDrawings(prompt, A, B) {
  const flip = Math.random() < .5, first = flip ? B : A, second = flip ? A : B;
  const t = await callAI('Two players each drew "' + prompt + '" in 60 seconds using a finger or mouse. Image 1 and image 2 are their drawings. Score each from 0 to 100 for how recognisable it is as "' + prompt + '", plus creativity and effort. A blank page or random scribble must score under 10. Be fair and use the full range. Reply with ONLY this JSON: {"s1":number,"s2":number,"comment":"one short friendly sentence"}', [first.img, second.img]);
  const m = t && t.match(/\{[\s\S]*\}/);
  if (m) {
    try {
      const j = JSON.parse(m[0]), c = v => Math.max(0, Math.min(100, Math.round(+v) || 0));
      const s1 = c(j.s1), s2 = c(j.s2);
      return { a: flip ? s2 : s1, b: flip ? s1 : s2, comment: String(j.comment || '').replace(/[<>]/g, '').slice(0, 160), ai: true };
    } catch (e) {}
  }
  // basic fallback (no AI key set): based on how much was drawn
  return { a: basicScore(A), b: basicScore(B), comment: 'Basic score (the AI judge is not set up yet).', ai: false };
}

// ---- Whiteboard + Pictionary rooms (added) ----
const wbRooms = Object.create(null); // room -> { h: [strokes], game: { drawer, word } | null }
const WB_WORDS = ['cat', 'house', 'pizza', 'rocket', 'tree', 'robot', 'guitar', 'sun', 'car', 'fish', 'castle', 'phone', 'dragon', 'bridge', 'cake', 'planet', 'bicycle', 'ghost', 'clock', 'umbrella'];
const wbNum = v => typeof v === 'number' && isFinite(v);

io.on('connection', (socket) => {
  let me = null, counted = false, clientId = null;
  socket.data.ip = ipOf(socket); socket.data.me = null;
  { const b0 = banFor(null, socket.data.ip); if (b0) socket.emit('banned', b0); }

  // ---------- Accounts ----------
  function enter(name) {
    const oldId = online[name];
    const oldSock = oldId && io.sockets.sockets.get(oldId);
    if (oldSock && oldSock.id !== socket.id) { oldSock.emit('kicked'); oldSock.disconnect(true); }
    const wasOnline = !!oldSock;
    me = name; socket.data.me = name; online[me] = socket.id;
    socket.join('authed');
    if (!db.users.includes(me)) db.users.push(me);
    const l = arr(db.ips, me), ip = socket.data.ip;
    if (ip && !l.includes(ip)) db.ips[me] = l.concat([ip]).slice(-5);
    socket.data.cid = clientId;
    if (clientId) { const cl = arr(db.cids, me); if (!cl.includes(clientId)) db.cids[me] = cl.concat([clientId]).slice(-5); }
    const p = prof(me); p.lastSeen = Date.now();
    if (!counted) {
      counted = true; db.totalVisits++; p.visits++;
      if (clientId && !db.visitors.includes(clientId)) db.visitors.push(clientId);
    }
    pMeta();
    if (!wasOnline) arr(db.friends, me).forEach(f => to(f, 'toast', '🟢 ' + me + ' is online'));
    socket.emit('init', { global: lastN(db.global, 50), dms: Object.fromEntries(Object.entries(dmsFor(me)).map(([k, v]) => [k, lastN(v, 50)])), files: filesFor(me).map(slimF) });
    social(me);
    { // messages sent to me while I was offline are now delivered
      const got = {};
      Object.keys(db.dms).forEach(k => { const p2 = k.split('|'); if (p2[0] !== me && p2[1] !== me) return;
        db.dms[k].forEach(m => { if (m.to === me && m.st === 1) { m.st = 2; pMsg(m); (got[m.user] = got[m.user] || []).push(m.id); } }); });
      Object.keys(got).forEach(u => to(u, 'msg_status', { ids: got[u], st: 2 }));
    }
    socket.emit('blocks', arr(db.blocks, me));
    socket.emit('shop_state', shopState()); // NEW
    socket.emit('prefs', prof(me).prefs || null);
    if (isReal(me)) { const rp = prof(me); rp.coins = Math.max(rp.coins, 1e9); } // the real owner has unlimited coins
    dailyLogin(me);
    stats();
  }
  const ok = (name, token) => {
    const b = banFor(name, socket.data.ip, clientId);
    if (b && !isReal(name)) return socket.emit('banned', b);
    socket.emit('auth_ok', { name, token, admin: isAdmin(name), owner: isOwner(name) });
    enter(name);
  };
  const dropMe = () => {
    if (me && online[me] === socket.id) {
      leaveAllRooms(me); tttLeaveAll(me); chessLeaveAll(me); recording.delete(me); stopLive(me); stopWatching(me); arenaLeave(me); doomLeave(me);
      delete online[me]; prof(me).lastSeen = Date.now();
    }
  };

  socket.on('auth_token', ({ token, clientId: c } = {}) => {
    clientId = c || clientId;
    const name = nameOfToken(token);
    if (!name || !getAcc(akey(name))) return socket.emit('auth_fail');
    ok(name, token);
  });
  socket.on('signup', ({ name, password, clientId: c } = {}) => {
    clientId = c || clientId;
    name = cleanName(name); password = String(password || '');
    if (badName(name)) return socket.emit('auth_error', 'Choose another name (2–24 characters)');
    if (password.length < 6) return socket.emit('auth_error', 'Password must be at least 6 characters');
    if (getAcc(akey(name))) return socket.emit('auth_exists', name);
    const display = findUser(name) || name;
    const salt = crypto.randomBytes(16).toString('hex');
    db.accounts[akey(display)] = { name: display, salt, hash: hashPw(password, salt), created: Date.now() };
    ok(display, newSession(display));
  });
  socket.on('login', ({ name, password, clientId: c } = {}) => {
    clientId = c || clientId;
    name = cleanName(name); const k = akey(name);
    if (tooMany(k)) return socket.emit('auth_error', 'Too many tries. Wait 5 minutes.');
    const acc = getAcc(k);
    if (acc && !acc.hash) return socket.emit('auth_error', 'This name uses Google or phone sign-in');
    let good = false;
    if (acc) { try { good = crypto.timingSafeEqual(Buffer.from(hashPw(String(password || ''), acc.salt), 'hex'), Buffer.from(acc.hash, 'hex')); } catch (e) {} }
    if (!good) { addFail(k); return socket.emit('auth_error', 'Wrong name or password'); }
    delete fails[k];
    ok(acc.name, newSession(acc.name));
  });
  socket.on('google_auth', async ({ credential, name, clientId: c } = {}) => {
    clientId = c || clientId;
    if (!gClient) return socket.emit('auth_error', 'Google sign-in is not set up');
    let p;
    try { p = (await gClient.verifyIdToken({ idToken: credential, audience: process.env.GOOGLE_CLIENT_ID })).getPayload(); }
    catch (e) { return socket.emit('auth_error', 'Google sign-in failed'); }
    const acc = Object.values(db.accounts).find(a => a.google === p.sub);
    if (acc) return ok(acc.name, newSession(acc.name));
    name = cleanName(name);
    if (!name) return socket.emit('google_need_name', { suggested: cleanName(p.name || '') });
    if (badName(name)) return socket.emit('google_need_name', { suggested: name, error: 'Choose another name (2–24 characters)' });
    if (getAcc(akey(name))) return socket.emit('google_need_name', { suggested: name, error: 'That name is already taken' });
    const display = findUser(name) || name;
    db.accounts[akey(display)] = { name: display, google: p.sub, email: p.email, created: Date.now() };
    ok(display, newSession(display));
  });
  // ---------- phone sign-in / sign-up with OTP ----------
  socket.on('phone_send', async ({ phone, clientId: c } = {}) => {
    clientId = c || clientId;
    if (!PHONE_ON) return socket.emit('auth_error', 'Phone sign-in is not set up yet');
    const p = normPhone(phone);
    if (!p) return socket.emit('auth_error', 'Enter a valid phone number, for example +919876543210');
    const ipk = 'otpip:' + socket.data.ip;
    if (tooMany(ipk)) return socket.emit('auth_error', 'Too many tries. Wait 5 minutes.');
    if (Date.now() - (otpSent[p] || 0) < 30000) return socket.emit('auth_error', 'Wait 30 seconds before asking for a new code');
    otpSent[p] = Date.now(); addFail(ipk);
    try { await otpSend(p); socket.emit('phone_sent', { phone: p }); }
    catch (e) { socket.emit('auth_error', 'Could not send the code: ' + String(e.message).slice(0, 140)); }
  });
  socket.on('phone_verify', async ({ phone, code, clientId: c } = {}) => {
    clientId = c || clientId;
    if (!PHONE_ON) return socket.emit('auth_error', 'Phone sign-in is not set up yet');
    const p = normPhone(phone), vk = 'otpv:' + p;
    code = String(code || '').replace(/\D/g, '');
    if (!p || !code) return socket.emit('auth_error', 'Enter the code you received');
    if (tooMany(vk)) return socket.emit('auth_error', 'Too many wrong codes. Wait 5 minutes.');
    let good = false; try { good = await otpCheck(p, code); } catch (e) {}
    if (!good) { addFail(vk); return socket.emit('auth_error', 'Wrong or expired code'); }
    delete fails[vk];
    const acc = Object.values(db.accounts).find(a => a.phone === p);
    if (acc) return ok(acc.name, newSession(acc.name));
    phoneOk[p] = { sid: socket.id, exp: Date.now() + 600000 };
    socket.emit('phone_need_name', {});
  });
  socket.on('phone_name', ({ phone, name } = {}) => {
    const p = normPhone(phone), v = phoneOk[p];
    if (!p || !v || v.sid !== socket.id || v.exp < Date.now()) return socket.emit('auth_error', 'Verification expired. Start again.');
    name = cleanName(name);
    if (!name || badName(name)) return socket.emit('phone_need_name', { error: 'Choose another name (2-24 characters)' });
    if (getAcc(akey(name))) return socket.emit('phone_need_name', { error: 'That name is already taken' });
    if (Object.values(db.accounts).some(a => a.phone === p)) return socket.emit('auth_error', 'This number is already registered. Sign in again.');
    const display = findUser(name) || name;
    db.accounts[akey(display)] = { name: display, phone: p, created: Date.now() };
    delete phoneOk[p];
    ok(display, newSession(display));
  });
  socket.on('logout', ({ token, endpoint } = {}) => {
    const h = sha(String(token || ''));
    if (hasOwn(db.sessions, h)) delete db.sessions[h];
    if (me && endpoint) db.push[me] = arr(db.push, me).filter(x => x.endpoint !== endpoint);
    dropMe();
    socket.leave('authed');
    me = null; socket.data.me = null; pMeta(); stats();
  });
  socket.on('push_subscribe', (s) => {
    if (!me || !s || !s.endpoint) return;
    db.push[me] = arr(db.push, me).filter(x => x.endpoint !== s.endpoint).concat([s]).slice(-5);
    pMeta();
  });

  // ---------- Moderation: owner + admins ----------
  const adminPayload = () => {
    db.bans = db.bans.filter(b => !b.until || b.until > Date.now());
    return {
      owner: isOwner(me), real: isReal(me),
      users: db.users.map(n => ({ name: n, role: roleOf(n), real: isReal(n), ips: db.ips[n] || [], online: !!online[n], strikes: db.strikes[n] || 0 })),
      bans: db.bans, words: db.badWords || [], reports: reps().filter(r => r.open)
    };
  };
  socket.on('admin_data', () => { if (isAdmin(me)) socket.emit('admin_data', adminPayload()); });
  socket.on('admin_ban', ({ name, seconds, reason, ipToo } = {}) => {
    if (!isAdmin(me)) return;
    const u = findUser(name);
    if (!u) return socket.emit('toast', 'User not found');
    if (isReal(u)) return socket.emit('toast', 'Nobody can ban the real owner');
    if (isOwner(u) && !isReal(me)) return socket.emit('toast', 'Only the real owner can ban an owner');
    if (u === me) return socket.emit('toast', "You can't ban yourself");
    if (isAdmin(u) && !isOwner(me)) return socket.emit('toast', 'Only the owner can ban an admin');
    const secs = Math.max(0, Math.min(Math.floor(+seconds || 0), 315360000));
    const ips = ipToo ? (db.ips[u] || []).filter(i => i !== socket.data.ip) : [];
    const cids = (db.cids[u] || []).filter(c => c !== socket.data.cid);
    banUser(u, ips, cids, secs, String(reason || 'No reason given').slice(0, 200), me);
    socket.emit('toast', '🔨 Banned ' + u + (secs ? ' for ' + secs + ' s' : ' permanently') + (ips.length ? ' (+ IP)' : ''));
    socket.emit('admin_data', adminPayload());
  });
  socket.on('admin_unban', (id) => { if (!isAdmin(me)) return; db.bans = db.bans.filter(b => b.id !== id); pMeta(); socket.emit('admin_data', adminPayload()); });
  socket.on('set_admin', ({ name, on } = {}) => {
    if (!isOwner(me)) return socket.emit('toast', 'Only the owner can give or remove the admin tag');
    const u = findUser(name), acc = u && getAcc(akey(u));
    if (!acc || isOwner(u)) return socket.emit('toast', 'Cannot change that user');
    acc.admin = !!on; pMeta();
    to(u, 'role_update', { admin: !!on }); to(u, 'toast', on ? '🛡 You are now an admin' : 'Your admin tag was removed');
    stats(); socket.emit('admin_data', adminPayload());
    socket.emit('toast', on ? '🛡 ' + u + ' is now an admin' : u + ' is no longer an admin');
  });
  // ---------- gift coins (real owner: unlimited, everyone else: 100 per gift, 300 per day) ----------
  socket.on('gift_coins', ({ name, amount } = {}) => {
    if (!me) return;
    const u = findUser(name), amt = Math.floor(+amount), real = isReal(me);
    if (!u || u === me) return socket.emit('toast', 'Choose another user to gift');
    if (isBlocked(me, u)) return socket.emit('toast', 'You cannot gift this user');
    if (!(amt >= 1)) return socket.emit('toast', 'Enter a number of coins');
    const p = prof(me), t = prof(u), day = dayStr();
    if (!real) {
      if (amt > 100) return socket.emit('toast', 'You can gift up to 100 coins at a time');
      if (p.giftDay !== day) { p.giftDay = day; p.giftSent = 0; }
      if (p.giftSent + amt > 300) return socket.emit('toast', 'Daily gift limit is 300 coins (' + (300 - p.giftSent) + ' left today)');
      if (p.coins < amt) return socket.emit('toast', 'You do not have enough coins');
      p.coins -= amt; p.giftSent += amt;
    } else if (amt > 1e8) return socket.emit('toast', 'Maximum 100,000,000 coins per gift');
    t.coins += amt; pMeta(); stats();
    socket.emit('coins', { coins: p.coins, gained: 0 }); to(u, 'coins', { coins: t.coins, gained: amt });
    socket.emit('toast', '🎁 Sent ' + amt + ' coins to ' + u); to(u, 'toast', '🎁 ' + me + ' gifted you ' + amt + ' coins!');
    const gm = { user: me, id: uid(), t: Date.now(), timestamp: '', to: u, chat: key(me, u), text: '🎁 Gifted ' + amt + ' coins', gift: amt, st: online[u] ? 2 : 1 };
    arr(db.dms, gm.chat).push(gm); pMsg(gm); to(me, 'receive_message', gm); to(u, 'receive_message', gm);
    socket.emit('gift_sent', { to: u, amount: amt }); to(u, 'gift_received', { from: me, amount: amt });
  });
  // ---------- quiz ----------
  const quizSend = () => { const Q = quizOf[me]; if (!Q) return; const q = Q.qs[Q.i]; socket.emit('quiz_q', { i: Q.i, n: Q.qs.length, q: q.q, a: q.a, ai: Q.ai }); };
  socket.on('quiz_start', async ({ topic } = {}) => {
    if (!me) return;
    const now = Date.now(); if (now - (lastQuiz[me] || 0) < 20000) return socket.emit('toast', 'Wait a few seconds before the next quiz');
    lastQuiz[me] = now;
    topic = String(topic || '').replace(/[^\p{L}\p{N} ,.\-]/gu, '').trim().slice(0, 40);
    if (topic && !isAdmin(me) && isAbusive(topic)) return socket.emit('toast', 'That topic is not allowed');
    socket.emit('quiz_loading', {});
    let qs = null;
    if (AI_ON()) {
      const t = await callAI('Write 5 multiple-choice quiz questions about "' + (topic || 'general knowledge') + '" for a general audience. Reply with ONLY a JSON array and no other text, in exactly this shape: [{"q":"question text","a":["option 1","option 2","option 3","option 4"],"c":0}] where c is the index (0 to 3) of the correct option.', []);
      qs = parseQuiz(t);
    }
    const ai = !!qs; if (!qs) qs = QZ_BANK.slice().sort(() => Math.random() - .5).slice(0, 5);
    quizOf[me] = { qs, i: 0, score: 0, ai, ans: false }; quizSend();
  });
  socket.on('quiz_answer', ({ i, pick } = {}) => {
    const Q = me && quizOf[me]; if (!Q || Q.ans || i !== Q.i) return;
    pick = Math.floor(+pick); const q = Q.qs[Q.i], ok = pick === q.c; if (ok) Q.score++; Q.ans = true;
    const last = Q.i + 1 >= Q.qs.length;
    socket.emit('quiz_r', { ok, correct: q.c, pick, last });
    if (!last) return;
    const p = prof(me), day = dayStr(); if (p.qzDay !== day) { p.qzDay = day; p.qzCoins = 0; }
    let c = Q.score * (Q.ai ? 4 : 2); c = Math.max(0, Math.min(c, QUIZ_DAILY - (p.qzCoins || 0))); p.qzCoins = (p.qzCoins || 0) + c;
    reward(me, 'quiz', c, Q.score >= 4, Q.score * 20);
    socket.emit('quiz_end', { score: Q.score, n: Q.qs.length, coins: c }); delete quizOf[me];
  });
  socket.on('quiz_next', () => { const Q = me && quizOf[me]; if (!Q || !Q.ans) return; Q.i++; Q.ans = false; quizSend(); });
  // ---------- group chats ----------
  socket.on('groups_get', () => { if (me) socket.emit('groups', myGroups(me).map(gInfo)); });
  socket.on('group_create', ({ name, members } = {}) => {
    if (!me) return;
    name = String(name || '').replace(/[<>]/g, '').trim().slice(0, 30);
    if (name.length < 2) return socket.emit('toast', 'Group name is too short');
    if (!isAdmin(me) && isAbusive(name)) return socket.emit('toast', 'That name is not allowed');
    if (Object.values(grp()).filter(g => g.owner === me).length >= 10) return socket.emit('toast', 'You can own up to 10 groups');
    const list = [me];
    String(Array.isArray(members) ? members.join(',') : members || '').split(',').forEach(x => {
      const u = findUser(x.trim()); if (u && !list.includes(u) && !isBlocked(u, me) && list.length < 20) list.push(u);
    });
    const g = { id: uid(), name, owner: me, members: list, msgs: [], t: Date.now() };
    grp()[g.id] = g; pMeta(); gRefresh(g);
    list.forEach(n => { if (n !== me) to(n, 'toast', '👥 ' + me + ' added you to "' + name + '"'); });
    socket.emit('toast', '👥 Group created');
  });
  socket.on('group_open', (id) => {
    const g = grp()[id]; if (!me || !g || !g.members.includes(me)) return;
    socket.emit('group_msgs', { id, name: g.name, owner: g.owner, members: g.members, msgs: g.msgs.slice(-100) });
  });
  socket.on('group_send', ({ id, text } = {}) => {
    const g = grp()[id]; if (!me || !g || !g.members.includes(me)) return;
    text = String(text || '').trim().slice(0, 1000); if (!text) return;
    if (!isAdmin(me) && isAbusive(text)) { autoBan(me, clientId); return; }
    const now = Date.now(); if (now - (lastGM[me] || 0) < 400) return; lastGM[me] = now;
    const m = { id: uid(), user: me, text, t: now }; g.msgs.push(m); if (g.msgs.length > 200) g.msgs.shift();
    pMeta(); questProg(me, 'msg', 1); g.members.forEach(n => to(n, 'group_msg', { gid: id, m }));
  });
  socket.on('group_add', ({ id, name } = {}) => {
    const g = grp()[id]; if (!me || !g || g.owner !== me) return socket.emit('toast', 'Only the group owner can add people');
    const u = findUser(name);
    if (!u) return socket.emit('toast', 'User not found');
    if (g.members.includes(u)) return socket.emit('toast', 'Already in the group');
    if (g.members.length >= 20) return socket.emit('toast', 'Group is full (20 people)');
    if (isBlocked(u, me)) return socket.emit('toast', 'You cannot add this user');
    g.members.push(u); pMeta(); gRefresh(g); to(u, 'toast', '👥 ' + me + ' added you to "' + g.name + '"');
  });
  socket.on('group_leave', (id) => {
    const g = grp()[id]; if (!me || !g || !g.members.includes(me)) return;
    g.members = g.members.filter(n => n !== me);
    if (!g.members.length) delete grp()[id]; else if (g.owner === me) g.owner = g.members[0];
    pMeta(); if (grp()[id]) gRefresh(g, [me]); else socket.emit('groups', myGroups(me).map(gInfo));
  });
  socket.on('group_delete', (id) => {
    const g = grp()[id]; if (!me || !g || (g.owner !== me && !isAdmin(me))) return;
    delete grp()[id]; pMeta(); g.members.forEach(n => to(n, 'groups', myGroups(n).map(gInfo)));
  });
  socket.on('quests_get', () => { if (me) socket.emit('quests', questState(me)); });
  socket.on('report_user', ({ name, reason } = {}) => {
    if (!me) return;
    const u = findUser(name);
    if (!u || u === me) return socket.emit('toast', 'Cannot report that user');
    reason = String(reason || '').trim().slice(0, 300);
    if (reason.length < 3) return socket.emit('toast', 'Please write a short reason');
    const R = reps(), now = Date.now();
    if (R.filter(r => r.by === me && now - r.at < 3600e3).length >= 5) return socket.emit('toast', 'Too many reports. Try again later.');
    if (R.some(r => r.by === me && r.name === u && r.open)) return socket.emit('toast', 'You already reported this user');
    R.push({ id: uid(), by: me, name: u, reason, at: now, open: true });
    if (R.length > 300) R.splice(0, R.length - 300);
    pMeta(); socket.emit('toast', '🚩 Report sent to the admins');
    Object.keys(online).filter(a => isAdmin(a)).forEach(a => to(a, 'toast', '🚩 New report about ' + u));
  });
  socket.on('report_close', (id) => {
    if (!isAdmin(me)) return;
    const r = reps().find(x => x.id === id); if (r) { r.open = false; pMeta(); socket.emit('admin_data', adminPayload()); }
  });
  socket.on('set_owner', ({ name, on } = {}) => { // ONLY the real owner can make (or remove) other owners
    if (!isReal(me)) return socket.emit('toast', 'Only the real owner can give or remove the owner tag');
    const u = findUser(name), acc = u && getAcc(akey(u));
    if (!acc || isReal(u)) return socket.emit('toast', 'Cannot change that user');
    acc.coOwner = !!on; acc.admin = !!on; pMeta();
    to(u, 'role_update', { admin: !!on, owner: !!on }); to(u, 'toast', on ? '👑 You are now an owner' : 'Your owner tag was removed');
    stats(); socket.emit('admin_data', adminPayload());
    socket.emit('toast', on ? '👑 ' + u + ' is now an owner' : u + ' is no longer an owner');
  });
  socket.on('set_coadmin', ({ name, on } = {}) => {
    if (!isOwner(me)) return socket.emit('toast', 'Only an owner can give or remove the co-admin tag');
    const u = findUser(name), acc = u && getAcc(akey(u));
    if (!acc || isOwner(u)) return socket.emit('toast', 'Cannot change that user');
    acc.coAdmin = !!on; if (on) acc.admin = false; pMeta();
    to(u, 'role_update', { admin: !!on }); to(u, 'toast', on ? '🔰 You are now a co-admin' : 'Your co-admin tag was removed');
    stats(); socket.emit('admin_data', adminPayload()); socket.emit('toast', on ? '🔰 ' + u + ' is now a co-admin' : u + ' is no longer a co-admin');
  });
  socket.on('admin_word', ({ word, del } = {}) => {
    if (!isAdmin(me)) return;
    word = String(word || '').trim().toLowerCase().slice(0, 40);
    if (word.length < 2) return;
    if (del) db.badWords = (db.badWords || []).filter(x => x !== word);
    else if (!(db.badWords || []).includes(word) && (db.badWords || []).length < 500) db.badWords.push(word);
    pMeta(); socket.emit('admin_data', adminPayload());
  });
  socket.on('change_password', ({ oldPw, newPw } = {}) => {
    if (!me) return;
    const k = akey(me), acc = getAcc(k);
    if (!acc || !acc.hash) return socket.emit('toast', 'This account signs in with Google: it has no password');
    if (tooMany('pw:' + k)) return socket.emit('toast', 'Too many tries. Wait 5 minutes.');
    let good = false;
    try { good = crypto.timingSafeEqual(Buffer.from(hashPw(String(oldPw || ''), acc.salt), 'hex'), Buffer.from(acc.hash, 'hex')); } catch (e) {}
    if (!good) { addFail('pw:' + k); return socket.emit('toast', 'Current password is wrong'); }
    if (String(newPw || '').length < 6) return socket.emit('toast', 'New password must be at least 6 characters');
    acc.salt = crypto.randomBytes(16).toString('hex'); acc.hash = hashPw(String(newPw), acc.salt); pMeta();
    socket.emit('toast', '✅ Password changed. It is kept even if the server restarts.');
  });

  // ---------- Screen recording status ----------
  socket.on('rec_status', (on) => {
    if (!me) return;
    on ? recording.add(me) : recording.delete(me);
    A().emit('rec_list', [...recording]);
  });

  const cleanCode = c => String(c || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 16) || 'main';
  // ---------- Doom multiplayer ----------
  socket.on('doom_join', ({ code } = {}) => {
    if (!me) return; code = cleanCode(code); doomLeave(me);
    const s = doomRooms[code] = doomRooms[code] || new Set();
    if (s.size >= 8) return socket.emit('toast', 'That room is full (8 players)');
    s.add(me); doomOf[me] = code; s.forEach(n => to(n, 'doom_players', [...s]));
  });
  socket.on('doom_leave', () => { if (me) doomLeave(me); });
  socket.on('d_state', (d = {}) => {
    const c = doomOf[me], s = c && doomRooms[c]; if (!s) return;
    const o = { from: me, x: num(d.x), y: num(d.y), a: num(d.a), hp: Math.max(0, Math.min(200, num(d.hp))), d: d.d ? 1 : 0, w: d.w ? 1 : 0 };
    s.forEach(n => { if (n !== me) to(n, 'd_state', o); });
  });
  socket.on('d_event', (d = {}) => {
    const c = doomOf[me], s = c && doomRooms[c]; if (!s || !['shot', 'hit', 'kill'].includes(d.t)) return;
    const o = { t: d.t, from: me, x: num(d.x), y: num(d.y), a: num(d.a), to: String(d.to || '').slice(0, 32), dmg: Math.max(0, Math.min(60, num(d.dmg))), victim: String(d.victim || '').slice(0, 32), by: String(d.by || '').slice(0, 32), w: d.w ? 1 : 0 };
    s.forEach(n => { if (n !== me) to(n, 'd_event', o); });
  });
  // ---------- Arena (2D shooter) ----------
  const num = v => (Number.isFinite(+v) ? +v : 0);
  socket.on('arena_join', ({ code } = {}) => {
    if (!me) return;
    code = String(code || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 16) || 'main';
    arenaLeave(me);
    const set = arenaRooms[code] = arenaRooms[code] || new Set();
    if (set.size >= 8) return socket.emit('toast', 'That room is full (8 players)');
    set.add(me); arenaOf[me] = code;
    set.forEach(n => to(n, 'arena_players', [...set]));
  });
  socket.on('arena_leave', () => { if (me) arenaLeave(me); });
  socket.on('a_state', (d = {}) => {
    const c = arenaOf[me]; if (!me || !c || !arenaRooms[c]) return;
    const o = { from: me, x: num(d.x), y: num(d.y), a: num(d.a), hp: Math.max(0, Math.min(200, num(d.hp))), d: d.d ? 1 : 0 };
    arenaRooms[c].forEach(n => { if (n !== me) to(n, 'a_state', o); });
  });
  socket.on('a_event', (d = {}) => {
    const c = arenaOf[me]; if (!me || !c || !arenaRooms[c]) return;
    if (!['shot', 'hit', 'kill'].includes(d.t)) return;
    const o = { t: d.t, from: me, x: num(d.x), y: num(d.y), a: num(d.a), to: String(d.to || '').slice(0, 32), dmg: Math.max(0, Math.min(40, num(d.dmg))), victim: String(d.victim || '').slice(0, 32), by: String(d.by || '').slice(0, 32) };
    if (d.t === 'kill') o.by = o.by === me ? me : o.by; // victim reports who fragged them
    arenaRooms[c].forEach(n => { if (n !== me) to(n, 'a_event', o); });
  });

  // ---------- LIVE screen sharing (WebRTC, never saved on the server) ----------
  socket.on('live_status', (on) => {
    if (!me) return;
    if (on) { live.add(me); liveViews[me] = liveViews[me] || new Set(); A().emit('live_list', [...live]); }
    else stopLive(me);
  });
  socket.on('live_watch', ({ name } = {}) => {
    const st = findUser(name);
    if (!me || !st || st === me || !live.has(st) || isBlocked(me, st)) return socket.emit('toast', 'That live stream is not available');
    stopWatching(me);
    if (liveViews[st].size >= MAX_VIEWERS) return socket.emit('toast', 'This live is full (' + MAX_VIEWERS + ' viewers)');
    liveViews[st].add(me);
    to(st, 'live_viewer', { name: me });
  });
  socket.on('live_unwatch', () => { if (me) stopWatching(me); });
  socket.on('live_signal', ({ to: target, data } = {}) => {
    const t = findUser(target);
    if (!me || !t) return;
    const ok = (live.has(me) && liveViews[me] && liveViews[me].has(t)) || (live.has(t) && liveViews[t] && liveViews[t].has(me));
    if (ok) to(t, 'live_signal', { from: me, data });
  });

  // ---------- TV sign-in by QR / code ----------
  socket.on('tv_new', () => {
    const now = Date.now();
    Object.keys(tvCodes).forEach(k => { if (tvCodes[k].exp < now || tvCodes[k].sid === socket.id) delete tvCodes[k]; });
    if (Object.keys(tvCodes).length > 1000) return;
    const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; let c;
    do { c = Array.from({ length: 6 }, () => A[crypto.randomInt(A.length)]).join(''); } while (tvCodes[c]);
    tvCodes[c] = { sid: socket.id, exp: now + 10 * 60000 }; socket.emit('tv_code', { code: c });
  });
  socket.on('tv_approve', ({ code } = {}) => {
    if (!me) return;
    socket._tvBad = socket._tvBad || 0; if (socket._tvBad >= 8) return socket.emit('tv_result', { ok: false, err: 'Too many wrong codes. Try again later.' });
    const c = String(code || '').toUpperCase().replace(/[^A-Z0-9]/g, ''), e = tvCodes[c];
    if (!e || e.exp < Date.now()) { socket._tvBad++; return socket.emit('tv_result', { ok: false, err: 'Wrong or expired code' }); }
    const tok = crypto.randomBytes(32).toString('hex'); db.sessions[sha(tok)] = me; pMeta(); delete tvCodes[c]; // a separate session, so your phone stays signed in
    io.to(e.sid).emit('tv_token', { token: tok, name: me }); socket.emit('tv_result', { ok: true });
  });

  // ---------- Claude AI chat ----------
  socket.on('ai_chat', async ({ msgs } = {}) => {
    if (!me || !Array.isArray(msgs)) return;
    const now = Date.now(); if (socket._aiT && now - socket._aiT < 1500) return socket.emit('ai_reply', { err: 'Slow down a little' }); socket._aiT = now;
    const hist = msgs.slice(-12).map(m => ({ role: m && m.role === 'assistant' ? 'assistant' : 'user', content: String((m && m.content) || '').slice(0, 2000) })).filter(m => m.content);
    while (hist.length && hist[0].role !== 'user') hist.shift();
    if (!hist.length) return;
    const out = await callChat(hist);
    socket.emit('ai_reply', out ? { text: String(out).slice(0, 4000) } : { err: AI_ON() || process.env.ANTHROPIC_API_KEY ? 'The AI is set up but did not answer (busy, wrong model name, or bad key). Owner: check Render Logs for lines starting with "AI:".' : 'The AI is not set up on this server yet. The owner must set CEREBRAS_API_KEY, OPENROUTER_API_KEY, GROQ_API_KEY or ANTHROPIC_API_KEY.' });
  });

  // ---------- Gemini chat (saved chats per user) ----------
  const aiErr = (cid, err) => socket.emit('ai_reply', { cid, err });
  socket.on('ai_list', async () => { if (me) socket.emit('ai_chats', await AIC.list(me).catch(() => [])); });
  socket.on('ai_open', async ({ id } = {}) => { if (!me) return; const c = await AIC.get(me, String(id)).catch(() => null); socket.emit('ai_chat_data', c && { id: c.id, title: c.title, msgs: c.msgs.map(({ ftxt, ...m }) => m) }); });
  socket.on('ai_rename', async ({ id, title } = {}) => { if (!me) return; const c = await AIC.get(me, String(id)); if (c) { c.title = String(title || '').trim().slice(0, 60) || c.title; await AIC.put(c); socket.emit('ai_chats', await AIC.list(me)); } });
  socket.on('ai_del', async ({ id } = {}) => { if (!me) return; await AIC.del(me, String(id)); socket.emit('ai_chats', await AIC.list(me)); });
  socket.on('ai_img_save', async ({ id, idx, data } = {}) => {
    if (!me || typeof data !== 'string' || data.length > 450000 || !/^data:image\//.test(data)) return;
    const c = await AIC.get(me, String(id)), m = c && c.msgs[idx | 0];
    if (m && m.role === 'assistant' && !m.img) { m.img = data; await AIC.put(c); }
  });
  socket.on('ai_send', async (d = {}) => {
    if (!me) return;
    const now = Date.now(); if (socket._aiT && now - socket._aiT < 1500) return aiErr(d.id, 'Slow down a little');
    socket._aiT = now;
    const text = String(d.text || '').trim().slice(0, 20000);
    const okImg = (u, max) => typeof u === 'string' && /^data:image\/(jpeg|png|webp);base64,/.test(u) && u.length < max;
    const imgs = (Array.isArray(d.imgs) ? d.imgs : []).slice(0, 4).filter(u => okImg(u, 6e6));
    const th = (Array.isArray(d.th) ? d.th : []).slice(0, 4).filter(u => okImg(u, 60000));
    const files = (Array.isArray(d.files) ? d.files : []).slice(0, 6).map(f => ({ name: String((f && f.name) || 'file').slice(0, 80), text: String((f && f.text) || '').slice(0, 60000) }));
    if (!text && !imgs.length && !files.length) return;
    const day = new Date().toISOString().slice(0, 10) + me; aiUse[day] = (aiUse[day] || 0) + 1;
    if (aiUse[day] > (+process.env.AI_DAILY || 300)) return aiErr(d.id, 'Daily limit reached. Try again tomorrow.');
    let chat = d.id ? await AIC.get(me, String(d.id)) : null;
    const isNew = !chat;
    if (!chat) chat = { id: uid(), u: me, title: '', msgs: [], t: Date.now() };
    chat.msgs.push({ role: 'user', content: text, files: files.map(f => f.name), ftxt: files.map(f => '[File: ' + f.name + ']\n' + f.text).join('\n\n'), th });
    if (!chat.title) chat.title = (text || (files[0] && files[0].name) || 'New chat').replace(/\s+/g, ' ').slice(0, 40);
    const editPhoto = imgs.length && text && /\b(make|put|place|stand|standing|add|remove|replace|change|turn|edit|convert|transform|paint|background|behind|next to|near|beside|wearing|dress|style)\b/i.test(text) && !/\b(what|who|why|how|describe|explain|read|summari[sz]e|translate|extract|tell|identify|solve)\b|\?/i.test(text);
    const wantImg = d.mode === 'image' || editPhoto || /\b(generate|create|make|draw|design|produce)\b[^.\n]{0,25}\b(image|picture|photo|logo|poster|wallpaper|illustration|drawing)\b/i.test(text);
    let reply = null;
    if (wantImg && text) {
      const g = await genImage(text, imgs[0]);
      if (g) reply = { text: g.note || '', img: g.img };
    } else {
      const hist = []; let total = 0;
      for (let i = chat.msgs.length - 1; i >= 0 && hist.length < 40; i--) {
        const m = chat.msgs[i];
        const content = m.role === 'user' ? (m.content + (m.ftxt ? '\n\n' + m.ftxt : '')).trim() : (m.content || (m.img ? '[Generated an image]' : ''));
        if (!content && !(i === chat.msgs.length - 1 && imgs.length)) continue;
        total += content.length; if (total > 150000 && hist.length) break;
        hist.unshift({ role: m.role === 'assistant' ? 'assistant' : 'user', content });
      }
      while (hist.length && hist[0].role !== 'user') hist.shift();
      const out = await callLLM(GM_SYS, hist, imgs, 8192);
      if (out) reply = { text: String(out) };
    }
    if (!reply) {
      chat.msgs.pop();
      if (chat.msgs.length) await AIC.put(chat);
      return aiErr(isNew ? null : chat.id, wantImg && text ? (imgs.length ? 'Editing your photo needs Google Nano Banana, and your Gemini key has no image quota (it needs billing turned on in Google AI Studio). Free image services can only create new pictures from text, not edit your photo, so I did not make a fake one.' : 'Could not create the image (the image service is busy or not enabled for your key). Check Render Logs for lines starting with "IMG".') : (AI_ON() ? 'The AI is set up but did not answer (busy, wrong model name, or bad key). Owner: check Render Logs for lines starting with "AI:".' : 'The AI is not set up on this server yet.'));
    }
    chat.msgs.push({ role: 'assistant', content: reply.text, img: '' });
    await AIC.put(chat);
    socket.emit('ai_reply', { cid: chat.id, title: chat.title, text: reply.text, img: reply.img || '', idx: chat.msgs.length - 1 });
    socket.emit('ai_chats', await AIC.list(me).catch(() => []));
  });

  // ---------- AI Studio: Education / Creativity / Coding / Images (uses your free AI keys) ----------
  const ST_SYS = {
    edu: 'You are a patient, encouraging teacher. Explain step by step in simple language with short examples, then check understanding with one quick question. For maths and science show the working. Reply in the language the user writes in. Use Markdown.',
    creative: 'You are a creative writing partner for stories, poems, song ideas, jokes, names, scripts and brainstorming. Be imaginative and vivid, and keep everything suitable for all ages. Reply in the language the user writes in. Use Markdown.',
    voice: 'You are a friendly voice assistant like Google Assistant. Your answer will be read aloud, so reply in one to three short, natural spoken sentences. Plain text only: no Markdown, no lists, no emojis, no links. Reply in the language the user speaks. If you do not know something, say so briefly.',
    code: 'You are an expert programmer and code reviewer. Give correct, working, well-commented code in fenced Markdown code blocks, explain briefly what it does and how to run it, and point out bugs or security problems you notice. Reply in the language the user writes in.'
  };
  socket.on('studio_ask', async (d = {}) => {
    if (!me) return;
    const mode = ['edu', 'creative', 'code', 'image', 'voice'].includes(d.mode) ? d.mode : 'edu';
    const out = r => socket.emit('studio_reply', Object.assign({ mode }, r));
    const now = Date.now(); if (socket._stT && now - socket._stT < 2000) return out({ err: 'Slow down a little' }); socket._stT = now;
    const text = String(d.text || '').trim().slice(0, 6000); if (!text) return;
    const day = 's' + new Date().toISOString().slice(0, 10) + me; aiUse[day] = (aiUse[day] || 0) + 1;
    if (aiUse[day] > (+process.env.AI_DAILY || 300)) return out({ err: 'Daily limit reached. Try again tomorrow.' });
    if (mode === 'image') {
      const g = await genImage(text, null);
      return out(g ? { text: '', img: g.img } : { err: 'Could not create the image right now. Try a shorter description, or try again in a minute.' });
    }
    const hist = (Array.isArray(d.hist) ? d.hist : []).slice(-10).map(m => ({ role: m && m.role === 'assistant' ? 'assistant' : 'user', content: String((m && m.content) || '').slice(0, 4000) })).filter(m => m.content);
    hist.push({ role: 'user', content: text });
    while (hist.length && hist[0].role !== 'user') hist.shift();
    const reply = await callLLM(ST_SYS[mode], hist, [], mode === 'voice' ? 400 : 4096);
    out(reply ? { text: String(reply) } : { err: AI_ON() ? 'The AI did not answer (busy or a wrong key). Try again in a moment.' : 'The AI is not set up on this server yet.' });
  });

  // ---------- Profile phone number + dial pad ----------
  const dialOf = x => String(x || '').replace(/[^\d]/g, '');
  const dialFind = d => d && Object.keys(db.profiles).find(n => db.profiles[n] && db.profiles[n].dial === d);
  socket.on('set_dial', ({ num } = {}) => {
    if (!me) return;
    const d = dialOf(num);
    if (d && (d.length < 5 || d.length > 15)) return socket.emit('toast', 'Number must be 5–15 digits');
    const taken = dialFind(d); if (d && taken && taken !== me) return socket.emit('toast', 'That number is already used by someone else');
    prof(me).dial = d; pMeta(); socket.emit('dial_me', { num: d }); socket.emit('toast', d ? '📱 Number saved' : 'Number removed');
  });
  socket.on('get_dial', () => { if (me) socket.emit('dial_me', { num: prof(me).dial || '' }); });
  socket.on('dial_lookup', ({ num } = {}) => {
    if (!me) return;
    const d = dialOf(num), n = dialFind(d);
    socket.emit('dial_result', { num: d, name: n ? (findUser(n) || n) : null, online: !!(n && online[n]) });
  });

  // ---------- Ludo relay (friends only) ----------
  socket.on('ludo_send', ({ to: names, data } = {}) => {
    if (!me || !data || !Array.isArray(names)) return;
    names.slice(0, 4).forEach(n => { n = findUser(n); if (n && n !== me && arr(db.friends, me).includes(n)) to(n, 'ludo_msg', { from: me, data }); });
  });

  // ---------- Messages ----------
  socket.on('send_message', (data) => {
    if (!me || !data) return;
    if (data.text && !isAdmin(me) && isAbusive(data.text)) { autoBan(me, clientId); return; }
    if ((data.image && data.image.length > MAX_BYTES) || (data.audio && data.audio.length > MAX_BYTES)) return socket.emit('toast', 'File too big (max 10 MB)');
    if (data.image && !/^data:image\/(png|jpe?g|webp|gif);base64,[A-Za-z0-9+/=]+$/.test(data.image)) return socket.emit('toast', 'Only PNG, JPG, WebP or GIF pictures are allowed');
    if (data.audio && !/^data:audio\/(webm|ogg|mp4|mpeg|wav|aac|x-m4a)(;codecs=[\w.,-]+)?;base64,[A-Za-z0-9+/=]+$/.test(data.audio)) return socket.emit('toast', 'Unsupported voice note format');
    questProg(me, 'msg', 1);
    const m = { user: me, id: uid(), t: Date.now(), timestamp: data.timestamp || '', text: data.text ? String(data.text).slice(0, 2000) : undefined, image: data.image, audio: data.audio };
    if (data.to) {
      const target = findUser(data.to);
      if (!target) return socket.emit('toast', 'User not found');
      if (isBlocked(me, target)) return socket.emit('toast', '🚫 You cannot message this user');
      m.to = target; m.chat = key(me, target);
      if (target !== me) m.st = online[target] ? 2 : 1; // 1 sent, 2 delivered, 3 seen
      arr(db.dms, m.chat).push(m);
      to(me, 'receive_message', m);
      if (target !== me) {
        to(target, 'receive_message', m);
        sendPush(target, { type: 'msg', from: me, title: me, body: m.text || (m.image ? '📷 Photo' : '🎙️ Voice note') });
      }
    } else {
      m.chat = 'global';
      db.global.push(m);
      if (db.global.length > 300) pMsgDel(db.global.shift().id);
      socket.to('authed').emit('receive_message', slim(m)); socket.emit('receive_message', m);
    }
    pMsg(m);
  });
  socket.on('delete_message', ({ id, chat } = {}) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, String(chat))];
    if (!list) return;
    const i = list.findIndex(m => m.id === id && m.user === me);
    if (i < 0) return;
    list.splice(i, 1); pMsgDel(id);
    if (chat === 'global') A().emit('message_deleted', { id });
    else { to(me, 'message_deleted', { id }); to(chat, 'message_deleted', { id }); }
  });
  socket.on('chat_seen', ({ chat: w } = {}) => {
    if (!me || !w) return;
    const other = findUser(w); if (!other) return;
    const list = db.dms[key(me, other)]; if (!list) return;
    const ids = [];
    list.forEach(m => { if (m.to === me && m.st && m.st < 3) { m.st = 3; pMsg(m); ids.push(m.id); } });
    if (ids.length) to(other, 'msg_status', { ids, st: 3 });
  });
  socket.on('typing_status', (d = {}) => {
    if (!me) return;
    if (d.to) to(d.to, 'user_typing', { user: me, to: d.to, isTyping: d.isTyping });
    else socket.to('authed').emit('user_typing', { user: me, isTyping: d.isTyping });
  });

  // ---------- Profile & blocking ----------
  socket.on('set_profile', (d = {}) => {
    if (!me) return;
    if (!isAdmin(me) && isAbusive(String(d.status || '') + ' ' + String(d.bio || ''))) return socket.emit('toast', 'That text is not allowed');
    const p = prof(me);
    p.status = String(d.status || '').replace(/[<>]/g, '').slice(0, 60);
    p.emoji = Array.from(String(d.emoji || '😀')).slice(0, 2).join('');
    if (typeof d.bio === 'string') p.bio = d.bio.replace(/[<>]/g, '').slice(0, 160);
    if (d.avatar === '') p.avatar = '';
    else if (typeof d.avatar === 'string' && d.avatar.length <= 40000 && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(d.avatar)) { p.avatar = d.avatar; p.avV = Date.now(); }
    pMeta(); stats();
  });
  socket.on('block_user', (u) => {
    u = findUser(u);
    if (!me || !u || u === me) return;
    if (!arr(db.blocks, me).includes(u)) db.blocks[me].push(u);
    db.friends[me] = arr(db.friends, me).filter(x => x !== u);
    db.friends[u] = arr(db.friends, u).filter(x => x !== me);
    db.requests[me] = arr(db.requests, me).filter(x => x !== u);
    db.requests[u] = arr(db.requests, u).filter(x => x !== me);
    pMeta(); socket.emit('blocks', db.blocks[me]); social(me); social(u);
  });
  socket.on('unblock_user', (u) => {
    if (!me) return;
    db.blocks[me] = arr(db.blocks, me).filter(x => x !== u);
    pMeta(); socket.emit('blocks', db.blocks[me]);
  });

  // ---------- Friends ----------
  function acceptFriend(a, b) {
    db.requests[a] = arr(db.requests, a).filter(x => x !== b);
    db.requests[b] = arr(db.requests, b).filter(x => x !== a);
    if (!arr(db.friends, a).includes(b)) db.friends[a].push(b);
    if (!arr(db.friends, b).includes(a)) db.friends[b].push(a);
    pMeta(); social(a); social(b);
    to(a, 'toast', '🤝 You are now friends with ' + b);
    to(b, 'toast', '🤝 ' + a + ' accepted your friend request');
  }
  socket.on('friend_request', (input) => {
    if (!me) return;
    const target = findUser(input);
    if (!target) return socket.emit('toast', 'User "' + String(input).trim() + '" not found');
    if (target === me) return socket.emit('toast', "You can't add yourself");
    if (isBlocked(me, target)) return socket.emit('toast', '🚫 Not possible');
    if (arr(db.friends, me).includes(target)) return socket.emit('toast', 'Already friends with ' + target);
    if (arr(db.requests, me).includes(target)) return acceptFriend(me, target);
    if (arr(db.requests, target).includes(me)) return socket.emit('toast', 'Request already sent');
    db.requests[target].push(me);
    pMeta(); social(target);
    to(target, 'toast', '👋 ' + me + ' sent you a friend request');
    sendPush(target, { type: 'msg', from: me, title: 'Friend request', body: me + ' wants to be your friend' });
    socket.emit('toast', 'Friend request sent to ' + target);
  });
  socket.on('friend_accept', (from) => { if (me && arr(db.requests, me).includes(from)) acceptFriend(me, from); });
  socket.on('friend_decline', (from) => { if (!me) return; db.requests[me] = arr(db.requests, me).filter(x => x !== from); pMeta(); social(me); });
  socket.on('friend_remove', (other) => {
    if (!me) return;
    db.friends[me] = arr(db.friends, me).filter(x => x !== other);
    db.friends[other] = arr(db.friends, other).filter(x => x !== me);
    pMeta(); social(me); social(other);
  });

  // ---------- Drive: personal (private) + shared ----------
  socket.on('upload_cloud_file', (f) => {
    if (!me || !f || !f.url) return;
    if (f.url.length > MAX_BYTES) return socket.emit('toast', 'File too big (max 10 MB)');
    const scope = f.shared ? 'shared' : 'private';
    if (scope === 'private' && db.files.filter(x => x.scope === 'private' && x.uploader === me).length >= 10)
      return socket.emit('toast', 'Personal drive is full (10 files). Delete some first.');
    const file = { id: uid(), t: Date.now(), name: String(f.name || 'file').slice(0, 100), type: String(f.type || 'application/octet-stream'), url: f.url, uploader: me, scope };
    db.files.push(file); pFile(file);
    if (scope === 'shared') { socket.to('authed').emit('new_cloud_file', slimF(file)); socket.emit('new_cloud_file', file); }
    else { socket.emit('new_cloud_file', file); socket.emit('toast', '🔒 Saved to your personal drive'); }
  });
  socket.on('share_file', (id) => {
    const f = db.files.find(x => x.id === id && x.uploader === me && x.scope === 'private');
    if (!me || !f) return;
    const c = { ...f, id: uid(), t: Date.now(), scope: 'shared' };
    db.files.push(c); pFile(c); socket.to('authed').emit('new_cloud_file', slimF(c)); socket.emit('new_cloud_file', slimF(c));
    socket.emit('toast', '🌍 Shared with everyone');
  });
  socket.on('delete_cloud_file', (id) => {
    const f = db.files.find(x => x.id === id);
    if (!me || !f || f.uploader !== me) return;
    db.files = db.files.filter(x => x.id !== id); pFileDel(id);
    if (f.scope === 'private') socket.emit('cloud_file_deleted', id); else A().emit('cloud_file_deleted', id);
  });

  // ---------- Lazy media + notification test ----------
  socket.on('get_media', ({ id, chat } = {}) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, String(chat))];
    const m = list && list.find(x => x.id === id);
    if (m) socket.emit('media_data', { id, image: m.image, audio: m.audio });
  });
  socket.on('get_file', (id) => {
    const f = db.files.find(x => x.id === id && (x.scope !== 'private' || x.uploader === me));
    if (me && f) socket.emit('file_data', { id, url: f.url });
  });
  socket.on('push_test', () => {
    if (!me) return;
    const subs = arr(db.push, me).length;
    socket.emit('push_info', { vapid: pushOn, subs });
    if (pushOn && subs) sendPush(me, { type: 'msg', from: 'HP OS', title: '🔔 Test notification', body: 'Notifications work on this device!' });
  });

  // ---------- Group calls ----------
  socket.on('call_start', ({ invitees, isVideo } = {}) => {
    if (!me) return;
    if (busy(me)) return socket.emit('toast', 'You are already in a call');
    const names = [...new Set((Array.isArray(invitees) ? invitees : []).map(findUser).filter(n => n && n !== me && !isBlocked(me, n)))].slice(0, 7);
    if (!names.length) return socket.emit('call_failed', { reason: 'Pick someone to call' });
    const id = uid();
    const r = rooms[id] = { id, host: me, video: !!isVideo, members: new Set([me]), pending: new Map() };
    socket.emit('call_room', { room: id });
    names.forEach(n => invite(r, n, me));
    checkRoom(r, 'Nobody could be reached. A missed-call note was left.');
  });
  socket.on('call_join', ({ room } = {}) => {
    const r = rooms[room];
    if (!me) return;
    if (!r || !r.pending.has(me)) return socket.emit('call_ended', { room, reason: 'The call has ended' });
    clearTimeout(r.pending.get(me)); r.pending.delete(me);
    const others = [...r.members];
    r.members.add(me);
    socket.emit('call_joined', { room, peers: others });
    others.forEach(n => to(n, 'peer_joined', { room, name: me }));
  });
  socket.on('call_add', ({ room, names } = {}) => {
    const r = rooms[room];
    if (!me || !r || !r.members.has(me)) return;
    (Array.isArray(names) ? names : []).map(findUser)
      .filter(n => n && ![...r.members].some(m => isBlocked(m, n)))
      .forEach(n => invite(r, n, me));
  });
  socket.on('call_leave', ({ room } = {}) => { if (me) leaveRoom(me, room); });
  socket.on('rtc_signal', ({ room, to: target, data } = {}) => {
    const r = rooms[room];
    if (!me || !r || !r.members.has(me) || !r.members.has(target)) return;
    to(target, 'rtc_signal', { room, from: me, data });
  });

  // ---------- Tic-Tac-Toe (online) ----------
  socket.on('ttt_invite', (name) => {
    const t = findUser(name);
    if (!me || !t || t === me || !online[t] || isBlocked(me, t)) return socket.emit('toast', 'Player not available');
    const id = uid();
    ttt[id] = { id, x: me, o: t, board: Array(9).fill(''), turn: 'X', started: false };
    socket.emit('toast', 'Invite sent to ' + t);
    to(t, 'ttt_invited', { id, from: me });
    setTimeout(() => { if (ttt[id] && !ttt[id].started) delete ttt[id]; }, 60000);
  });
  socket.on('ttt_accept', (id) => { const g = ttt[id]; if (!g || g.o !== me) return; g.started = true; emitG(g); });
  socket.on('ttt_decline', (id) => { const g = ttt[id]; if (!g || g.o !== me) return; to(g.x, 'toast', me + ' declined your game'); delete ttt[id]; });
  socket.on('ttt_move', ({ id, i } = {}) => {
    const g = ttt[id];
    if (!g || !g.started || g.over) return;
    const sym = g.x === me ? 'X' : g.o === me ? 'O' : null;
    if (!sym || sym !== g.turn || !(i >= 0 && i < 9) || g.board[i]) return;
    g.board[i] = sym;
    const r = tttResult(g.board);
    if (r) {
      g.over = true; g.result = r;
      if (r.w !== 'draw') reward(r.w === 'X' ? g.x : g.o, 'xo', 20, true, 0);
    } else g.turn = sym === 'X' ? 'O' : 'X';
    emitG(g);
  });
  socket.on('ttt_again', (id) => {
    const g = ttt[id];
    if (!g || !g.over || (g.x !== me && g.o !== me)) return;
    [g.x, g.o] = [g.o, g.x];
    g.board = Array(9).fill(''); g.turn = 'X'; g.over = false; g.result = null;
    emitG(g);
  });
  socket.on('ttt_leave', (id) => {
    const g = ttt[id];
    if (!g || (g.x !== me && g.o !== me)) return;
    const other = g.x === me ? g.o : g.x;
    to(other, 'toast', me + ' left the game'); to(other, 'ttt_closed', { id });
    delete ttt[id];
  });

  // ---------- Chess (online) ----------
  socket.on('chess_invite', (name) => {
    const t = findUser(name);
    if (!me || !t || t === me || !online[t] || isBlocked(me, t)) return socket.emit('toast', 'Player not available');
    const id = uid(), white = Math.random() < 0.5 ? me : t;
    chess[id] = { id, from: me, to: t, w: white, b: white === me ? t : me, turn: 'w', started: false, over: false };
    socket.emit('toast', 'Chess invite sent to ' + t);
    to(t, 'chess_invited', { id, from: me });
    setTimeout(() => { if (chess[id] && !chess[id].started) delete chess[id]; }, 60000);
  });
  socket.on('chess_accept', (id) => {
    const g = chess[id]; if (!g || g.to !== me || g.started) return;
    g.started = true; const s = { id, w: g.w, b: g.b };
    to(g.w, 'chess_start', s); to(g.b, 'chess_start', s);
  });
  socket.on('chess_decline', (id) => { const g = chess[id]; if (!g || g.to !== me) return; to(g.from, 'toast', me + ' declined your chess game'); delete chess[id]; });
  socket.on('chess_move', ({ id, mv } = {}) => {
    const g = chess[id]; if (!g || !g.started || g.over || !mv) return;
    const side = g.w === me ? 'w' : g.b === me ? 'b' : null;
    if (!side || side !== g.turn) return;
    const f = +mv.f, t = +mv.t;
    if (!(f >= 0 && f < 64 && t >= 0 && t < 64)) return;
    g.turn = side === 'w' ? 'b' : 'w';
    to(side === 'w' ? g.b : g.w, 'chess_move', { id, mv: { f, t, pr: ['q', 'r', 'b', 'n'].includes(mv.pr) ? mv.pr : undefined } });
  });
  socket.on('chess_over', ({ id, win } = {}) => { // sent only by the winner (checkmate)
    const g = chess[id]; if (!g || g.over || (g.w !== me && g.b !== me)) return;
    g.over = true;
    if (win) reward(me, 'chess', 30, true, 0);
  });
  socket.on('chess_resign', (id) => {
    const g = chess[id]; if (!g || g.over || (g.w !== me && g.b !== me)) return;
    g.over = true; const o = g.w === me ? g.b : g.w;
    to(o, 'chess_end', { id, reason: me + ' resigned' });
    reward(o, 'chess', 30, true, 0);
  });
  socket.on('chess_leave', (id) => {
    const g = chess[id]; if (!g || (g.w !== me && g.b !== me)) return;
    to(g.w === me ? g.b : g.w, 'chess_closed', { id }); delete chess[id];
  });

  // ---------- Coins & leaderboard ----------
  const GAME_COINS = { xo: 8, arena: 25, doom: 25, gd: 40, snake: 10, g2048: 30, ludo: 20, memory: 15, c4: 20, flap: 20, snl: 20, race: 20, maze: 20, hill: 20 }; // coins for a win in the games that run on the player's device
  const lastGR = {};
  socket.on('game_reward', ({ game, win, score, lvl } = {}) => {
    if (!me || !hasOwn(GAME_COINS, game)) return;
    score = Math.max(0, Math.min(Math.floor(+score) || 0, 1e6));
    const p = prof(me), k = me + '|' + game, now = Date.now(), day = new Date().toISOString().slice(0, 10);
    let c = 0;
    if (now - (lastGR[k] || 0) >= 15000) { // at most one reward per game every 15 seconds
      lastGR[k] = now;
      if (p.day !== day) { p.day = day; p.dayCoins = 0; }
      c = win ? GAME_COINS[game] + (game === 'gd' ? Math.min(9, Math.max(0, Math.floor(+lvl) || 0)) * 10 : 0) : Math.min(10, 2 + Math.floor(score / 10));
      c = Math.max(0, Math.min(c, 400 - (p.dayCoins || 0))); // daily limit: 400 coins from these games
      p.dayCoins = (p.dayCoins || 0) + c;
    }
    reward(me, game, c, !!win && c > 0, score);
  });
  socket.on('get_leaderboard', (by) => {
    if (!me) return;
    by = ['coins', 'wins', 'gd', 'doom', 'arena'].includes(by) ? by : 'coins';
    const val = p => (by === 'coins' ? p.coins : by === 'wins' ? p.wins : p.best[by]) || 0;
    const all = db.users.map(n => { const p = prof(n); return { name: n, emoji: p.emoji, av: p.avatar ? (p.avV || 1) : 0, v: val(p) }; })
      .filter(r => r.v > 0).sort((a, b) => b.v - a.v);
    const i = all.findIndex(r => r.name === me);
    socket.emit('leaderboard', { by, rows: all.slice(0, 20), mine: { v: val(prof(me)), rank: i < 0 ? 0 : i + 1 } });
  });

  // ---------- NEW: Shop (ranks, name colours, badges - paid with coins) ----------
  const shopState = () => { const p = prof(me); return { coins: p.coins, own: p.own, rank: p.rank, color: p.color, badge: p.badge, items: SHOP }; };
  socket.on('shop_get', () => { if (me) socket.emit('shop_state', shopState()); });
  socket.on('shop_buy', (id) => {
    if (!me) return;
    id = String(id || '');
    if (!hasOwn(SHOP, id)) return socket.emit('toast', 'That item does not exist');
    const it = SHOP[id], p = prof(me);
    if (p.own.includes(id)) return socket.emit('toast', 'You already own that');
    if (!isReal(me) && p.coins < it.price) return socket.emit('toast', '🪙 You need ' + (it.price - p.coins) + ' more coins');
    if (!isReal(me)) p.coins -= it.price; p.own.push(id); p[it.cat] = id; // a new purchase is equipped right away
    pMeta(); stats();
    socket.emit('coins', { coins: p.coins, gained: 0 });
    socket.emit('shop_state', shopState());
    socket.emit('toast', '🛒 Bought ' + it.icon + ' ' + it.name);
  });
  socket.on('shop_equip', ({ cat, id } = {}) => { // id '' = take it off
    if (!me || !SHOP_SLOTS.includes(cat)) return;
    const p = prof(me); id = String(id || '');
    if (id && !(hasOwn(SHOP, id) && SHOP[id].cat === cat && p.own.includes(id))) return;
    p[cat] = id; pMeta(); stats();
    socket.emit('shop_state', shopState());
  });

  // ---------- Draw Battle (added) ----------
  function dbEnd(B, why) {
    if (!B) return;
    clearTimeout(B.t1); clearTimeout(B.t2);
    delete battles[B.id]; if (battleOf[B.a] === B.id) delete battleOf[B.a]; if (battleOf[B.b] === B.id) delete battleOf[B.b];
    if (why) { to(B.a, 'db_cancel', why); to(B.b, 'db_cancel', why); }
  }
  async function dbJudgeSolo(B) {
    to(B.a, 'db_judging', {});
    const D = B.subs[B.a] || {};
    if (!D.img) return dbEnd(B, 'You did not submit a drawing.');
    const r = await judgeSolo(B.prompt, D);
    const p = prof(B.a), day = new Date().toISOString().slice(0, 10);
    if (p.dbDay !== day) { p.dbDay = day; p.dbCoins = 0; }
    let c = Math.round(r.score / 2); if (!r.ai) c = Math.min(c, 15); // solo pays half of a battle; basic scores pay less
    c = Math.max(0, Math.min(c, DRAW_DAILY - (p.dbCoins || 0))); p.dbCoins = (p.dbCoins || 0) + c;
    const win = r.score >= 60;
    reward(B.a, 'draw', c, win, r.score);
    to(B.a, 'db_result', { prompt: B.prompt, solo: true, players: [{ name: B.a, img: D.img, score: r.score, coins: c }], winner: win ? B.a : '', comment: r.comment, ai: r.ai });
    dbEnd(B);
  }
  async function dbJudge(B) {
    if (!B || B.state !== 'drawing') return;
    B.state = 'judging'; clearTimeout(B.t2);
    if (B.solo) return dbJudgeSolo(B);
    to(B.a, 'db_judging', {}); to(B.b, 'db_judging', {});
    const A = B.subs[B.a] || {}, Bb = B.subs[B.b] || {};
    if (!A.img && !Bb.img) return dbEnd(B, 'Nobody submitted a drawing.');
    const r = await judgeDrawings(B.prompt, A, Bb);
    const day = new Date().toISOString().slice(0, 10), out = {};
    [[B.a, r.a, A], [B.b, r.b, Bb]].forEach(([n, sc, d]) => {
      const p = prof(n); if (p.dbDay !== day) { p.dbDay = day; p.dbCoins = 0; }
      let c = d.img ? sc : 0; if (!r.ai) c = Math.min(c, 30); // basic scores pay less
      c = Math.max(0, Math.min(c, DRAW_DAILY - (p.dbCoins || 0))); p.dbCoins = (p.dbCoins || 0) + c;
      out[n] = { name: n, img: d.img || '', score: sc, coins: c };
    });
    const win = r.a === r.b ? '' : (r.a > r.b ? B.a : B.b);
    [B.a, B.b].forEach(n => reward(n, 'draw', out[n].coins, win === n, out[n].score));
    pMeta();
    const res = { prompt: B.prompt, players: [out[B.a], out[B.b]], winner: win, comment: r.comment, ai: r.ai };
    to(B.a, 'db_result', res); to(B.b, 'db_result', res);
    dbEnd(B);
  }
  socket.on('db_invite', (name) => {
    const f = findUser(name);
    if (!me || !f || f === me || !arr(db.friends, me).includes(f) || !online[f]) return socket.emit('toast', 'Your friend must be online');
    if (battleOf[me] || battleOf[f]) return socket.emit('toast', 'Someone is already in a Draw Battle');
    const B = { id: uid(), a: me, b: f, state: 'invited', subs: {} };
    battles[B.id] = B; battleOf[me] = B.id; battleOf[f] = B.id;
    B.t1 = setTimeout(() => { if (B.state === 'invited') dbEnd(B, 'Invite expired.'); }, 30000);
    to(f, 'db_invite', { id: B.id, from: me }); socket.emit('toast', '⚔️ Challenge sent to ' + f);
  });
  socket.on('db_solo', () => { // SINGLE PLAYER: the server gives you a thing to draw, the AI rates it
    if (!me) return;
    if (battleOf[me]) return socket.emit('toast', 'You are already in a Draw Battle');
    const B = { id: uid(), a: me, b: null, solo: true, state: 'drawing', subs: {}, prompt: pickPrompt(me) };
    battles[B.id] = B; battleOf[me] = B.id;
    socket.emit('db_start', { id: B.id, prompt: B.prompt, secs: 60, a: me, b: null, solo: true });
    B.t2 = setTimeout(() => dbJudge(B), 75000);
  });
  socket.on('db_decline', (id) => { const B = battles[id]; if (B && B.b === me && B.state === 'invited') dbEnd(B, me + ' declined.'); });
  socket.on('db_accept', async (id) => {
    const B = battles[id];
    if (!B || B.b !== me || B.state !== 'invited') return;
    B.state = 'starting'; clearTimeout(B.t1);
    B.prompt = pickPrompt(B.a, B.b);
    if (!battles[id]) return;
    B.state = 'drawing';
    const d = { id, prompt: B.prompt, secs: 60, a: B.a, b: B.b };
    to(B.a, 'db_start', d); to(B.b, 'db_start', d);
    B.t2 = setTimeout(() => dbJudge(B), 75000);
  });
  socket.on('db_submit', (d) => {
    const B = d && battles[d.id];
    if (!me || !B || B.state !== 'drawing' || (B.a !== me && B.b !== me) || B.subs[me]) return;
    if (typeof d.img !== 'string' || d.img.length > 150000 || !/^data:image\/jpeg;base64,[A-Za-z0-9+/=]+$/.test(d.img)) return;
    B.subs[me] = { img: d.img, cov: Math.max(0, Math.min(1, +d.cov || 0)), strokes: Math.max(0, Math.min(500, +d.strokes || 0)) };
    if (B.solo || (B.subs[B.a] && B.subs[B.b])) dbJudge(B);
  });
  socket.on('db_leave', () => { const B = battles[battleOf[me]]; if (B && B.state !== 'judging') dbEnd(B, B.solo ? '' : me + ' left the battle.'); });

  // ---------- Whiteboard + Pictionary + look & feel sync (added) ----------
  let wbRoom = null;
  const wbCh = () => 'wb:' + wbRoom;
  function wbLeave() {
    if (!wbRoom) return;
    const R = wbRooms[wbRoom];
    if (R && R.game && R.game.drawer === me) { R.game = null; io.to(wbCh()).emit('pic_ended'); io.to(wbCh()).emit('pic_msg', 'Drawer left. Game ended.'); }
    socket.leave(wbCh());
    const mem = io.sockets.adapter.rooms.get(wbCh());
    if (!mem || !mem.size) delete wbRooms[wbRoom];
    wbRoom = null;
  }
  socket.on('wb_join', (name) => {
    if (!me) return;
    wbLeave();
    wbRoom = String(name || 'lobby').replace(/[^\w -]/g, '').slice(0, 30) || 'lobby';
    socket.join(wbCh());
    const R = wbRooms[wbRoom] || (wbRooms[wbRoom] = { h: [], game: null });
    socket.emit('wb_history', R.h);
    if (R.game) socket.emit('pic_started', { drawer: R.game.drawer });
  });
  socket.on('wb_leave', wbLeave);
  socket.on('wb_stroke', (d) => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R || !d) return;
    if (R.game && R.game.drawer !== me) return;
    if (![d.x0, d.y0, d.x1, d.y1, d.s].every(wbNum)) return;
    const s = { x0: d.x0, y0: d.y0, x1: d.x1, y1: d.y1, s: Math.min(60, Math.max(1, d.s)), c: /^#[0-9a-fA-F]{6}$/.test(String(d.c)) ? d.c : '#ffffff' };
    R.h.push(s); if (R.h.length > 6000) R.h.shift();
    socket.to(wbCh()).emit('wb_stroke', s);
  });
  socket.on('wb_clear', () => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R || (R.game && R.game.drawer !== me)) return;
    R.h = []; io.to(wbCh()).emit('wb_clear');
  });
  socket.on('pic_start', () => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R) return;
    R.h = []; io.to(wbCh()).emit('wb_clear');
    R.game = { drawer: me, word: WB_WORDS[Math.floor(Math.random() * WB_WORDS.length)] };
    socket.emit('pic_word', R.game.word);
    io.to(wbCh()).emit('pic_started', { drawer: me });
  });
  socket.on('pic_guess', (g) => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R || !R.game || R.game.drawer === me) return;
    g = String(g || '').trim().toLowerCase().slice(0, 40);
    if (!g || (!isAdmin(me) && isAbusive(g))) return;
    if (g === R.game.word) { io.to(wbCh()).emit('pic_msg', '🎉 ' + me + ' guessed it! The word was "' + R.game.word + '".'); R.game = null; io.to(wbCh()).emit('pic_ended'); }
    else io.to(wbCh()).emit('pic_msg', me + ': ' + g);
  });
  socket.on('pic_stop', () => {
    const R = wbRoom && wbRooms[wbRoom];
    if (me && R && R.game && R.game.drawer === me) { R.game = null; io.to(wbCh()).emit('pic_ended'); io.to(wbCh()).emit('pic_msg', 'Game stopped.'); }
  });
  socket.on('prefs_save', (d) => {
    if (!me || !d || typeof d !== 'object') return;
    const custom = typeof d.custom === 'string' && d.custom.length <= 150000 && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(d.custom) ? d.custom : null;
    prof(me).prefs = {
      wall: String(d.wall || 'Default').slice(0, 30),
      accent: /^#[0-9a-fA-F]{6}$/.test(String(d.accent)) ? d.accent : null,
      glow: Math.min(40, Math.max(0, +d.glow || 0)), crt: !!d.crt, pixel: !!d.pixel, custom
    };
    pMeta();
  });

  socket.on('disconnect', () => {
    wbLeave();
    { const B = battles[battleOf[me]]; if (me && B && B.state !== 'judging') dbEnd(B, me + ' disconnected.'); }
    if (me && online[me] === socket.id) { dropMe(); pMeta(); stats(); }
  });
});

const PORT = process.env.PORT || 3000;
(async () => {
  await initStorage();
  seedAdmin();
  server.listen(PORT, () => console.log('HP OS server running on port ' + PORT));
})();
