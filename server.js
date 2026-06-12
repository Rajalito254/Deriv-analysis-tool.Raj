// ================================================================
//  DERIV DIGITS PRO — Production Trading Server
//  Over 2 / Under 7 Analysis & Auto-Trading Engine
//  WebSocket ↔ Deriv API | REST API ↔ Frontend
// ================================================================

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');

// ================================================================
// CONFIGURATION
// ================================================================
const CONFIG = {
    port: process.env.PORT || 3000,
    derivWsUrl: 'wss://ws.derivws.com/websockets/v3',
    appId: process.env.DERIV_APP_ID || '1089', // 1089 = test; get your own at app.deriv.com
    defaultSymbol: 'R_50',
    maxConcurrentTrades: 3,
    tradeCooldownMs: 3000,
    dailyLossLimit: parseFloat(process.env.DAILY_LOSS_LIMIT) || 50,
    dailyProfitTarget: parseFloat(process.env.DAILY_PROFIT_TARGET) || 80,
    maxStake: parseFloat(process.env.MAX_STAKE) || 50,
    minStake: parseFloat(process.env.MIN_STAKE) || 1,
};

// ================================================================
// STATE
// ================================================================
const state = {
    derivWs: null,
    connected: false,
    authenticated: false,
    apiToken: null,
    tickHistory: {},      // symbol -> [{digit, epoch, quote}]
    activeTrades: {},     // contractId -> trade details
    tradeLog: [],         // all completed trades
    dailyPnL: 0,
    dailyTrades: 0,
    isTrading: false,
    subscribers: new Set(), // frontend WebSocket clients
    lastSignals: {},      // symbol -> signal object
};

// ================================================================
// SQLITE DATABASE (Trade Journal)
// ================================================================
let db;
try {
    const Database = require('better-sqlite3');
    db = new Database(path.join(__dirname, 'trades.db'));
    db.exec(`
        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            contract_id TEXT UNIQUE,
            symbol TEXT,
            contract_type TEXT,
            barrier TEXT,
            entry_digit INTEGER,
            exit_digit INTEGER,
            stake REAL,
            payout REAL,
            profit REAL,
            status TEXT,
            entry_price REAL,
            exit_price REAL,
            entry_time INTEGER,
            exit_time INTEGER,
            signal_confidence REAL,
            signal_reason TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            pnl REAL DEFAULT 0,
            volume REAL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS analysis_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timestamp INTEGER,
            signal TEXT,
            confidence REAL,
            over2_pct REAL,
            under7_pct REAL,
            details TEXT
        );
    `);
    console.log('[DB] SQLite database initialized');
} catch (e) {
    console.warn('[DB] SQLite unavailable, using in-memory logging:', e.message);
    db = null;
}

// ================================================================
// DERIV WEBSOCKET CONNECTION
// ================================================================
function connectDeriv() {
    if (state.derivWs && state.derivWs.readyState === WebSocket.OPEN) return;

    const url = `${CONFIG.derivWsUrl}?app_id=${CONFIG.appId}`;
    console.log(`[Deriv WS] Connecting to ${url}...`);

    state.derivWs = new WebSocket(url);

    state.derivWs.on('open', () => {
        console.log('[Deriv WS] Connected');
        state.connected = true;
        broadcast({ type: 'connection', status: 'connected' });

        // Authenticate if token available
        if (state.apiToken) {
            authenticate(state.apiToken);
        }
    });

    state.derivWs.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw);
            handleDerivMessage(msg);
        } catch (e) {
            console.error('[Deriv WS] Parse error:', e.message);
        }
    });

    state.derivWs.on('error', (err) => {
        console.error('[Deriv WS] Error:', err.message);
    });

    state.derivWs.on('close', (code, reason) => {
        console.log(`[Deriv WS] Disconnected (${code}): ${reason}`);
        state.connected = false;
        state.authenticated = false;
        broadcast({ type: 'connection', status: 'disconnected' });

        // Auto-reconnect after 3s
        setTimeout(connectDeriv, 3000);
    });
}

function authenticate(token) {
    if (!state.derivWs || state.derivWs.readyState !== WebSocket.OPEN) return;
    
    state.apiToken = token;
    state.derivWs.send(JSON.stringify({ authorize: token }));
    console.log('[Deriv WS] Sent authorization');
}

function subscribeTicks(symbol) {
    if (!state.derivWs || state.derivWs.readyState !== WebSocket.OPEN) return;
    
    if (!state.tickHistory[symbol]) {
        state.tickHistory[symbol] = [];
    }

    state.derivWs.send(JSON.stringify({
        ticks: symbol,
        subscribe: 1
    }));
    console.log(`[Deriv WS] Subscribed to ticks: ${symbol}`);
}

function unsubscribeTicks(symbol) {
    if (!state.derivWs || state.derivWs.readyState !== WebSocket.OPEN) return;
    state.derivWs.send(JSON.stringify({
        forget: symbol
    }));
}

// ================================================================
// DERIV MESSAGE HANDLER
// ================================================================
function handleDerivMessage(msg) {
    // Authorization response
    if (msg.msg_type === 'authorize') {
        if (msg.authorize) {
            state.authenticated = true;
            const loginid = msg.authorize.loginid;
            const balance = msg.authorize.balance;
            console.log(`[Deriv WS] Authenticated: ${loginid} | Balance: ${balance}`);
            broadcast({
                type: 'auth',
                status: 'authenticated',
                loginid,
                balance,
                currency: msg.authorize.currency,
                email: msg.authorize.email
            });

            // Auto-subscribe to default symbol
            subscribeTicks(CONFIG.defaultSymbol);
        } else {
            console.error('[Deriv WS] Authorization failed');
            broadcast({ type: 'auth', status: 'failed', error: msg.error?.message });
        }
        return;
    }

    // Tick data
    if (msg.msg_type === 'tick' && msg.tick) {
        const { symbol, quote, epoch } = msg.tick;
        const priceStr = String(quote);
        const lastDigit = parseInt(priceStr.replace('.', '').slice(-1)) || 0;

        if (!state.tickHistory[symbol]) state.tickHistory[symbol] = [];
        state.tickHistory[symbol].push({ digit: lastDigit, epoch, quote });

        // Keep last 1000 ticks
        if (state.tickHistory[symbol].length > 1000) {
            state.tickHistory[symbol] = state.tickHistory[symbol].slice(-1000);
        }

        // Run analysis & broadcast
        const signal = runAnalysis(symbol);
        broadcast({
            type: 'tick',
            symbol,
            digit: lastDigit,
            quote,
            epoch,
            history: state.tickHistory[symbol].slice(-60).map(t => t.digit),
            signal
        });

        // Auto-trade check
        if (state.isTrading && signal && signal.action !== 'WAIT' && signal.confidence >= 60) {
            checkAndExecuteTrade(symbol, signal);
        }
        return;
    }

    // Proposal (price quote)
    if (msg.msg_type === 'proposal' && msg.proposal) {
        broadcast({
            type: 'proposal',
            proposal: msg.proposal,
            id: msg.proposal.id,
            payout: msg.proposal.payout,
            ask_price: msg.proposal.ask_price,
            longcode: msg.proposal.longcode
        });
        return;
    }

    // Buy confirmation
    if (msg.msg_type === 'buy' && msg.buy) {
        const contractId = msg.buy.contract_id;
        const trade = state.pendingTrade;
        
        if (trade) {
            const tradeRecord = {
                id: uuidv4(),
                contract_id: contractId,
                symbol: trade.symbol,
                contract_type: trade.contract_type,
                barrier: trade.barrier,
                entry_digit: trade.entryDigit,
                stake: msg.buy.buy_price || trade.stake,
                payout: msg.buy.payout,
                profit: 0,
                status: 'open',
                entry_price: msg.buy.spot || 0,
                entry_time: msg.buy.purchase_time || Math.floor(Date.now() / 1000),
                signal_confidence: trade.confidence,
                signal_reason: trade.reason
            };

            state.activeTrades[contractId] = tradeRecord;
            delete state.pendingTrade;

            console.log(`[Trade] Opened ${trade.contract_type} ${trade.symbol} ID=${contractId} stake=${tradeRecord.stake}`);
            broadcast({
                type: 'trade_opened',
                trade: tradeRecord,
                balance: msg.buy.balance_after
            });

            // Subscribe to contract status
            state.derivWs.send(JSON.stringify({
                proposal_open_contract: 1,
                contract_id: contractId,
                subscribe: 1
            }));
        }
        return;
    }

    // Contract status update
    if (msg.msg_type === 'proposal_open_contract' && msg.proposal_open_contract) {
        const contract = msg.proposal_open_contract;
        const contractId = contract.contract_id;
        
        if (state.activeTrades[contractId]) {
            const trade = state.activeTrades[contractId];
            trade.status = contract.status; // 'open', 'won', 'lost', 'sold'
            
            if (contract.status === 'won' || contract.status === 'lost') {
                trade.profit = contract.profit || 0;
                trade.exit_digit = contract.exit_tick ? 
                    parseInt(String(contract.exit_tick).slice(-1)) : null;
                trade.exit_price = contract.exit_spot || 0;
                trade.exit_time = contract.expiry_time || Math.floor(Date.now() / 1000);

                // Update daily stats
                state.dailyPnL += trade.profit;
                state.dailyTrades++;
                state.tradeLog.push(trade);
                
                // Save to DB
                saveTradeToDB(trade);

                console.log(`[Trade] Closed ${trade.contract_type} ID=${contractId} ${trade.status} PnL=${trade.profit}`);
                broadcast({
                    type: 'trade_closed',
                    trade,
                    dailyPnL: state.dailyPnL,
                    dailyTrades: state.dailyTrades
                });

                // Remove from active
                delete state.activeTrades[contractId];
            }

            broadcast({ type: 'contract_update', contract });
        }
        return;
    }

    // Error handling
    if (msg.error) {
        console.error(`[Deriv WS] Error:`, msg.error);
        broadcast({ type: 'error', error: msg.error, request: msg.echo_req });
        return;
    }

    // Balance update
    if (msg.msg_type === 'balance') {
        broadcast({ type: 'balance', balance: msg.balance });
    }
}

// ================================================================
// ANALYSIS ENGINE (identical logic to frontend but runs server-side)
// ================================================================
function runAnalysis(symbol) {
    const history = state.tickHistory[symbol];
    if (!history || history.length < 25) return null;

    const windowSize = Math.min(100, history.length);
    const recent = history.slice(-windowSize);
    const digits = recent.map(t => t.digit);
    const total = digits.length;

    // Frequency
    const freq = Array(10).fill(0);
    digits.forEach(d => freq[d]++);

    // Over 2 analysis
    const over2Hits = freq.slice(3).reduce((a, b) => a + b, 0);
    const over2Pct = (over2Hits / total) * 100;
    const over2Conf = Math.max(0, Math.min(100, 70 - Math.abs(over2Pct - 70) + (over2Pct > 70 ? 5 : 0)));

    // Under 7 analysis
    const under7Hits = freq.slice(0, 7).reduce((a, b) => a + b, 0);
    const under7Pct = (under7Hits / total) * 100;
    const under7Conf = Math.max(0, Math.min(100, 70 - Math.abs(under7Pct - 70) + (under7Pct > 70 ? 5 : 0)));

    const lastDigit = digits[digits.length - 1];
    let action = 'WAIT';
    let prediction = '';
    let confidence = 0;
    let recommendation = '';

    if (over2Conf >= 55 && (lastDigit === 0 || lastDigit === 1)) {
        action = 'OVER';
        prediction = `Over 2 (barrier=2)`;
        confidence = over2Conf;
        recommendation = `Digit ${lastDigit} touched. Over 2 bias ${over2Conf.toFixed(1)}%. Enter now.`;
    } else if (under7Conf >= 55 && (lastDigit === 8 || lastDigit === 9)) {
        action = 'UNDER';
        prediction = `Under 7 (barrier=7)`;
        confidence = under7Conf;
        recommendation = `Digit ${lastDigit} touched. Under 7 bias ${under7Conf.toFixed(1)}%. Enter now.`;
    } else if (over2Conf >= 70 && over2Conf > under7Conf + 10) {
        action = 'OVER';
        prediction = `Over 2 (barrier=2)`;
        confidence = over2Conf;
        recommendation = `Strong Over 2 bias. Wait for 0 or 1 touch.`;
    } else if (under7Conf >= 70 && under7Conf > over2Conf + 10) {
        action = 'UNDER';
        prediction = `Under 7 (barrier=7)`;
        confidence = under7Conf;
        recommendation = `Strong Under 7 bias. Wait for 8 or 9 touch.`;
    } else if (over2Conf >= 55 || under7Conf >= 55) {
        if (over2Conf >= under7Conf) {
            action = 'OVER';
            prediction = `Over 2 (barrier=2)`;
            confidence = over2Conf;
            recommendation = `Moderate Over bias (${over2Conf.toFixed(1)}%). Wait for 0/1.`;
        } else {
            action = 'UNDER';
            prediction = `Under 7 (barrier=7)`;
            confidence = under7Conf;
            recommendation = `Moderate Under bias (${under7Conf.toFixed(1)}%). Wait for 8/9.`;
        }
    } else {
        recommendation = `No edge. Over: ${over2Conf.toFixed(1)}% Under: ${under7Conf.toFixed(1)}%. Waiting.`;
    }

    // Generate entry points
    const entries = [];
    [0, 1].forEach(d => {
        const freqPct = (freq[d] / total) * 100;
        const deviation = freqPct - 10;
        const adjConf = over2Conf + (deviation < -2 ? 5 : deviation > 2 ? -5 : 0);
        entries.push({
            type: 'OVER',
            digit: d,
            barrier: 2,
            confidence: Math.min(95, Math.max(40, adjConf)),
            reasoning: `Digit ${d}: ${freqPct.toFixed(1)}% occurrence. Enter Over 2 → win on {3-9}.`
        });
    });
    [8, 9].forEach(d => {
        const freqPct = (freq[d] / total) * 100;
        const deviation = freqPct - 10;
        const adjConf = under7Conf + (deviation < -2 ? 5 : deviation > 2 ? -5 : 0);
        entries.push({
            type: 'UNDER',
            digit: d,
            barrier: 7,
            confidence: Math.min(95, Math.max(40, adjConf)),
            reasoning: `Digit ${d}: ${freqPct.toFixed(1)}% occurrence. Enter Under 7 → win on {0-6}.`
        });
    });
    entries.sort((a, b) => b.confidence - a.confidence);

    const signal = {
        symbol,
        action,
        prediction,
        confidence: Math.round(confidence * 10) / 10,
        over2Conf: Math.round(over2Conf * 10) / 10,
        under7Conf: Math.round(under7Conf * 10) / 10,
        over2Pct: Math.round(over2Pct * 10) / 10,
        under7Pct: Math.round(under7Pct * 10) / 10,
        lastDigit,
        recommendation,
        entries: entries.slice(0, 6),
        timestamp: Date.now(),
        windowSize: total,
        digitFrequencies: freq.map((f, i) => ({ digit: i, count: f, pct: Math.round((f/total)*1000)/10 }))
    };

    state.lastSignals[symbol] = signal;
    return signal;
}

// ================================================================
// TRADE EXECUTION
// ================================================================
let lastTradeTime = 0;

function checkAndExecuteTrade(symbol, signal) {
    if (!state.authenticated) {
        console.log('[Trade] Cannot trade: not authenticated');
        return;
    }

    // Rate limiting
    const now = Date.now();
    if (now - lastTradeTime < CONFIG.tradeCooldownMs) return;
    
    // Max concurrent trades
    const activeCount = Object.keys(state.activeTrades).length;
    if (activeCount >= CONFIG.maxConcurrentTrades) return;

    // Daily limits
    if (state.dailyPnL <= -CONFIG.dailyLossLimit) {
        console.log('[Trade] Daily loss limit reached. Stopping.');
        state.isTrading = false;
        broadcast({ type: 'trading_stopped', reason: 'daily_loss_limit' });
        return;
    }
    if (state.dailyPnL >= CONFIG.dailyProfitTarget) {
        console.log('[Trade] Daily profit target reached. Stopping.');
        state.isTrading = false;
        broadcast({ type: 'trading_stopped', reason: 'daily_profit_target' });
        return;
    }

    const contractType = signal.action === 'OVER' ? 'DIGITOVER' : 'DIGITUNDER';
    const barrier = signal.action === 'OVER' ? '2' : '7';

    state.pendingTrade = {
        symbol,
        contract_type: contractType,
        barrier,
        entryDigit: signal.lastDigit,
        stake: parseFloat(process.env.TRADE_STAKE) || 5,
        confidence: signal.confidence,
        reason: signal.recommendation
    };

    lastTradeTime = now;

    state.derivWs.send(JSON.stringify({
        proposal: 1,
        amount: state.pendingTrade.stake,
        barrier: barrier,
        basis: 'stake',
        contract_type: contractType,
        currency: 'USD',
        duration: 5,
        duration_unit: 't',
        symbol: symbol
    }));

    console.log(`[Trade] Requesting proposal: ${contractType} ${symbol} barrier=${barrier} stake=${state.pendingTrade.stake}`);
}

// ================================================================
// DATABASE HELPERS
// ================================================================
function saveTradeToDB(trade) {
    if (!db) return;
    try {
        db.prepare(`
            INSERT OR REPLACE INTO trades 
            (id, contract_id, symbol, contract_type, barrier, entry_digit, exit_digit,
             stake, payout, profit, status, entry_price, exit_price, 
             entry_time, exit_time, signal_confidence, signal_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
            trade.id, trade.contract_id, trade.symbol, trade.contract_type, trade.barrier,
            trade.entry_digit, trade.exit_digit || null,
            trade.stake, trade.payout || 0, trade.profit || 0, trade.status,
            trade.entry_price || 0, trade.exit_price || 0,
            trade.entry_time || Math.floor(Date.now()/1000), trade.exit_time || null,
            trade.signal_confidence || null, trade.signal_reason || null
        );

        // Update daily stats
        const today = new Date().toISOString().split('T')[0];
        db.prepare(`
            INSERT INTO daily_stats (date, total_trades, wins, losses, pnl, volume)
            VALUES (?, 1, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                total_trades = total_trades + 1,
                wins = wins + ?,
                losses = losses + ?,
                pnl = pnl + ?,
                volume = volume + ?
        `).run(
            today,
            trade.status === 'won' ? 1 : 0,
            trade.status === 'lost' ? 1 : 0,
            trade.profit || 0,
            trade.stake || 0,
            trade.status === 'won' ? 1 : 0,
            trade.status === 'lost' ? 1 : 0,
            trade.profit || 0,
            trade.stake || 0
        );
    } catch (e) {
        console.error('[DB] Error saving trade:', e.message);
    }
}

// ================================================================
// FRONTEND WEBSOCKET (for real-time push to browser)
// ================================================================
function broadcast(data) {
    const msg = JSON.stringify(data);
    state.subscribers.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
            try { ws.send(msg); } catch (e) { /* ignore */ }
        }
    });
}

// ================================================================
// EXPRESS REST API
// ================================================================
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Auth
app.post('/api/auth', (req, res) => {
    const { token } = req.body;
    if (!token) return res.status(400).json({ error: 'Token required' });
    
    state.apiToken = token;
    
    if (!state.connected) {
        connectDeriv();
        // Wait for connection then auth
        const checkConn = setInterval(() => {
            if (state.connected) {
                clearInterval(checkConn);
                authenticate(token);
            }
        }, 500);
        setTimeout(() => clearInterval(checkConn), 15000);
    } else {
        authenticate(token);
    }

    res.json({ status: 'authenticating', message: 'Token received. Authenticating...' });
});

// Market data
app.get('/api/symbols', (req, res) => {
    res.json({
        volatility: [
            { sym: 'R_10', name: 'Volatility 10 Index (2s)', vol: '10%' },
            { sym: 'R_25', name: 'Volatility 25 Index (2s)', vol: '25%' },
            { sym: 'R_50', name: 'Volatility 50 Index (2s)', vol: '50%' },
            { sym: 'R_75', name: 'Volatility 75 Index (2s)', vol: '75%' },
            { sym: 'R_100', name: 'Volatility 100 Index (2s)', vol: '100%' },
            { sym: '1HZ10V', name: 'Volatility 10 (1s) Index', vol: '10%' },
            { sym: '1HZ25V', name: 'Volatility 25 (1s) Index', vol: '25%' },
            { sym: '1HZ50V', name: 'Volatility 50 (1s) Index', vol: '50%' },
            { sym: '1HZ75V', name: 'Volatility 75 (1s) Index', vol: '75%' },
            { sym: '1HZ100V', name: 'Volatility 100 (1s) Index', vol: '100%' },
            { sym: '1HZ150V', name: 'Volatility 150 (1s) Index', vol: '150%' },
            { sym: '1HZ250V', name: 'Volatility 250 (1s) Index', vol: '250%' },
            { sym: '1HZ300V', name: 'Volatility 300 (1s) Index', vol: '300%' },
            { sym: '1HZ15V', name: 'Volatility 15 (1s) Index', vol: '15%' },
            { sym: '1HZ30V', name: 'Volatility 30 (1s) Index', vol: '30%' },
            { sym: '1HZ90V', name: 'Volatility 90 (1s) Index', vol: '90%' },
        ],
        crash: [
            { sym: 'CRASH150', name: 'Crash 150 Index', vol: '1/150 ticks' },
            { sym: 'CRASH300', name: 'Crash 300 Index', vol: '1/300 ticks' },
            { sym: 'CRASH500', name: 'Crash 500 Index', vol: '1/500 ticks' },
            { sym: 'CRASH600', name: 'Crash 600 Index', vol: '1/600 ticks' },
            { sym: 'CRASH900', name: 'Crash 900 Index', vol: '1/900 ticks' },
            { sym: 'CRASH1000', name: 'Crash 1000 Index', vol: '1/1000 ticks' },
        ],
        boom: [
            { sym: 'BOOM150', name: 'Boom 150 Index', vol: '1/150 ticks' },
            { sym: 'BOOM300', name: 'Boom 300 Index', vol: '1/300 ticks' },
            { sym: 'BOOM500', name: 'Boom 500 Index', vol: '1/500 ticks' },
            { sym: 'BOOM600', name: 'Boom 600 Index', vol: '1/600 ticks' },
            { sym: 'BOOM900', name: 'Boom 900 Index', vol: '1/900 ticks' },
            { sym: 'BOOM1000', name: 'Boom 1000 Index', vol: '1/1000 ticks' },
        ],
        jump: [
            { sym: 'JUMP10', name: 'Jump 10 Index', vol: '10%' },
            { sym: 'JUMP25', name: 'Jump 25 Index', vol: '25%' },
            { sym: 'JUMP50', name: 'Jump 50 Index', vol: '50%' },
            { sym: 'JUMP75', name: 'Jump 75 Index', vol: '75%' },
            { sym: 'JUMP100', name: 'Jump 100 Index', vol: '100%' },
        ],
        other: [
            { sym: 'RDBULL', name: 'Bull Market Index', vol: 'Bullish' },
            { sym: 'RDBEAR', name: 'Bear Market Index', vol: 'Bearish' },
            { sym: 'RANGE100', name: 'Range Break 100', vol: 'Break 100' },
            { sym: 'RANGE200', name: 'Range Break 200', vol: 'Break 200' },
            { sym: 'STEP100', name: 'Step 100 Index', vol: '100 steps' },
            { sym: 'STEP200', name: 'Step 200 Index', vol: '200 steps' },
            { sym: 'STEP500', name: 'Step 500 Index', vol: '500 steps' },
        ]
    });
});

// Subscribe to symbol
app.post('/api/subscribe', (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });
    
    // Unsubscribe from current if different
    if (state.currentSymbol && state.currentSymbol !== symbol) {
        unsubscribeTicks(state.currentSymbol);
    }
    
    state.currentSymbol = symbol;
    subscribeTicks(symbol);
    res.json({ status: 'subscribed', symbol });
});

// Get current signal
app.get('/api/signal/:symbol', (req, res) => {
    const signal = state.lastSignals[req.params.symbol];
    if (!signal) return res.json({ status: 'analyzing', message: 'Collecting data...' });
    res.json(signal);
});

// Get tick history
app.get('/api/history/:symbol', (req, res) => {
    const history = state.tickHistory[req.params.symbol] || [];
    res.json({
        symbol: req.params.symbol,
        count: history.length,
        digits: history.slice(-200).map(t => t.digit),
        ticks: history.slice(-100).map(t => ({ digit: t.digit, quote: t.quote, epoch: t.epoch }))
    });
});

// Auto-trading controls
app.post('/api/trading/start', (req, res) => {
    if (!state.authenticated) return res.status(400).json({ error: 'Not authenticated' });
    if (state.dailyPnL <= -CONFIG.dailyLossLimit) return res.status(400).json({ error: 'Daily loss limit reached' });
    
    state.isTrading = true;
    console.log('[Trade] Auto-trading STARTED');
    res.json({ status: 'trading_started' });
});

app.post('/api/trading/stop', (req, res) => {
    state.isTrading = false;
    console.log('[Trade] Auto-trading STOPPED');
    res.json({ status: 'trading_stopped' });
});

app.get('/api/trading/status', (req, res) => {
    res.json({
        isTrading: state.isTrading,
        authenticated: state.authenticated,
        connected: state.connected,
        activeTrades: Object.keys(state.activeTrades).length,
        dailyPnL: state.dailyPnL,
        dailyTrades: state.dailyTrades,
        dailyLossLimit: CONFIG.dailyLossLimit,
        dailyProfitTarget: CONFIG.dailyProfitTarget
    });
});

// Trade history
app.get('/api/trades', (req, res) => {
    const limit = parseInt(req.query.limit) || 50;
    const trades = state.tradeLog.slice(-limit).reverse();
    
    if (db) {
        try {
            const dbTrades = db.prepare('SELECT * FROM trades ORDER BY entry_time DESC LIMIT ?').all(limit);
            return res.json({ trades: dbTrades, inMemory: trades });
        } catch (e) {
            // fall through
        }
    }
    
    res.json({ trades });
});

app.get('/api/trades/stats', (req, res) => {
    const trades = state.tradeLog;
    const wins = trades.filter(t => t.status === 'won').length;
    const losses = trades.filter(t => t.status === 'lost').length;
    
    let dailyStats = null;
    if (db) {
        try {
            dailyStats = db.prepare('SELECT * FROM daily_stats ORDER BY date DESC LIMIT 7').all();
        } catch (e) {}
    }

    res.json({
        totalTrades: trades.length,
        wins,
        losses,
        winRate: trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : 0,
        totalPnL: trades.reduce((s, t) => s + (t.profit || 0), 0),
        dailyPnL: state.dailyPnL,
        activeTrades: Object.keys(state.activeTrades).length,
        dailyStats
    });
});

// Connection status
app.get('/api/status', (req, res) => {
    res.json({
        connected: state.connected,
        authenticated: state.authenticated,
        symbol: state.currentSymbol,
        tickCount: state.tickHistory[state.currentSymbol]?.length || 0,
        activeTrades: Object.keys(state.activeTrades).length,
        totalTrades: state.tradeLog.length,
        dailyPnL: state.dailyPnL,
        isTrading: state.isTrading,
        uptime: Math.floor((Date.now() - startTime) / 1000)
    });
});

const startTime = Date.now();

// ================================================================
// WEBOCKET UPGRADE FOR FRONTEND (real-time push)
// ================================================================
const server = require('http').createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws) => {
    console.log('[WS] Frontend client connected');
    state.subscribers.add(ws);

    // Send initial state
    ws.send(JSON.stringify({
        type: 'init',
        connected: state.connected,
        authenticated: state.authenticated,
        symbol: state.currentSymbol,
        isTrading: state.isTrading,
        dailyPnL: state.dailyPnL,
        dailyTrades: state.dailyTrades,
        signals: state.lastSignals
    }));

    ws.on('close', () => {
        state.subscribers.delete(ws);
        console.log('[WS] Frontend client disconnected');
    });

    ws.on('error', (err) => {
        state.subscribers.delete(ws);
    });
});

// ================================================================
// START
// ================================================================
server.listen(CONFIG.port, () => {
    console.log(`
╔══════════════════════════════════════════════════════════╗
║          DERIV DIGITS PRO — Trading Server              ║
║          Over 2 / Under 7 Analysis Engine               ║
╠══════════════════════════════════════════════════════════╣
║  REST API:    http://localhost:${CONFIG.port}/api             ║
║  WebSocket:   ws://localhost:${CONFIG.port}/ws               ║
║  Frontend:    http://localhost:${CONFIG.port}                  ║
║  Deriv WS:    ${CONFIG.derivWsUrl}    ║
║  App ID:      ${CONFIG.appId}                              ║
║  Max Trades:  ${CONFIG.maxConcurrentTrades} concurrent            ║
║  Daily Limit: -$${CONFIG.dailyLossLimit} / +$${CONFIG.dailyProfitTarget}              ║
╚══════════════════════════════════════════════════════════╝
    `);
    connectDeriv();
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\n[Server] Shutting down...');
    state.isTrading = false;
    if (state.derivWs) state.derivWs.close();
    wss.close();
    server.close(() => process.exit(0));
});
