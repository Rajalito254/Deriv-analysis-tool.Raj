// ================================================================
//  DERIV DIGITS PRO — v2.2 (Dual-Mode Connection)
//  Tries Legacy WebSocket first → Falls back to OTP WebSocket
// ================================================================

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

// ================================================================
// CONFIG
// ================================================================
const CONFIG = {
    port: process.env.PORT || 3000,
    appId: process.env.DERIV_APP_ID || '1089',
    defaultSymbol: 'R_50',
    maxConcurrentTrades: 3,
    tradeCooldownMs: 3000,
    dailyLossLimit: parseFloat(process.env.DAILY_LOSS_LIMIT) || 50,
    dailyProfitTarget: parseFloat(process.env.DAILY_PROFIT_TARGET) || 80,
};

const LEGACY_WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${CONFIG.appId}`;
const OTP_API_URL = 'https://api.derivws.com/trading/v1/options/accounts';

// ================================================================
// STATE
// ================================================================
const state = {
    derivWs: null,
    connected: false,
    authenticated: false,
    apiToken: null,
    accountId: null,
    connectionMode: null, // 'legacy' or 'otp'
    tickHistory: {},
    activeTrades: {},
    tradeLog: [],
    dailyPnL: 0,
    dailyTrades: 0,
    isTrading: false,
    subscribers: new Set(),
    lastSignals: {},
    currentSymbol: null,
    pendingTrade: null,
    connectionDiagnostics: [],
    lastError: null,
};

function logDiag(msg, type = 'info') {
    const entry = { time: new Date().toISOString(), type, msg };
    state.connectionDiagnostics.push(entry);
    if (state.connectionDiagnostics.length > 100) state.connectionDiagnostics.shift();
    const prefix = type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️';
    console.log(`[${prefix}] ${msg}`);
}

// ================================================================
// CONNECTION: TRY LEGACY FIRST
// ================================================================
function connectLegacy() {
    logDiag(`Connecting to legacy WebSocket: ${LEGACY_WS_URL}`);
    
    state.derivWs = new WebSocket(LEGACY_WS_URL);
    
    state.derivWs.on('open', () => {
        logDiag('Legacy WebSocket connected', 'success');
        state.connectionMode = 'legacy';
        
        if (state.apiToken) {
            logDiag('Sending authorize request...');
            state.derivWs.send(JSON.stringify({ authorize: state.apiToken }));
        }
    });
    
    state.derivWs.on('message', (raw) => {
        try {
            const msg = JSON.parse(raw);
            
            if (msg.msg_type === 'authorize') {
                if (msg.error) {
                    logDiag(`Authorization failed: ${msg.error.message} (code: ${msg.error.code})`, 'error');
                    state.lastError = msg.error;
                    broadcast({ type: 'auth_error', error: msg.error });
                    
                    // If legacy fails, try OTP
                    if (state.apiToken) {
                        logDiag('Legacy auth failed. Trying OTP flow...');
                        connectOTP();
                    }
                    return;
                }
                
                if (msg.authorize) {
                    state.authenticated = true;
                    state.accountId = msg.authorize.loginid;
                    logDiag(`Authorized! Account: ${state.accountId}`, 'success');
                    
                    broadcast({
                        type: 'auth',
                        status: 'authenticated',
                        loginid: state.accountId,
                        balance: msg.authorize.balance,
                        currency: msg.authorize.currency,
                        email: msg.authorize.email,
                        connectionMode: 'legacy'
                    });
                    
                    // Subscribe to default symbol
                    subscribeTicks(state.currentSymbol || CONFIG.defaultSymbol);
                }
                return;
            }
            
            // Handle all other messages (same as before)
            handleDerivMessage(msg);
            
        } catch (e) {
            console.error('[Parse error]:', e.message);
        }
    });
    
    state.derivWs.on('error', (err) => {
        logDiag(`Legacy WebSocket error: ${err.message}`, 'error');
        state.lastError = { message: err.message };
    });
    
    state.derivWs.on('close', (code, reason) => {
        logDiag(`Legacy WebSocket closed: ${code} ${reason || ''}`);
        state.connected = false;
        state.authenticated = false;
        broadcast({ type: 'connection', status: 'disconnected', mode: 'legacy' });
        
        // Auto-reconnect after 3s
        setTimeout(() => {
            if (state.apiToken && !state.connected) {
                logDiag('Reconnecting...');
                connectLegacy();
            }
        }, 3000);
    });
}

// ================================================================
// CONNECTION: OTP FLOW (Fallback)
// ================================================================
async function connectOTP() {
    if (!state.accountId && state.lastError) {
        // If we don't have accountId, try to derive it or ask user
        logDiag('Need account ID for OTP flow. Checking from authorize response...', 'error');
        broadcast({ 
            type: 'otp_needs_account_id',
            message: 'Legacy auth failed. To use OTP flow, please provide your Deriv account ID (e.g., CR12345678)'
        });
        return;
    }
    
    if (!state.accountId) {
        logDiag('No account ID available for OTP flow', 'error');
        return;
    }
    
    try {
        logDiag(`Requesting OTP for account ${state.accountId}...`);
        
        const response = await fetch(
            `${OTP_API_URL}/${state.accountId}/otp`,
            {
                method: 'POST',
                headers: {
                    'Deriv-App-ID': CONFIG.appId,
                    'Authorization': `Bearer ${state.apiToken}`,
                    'Content-Type': 'application/json'
                }
            }
        );
        
        const result = await response.json();
        
        if (result.data?.url) {
            const wsUrl = result.data.url;
            logDiag(`OTP received! Connecting to: ${wsUrl.slice(0, 60)}...`, 'success');
            
            state.derivWs = new WebSocket(wsUrl);
            state.connectionMode = 'otp';
            
            state.derivWs.on('open', () => {
                logDiag('OTP WebSocket connected!', 'success');
                state.connected = true;
                state.authenticated = true;
                
                broadcast({
                    type: 'auth',
                    status: 'authenticated',
                    accountId: state.accountId,
                    connectionMode: 'otp'
                });
                
                subscribeTicks(state.currentSymbol || CONFIG.defaultSymbol);
            });
            
            state.derivWs.on('message', (raw) => {
                try {
                    handleDerivMessage(JSON.parse(raw));
                } catch (e) {
                    console.error('[Parse error]:', e.message);
                }
            });
            
            state.derivWs.on('error', (err) => {
                logDiag(`OTP WebSocket error: ${err.message}`, 'error');
            });
            
            state.derivWs.on('close', () => {
                state.connected = false;
                broadcast({ type: 'connection', status: 'disconnected', mode: 'otp' });
                setTimeout(() => {
                    if (state.apiToken) connectOTP();
                }, 3000);
            });
        } else {
            logDiag(`OTP failed: ${result.error?.message || JSON.stringify(result)}`, 'error');
            broadcast({ type: 'otp_error', error: result.error || result });
        }
    } catch (err) {
        logDiag(`OTP request failed: ${err.message}`, 'error');
    }
}

// ================================================================
// DERIV MESSAGE HANDLER (Shared between both modes)
// ================================================================
function handleDerivMessage(msg) {
    // Tick data
    if (msg.msg_type === 'tick' && msg.tick) {
        const { symbol, quote, epoch } = msg.tick;
        const priceStr = String(quote);
        const lastDigit = parseInt(priceStr.replace('.', '').slice(-1)) || 0;

        if (!state.tickHistory[symbol]) state.tickHistory[symbol] = [];
        state.tickHistory[symbol].push({ digit: lastDigit, epoch, quote });
        if (state.tickHistory[symbol].length > 1000) state.tickHistory[symbol] = state.tickHistory[symbol].slice(-1000);

        const signal = runAnalysis(symbol);
        broadcast({
            type: 'tick', symbol, digit: lastDigit, quote, epoch,
            history: state.tickHistory[symbol].slice(-60).map(t => t.digit),
            signal
        });

        if (state.isTrading && signal && signal.action !== 'WAIT' && signal.confidence >= 60) {
            checkAndExecuteTrade(symbol, signal);
        }
        return;
    }

    // Proposal
    if (msg.msg_type === 'proposal' && msg.proposal) {
        broadcast({ type: 'proposal', proposal: msg.proposal });
        return;
    }

    // Buy confirmation
    if (msg.msg_type === 'buy' && msg.buy) {
        const contractId = msg.buy.contract_id;
        if (state.pendingTrade) {
            const tradeRecord = {
                id: uuidv4(),
                contract_id: contractId,
                symbol: state.pendingTrade.symbol,
                contract_type: state.pendingTrade.contract_type,
                barrier: state.pendingTrade.barrier,
                entry_digit: state.pendingTrade.entryDigit,
                stake: msg.buy.buy_price || state.pendingTrade.stake,
                payout: msg.buy.payout,
                profit: 0,
                status: 'open',
                entry_price: msg.buy.spot || 0,
                entry_time: msg.buy.purchase_time || Math.floor(Date.now() / 1000),
                signal_confidence: state.pendingTrade.confidence,
                signal_reason: state.pendingTrade.reason
            };
            state.activeTrades[contractId] = tradeRecord;
            delete state.pendingTrade;
            
            logDiag(`Trade opened: ${tradeRecord.contract_type} $${tradeRecord.stake}`, 'success');
            broadcast({ type: 'trade_opened', trade: tradeRecord, balance: msg.buy.balance_after });

            // Subscribe to contract status
            state.derivWs.send(JSON.stringify({
                proposal_open_contract: 1,
                contract_id: contractId,
                subscribe: 1
            }));
        }
        return;
    }

    // Contract status
    if (msg.msg_type === 'proposal_open_contract' && msg.proposal_open_contract) {
        const contract = msg.proposal_open_contract;
        const contractId = contract.contract_id;
        
        if (state.activeTrades[contractId]) {
            const trade = state.activeTrades[contractId];
            trade.status = contract.status;
            
            if (contract.status === 'won' || contract.status === 'lost') {
                trade.profit = contract.profit || 0;
                trade.exit_digit = contract.exit_tick ? parseInt(String(contract.exit_tick).slice(-1)) : null;
                trade.exit_price = contract.exit_spot || 0;
                trade.exit_time = contract.expiry_time || Math.floor(Date.now() / 1000);

                state.dailyPnL += trade.profit;
                state.dailyTrades++;
                state.tradeLog.push(trade);
                
                logDiag(`Trade ${contract.status}: ${trade.contract_type} PnL=${trade.profit}`, 
                    contract.status === 'won' ? 'success' : 'error');
                broadcast({ type: 'trade_closed', trade, dailyPnL: state.dailyPnL, dailyTrades: state.dailyTrades });
                
                delete state.activeTrades[contractId];
            }
            broadcast({ type: 'contract_update', contract });
        }
        return;
    }

    // Error
    if (msg.error) {
        console.error('[Deriv] Error:', msg.error);
        broadcast({ type: 'error', error: msg.error });
        return;
    }
    
    // Balance
    if (msg.msg_type === 'balance') {
        broadcast({ type: 'balance', balance: msg.balance });
    }
}

// ================================================================
// ANALYSIS ENGINE (same as before)
// ================================================================
function runAnalysis(symbol) {
    const history = state.tickHistory[symbol];
    if (!history || history.length < 25) return null;

    const windowSize = Math.min(100, history.length);
    const recent = history.slice(-windowSize);
    const digits = recent.map(t => t.digit);
    const total = digits.length;

    const freq = Array(10).fill(0);
    digits.forEach(d => freq[d]++);

    const over2Hits = freq.slice(3).reduce((a, b) => a + b, 0);
    const over2Pct = (over2Hits / total) * 100;
    const over2Conf = Math.max(0, Math.min(100, 70 - Math.abs(over2Pct - 70) + (over2Pct > 70 ? 5 : 0)));

    const under7Hits = freq.slice(0, 7).reduce((a, b) => a + b, 0);
    const under7Pct = (under7Hits / total) * 100;
    const under7Conf = Math.max(0, Math.min(100, 70 - Math.abs(under7Pct - 70) + (under7Pct > 70 ? 5 : 0)));

    const lastDigit = digits[digits.length - 1];
    let action = 'WAIT', prediction = '', confidence = 0, recommendation = '';

    if (over2Conf >= 55 && (lastDigit === 0 || lastDigit === 1)) {
        action = 'OVER'; prediction = 'Over 2 (barrier=2)'; confidence = over2Conf;
        recommendation = `Digit ${lastDigit} touched. Over 2 bias ${over2Conf.toFixed(1)}%. Enter now.`;
    } else if (under7Conf >= 55 && (lastDigit === 8 || lastDigit === 9)) {
        action = 'UNDER'; prediction = 'Under 7 (barrier=7)'; confidence = under7Conf;
        recommendation = `Digit ${lastDigit} touched. Under 7 bias ${under7Conf.toFixed(1)}%. Enter now.`;
    } else if (over2Conf >= 70 && over2Conf > under7Conf + 10) {
        action = 'OVER'; prediction = 'Over 2 (barrier=2)'; confidence = over2Conf;
        recommendation = `Strong Over 2 bias. Wait for 0 or 1 touch.`;
    } else if (under7Conf >= 70 && under7Conf > over2Conf + 10) {
        action = 'UNDER'; prediction = 'Under 7 (barrier=7)'; confidence = under7Conf;
        recommendation = `Strong Under 7 bias. Wait for 8 or 9 touch.`;
    } else {
        recommendation = `No edge. Over: ${over2Conf.toFixed(1)}% Under: ${under7Conf.toFixed(1)}%. Waiting.`;
    }

    const entries = [];
    [0, 1].forEach(d => {
        const freqPct = (freq[d] / total) * 100;
        const adjConf = Math.min(95, Math.max(40, over2Conf + (freqPct < 8 ? 5 : freqPct > 12 ? -5 : 0)));
        entries.push({ type: 'OVER', digit: d, barrier: 2, confidence: adjConf,
            reasoning: `Digit ${d}: ${freqPct.toFixed(1)}% occurrence. Enter Over 2 → win on {3-9}.` });
    });
    [8, 9].forEach(d => {
        const freqPct = (freq[d] / total) * 100;
        const adjConf = Math.min(95, Math.max(40, under7Conf + (freqPct < 8 ? 5 : freqPct > 12 ? -5 : 0)));
        entries.push({ type: 'UNDER', digit: d, barrier: 7, confidence: adjConf,
            reasoning: `Digit ${d}: ${freqPct.toFixed(1)}% occurrence. Enter Under 7 → win on {0-6}.` });
    });
    entries.sort((a, b) => b.confidence - a.confidence);

    const signal = {
        symbol, action, prediction, confidence: Math.round(confidence * 10) / 10,
        over2Conf: Math.round(over2Conf * 10) / 10,
        under7Conf: Math.round(under7Conf * 10) / 10,
        over2Pct: Math.round(over2Pct * 10) / 10,
        under7Pct: Math.round(under7Pct * 10) / 10,
        lastDigit, recommendation,
        entries: entries.slice(0, 6),
        timestamp: Date.now(), windowSize: total,
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
    if (!state.authenticated) return;
    if (!state.derivWs || state.derivWs.readyState !== WebSocket.OPEN) return;
    
    const now = Date.now();
    if (now - lastTradeTime < CONFIG.tradeCooldownMs) return;
    if (Object.keys(state.activeTrades).length >= CONFIG.maxConcurrentTrades) return;
    if (state.dailyPnL <= -CONFIG.dailyLossLimit || state.dailyPnL >= CONFIG.dailyProfitTarget) {
        state.isTrading = false;
        broadcast({ type: 'trading_stopped', reason: 'daily_limit' });
        return;
    }

    const contractType = signal.action === 'OVER' ? 'DIGITOVER' : 'DIGITUNDER';
    const barrier = signal.action === 'OVER' ? '2' : '7';
    const stake = parseFloat(process.env.TRADE_STAKE) || 5;

    state.pendingTrade = { symbol, contract_type: contractType, barrier, entryDigit: signal.lastDigit, stake, confidence: signal.confidence, reason: signal.recommendation };
    lastTradeTime = now;

    state.derivWs.send(JSON.stringify({
        proposal: 1, amount: stake, barrier, basis: 'stake',
        contract_type: contractType, currency: 'USD',
        duration: 5, duration_unit: 't', symbol
    }));
}

function subscribeTicks(symbol) {
    if (!state.derivWs || state.derivWs.readyState !== WebSocket.OPEN) return;
    if (!state.tickHistory[symbol]) state.tickHistory[symbol] = [];
    state.currentSymbol = symbol;
    state.derivWs.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
    logDiag(`Subscribed to ticks: ${symbol}`, 'success');
}

// ================================================================
// WEBSOCKET FOR FRONTEND
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

// Auth endpoints
app.post('/api/auth', (req, res) => {
    const { token } = req.body;
    if (!token) return res.status(400).json({ error: 'Token required' });
    
    state.apiToken = token;
    state.connectionDiagnostics = [];
    state.lastError = null;
    
    logDiag('Token received. Starting connection...', 'info');
    
    if (state.derivWs) {
        try { state.derivWs.close(); } catch(e) {}
    }
    
    connectLegacy();
    
    res.json({ 
        status: 'connecting', 
        message: 'Connecting to Deriv...',
        mode: 'legacy_first',
        appId: CONFIG.appId
    });
});

// Provide account ID for OTP fallback
app.post('/api/auth/otp', (req, res) => {
    const { accountId } = req.body;
    if (!accountId) return res.status(400).json({ error: 'Account ID required (e.g., CR12345678)' });
    
    state.accountId = accountId;
    logDiag(`Account ID set: ${accountId}`, 'info');
    
    if (!state.apiToken) return res.status(400).json({ error: 'No API token. First connect with token.' });
    
    connectOTP();
    res.json({ status: 'connecting_otp', message: 'Connecting via OTP...' });
});

// Diagnostics
app.get('/api/diagnostics', (req, res) => {
    res.json({
        logs: state.connectionDiagnostics.slice(-50),
        lastError: state.lastError,
        connectionMode: state.connectionMode,
        connected: state.connected,
        authenticated: state.authenticated,
        appId: CONFIG.appId,
        accountId: state.accountId,
        wsReadyState: state.derivWs ? state.derivWs.readyState : -1,
        // readyState: 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
    });
});

// Symbols endpoint
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
        ]
    });
});

app.post('/api/subscribe', (req, res) => {
    const { symbol } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol required' });
    subscribeTicks(symbol);
    res.json({ status: 'subscribed', symbol });
});

app.get('/api/signal/:symbol', (req, res) => {
    const signal = state.lastSignals[req.params.symbol];
    res.json(signal || { status: 'analyzing', message: 'Collecting data...' });
});

app.get('/api/history/:symbol', (req, res) => {
    const history = state.tickHistory[req.params.symbol] || [];
    res.json({ symbol: req.params.symbol, count: history.length, digits: history.slice(-200).map(t => t.digit) });
});

app.post('/api/trading/start', (req, res) => {
    if (!state.authenticated) return res.status(400).json({ error: 'Not authenticated' });
    state.isTrading = true;
    logDiag('Auto-trading started', 'success');
    res.json({ status: 'trading_started' });
});

app.post('/api/trading/stop', (req, res) => {
    state.isTrading = false;
    res.json({ status: 'trading_stopped' });
});

app.get('/api/trading/status', (req, res) => {
    res.json({
        isTrading: state.isTrading,
        authenticated: state.authenticated,
        connected: state.connected,
        connectionMode: state.connectionMode,
        activeTrades: Object.keys(state.activeTrades).length,
        dailyPnL: state.dailyPnL,
        dailyTrades: state.dailyTrades,
        accountId: state.accountId,
    });
});

app.get('/api/trades', (req, res) => {
    res.json({ trades: state.tradeLog.slice(-50).reverse() });
});

app.get('/api/trades/stats', (req, res) => {
    const trades = state.tradeLog;
    const wins = trades.filter(t => t.status === 'won').length;
    const losses = trades.filter(t => t.status === 'lost').length;
    res.json({
        totalTrades: trades.length, wins, losses,
        winRate: trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : 0,
        totalPnL: trades.reduce((s, t) => s + (t.profit || 0), 0),
        dailyPnL: state.dailyPnL,
        activeTrades: Object.keys(state.activeTrades).length,
    });
});

app.get('/api/status', (req, res) => {
    res.json({
        connected: state.connected,
        authenticated: state.authenticated,
        connectionMode: state.connectionMode,
        symbol: state.currentSymbol,
        appId: CONFIG.appId,
        activeTrades: Object.keys(state.activeTrades).length,
        totalTrades: state.tradeLog.length,
        dailyPnL: state.dailyPnL,
        isTrading: state.isTrading,
        lastError: state.lastError,
    });
});

// ================================================================
// WEBSOCKET FOR FRONTEND
// ================================================================
const server = require('http').createServer(app);
const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws) => {
    state.subscribers.add(ws);
    ws.send(JSON.stringify({
        type: 'init',
        connected: state.connected,
        authenticated: state.authenticated,
        connectionMode: state.connectionMode,
        symbol: state.currentSymbol,
        isTrading: state.isTrading,
        dailyPnL: state.dailyPnL,
        dailyTrades: state.dailyTrades,
        signals: state.lastSignals,
        diagnostics: state.connectionDiagnostics.slice(-20),
        lastError: state.lastError,
        appId: CONFIG.appId,
    }));
    ws.on('close', () => state.subscribers.delete(ws));
});

// ================================================================
// START
// ================================================================
server.listen(CONFIG.port, () => {
    console.log(`
╔══════════════════════════════════════════════════════════╗
║       DERIV DIGITS PRO — v2.2 (Dual-Mode Connection)    ║
╚══════════════════════════════════════════════════════════╝
  Server:   http://localhost:${CONFIG.port}
  API:      http://localhost:${CONFIG.port}/api
  WS:       ws://localhost:${CONFIG.port}/ws
  
  App ID:   ${CONFIG.appId}
  Legacy:   ${LEGACY_WS_URL}
  OTP:      ${OTP_API_URL}/{accountId}/otp
  
  🔑 1. Go to app.deriv.com → Settings → API Token
  🔑 2. Create token with Read + Trade + Trading Information
  🔑 3. Paste token in the app and click Connect
  🔑 4. If legacy fails, enter your Account ID for OTP flow
  
  Need help? Check /api/diagnostics for connection logs
    `);
});
