// ==============================================================================
// AURUM INTEL — CORE PRODUCTION MACRO INTELLIGENCE PLATFORM ENGINE
// ==============================================================================
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const cron = require('node-cron');
const { Anthropic } = require('@anthropic-ai/sdk');

// Ensure system environment variables are forced to handle execution safely
process.env.TZ = process.env.TZ || 'America/New_York';

const app = express();
const PORT = process.env.PORT || 3000;

// ── INITIALIZE COGNITIVE CLIENTS ─────────────────────────────────────────────
const anthropic = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

// ── IN-MEMORY PERSISTENCE SUBSYSTEM (GLOBAL CACHE MAPPING) ────────────────────
const globalCache = {
  marketData: {
    data: null,
    lastUpdated: 0
  },
  macroBriefing: {
    data: null,
    lastUpdated: 0
  }
};

// ── SECURITY & PARSING MIDDLEWARE COMPILATION MATRIX ─────────────────────────
app.use(express.json());

// Proxy Trust Flag configuration to support accurate rate-limiting behind PaaS Layers
if (process.env.TRUST_PROXY === 'true') {
  app.set('trust proxy', 1);
}

// Configured CORS Domain Validation Pipeline
const allowedOrigin = process.env.ALLOWED_ORIGIN || 'http://localhost:3000';
app.use(cors({
  origin: (origin, callback) => {
    // Permit local server or tool scraping executions without strict explicit headers
    if (!origin || origin === allowedOrigin || process.env.NODE_ENV !== 'production') {
      callback(null, true);
    } else {
      callback(new Error('CORS Access Violation: Domain origin signature rejected.'));
    }
  },
  optionsSuccessStatus: 200
}));

// DDoS Protection & API Rate Limit Management Gates
const generalRateLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 Minute window
  max: 100, // Limit each client signature to 100 endpoints checks per window frame
  message: { error: 'Too many requests generated from this network footprint. Please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});
app.use('/api/', generalRateLimiter);

// ── STATIC FRONTEND CONTENT WITH CACHE-BUSTING GUARDRAILS ────────────────────
app.use(express.static('public', {
  setHeaders: (res, path) => {
    if (path.endsWith('.html')) {
      // Force immediate revalidation of the HTML shell structure so updates reflect instantly
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    }
  }
}));

// ── EXTERNAL INGESTION DATA UTILITIES (FETCH STRUCTURAL APIS) ─────────────────

/**
 * Native safety wrapper for downstream asynchronous data fetches
 */
async function fetchWithTimeout(url, options = {}, timeout = 6000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(id);
    if (!response.ok) throw new Error(`HTTP network error status: ${response.status}`);
    return await response.json();
  } catch (error) {
    clearTimeout(id);
    throw error;
  }
}

/**
 * Refreshes pricing metrics and builds structural localized cache assets
 */
async function refreshMarketDataCache() {
  try {
    console.log('[ENG-LOG] Compiling upstream global market tick data maps...');

    // 1. Alpha Vantage — Gold (XAUUSD) Data Ingestion Pipeline
    const avUrl = `https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=GLD&apikey=${process.env.ALPHA_VANTAGE_API_KEY}`;
    const avData = await fetchWithTimeout(avUrl).catch(err => {
      console.error('[ERR-GLD] Alpha Vantage endpoint drop:', err.message);
      return null;
    });

    // 2. CoinGecko — Crypto Pricing API Array
    const cgUrl = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd';
    const cgData = await fetchWithTimeout(cgUrl).catch(err => {
      console.error('[ERR-BTC] CoinGecko public access node drop:', err.message);
      return null;
    });

    // 3. FRED Repository — Federal Reserve 10-Year Bond Rate Ingestion
    const fredUrl = `https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&limit=1&sort_order=desc&file_type=json&api_key=${process.env.FRED_API_KEY}`;
    const fredData = await fetchWithTimeout(fredUrl).catch(err => {
      console.error('[ERR-FRD] FRED economic series tracking down:', err.message);
      return null;
    });

    // ── DATA PARSING AND MATRIX NORMALIZATION ────────────────────────────────
    const goldQuote = avData?.['Global Quote'] || {};
    const btcPrice = cgData?.['bitcoin']?.['usd'] || null;
    const ethPrice = cgData?.['ethereum']?.['usd'] || null;
    
    const fredObs = fredData?.observations || [];
    const yield10Y = fredObs.length > 0 ? fredObs[0].value : null;

    globalCache.marketData.data = {
      assets: {
        gold: {
          ticker: "XAUUSD (GLD Proxy)",
          price: goldQuote['05. price'] ? parseFloat(goldQuote['05. price']) : null,
          changePercent: goldQuote['10. change percent'] ? parseFloat(goldQuote['10. change percent'].replace('%', '')) : null,
          volume: goldQuote['06. volume'] ? parseInt(goldQuote['06. volume']) : null
        },
        bitcoin: {
          ticker: "BTCUSD",
          price: btcPrice,
        },
        ethereum: {
          ticker: "ETHUSD",
          price: ethPrice
        },
        treasury10Y: {
          ticker: "DGS10",
          yield: yield10Y ? parseFloat(yield10Y) : null
        }
      },
      timestamp: Date.now()
    };
    
    globalCache.marketData.lastUpdated = Date.now();
    console.log('[ENG-LOG] Market intelligence memory vectors successfully updated.');
  } catch (globalErr) {
    console.error('[CRIT-ERR] Failure inside market loop data aggregation system:', globalErr.message);
  }
}

/**
 * Orchestrates intelligence construction workflows via Anthropic Claude Context Layouts
 */
async function regenerateMacroBriefingCache() {
  try {
    console.log('[ENG-LOG] Running downstream financial narrative parsing workflow via Claude...');

    // Ingest modern macroeconomic headlines
    const newsUrl = `https://newsapi.org/v2/everything?q=gold%20AND%20macroeconomics%20AND%20fed&language=en&sortBy=publishedAt&pageSize=12&apiKey=${process.env.NEWS_API_KEY}`;
    const newsPayload = await fetchWithTimeout(newsUrl).catch(err => {
      console.error('[ERR-NWS] NewsAPI feed aggregation break:', err.message);
      return { articles: [] };
    });

    const marketContextString = JSON.stringify(globalCache.marketData.data || {});
    const headlinesString = (newsPayload.articles || []).map(a => `[${a.publishedAt}] ${a.title}: ${a.description}`).join('\n');

    const systemInstruction = `You are a professional macro trading strategist specializing in Gold (XAUUSD), US Treasury yields, and digital store-of-value assets (Bitcoin). Analyze the provided market structure values and news reports.
Output your intelligence brief strictly inside a valid JSON structural container matching this outline exactly:
{
  "marketBias": "BULLISH / BEARISH / NEUTRAL",
  "executiveSummary": "A concise breakdown tracking underlying correlation changes.",
  "goldAnalysis": "Direct tactical analysis tracking physical bullion impacts relative to yields.",
  "cryptoAnalysis": "Macro assessment on liquidity parameters shifting Bitcoin volume.",
  "actionableTradeIdea": {
    "setup": "Description of tactical setup framework",
    "invalidation": "Technical or macro threshold that disproves the thesis"
  }
}
Do not return any conversational introductory formatting. Return ONLY raw valid structural JSON text strings.`;

    const responseMessage = await anthropic.messages.create({
      model: 'claude-3-5-sonnet-20241022',
      max_tokens: 1800,
      temperature: 0.1, 
      system: systemInstruction,
      messages: [
        {
          role: 'user',
          content: `Current Market Prices Framework Context:\n${marketContextString}\n\nRecent Global Macro Headlines Matrix:\n${headlinesString}`
        }
      ]
    });

    const outputText = responseMessage.content[0].text.trim();
    
    // Failsafe structural extraction block to isolate JSON boundary anchors safely
    try {
      const startBracket = outputText.indexOf('{');
      const endBracket = outputText.lastIndexOf('}');

      if (startBracket !== -1 && endBracket !== -1) {
        const cleanJsonString = outputText.substring(startBracket, endBracket + 1);
        globalCache.macroBriefing.data = JSON.parse(cleanJsonString);
        globalCache.macroBriefing.lastUpdated = Date.now();
        console.log('[ENG-LOG] AI Macro intelligence briefing matrix generated and cached securely.');
      } else {
        throw new Error("Target curly bracket markers missing from response token buffer.");
      }
    } catch (parseError) {
      console.error('[PARSE-CRIT] Anthropic synthesis response returned malformed formatting:', parseError.message);
      console.log('Raw output trace for diagnostic logs:', outputText);
    }

  } catch (aiMatrixError) {
    console.error('[CRIT-ERR] Failure inside intelligence generation framework node:', aiMatrixError.message);
  }
}

// ── SCHEDULING INFRASTRUCTURE SEEDING (BACKGROUND LOOPS) ─────────────────────

// Poll data vectors automatically (adjusted to 1 hour to protect Alpha Vantage standard baseline quotas)
setInterval(refreshMarketDataCache, 3600000);

// Initialize system clock tasks matching standard configuration variables
const cronScheduleString = process.env.BRIEFING_CRON || '0 */4 * * *';
cron.schedule(cronScheduleString, async () => {
  console.log('[CRON-EXEC] Periodic scheduled task triggered: Updating Macro Intelligence Matrix.');
  await regenerateMacroBriefingCache();
});

// ── EXPRESS APPLICATION EXPOSED ENTRY API ROUTES ──────────────────────────────

/**
 * Endpoint tracking real-time asset pricing metrics
 */
app.get('/api/market-data', async (req, res) => {
  if (!globalCache.marketData.data) {
    await refreshMarketDataCache();
  }
  return res.status(200).json(globalCache.marketData.data);
});

/**
 * Endpoint delivering AI synthesized macro research matrices
 */
app.get('/api/macro-briefing', async (req, res) => {
  const maxAgeLimitMs = (parseInt(process.env.BRIEFING_MAX_AGE_MINUTES) || 240) * 60 * 1000;
  const currentTimestamp = Date.now();
  const cacheAge = currentTimestamp - globalCache.macroBriefing.lastUpdated;

  // Validate current dataset freshness values or perform manual rehydration on cold start
  if (!globalCache.macroBriefing.data || cacheAge > maxAgeLimitMs) {
    console.log('[ENG-LOG] Cache empty or threshold marked stale. Generating inline structural asset...');
    if (!globalCache.marketData.data) await refreshMarketDataCache();
    await regenerateMacroBriefingCache();
  }

  return res.status(200).json({
    briefing: globalCache.macroBriefing.data,
    cacheAgeMinutes: Math.round(cacheAge / 60000),
    isStale: cacheAge > maxAgeLimitMs,
    // Embedded protection payload parameter flag
    regulatoryNotice: "INFORMATIONAL AND EDUCATIONAL PURPOSES ONLY — NOT FINANCIAL ADVICE"
  });
});

/**
 * Handle incoming institutional and platform user waitlist registrations
 */
app.post('/api/waitlist', async (req, res) => {
  const { email, name } = req.body;

  if (!email || !email.includes('@')) {
    return res.status(400).json({ success: false, error: 'A valid email payload signature is required.' });
  }

  console.log(`[WAITLIST-REGISTRATION] New trader application logged -> Name: ${name || 'N/A'}, Email: ${email}`);

  return res.status(201).json({
    success: true,
    message: 'User identity successfully captured. Verification loop pending.'
  });
});

// ── INITIAL SYSTEM STARTUP CORRELATION SEEDING ────────────────────────────────
async function initializeSystemCore() {
  console.log('================================================================');
  console.log('AURUM INTEL ENGINE SYSTEM BOOT SEQUENCE INITIATED');
  console.log('================================================================');
  
  // Fire up listener immediately to avoid execution lockouts on container networks
  app.listen(PORT, async () => {
    console.log(`[ONLINE] Core cluster executing traffic routes over interface PORT: ${PORT}`);
    console.log(`[ENV] Mode Flag: ${process.env.NODE_ENV || 'development'}`);
    
    // Populate storage caches asynchronously out of line
    try {
      await refreshMarketDataCache();
      await regenerateMacroBriefingCache();
    } catch (cacheErr) {
      console.error('[WARN] Initial cache hydration bottleneck caught during boot cycle:', cacheErr.message);
    }
  });
}

initializeSystemCore();
