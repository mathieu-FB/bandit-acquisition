// ============================================================
// TEST MONITOR — moniteur de la campagne de test créas Meta
//
// Pour chaque ad d'une campagne dont le nom contient
// $rules.campaign_name_match, agrège meta_ad_daily depuis le
// PREMIER JOUR DE DÉPENSE de l'ad (pas une fenêtre glissante) et
// produit un verdict :
//   - KILL           : 0 achat ET spend ≥ kill_spend
//                      OU ctr_link < kill_ctr_link_min ET impressions ≥ kill_ctr_min_impressions
//   - KILL_EXTENDED  : spend ≥ extend_spend ET cpa > extend_cpa_max
//   - GRADUATE       : purchases ≥ graduate_min_purchases ET cpa ≤ graduate_cpa_max
//   - CONTINUE       : sinon
//
// Aucune action d'écriture sur Meta : le moniteur recommande, Mathieu
// exécute manuellement.
//
// Config : data/test_rules.json (rechargée à chaque évaluation → pas
// besoin de redéployer pour changer un seuil).
// ============================================================

const fs = require('fs');
const path = require('path');
const sgMail = require('@sendgrid/mail');
const cache = require('./cache');

const RULES_PATH = path.join(__dirname, 'data', 'test_rules.json');

// ------------------------------------------------------------
// Config
// ------------------------------------------------------------

/**
 * Charge test_rules.json. Rechargé à chaque appel — les changements
 * de config sont pris en compte sans redéploiement.
 */
function loadRules() {
  if (!fs.existsSync(RULES_PATH)) {
    throw new Error(`test_rules.json introuvable à ${RULES_PATH}`);
  }
  const raw = fs.readFileSync(RULES_PATH, 'utf8');
  const rules = JSON.parse(raw);
  const required = [
    'campaign_name_match', 'target_cpa', 'kill_spend',
    'kill_ctr_link_min', 'kill_ctr_min_impressions',
    'extend_spend', 'extend_cpa_max',
    'graduate_min_purchases', 'graduate_cpa_max',
  ];
  for (const k of required) {
    if (rules[k] === undefined || rules[k] === null) {
      throw new Error(`test_rules.json : champ manquant "${k}"`);
    }
  }
  return rules;
}

// ------------------------------------------------------------
// Évaluation
// ------------------------------------------------------------

/**
 * Applique les règles à un ad agrégé. Ordre d'évaluation :
 *   KILL → GRADUATE → KILL_EXTENDED → CONTINUE
 *
 * Rationale : KILL couvre les cas 0-achat qui produiraient CPA infini
 * (donc KILL_EXTENDED serait trivialement vrai) ; on tranche avant.
 * GRADUATE et KILL_EXTENDED sont mutuellement exclusifs par construction
 * (cpa≤graduate_cpa_max ⇒ cpa≤35 < extend_cpa_max=45).
 */
function classify(ad, rules) {
  const {
    kill_spend, kill_ctr_link_min, kill_ctr_min_impressions,
    extend_spend, extend_cpa_max,
    graduate_min_purchases, graduate_cpa_max,
  } = rules;

  // KILL — pas d'achat malgré assez de dépense OU CTR link trop faible sur volume suffisant
  if (ad.purchases === 0 && ad.spend >= kill_spend) {
    return {
      verdict: 'KILL',
      reason: `0 achat pour ${ad.spend.toFixed(0)} € dépensés (≥ ${kill_spend} €)`,
    };
  }
  if (ad.impressions >= kill_ctr_min_impressions && ad.ctr_link < kill_ctr_link_min) {
    return {
      verdict: 'KILL',
      reason: `CTR link ${(ad.ctr_link * 100).toFixed(2)}% < ${(kill_ctr_link_min * 100).toFixed(2)}% sur ${ad.impressions.toLocaleString('fr-FR')} impressions (≥ ${kill_ctr_min_impressions.toLocaleString('fr-FR')})`,
    };
  }

  // GRADUATE — validation par la performance (avant KILL_EXTENDED car exclusifs)
  if (ad.purchases >= graduate_min_purchases && ad.cpa <= graduate_cpa_max) {
    return {
      verdict: 'GRADUATE',
      reason: `${ad.purchases} achats à CPA ${ad.cpa.toFixed(1)} € (≤ ${graduate_cpa_max} €), ≥ ${graduate_min_purchases} achats`,
    };
  }

  // KILL_EXTENDED — a eu son extended budget mais CPA reste hors seuil
  if (ad.spend >= extend_spend && ad.cpa > extend_cpa_max) {
    return {
      verdict: 'KILL_EXTENDED',
      reason: `${ad.spend.toFixed(0)} € dépensés (≥ ${extend_spend} €), CPA ${ad.cpa.toFixed(1)} € > ${extend_cpa_max} €`,
    };
  }

  return {
    verdict: 'CONTINUE',
    reason: `spend ${ad.spend.toFixed(0)} €, ${ad.purchases} achats, CPA ${ad.purchases > 0 ? ad.cpa.toFixed(1) + ' €' : '—'}`,
  };
}

/**
 * Retourne la liste des ads de la campagne test avec verdicts.
 * Agrège meta_ad_daily depuis MIN(day WHERE spend>0) par ad_id.
 * Filtre les campagnes par nom (LIKE %campaign_name_match%).
 */
function evaluate(rules) {
  const db = cache.getDb();
  const pattern = `%${rules.campaign_name_match}%`;

  // Sous-requête : pour chaque ad_id, le premier jour de dépense (spend > 0).
  // Puis agrège depuis ce jour inclus.
  const rows = db.prepare(`
    WITH first_spend AS (
      SELECT ad_id, MIN(day) AS first_day
      FROM meta_ad_daily
      WHERE spend > 0
        AND campaign_name LIKE ?
      GROUP BY ad_id
    )
    SELECT
      m.ad_id,
      MAX(m.ad_name)       AS ad_name,
      MAX(m.adset_id)      AS adset_id,
      MAX(m.adset_name)    AS adset_name,
      MAX(m.campaign_id)   AS campaign_id,
      MAX(m.campaign_name) AS campaign_name,
      MAX(m.creative_id)   AS creative_id,
      fs.first_day         AS first_spend_day,
      MAX(m.day)           AS last_day,
      COUNT(DISTINCT m.day) AS days_active,
      SUM(m.spend)         AS spend,
      SUM(m.impressions)   AS impressions,
      SUM(m.clicks)        AS clicks,
      SUM(m.link_clicks)   AS link_clicks,
      SUM(m.purchases)     AS purchases,
      SUM(m.revenue)       AS revenue,
      SUM(m.video_views_3s) AS video_views_3s,
      SUM(m.thruplays)     AS thruplays
    FROM meta_ad_daily m
    JOIN first_spend fs USING(ad_id)
    WHERE m.day >= fs.first_day
      AND m.campaign_name LIKE ?
    GROUP BY m.ad_id
    ORDER BY spend DESC
  `).all(pattern, pattern);

  const items = rows.map(r => {
    const spend = Number(r.spend || 0);
    const impressions = Number(r.impressions || 0);
    const clicks = Number(r.clicks || 0);
    const linkClicks = Number(r.link_clicks || 0);
    const purchases = Number(r.purchases || 0);
    const revenue = Number(r.revenue || 0);
    const videoViews3s = Number(r.video_views_3s || 0);
    const thruplays = Number(r.thruplays || 0);
    const cpa = purchases > 0 ? spend / purchases : Infinity;
    const ctr = impressions > 0 ? clicks / impressions : 0;
    const ctr_link = impressions > 0 ? linkClicks / impressions : 0;
    const roas = spend > 0 ? revenue / spend : 0;
    const isVideo = videoViews3s > 0;
    const hookRate = isVideo && impressions > 0 ? videoViews3s / impressions : null;
    const holdRate = isVideo && videoViews3s > 0 ? thruplays / videoViews3s : null;

    const metrics = {
      ad_id: r.ad_id,
      ad_name: r.ad_name,
      adset_name: r.adset_name,
      campaign_name: r.campaign_name,
      creative_id: r.creative_id,
      first_spend_day: r.first_spend_day,
      last_day: r.last_day,
      days_active: Number(r.days_active || 0),
      spend, impressions, clicks, link_clicks: linkClicks,
      purchases, revenue,
      cpa, ctr, ctr_link, roas,
      is_video: isVideo,
      hook_rate: hookRate,
      hold_rate: holdRate,
    };
    const decision = classify(metrics, rules);
    return { ...metrics, verdict: decision.verdict, reason: decision.reason };
  });

  const byVerdict = { KILL: 0, KILL_EXTENDED: 0, GRADUATE: 0, CONTINUE: 0 };
  for (const it of items) byVerdict[it.verdict]++;

  return {
    generatedAt: new Date().toISOString(),
    rules,
    total: items.length,
    byVerdict,
    items,
  };
}

// ------------------------------------------------------------
// Email
// ------------------------------------------------------------

function fmtEur(v) {
  return new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(v || 0);
}
function fmtPct(v, digits = 2) {
  if (v == null) return '—';
  return `${(v * 100).toFixed(digits)}%`;
}

const VERDICT_COLORS = {
  KILL: { bg: '#fee2e2', fg: '#991b1b' },
  KILL_EXTENDED: { bg: '#fef3c7', fg: '#92400e' },
  GRADUATE: { bg: '#d1fae5', fg: '#065f46' },
  CONTINUE: { bg: '#f3f4f6', fg: '#374151' },
};

function verdictBadge(v) {
  const c = VERDICT_COLORS[v] || VERDICT_COLORS.CONTINUE;
  return `<span style="display:inline-block;padding:3px 10px;border-radius:6px;background:${c.bg};color:${c.fg};font-weight:600;font-size:11px;letter-spacing:0.05em;">${v}</span>`;
}

function buildEmailHTML(report) {
  const { items, byVerdict, rules, generatedAt } = report;
  const actionable = items.filter(i => i.verdict !== 'CONTINUE');
  // Ordre d'affichage : KILL → KILL_EXTENDED → GRADUATE → CONTINUE (par spend desc dans chaque)
  const order = { KILL: 0, KILL_EXTENDED: 1, GRADUATE: 2, CONTINUE: 3 };
  const rows = [...items].sort((a, b) => order[a.verdict] - order[b.verdict] || b.spend - a.spend);

  const trs = rows.map(i => {
    const c = VERDICT_COLORS[i.verdict] || VERDICT_COLORS.CONTINUE;
    return `
      <tr style="border-bottom:1px solid #e5e7eb;">
        <td style="padding:8px 10px;">${verdictBadge(i.verdict)}</td>
        <td style="padding:8px 10px;font-family:monospace;font-size:11px;">${escapeHtml(i.ad_name || i.ad_id)}</td>
        <td style="padding:8px 10px;font-size:11px;color:#6b7280;">${escapeHtml(i.adset_name || '')}</td>
        <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">${i.days_active} j</td>
        <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">${fmtEur(i.spend)}</td>
        <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">${i.purchases}</td>
        <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">${i.purchases > 0 ? fmtEur(i.cpa) : '—'}</td>
        <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">${fmtPct(i.ctr_link)}</td>
        <td style="padding:8px 10px;text-align:right;font-variant-numeric:tabular-nums;">${i.is_video ? fmtPct(i.hook_rate) : '—'}</td>
        <td style="padding:8px 10px;font-size:11px;color:${c.fg};">${escapeHtml(i.reason)}</td>
      </tr>
    `;
  }).join('');

  return `
<!DOCTYPE html>
<html>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f6f7f9;color:#1a1a1a;padding:24px;">
  <div style="max-width:1100px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">
    <div style="background:#0f172a;color:#fff;padding:16px 24px;">
      <div style="font-size:16px;font-weight:600;">Test créas — verdicts du jour</div>
      <div style="font-size:12px;opacity:0.7;margin-top:2px;">Généré ${new Date(generatedAt).toLocaleString('fr-FR', { timeZone: 'Europe/Paris' })} · règles : campagnes contenant "${escapeHtml(rules.campaign_name_match)}"</div>
    </div>
    <div style="padding:20px 24px;">
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px;">
        <div style="background:#fee2e2;padding:12px;border-radius:8px;">
          <div style="font-size:11px;color:#991b1b;text-transform:uppercase;font-weight:600;">KILL</div>
          <div style="font-size:24px;font-weight:700;color:#991b1b;">${byVerdict.KILL}</div>
        </div>
        <div style="background:#fef3c7;padding:12px;border-radius:8px;">
          <div style="font-size:11px;color:#92400e;text-transform:uppercase;font-weight:600;">KILL_EXTENDED</div>
          <div style="font-size:24px;font-weight:700;color:#92400e;">${byVerdict.KILL_EXTENDED}</div>
        </div>
        <div style="background:#d1fae5;padding:12px;border-radius:8px;">
          <div style="font-size:11px;color:#065f46;text-transform:uppercase;font-weight:600;">GRADUATE</div>
          <div style="font-size:24px;font-weight:700;color:#065f46;">${byVerdict.GRADUATE}</div>
        </div>
        <div style="background:#f3f4f6;padding:12px;border-radius:8px;">
          <div style="font-size:11px;color:#374151;text-transform:uppercase;font-weight:600;">CONTINUE</div>
          <div style="font-size:24px;font-weight:700;color:#374151;">${byVerdict.CONTINUE}</div>
        </div>
      </div>
      <div style="font-size:12px;color:#6b7280;margin-bottom:12px;">
        ${actionable.length} ads actionnables sur ${items.length} au total.
        Ce rapport ne fait aucune action sur Meta — les recommandations sont à exécuter manuellement.
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:12px;">
        <thead>
          <tr style="background:#fafbfc;text-align:left;">
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;">Verdict</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;">Ad</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;">Adset</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;text-align:right;">Jours</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;text-align:right;">Spend</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;text-align:right;">Achats</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;text-align:right;">CPA</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;text-align:right;">CTR link</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;text-align:right;">Hook rate</th>
            <th style="padding:10px;font-size:10px;text-transform:uppercase;color:#6b7280;">Raison</th>
          </tr>
        </thead>
        <tbody>${trs}</tbody>
      </table>
    </div>
  </div>
</body>
</html>`;
}

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

/**
 * Génère le rapport et envoie l'email SI au moins un item est actionnable
 * (KILL / KILL_EXTENDED / GRADUATE). Réutilise SENDGRID_API_KEY /
 * REPORT_EMAIL_TO / REPORT_EMAIL_FROM comme daily-report.js.
 *
 * Retourne { report, sent, reason }.
 */
async function runDailyEmail({ dryRun = false } = {}) {
  const rules = loadRules();
  const report = evaluate(rules);
  const actionableCount = report.byVerdict.KILL + report.byVerdict.KILL_EXTENDED + report.byVerdict.GRADUATE;
  if (actionableCount === 0) {
    return { report, sent: false, reason: 'no_actionable_verdict' };
  }

  const html = buildEmailHTML(report);

  if (dryRun) {
    return { report, sent: false, html, reason: 'dry_run' };
  }

  const sgApiKey = process.env.SENDGRID_API_KEY;
  const emailTo = process.env.REPORT_EMAIL_TO;
  const emailFrom = process.env.REPORT_EMAIL_FROM;
  if (!sgApiKey || !emailTo || !emailFrom) {
    return { report, sent: false, reason: 'sendgrid_not_configured' };
  }
  sgMail.setApiKey(sgApiKey);
  const subject = `Bandit — Test créas — ${report.byVerdict.KILL} KILL · ${report.byVerdict.KILL_EXTENDED} KILL_EXTENDED · ${report.byVerdict.GRADUATE} GRADUATE`;
  await sgMail.send({ to: emailTo, from: emailFrom, subject, html });
  return { report, sent: true, subject };
}

module.exports = {
  loadRules,
  evaluate,
  classify,
  buildEmailHTML,
  runDailyEmail,
};
