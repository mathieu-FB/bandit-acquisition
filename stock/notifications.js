// ============================================================
// STOCK NOTIFICATIONS — daily + weekly emails via SendGrid.
//
//   Daily (lun-sam 8h Europe/Paris) : envoie uniquement si le niveau
//     d'un SKU s'est AGGRAVÉ depuis la dernière exécution (RUPTURE
//     nouveau, CRITIQUE nouveau, etc.). Pas d'email si aucune diff.
//
//   Weekly (lundi 7h30 Europe/Paris) : récap complet — synthèse familles
//     + top 30 SKU actionnables.
// ============================================================

const sgMail = require('@sendgrid/mail');
const stockDb = require('./db');
const stockMoteur = require('./moteur');

// Sévérité croissante — utile pour détecter aggravation vs simple mouvement.
const NIVEAU_SEVERITE = {
  OK: 0,
  DONNEE_MANQUANTE: 1,
  A_COMMANDER: 2,
  URGENT: 3,
  CRITIQUE: 4,
  RUPTURE: 5,
};

const NIVEAU_LABEL = {
  RUPTURE: 'Rupture',
  CRITIQUE: 'Critique',
  URGENT: 'Urgent',
  A_COMMANDER: 'À commander',
  DONNEE_MANQUANTE: 'Data manquante',
  OK: 'OK',
};

const NIVEAU_COLOR = {
  RUPTURE: '#991b1b',
  CRITIQUE: '#b45309',
  URGENT: '#b91c1c',
  A_COMMANDER: '#1e40af',
  DONNEE_MANQUANTE: '#6b7280',
  OK: '#065f46',
};

function fmtEur(v) {
  if (v == null) return '—';
  return new Intl.NumberFormat('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(v);
}
function fmtInt(v) {
  if (v == null) return '—';
  return new Intl.NumberFormat('fr-FR').format(Math.round(v));
}
function fmtDateShort(iso) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: '2-digit' });
}

function isAggravated(prev, current) {
  // Une transition vers OK n'est pas une "aggravation".
  if (current === 'OK') return false;
  const sPrev = NIVEAU_SEVERITE[prev] ?? 0;
  const sCur = NIVEAU_SEVERITE[current] ?? 0;
  return sCur > sPrev;
}

// ------------------------------------------------------------
// SendGrid wrapper
// ------------------------------------------------------------
async function sendEmail({ to, subject, html }) {
  const apiKey = process.env.SENDGRID_API_KEY;
  const from = process.env.REPORT_EMAIL_FROM;
  if (!apiKey || !from) {
    return { sent: false, reason: 'SendGrid non configuré (SENDGRID_API_KEY / REPORT_EMAIL_FROM manquant)' };
  }
  if (!to) {
    return { sent: false, reason: 'Aucun destinataire' };
  }
  sgMail.setApiKey(apiKey);
  await sgMail.send({ to, from, subject, html });
  return { sent: true, to, subject };
}

function getRecipient() {
  return stockDb.getParametreGlobal('alerte_email_destinataire') || process.env.REPORT_EMAIL_TO || null;
}

// ------------------------------------------------------------
// Détection des DIFFs (aggravations depuis dernier run)
// ------------------------------------------------------------
function computeDiffs() {
  // 1. Snapshot état actuel avant le calcul
  const beforeBySku = {};
  for (const a of stockDb.listAllAlertes()) {
    beforeBySku[a.sku] = a.niveau;
  }
  // 2. Calcul actuel via moteur (dryRun — pas de persistance ici, on gère ça au niveau appelant)
  const report = stockMoteur.runAll({ dryRun: true });
  // 3. Diff = aggravations uniquement
  const aggravations = [];
  for (const d of report.details) {
    const prev = beforeBySku[d.sku];
    if (prev === d.niveau) continue;
    if (isAggravated(prev, d.niveau)) {
      const ref = stockDb.getReferentielSku(d.sku);
      aggravations.push({
        sku: d.sku,
        famille: d.famille,
        animal: d.animal,
        nom_court: ref ? ref.nom_court : null,
        image_url: ref ? ref.image_url : null,
        niveau_precedent: prev || 'nouveau',
        niveau: d.niveau,
        stockActuel: d.stockActuel,
        proposition_qte: d.proposition_qte,
        proposition_montant: d.proposition_montant,
        dateRuptureEstimee: d.dateRuptureEstimee,
      });
    }
  }
  aggravations.sort((a, b) => (NIVEAU_SEVERITE[b.niveau] || 0) - (NIVEAU_SEVERITE[a.niveau] || 0));
  return { report, aggravations };
}

// ------------------------------------------------------------
// HTML — Daily (aggravations uniquement)
// ------------------------------------------------------------
function buildDailyHtml(aggravations, report) {
  const groupsByNiveau = {};
  for (const a of aggravations) {
    if (!groupsByNiveau[a.niveau]) groupsByNiveau[a.niveau] = [];
    groupsByNiveau[a.niveau].push(a);
  }
  const orderedLevels = ['RUPTURE', 'CRITIQUE', 'URGENT', 'A_COMMANDER'];
  const groupBlocks = orderedLevels.map(niv => {
    const items = groupsByNiveau[niv] || [];
    if (items.length === 0) return '';
    const rows = items.map(i => `
      <tr>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;font-family:monospace;font-size:12px;">${i.sku}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;font-size:13px;">${i.nom_court || '—'}<br><span style="color:#6b7280;font-size:11px;">${i.famille || ''} / ${i.animal || ''}</span></td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;font-size:11px;color:#6b7280;">${i.niveau_precedent === 'nouveau' ? 'nouveau' : NIVEAU_LABEL[i.niveau_precedent] || i.niveau_precedent}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:13px;">${fmtInt(i.stockActuel)}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:13px;">${fmtDateShort(i.dateRuptureEstimee ? new Date(i.dateRuptureEstimee).toISOString() : null)}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:13px;font-weight:600;">${i.proposition_qte ? fmtInt(i.proposition_qte) + ' u' : '—'}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:13px;">${i.proposition_montant ? fmtEur(i.proposition_montant) : '—'}</td>
      </tr>
    `).join('');
    return `
      <div style="margin-bottom:20px;">
        <h3 style="margin:0 0 10px;padding:8px 12px;color:#fff;background:${NIVEAU_COLOR[niv]};border-radius:6px;font-size:14px;display:inline-block;">${NIVEAU_LABEL[niv]} — ${items.length} SKU</h3>
        <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;">
          <thead>
            <tr style="background:#fafbfc;">
              <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">SKU</th>
              <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Produit</th>
              <th style="padding:8px 12px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Avant</th>
              <th style="padding:8px 12px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Stock</th>
              <th style="padding:8px 12px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Rupture</th>
              <th style="padding:8px 12px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Proposition</th>
              <th style="padding:8px 12px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Montant</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>`;
  }).join('');

  const kpis = [
    { label: 'Nouvelles alertes', value: aggravations.length },
    { label: 'Rupture', value: report.byNiveau.RUPTURE || 0, color: NIVEAU_COLOR.RUPTURE },
    { label: 'Critique', value: report.byNiveau.CRITIQUE || 0, color: NIVEAU_COLOR.CRITIQUE },
    { label: 'Urgent', value: report.byNiveau.URGENT || 0, color: NIVEAU_COLOR.URGENT },
    { label: 'À commander', value: report.byNiveau.A_COMMANDER || 0, color: NIVEAU_COLOR.A_COMMANDER },
  ];
  const kpiHtml = kpis.map(k => `
    <div style="display:inline-block;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:12px 16px;margin-right:10px;margin-bottom:10px;min-width:120px;">
      <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;font-weight:600;">${k.label}</div>
      <div style="font-size:22px;font-weight:700;color:${k.color || '#1a1a1a'};margin-top:4px;">${k.value}</div>
    </div>
  `).join('');

  return `
    <div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;background:#f6f7f9;padding:24px;color:#1a1a1a;">
      <div style="max-width:820px;margin:0 auto;">
        <h1 style="margin:0 0 6px;font-size:18px;">Bandit — Alertes stock du ${new Date().toLocaleDateString('fr-FR', { weekday: 'long', day: '2-digit', month: 'long', year: 'numeric' })}</h1>
        <p style="margin:0 0 20px;color:#6b7280;font-size:13px;">Seules les <strong>aggravations depuis la dernière exécution</strong> sont listées. Un lien direct vers le dashboard est disponible en bas.</p>
        <div style="margin-bottom:24px;">${kpiHtml}</div>
        ${groupBlocks || '<div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:24px;text-align:center;color:#6b7280;">Aucune aggravation depuis hier.</div>'}
        <div style="margin-top:24px;padding-top:16px;border-top:1px solid #e5e7eb;font-size:12px;color:#6b7280;">
          <a href="${process.env.DASHBOARD_URL || 'https://web-production-1b6dc.up.railway.app'}/stock.html" style="color:#0f172a;">→ Ouvrir la vue Alertes complète</a>
        </div>
      </div>
    </div>`;
}

// ------------------------------------------------------------
// HTML — Weekly recap (top actionables + synthèse familles)
// ------------------------------------------------------------
function buildWeeklyHtml(report, top, totalMontant, familles) {
  const rows = top.map(d => {
    const ref = stockDb.getReferentielSku(d.sku);
    const nom = ref ? ref.nom_court : '—';
    return `
      <tr>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-family:monospace;font-size:11px;">${d.sku}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:12px;">${nom}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:11px;color:#6b7280;">${d.famille || ''}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:11px;"><span style="background:${NIVEAU_COLOR[d.niveau]};color:#fff;padding:2px 6px;border-radius:8px;font-size:10px;font-weight:600;">${NIVEAU_LABEL[d.niveau]}</span></td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:12px;">${fmtInt(d.stockActuel)}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:12px;">${fmtDateShort(d.dateRuptureEstimee)}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:12px;font-weight:600;">${fmtInt(d.proposition_qte)}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:12px;">${fmtEur(d.proposition_montant)}</td>
      </tr>`;
  }).join('');

  const famRows = familles.slice(0, 15).map(f => {
    const alertes = ['RUPTURE', 'CRITIQUE', 'URGENT']
      .filter(k => f.alertes[k] > 0)
      .map(k => `<span style="background:${NIVEAU_COLOR[k]};color:#fff;padding:1px 5px;border-radius:6px;font-size:10px;font-weight:600;">${f.alertes[k]}</span>`)
      .join(' ');
    return `
      <tr>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:12px;"><strong>${f.famille}</strong></td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:11px;color:#6b7280;">${f.animal}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:11px;">${f.nb_sku}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:11px;">${fmtInt(f.stock_total)}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;font-size:11px;">${alertes || '<span style="color:#6b7280;">—</span>'}</td>
        <td style="padding:6px 10px;border-bottom:1px solid #e5e7eb;text-align:right;font-size:12px;font-weight:600;">${fmtEur(f.proposition_montant)}</td>
      </tr>`;
  }).join('');

  const kpis = [
    { label: 'SKU actionnables', value: report.details.filter(d => !['OK', 'DONNEE_MANQUANTE'].includes(d.niveau)).length },
    { label: 'Montant total propositions', value: fmtEur(totalMontant) },
    { label: 'Rupture', value: report.byNiveau.RUPTURE || 0, color: NIVEAU_COLOR.RUPTURE },
    { label: 'Critique', value: report.byNiveau.CRITIQUE || 0, color: NIVEAU_COLOR.CRITIQUE },
    { label: 'Urgent', value: report.byNiveau.URGENT || 0, color: NIVEAU_COLOR.URGENT },
    { label: 'À commander', value: report.byNiveau.A_COMMANDER || 0, color: NIVEAU_COLOR.A_COMMANDER },
  ];
  const kpiHtml = kpis.map(k => `
    <div style="display:inline-block;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:12px 16px;margin-right:10px;margin-bottom:10px;min-width:130px;">
      <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.05em;font-weight:600;">${k.label}</div>
      <div style="font-size:20px;font-weight:700;color:${k.color || '#1a1a1a'};margin-top:4px;">${k.value}</div>
    </div>
  `).join('');

  return `
    <div style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;background:#f6f7f9;padding:24px;color:#1a1a1a;">
      <div style="max-width:960px;margin:0 auto;">
        <h1 style="margin:0 0 6px;font-size:18px;">Bandit — Récap stock hebdomadaire (${new Date().toLocaleDateString('fr-FR', { weekday: 'long', day: '2-digit', month: 'long', year: 'numeric' })})</h1>
        <p style="margin:0 0 20px;color:#6b7280;font-size:13px;">Vue d'ensemble complète : familles, top ${top.length} SKU actionnables, montant global.</p>
        <div style="margin-bottom:24px;">${kpiHtml}</div>

        <h2 style="font-size:15px;margin:20px 0 10px;">Synthèse par famille (top 15)</h2>
        <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;margin-bottom:24px;">
          <thead>
            <tr style="background:#fafbfc;">
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Famille</th>
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Animal</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Nb SKU</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Stock (u)</th>
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Alertes</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Montant propositions</th>
            </tr>
          </thead>
          <tbody>${famRows}</tbody>
        </table>

        <h2 style="font-size:15px;margin:20px 0 10px;">Top ${top.length} SKU actionnables (par sévérité + date rupture)</h2>
        <table style="width:100%;border-collapse:collapse;background:#fff;border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;">
          <thead>
            <tr style="background:#fafbfc;">
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">SKU</th>
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Produit</th>
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Famille</th>
              <th style="padding:8px 10px;text-align:left;font-size:11px;color:#6b7280;text-transform:uppercase;">Niveau</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Stock</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Rupture</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Prop (u)</th>
              <th style="padding:8px 10px;text-align:right;font-size:11px;color:#6b7280;text-transform:uppercase;">Prop (€)</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>

        <div style="margin-top:24px;padding-top:16px;border-top:1px solid #e5e7eb;font-size:12px;color:#6b7280;">
          <a href="${process.env.DASHBOARD_URL || 'https://web-production-1b6dc.up.railway.app'}/stock.html" style="color:#0f172a;">→ Ouvrir la vue Alertes complète</a>
        </div>
      </div>
    </div>`;
}

// ------------------------------------------------------------
// Entrypoints
// ------------------------------------------------------------
async function runDailyAlerts({ dryRun = false } = {}) {
  const { report, aggravations } = computeDiffs();
  // Persist les nouveaux niveaux uniquement en mode réel
  if (!dryRun) {
    const upserts = report.details.map(d => ({
      sku: d.sku,
      niveau: d.niveau,
      date_rupture_estimee: d.dateRuptureEstimee,
      proposition_qte: d.proposition_qte,
      proposition_montant: d.proposition_montant,
      message: null,
    }));
    stockDb.upsertAlertesEtatBulk(upserts);
  }
  const to = getRecipient();
  if (aggravations.length === 0) {
    return { sent: false, reason: 'Aucune aggravation depuis la dernière exécution', diffsCount: 0, dryRun };
  }
  const subject = `[Bandit Stock] ${aggravations.length} nouvelle${aggravations.length > 1 ? 's' : ''} alerte${aggravations.length > 1 ? 's' : ''} — ${new Date().toLocaleDateString('fr-FR')}`;
  const html = buildDailyHtml(aggravations, report);
  if (dryRun) {
    return { dryRun: true, subject, html, diffsCount: aggravations.length, aggravations, to };
  }
  const result = await sendEmail({ to, subject, html });
  return { ...result, diffsCount: aggravations.length };
}

async function runWeeklyRecap({ dryRun = false, topN = 30 } = {}) {
  const report = stockMoteur.runAll({ dryRun: false }); // persist en cron réel
  // Actionable = tout sauf OK et DONNEE_MANQUANTE
  const actionable = report.details.filter(d => !['OK', 'DONNEE_MANQUANTE'].includes(d.niveau));
  actionable.sort((a, b) => {
    const sA = NIVEAU_SEVERITE[a.niveau] || 0;
    const sB = NIVEAU_SEVERITE[b.niveau] || 0;
    if (sA !== sB) return sB - sA;
    const rA = a.dateRuptureEstimee ? new Date(a.dateRuptureEstimee).getTime() : Infinity;
    const rB = b.dateRuptureEstimee ? new Date(b.dateRuptureEstimee).getTime() : Infinity;
    return rA - rB;
  });
  const top = actionable.slice(0, topN);
  const totalMontant = actionable.reduce((s, d) => s + (d.proposition_montant || 0), 0);
  // Famille synthesis — reuse the moteur.runAll details
  const familleAgg = {};
  for (const d of report.details) {
    const key = `${d.famille || '(sans)'}|${d.animal || '(sans)'}`;
    if (!familleAgg[key]) {
      familleAgg[key] = {
        famille: d.famille || '(sans)',
        animal: d.animal || '(sans)',
        nb_sku: 0,
        stock_total: 0,
        alertes: { RUPTURE: 0, CRITIQUE: 0, URGENT: 0, A_COMMANDER: 0 },
        proposition_montant: 0,
      };
    }
    const g = familleAgg[key];
    g.nb_sku++;
    g.stock_total += d.stockActuel || 0;
    if (['RUPTURE', 'CRITIQUE', 'URGENT', 'A_COMMANDER'].includes(d.niveau)) {
      g.alertes[d.niveau] += 1;
    }
    if (d.proposition_montant) g.proposition_montant += d.proposition_montant;
  }
  const familles = Object.values(familleAgg).sort((a, b) => b.proposition_montant - a.proposition_montant);

  const to = getRecipient();
  const subject = `[Bandit Stock] Récap hebdo — ${actionable.length} SKU · ${fmtEur(totalMontant)}`;
  const html = buildWeeklyHtml(report, top, totalMontant, familles);
  if (dryRun) {
    return { dryRun: true, subject, html, actionableCount: actionable.length, totalMontant, to };
  }
  const result = await sendEmail({ to, subject, html });
  return { ...result, actionableCount: actionable.length, totalMontant };
}

module.exports = {
  runDailyAlerts,
  runWeeklyRecap,
  computeDiffs,
  sendEmail,
  NIVEAU_SEVERITE,
};
