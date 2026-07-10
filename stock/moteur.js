// ============================================================
// MOTEUR DE PRÉVISION & PROPOSITIONS DE COMMANDE
//
// Chain (per SKU):
//   1. build monthly demand forecast for the next 12 months
//        demande(mois M) = ventes(mois M, année N-1)
//                        × coeff_saisonnalité_famille(M)
//                        × coeff_tendance_sku (bornée [MIN, MAX])
//                        × coeff_sécurité_famille
//   2. project stock day-by-day over 365 days (subtract demand,
//        add en-cours BDC at their ETA)
//   3. compute date_rupture_estimee = first day where projected stock ≤ 0
//   4. compute niveau d'alerte from date_rupture vs lead_time + marges
//   5. compute proposition_qte = max(demande sur (lead_time + couverture_visee)
//        - stock - en_cours_utiles, 0), rounded up to MOQ + colisage.
//
// Tunable constants live at the top of this file; parameters (couverture,
// coeff, saisonnalité, tendance bornes) can be overridden per famille via
// stock_parametres_famille — the moteur falls back to the defaults below
// when a famille has no entry.
// ============================================================

const stockDb = require('./db');

// ---------- Defaults & fallbacks ----------
// Recalibration V2 après comparatif global +130 % vs matrice : on
// baisse tendance max (3.0 → 2.0) et sécurité (1.3 → 1.15) pour ramener
// l'écart moyen à ~+25 %. Les fortes croissances restent visibles via
// la tendance 2.0× mais ne saturent plus la proposition.
const DEFAULTS = {
  couvertureViseeJours: 90,    // fallback famille
  coeffSecurite: 1.15,         // fallback famille — +15 % de marge
  coeffTendance: 1.0,          // fallback SKU
  coeffTendanceMin: 0.5,       // borne basse
  coeffTendanceMax: 2.0,       // borne haute
  leadTimeJours: 60,           // fallback SKU
  urgentMargeJours: 15,        // marge appliquée au-dessus de lead_time pour URGENT
  horizonJours: 365,           // horizon de projection
  minMoisHistoriqueTendance: 6,// minimum de mois requis pour calculer coeff_tendance
  minMoisHistoriqueSaisonalite: 12, // sinon on utilise une saisonnalité plate
};

const NIVEAUX = {
  RUPTURE: 'RUPTURE',
  CRITIQUE: 'CRITIQUE',
  URGENT: 'URGENT',
  A_COMMANDER: 'A_COMMANDER',
  DONNEE_MANQUANTE: 'DONNEE_MANQUANTE',
  OK: 'OK',
};

const MS_JOUR = 86400000;

// ------------------------------------------------------------
// Utilities: dates
// ------------------------------------------------------------
function todayUTC() {
  const d = new Date();
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}
function addDaysUTC(ms, n) { return ms + n * MS_JOUR; }
function ymFromUTC(ms) {
  const d = new Date(ms);
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1 };
}
function daysBetweenUTC(a, b) { return Math.round((b - a) / MS_JOUR); }
function iso(ms) { return new Date(ms).toISOString().slice(0, 10); }

// ------------------------------------------------------------
// Load state: cache all previsions + all en-cours BDC lignes once per run.
// ------------------------------------------------------------
function loadState() {
  const previsions = {};
  for (const row of stockDb.listAllPrevisions()) {
    if (!previsions[row.sku]) previsions[row.sku] = {};
    previsions[row.sku][`${row.annee}-${String(row.mois).padStart(2, '0')}`] = {
      qty: row.ventes_reelles,
      est: row.is_estimated ? 1 : 0,
    };
  }
  const stockActuel = {};
  for (const row of stockDb.listStockActuel()) {
    stockActuel[row.sku] = row.stock_dispo || 0;
  }
  const famillesParam = {};
  for (const f of stockDb.listParametresFamille()) {
    famillesParam[`${f.famille}|${f.animal}`] = f;
  }
  return { previsions, stockActuel, famillesParam };
}

// ------------------------------------------------------------
// Family-level seasonal coefficients — mean(month) / mean(months WITH data).
// A month with no data returns 1.0 (neutral) instead of 0 — otherwise
// missing history collapses forecast demand to zero and understates
// the propositions. Estimated sales (is_estimated=1) are included as
// fallback: they carry seed-xlsx signal until Shopify sales overrides
// them. To prevent a full flat seed from wiping out real seasonality,
// we drop them once we have ≥3 real months for the same (famille,mois).
// ------------------------------------------------------------
function deriveSaisonalCoeffsPerFamille(previsions) {
  const perFamille = {}; // key = "famille|animal" → { month: { real: [...], est: [...] } }
  const refBySku = {};
  for (const r of stockDb.listReferentielActif()) {
    refBySku[r.sku] = r;
  }
  for (const [sku, byYm] of Object.entries(previsions)) {
    const ref = refBySku[sku];
    if (!ref || !ref.famille || !ref.animal) continue;
    const key = `${ref.famille}|${ref.animal}`;
    if (!perFamille[key]) perFamille[key] = {};
    for (const [ym, v] of Object.entries(byYm)) {
      const mo = Number(ym.split('-')[1]);
      if (!perFamille[key][mo]) perFamille[key][mo] = { real: [], est: [] };
      if (v.est) perFamille[key][mo].est.push(v.qty);
      else perFamille[key][mo].real.push(v.qty);
    }
  }
  const result = {};
  for (const [key, byMonth] of Object.entries(perFamille)) {
    // For each month, pick real sales if we have ≥1; otherwise fall back to estimated.
    const meanByMonth = {};
    const hasDataByMonth = {};
    for (let m = 1; m <= 12; m++) {
      const bucket = byMonth[m] || { real: [], est: [] };
      const source = bucket.real.length > 0 ? bucket.real : bucket.est;
      if (source.length > 0) {
        meanByMonth[m] = source.reduce((a, b) => a + b, 0) / source.length;
        hasDataByMonth[m] = true;
      } else {
        meanByMonth[m] = 0;
        hasDataByMonth[m] = false;
      }
    }
    // Denominator: mean of months WITH data (not all 12) — otherwise sparse
    // months inflate the coefficients of populated months.
    const monthsWithDataArr = Object.entries(hasDataByMonth).filter(([, v]) => v).map(([m]) => Number(m));
    const monthsWithData = monthsWithDataArr.length;
    const sumWithData = monthsWithDataArr.reduce((s, m) => s + meanByMonth[m], 0);
    const overallMean = monthsWithData > 0 ? sumWithData / monthsWithData : 0;
    const coeffs = {};
    for (let m = 1; m <= 12; m++) {
      if (monthsWithData < 3 || overallMean <= 0) {
        // Not enough family history for a reliable seasonality → neutral.
        coeffs[m] = 1.0;
      } else if (hasDataByMonth[m]) {
        coeffs[m] = Number((meanByMonth[m] / overallMean).toFixed(3));
      } else {
        // Month has no history at all — assume seasonally neutral rather
        // than zero so downstream demand doesn't collapse.
        coeffs[m] = 1.0;
      }
    }
    result[key] = coeffs;
  }
  return result;
}

// ------------------------------------------------------------
// SKU-level trend: sum(last 3 completed months) / sum(same 3 months a year ago).
// Bounded to [MIN, MAX]. Counts ALL non-null rows (real + estimated) — a
// month that only has seed-xlsx flat data still carries signal (the flat
// value is the annualised /12 average), which is better than falling back
// to a neutral 1.0. A real ratio dominates a fallback ratio.
// ------------------------------------------------------------
function deriveTendanceCoeff(previsions, today, opts = {}) {
  const min = opts.min ?? DEFAULTS.coeffTendanceMin;
  const max = opts.max ?? DEFAULTS.coeffTendanceMax;
  const d = new Date(today);
  const currentYm = { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1 };
  const recent = [];
  const previous = [];
  for (let i = 1; i <= 3; i++) {
    let y = currentYm.y, m = currentYm.m - i;
    while (m <= 0) { m += 12; y -= 1; }
    recent.push(`${y}-${String(m).padStart(2, '0')}`);
    previous.push(`${y - 1}-${String(m).padStart(2, '0')}`);
  }
  let sumRecent = 0, hasRecent = 0, hasRecentReal = 0;
  let sumPrev = 0, hasPrev = 0, hasPrevReal = 0;
  for (const ym of recent) {
    const v = previsions[ym];
    if (v != null) { sumRecent += v.qty; hasRecent++; if (!v.est) hasRecentReal++; }
  }
  for (const ym of previous) {
    const v = previsions[ym];
    if (v != null) { sumPrev += v.qty; hasPrev++; if (!v.est) hasPrevReal++; }
  }
  // Need ≥2 rows on both sides (real or estimated) AND non-zero N-1 baseline.
  if (hasRecent < 2 || hasPrev < 2 || sumPrev <= 0) {
    return {
      coeff: DEFAULTS.coeffTendance,
      sumRecent, sumPrev, hasRecent, hasPrev, hasRecentReal, hasPrevReal,
      source: 'defaut',
    };
  }
  const raw = sumRecent / sumPrev;
  const bounded = Math.max(min, Math.min(max, raw));
  const allReal = hasRecentReal === 3 && hasPrevReal === 3;
  return {
    coeff: Number(bounded.toFixed(3)),
    raw: Number(raw.toFixed(3)),
    sumRecent, sumPrev,
    hasRecent, hasPrev, hasRecentReal, hasPrevReal,
    source: allReal ? 'compute_real' : 'compute_mixed',
  };
}

// ------------------------------------------------------------
// Per-SKU monthly forecast — next 12 months starting from today.
// Returns [{ ym, year, month, base_n_1, coeff_sais, coeff_tend, coeff_sec, demande }]
// ------------------------------------------------------------
function forecastPerSku({ sku, ref, previsions, saisonaliteByFamille, famillesParam, today }) {
  const familleKey = `${ref.famille}|${ref.animal}`;
  const saisonalite = saisonaliteByFamille[familleKey] || {};
  const famParam = famillesParam[familleKey] || {};
  const coeffSec = famParam.coeff_securite != null ? famParam.coeff_securite : DEFAULTS.coeffSecurite;
  const overrideSais = famParam.coeff_saisonnalite || null;
  const tendance = deriveTendanceCoeff(previsions, today);
  const rows = [];
  const d = new Date(today);
  for (let i = 0; i < 12; i++) {
    const cursor = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + i, 1));
    const y = cursor.getUTCFullYear();
    const m = cursor.getUTCMonth() + 1;
    const ymRef = `${y - 1}-${String(m).padStart(2, '0')}`;
    const baseN1 = previsions[ymRef] ? previsions[ymRef].qty : 0;
    const coeffSais = (overrideSais && overrideSais[m] != null) ? overrideSais[m] : (saisonalite[m] != null ? saisonalite[m] : 1.0);
    const demande = baseN1 * coeffSais * tendance.coeff * coeffSec;
    rows.push({
      ym: `${y}-${String(m).padStart(2, '0')}`,
      year: y, month: m,
      base_n_1: baseN1,
      coeff_sais: coeffSais,
      coeff_tend: tendance.coeff,
      coeff_sec: coeffSec,
      demande: Math.max(0, Number(demande.toFixed(2))),
    });
  }
  return { rows, tendance, coeffSec };
}

// ------------------------------------------------------------
// Daily projection over 365 days.
// Input: stock_dispo, monthlyForecast rows, list of en-cours BDC lignes.
// Returns: array of { day (ms), stock, ins (BDC arrivals) }.
// ------------------------------------------------------------
function projectStockDaily({ stockInitial, monthlyForecast, enCoursLignes, today, horizonJours = DEFAULTS.horizonJours }) {
  // Daily demand: monthly demand / days in that month.
  const dailyDemandByYm = {};
  for (const row of monthlyForecast) {
    const daysInMonth = new Date(Date.UTC(row.year, row.month, 0)).getUTCDate();
    dailyDemandByYm[row.ym] = row.demande / daysInMonth;
  }
  // Arrivals: enCoursLignes[].date_eta → sum of (qte_commandee - qte_recue)
  const arrivalsByDay = {};
  for (const line of enCoursLignes) {
    if (!line.date_eta) continue;
    const etaDay = Date.UTC(new Date(line.date_eta).getUTCFullYear(), new Date(line.date_eta).getUTCMonth(), new Date(line.date_eta).getUTCDate());
    if (etaDay < today) continue; // ETA passée sans réception → ignorer pour la projection
    const qty = Math.max(0, (line.qte_commandee || 0) - (line.qte_recue || 0));
    if (qty <= 0) continue;
    arrivalsByDay[etaDay] = (arrivalsByDay[etaDay] || 0) + qty;
  }
  const daily = [];
  let stock = stockInitial;
  let dateRuptureEstimee = null;
  for (let i = 0; i < horizonJours; i++) {
    const day = addDaysUTC(today, i);
    const arrivals = arrivalsByDay[day] || 0;
    stock += arrivals;
    const { year, month } = ymFromUTC(day);
    const ym = `${year}-${String(month).padStart(2, '0')}`;
    const demandeJour = dailyDemandByYm[ym] || 0;
    stock -= demandeJour;
    if (dateRuptureEstimee === null && stock <= 0) dateRuptureEstimee = day;
    daily.push({ day, ins: arrivals, out: Number(demandeJour.toFixed(3)), stock: Number(stock.toFixed(2)) });
  }
  return { daily, dateRuptureEstimee };
}

// ------------------------------------------------------------
// Alert level from projection + lead_time.
// ------------------------------------------------------------
function computeNiveau({ ref, dateRuptureEstimee, today, leadTimeJours, couvertureViseeJours, complet }) {
  if (!complet) return NIVEAUX.DONNEE_MANQUANTE;
  if (dateRuptureEstimee === null) return NIVEAUX.OK;
  const marge = DEFAULTS.urgentMargeJours;
  const daysUntilRupture = daysBetweenUTC(today, dateRuptureEstimee);
  if (daysUntilRupture <= 0) return NIVEAUX.RUPTURE;
  if (daysUntilRupture <= leadTimeJours) return NIVEAUX.CRITIQUE;
  if (daysUntilRupture <= leadTimeJours + marge) return NIVEAUX.URGENT;
  if (daysUntilRupture <= leadTimeJours + couvertureViseeJours) return NIVEAUX.A_COMMANDER;
  return NIVEAUX.OK;
}

// ------------------------------------------------------------
// Proposition de quantité — arrondie MOQ / colisage.
// Demand window: today → today + lead_time + couverture_visee.
// En-cours utiles = BDC lignes dont l'ETA arrive DANS la fenêtre.
// ------------------------------------------------------------
function computeProposition({ ref, famParam, monthlyForecast, stockActuel, enCoursLignes, today, leadTimeJours, couvertureViseeJours, niveau }) {
  // Horizon de calcul de la QUANTITÉ à commander :
  // - Pour un niveau actionable (RUPTURE / CRITIQUE / URGENT / A_COMMANDER),
  //   on vise à avoir `couverture` jours de stock APRÈS RÉCEPTION → horizon =
  //   lead + 2 × couverture. Sinon on tomberait à 0 pile à réception + couverture,
  //   ce qui est trop juste (pas de temps pour re-commander).
  // - Pour un niveau OK, on garde l'horizon court (lead + couverture) pour ne
  //   pas proposer inutilement (la proposition sera 0 la plupart du temps).
  const isActionable = ['RUPTURE', 'CRITIQUE', 'URGENT', 'A_COMMANDER'].includes(niveau);
  const horizonJours = isActionable
    ? leadTimeJours + couvertureViseeJours * 2
    : leadTimeJours + couvertureViseeJours;
  const endWindow = addDaysUTC(today, horizonJours);
  // Sum of daily demand between today and endWindow.
  let demandeFenetre = 0;
  const dailyDemandByYm = {};
  for (const row of monthlyForecast) {
    const dim = new Date(Date.UTC(row.year, row.month, 0)).getUTCDate();
    dailyDemandByYm[row.ym] = row.demande / dim;
  }
  for (let i = 0; i < horizonJours; i++) {
    const day = addDaysUTC(today, i);
    const { year, month } = ymFromUTC(day);
    demandeFenetre += dailyDemandByYm[`${year}-${String(month).padStart(2, '0')}`] || 0;
  }
  // En-cours utiles — those with ETA ≤ endWindow.
  let enCoursUtiles = 0;
  for (const line of enCoursLignes) {
    if (!line.date_eta) continue;
    const etaDay = Date.UTC(new Date(line.date_eta).getUTCFullYear(), new Date(line.date_eta).getUTCMonth(), new Date(line.date_eta).getUTCDate());
    if (etaDay > endWindow) continue;
    enCoursUtiles += Math.max(0, (line.qte_commandee || 0) - (line.qte_recue || 0));
  }
  const brut = Math.max(0, demandeFenetre - stockActuel - enCoursUtiles);
  // MOQ / PCB — cascade : override SKU (si > 1) → param famille → défaut 1.
  // Un SKU avec moq/colisage à 1 (défaut du schéma) est considéré comme "pas
  // d'override", on retombe alors sur la valeur famille si définie.
  const skuColisage = ref.colisage && ref.colisage > 1 ? ref.colisage : null;
  const skuMoq = ref.moq && ref.moq > 1 ? ref.moq : null;
  const colisage = skuColisage || (famParam && famParam.colisage > 0 ? famParam.colisage : 1);
  const moq = skuMoq || (famParam && famParam.moq > 0 ? famParam.moq : 1);
  const colisageSource = skuColisage ? 'sku' : (famParam && famParam.colisage > 0 ? 'famille' : 'defaut');
  const moqSource = skuMoq ? 'sku' : (famParam && famParam.moq > 0 ? 'famille' : 'defaut');
  let qte = Math.ceil(brut / colisage) * colisage;
  if (qte > 0 && qte < moq) qte = Math.ceil(moq / colisage) * colisage;
  if (brut === 0) qte = 0;
  const pa = ref.pa_vs != null ? ref.pa_vs : ref.pa_dernier;
  const montant = pa != null ? Number((qte * pa).toFixed(2)) : null;
  return {
    demandeFenetre: Number(demandeFenetre.toFixed(2)),
    stockActuel,
    enCoursUtiles,
    brut: Number(brut.toFixed(2)),
    colisage, moq,
    colisageSource, moqSource,
    qte,
    pa_unitaire: pa,
    montant,
    horizonJours,
  };
}

// ------------------------------------------------------------
// Full run for a single SKU. Returns a detailed audit trail — use for
// preview endpoints. This is the ground truth from which the bulk run
// materializes the alertes_etat table.
// ------------------------------------------------------------
function runForSku({ sku, ref, previsions, saisonaliteByFamille, famillesParam, stockActuel, today }) {
  const skuPrev = previsions || {};
  const enCoursLignes = stockDb.getEnCoursForSku(sku);
  const leadTimeJours = ref.lead_time_jours != null ? ref.lead_time_jours : DEFAULTS.leadTimeJours;
  // Priorité : param famille (défini dans stock_parametres_famille) → xlsx → défaut global.
  // Le param famille override la matrice pour permettre au user de recalibrer par famille sans
  // toucher au fichier Excel source.
  const famParam = famillesParam[`${ref.famille}|${ref.animal}`] || {};
  const couvertureViseeJours = famParam.couverture_visee_jours != null
    ? famParam.couverture_visee_jours
    : (ref.couverture_visee_jours != null ? ref.couverture_visee_jours : DEFAULTS.couvertureViseeJours);
  const couvertureViseeSource = famParam.couverture_visee_jours != null
    ? 'param_famille'
    : (ref.couverture_visee_jours != null ? 'matrice' : 'defaut_global');

  // Complétude check for niveau
  const paKnown = ref.pa_vs != null || ref.pa_dernier != null;
  const shopifyKnown = !!ref.shopify_variant_id && !!ref.shopify_inventory_item_id;
  const complet = paKnown && shopifyKnown;

  const forecast = forecastPerSku({ sku, ref, previsions: skuPrev, saisonaliteByFamille, famillesParam, today });
  const projection = projectStockDaily({
    stockInitial: stockActuel,
    monthlyForecast: forecast.rows,
    enCoursLignes,
    today,
    horizonJours: DEFAULTS.horizonJours,
  });
  const niveau = computeNiveau({
    ref,
    dateRuptureEstimee: projection.dateRuptureEstimee,
    today,
    leadTimeJours,
    couvertureViseeJours,
    complet,
  });
  const proposition = niveau === NIVEAUX.DONNEE_MANQUANTE
    ? null
    : computeProposition({
        ref, famParam,
        monthlyForecast: forecast.rows,
        stockActuel, enCoursLignes,
        today, leadTimeJours, couvertureViseeJours,
        niveau,
      });

  const messageParts = [];
  if (projection.dateRuptureEstimee) {
    const dj = daysBetweenUTC(today, projection.dateRuptureEstimee);
    messageParts.push(`Rupture estimée dans ${dj} j (${iso(projection.dateRuptureEstimee)})`);
  }
  if (proposition && proposition.qte > 0) {
    messageParts.push(`Proposition: ${proposition.qte} unités${proposition.montant != null ? ` (${proposition.montant.toFixed(2)} €)` : ''}`);
  }
  if (!complet) messageParts.push('Données incomplètes (PA ou Shopify manquant)');

  return {
    sku,
    niveau,
    complet,
    leadTimeJours,
    couvertureViseeJours,
    couvertureViseeSource,
    tendance: forecast.tendance,
    coeffSecurite: forecast.coeffSec,
    monthlyForecast: forecast.rows,
    projection: projection.daily.slice(0, 90), // first 90 days for preview payload size
    projectionFullLength: projection.daily.length,
    dateRuptureEstimee: projection.dateRuptureEstimee,
    dateRuptureEstimeeStr: projection.dateRuptureEstimee ? iso(projection.dateRuptureEstimee) : null,
    proposition,
    message: messageParts.join(' · '),
    enCoursLignes: enCoursLignes.map(l => ({
      bdc: l.numero, statut: l.statut, sku: l.sku,
      qte_commandee: l.qte_commandee, qte_recue: l.qte_recue,
      date_eta: l.date_eta ? iso(l.date_eta) : null,
    })),
  };
}

// ------------------------------------------------------------
// Bulk run — iterate all active SKU, materialize stock_alertes_etat.
// dryRun=true returns the report without writing.
// ------------------------------------------------------------
function runAll({ dryRun = true } = {}) {
  const t0 = Date.now();
  const today = todayUTC();
  const state = loadState();
  const saisonaliteByFamille = deriveSaisonalCoeffsPerFamille(state.previsions);
  const referentiels = stockDb.listReferentielActif();
  const upserts = [];
  const byNiveau = { RUPTURE: 0, CRITIQUE: 0, URGENT: 0, A_COMMANDER: 0, DONNEE_MANQUANTE: 0, OK: 0 };
  const details = [];
  for (const ref of referentiels) {
    const result = runForSku({
      sku: ref.sku, ref,
      previsions: state.previsions[ref.sku] || {},
      saisonaliteByFamille,
      famillesParam: state.famillesParam,
      stockActuel: state.stockActuel[ref.sku] || 0,
      today,
    });
    byNiveau[result.niveau] = (byNiveau[result.niveau] || 0) + 1;
    upserts.push({
      sku: ref.sku,
      niveau: result.niveau,
      niveau_precedent: null,
      date_rupture_estimee: result.dateRuptureEstimee,
      proposition_qte: result.proposition ? result.proposition.qte : null,
      proposition_montant: result.proposition ? result.proposition.montant : null,
      message: result.message || null,
    });
    details.push({
      sku: ref.sku,
      famille: ref.famille,
      animal: ref.animal,
      niveau: result.niveau,
      dateRuptureEstimee: result.dateRuptureEstimeeStr,
      proposition_qte: result.proposition ? result.proposition.qte : null,
      proposition_montant: result.proposition ? result.proposition.montant : null,
      stockActuel: state.stockActuel[ref.sku] || 0,
      leadTimeJours: result.leadTimeJours,
      couvertureViseeJours: result.couvertureViseeJours,
      matrice_bg_ref: ref.matrice_bg_ref,
      matrice_bh_ref: ref.matrice_bh_ref,
    });
  }
  let logId = null;
  if (!dryRun) {
    logId = stockDb.startSync('moteur_previsions');
    stockDb.upsertAlertesEtatBulk(upserts);
    stockDb.finishSync(logId, { status: 'ok', message: JSON.stringify(byNiveau) });
  }
  return {
    dryRun,
    today: iso(today),
    totalSkus: referentiels.length,
    byNiveau,
    durationMs: Date.now() - t0,
    details,
  };
}

// ------------------------------------------------------------
// Preview for a single SKU — full audit trail. Used by
// GET /api/stock/moteur/preview/:sku for debug / validation.
// ------------------------------------------------------------
function previewSku(sku) {
  const ref = stockDb.getReferentielSku(sku);
  if (!ref) return { error: `SKU introuvable: ${sku}` };
  const today = todayUTC();
  const state = loadState();
  const saisonaliteByFamille = deriveSaisonalCoeffsPerFamille(state.previsions);
  const result = runForSku({
    sku, ref,
    previsions: state.previsions[sku] || {},
    saisonaliteByFamille,
    famillesParam: state.famillesParam,
    stockActuel: state.stockActuel[sku] || 0,
    today,
  });
  return {
    today: iso(today),
    ref: {
      sku: ref.sku, nom_court: ref.nom_court, famille: ref.famille, animal: ref.animal,
      moq: ref.moq, colisage: ref.colisage,
      pa_vs: ref.pa_vs, pa_dernier: ref.pa_dernier,
      shopify_variant_id: ref.shopify_variant_id,
      shopify_inventory_item_id: ref.shopify_inventory_item_id,
      matrice_bg_ref: ref.matrice_bg_ref,
      matrice_bh_ref: ref.matrice_bh_ref,
    },
    stockActuel: state.stockActuel[sku] || 0,
    saisonaliteFamille: saisonaliteByFamille[`${ref.famille}|${ref.animal}`] || null,
    ...result,
  };
}

module.exports = {
  DEFAULTS,
  NIVEAUX,
  runAll,
  previewSku,
  // exposés pour tests / audits
  loadState,
  deriveSaisonalCoeffsPerFamille,
  deriveTendanceCoeff,
  forecastPerSku,
  projectStockDaily,
  computeNiveau,
  computeProposition,
  runForSku,
};
