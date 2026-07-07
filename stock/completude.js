// ============================================================
// COMPLÉTUDE — audit the referentiel_sku for missing data that
// would prevent the forecast engine (step 2) from producing
// reliable propositions.
//
// Returns a flat list of findings, each with a sévérité level:
//   - bloquant : forecast engine cannot compute for this SKU
//   - warning  : engine will compute but with fallback / lower confidence
//   - info     : cosmetic / to fill in when convenient
// ============================================================

const stockDb = require('./db');

// Recent months window used to check for a minimum sales history.
const HISTORY_MIN_MONTHS = 6;

function checkReferentiel() {
  const findings = [];
  const referentiels = stockDb.listReferentielActif();

  for (const r of referentiels) {
    const push = (severite, code, message) => findings.push({
      sku: r.sku,
      famille: r.famille,
      animal: r.animal,
      nom_court: r.nom_court,
      severite,
      code,
      message,
    });

    // ---- Bloquants ----
    if (r.pa_vs == null && r.pa_dernier == null) {
      push('bloquant', 'PA_MANQUANT',
        'Aucun prix d\'achat (PA VS / DERNIER PRU) : impossible de calculer le montant des propositions de commande.');
    }
    if (!r.shopify_variant_id || !r.shopify_inventory_item_id) {
      push('bloquant', 'SHOPIFY_MANQUANT',
        'Pas de correspondance Shopify (variant_id / inventory_item_id) : le sync stock ignorera ce SKU. Lance sync-shopify?type=variants.');
    }

    // ---- Warnings ----
    if (r.lead_time_jours == null) {
      push('warning', 'LEAD_TIME_MANQUANT',
        'Pas de lead time (Excel BD) : le moteur utilisera le défaut de la famille (60 j) — vérifie en Paramètres.');
    }
    if (r.fournisseur_defaut_id == null) {
      push('warning', 'FOURNISSEUR_DEFAUT_MANQUANT',
        'Pas de fournisseur par défaut : à choisir à chaque nouvelle commande.');
    }
    if (r.couverture_visee_jours == null) {
      push('warning', 'COUVERTURE_VISEE_MANQUANTE',
        'Pas de couverture visée (Excel BF) : le moteur utilisera le défaut de la famille (90 j).');
    }

    // ---- Info ----
    const nPrev = stockDb.countPrevisionsForSku(r.sku);
    if (nPrev < HISTORY_MIN_MONTHS) {
      push('info', 'HISTORIQUE_LIMITE',
        `Seulement ${nPrev} mois d'historique de ventes : la prévision saisonnière sera peu fiable. Lance un backfill Shopify sales.`);
    }
    if (!r.image_url) {
      push('info', 'IMAGE_MANQUANTE',
        'Pas d\'image Shopify : à récupérer via sync-shopify?type=variants.');
    }
    if (!r.ean_13) {
      push('info', 'EAN13_MANQUANT',
        'Pas d\'EAN13 : le template de commande fournisseur affichera vide dans la colonne EAN.');
    }
  }
  return findings;
}

function summary(findings) {
  const byLevel = { bloquant: 0, warning: 0, info: 0 };
  const bySku = new Set();
  for (const f of findings) {
    byLevel[f.severite] = (byLevel[f.severite] || 0) + 1;
    bySku.add(f.sku);
  }
  return {
    totalFindings: findings.length,
    skuAffectes: bySku.size,
    parNiveau: byLevel,
  };
}

function run() {
  const findings = checkReferentiel();
  return {
    generatedAt: new Date().toISOString(),
    summary: summary(findings),
    findings,
  };
}

module.exports = { run, checkReferentiel, summary };
