// ============================================================
// CSV IMPORT — Commandes fournisseur (format ELVETIS et similaires)
//
// Le fournisseur envoie un CSV avec un bloc d'en-tête libre (nom,
// adresse, SIRET, n° commande + date FRENCH BANDIT), puis une ligne
// header contenant au moins "NOM DU PRODUIT", "RÉF LABO" ou similaire,
// puis les lignes SKU / EAN / qté.
//
// Le parser :
// 1. Détecte le fournisseur = 1ère cellule non-vide de la 1ère ligne
// 2. Cherche la ligne "FRENCH BANDIT Commande n°:XXXX du: JJ.MM.AA"
//    pour extraire le n° commande + date d'envoi
// 3. Trouve la ligne header (contient RÉF LABO / SKU / QTÉ CDÉE ...)
// 4. Parse les lignes suivantes : SKU (RÉF LABO), EAN (GTIN), qté
// 5. Retourne un preview structuré
// ============================================================

const stockDb = require('./db');

// Séparateurs testés dans l'ordre : ; puis , puis \t
function detectSeparator(text) {
  const first = (text.split(/\r?\n/, 5).join('\n') || '').toString();
  const counts = {
    ';': (first.match(/;/g) || []).length,
    ',': (first.match(/,/g) || []).length,
    '\t': (first.match(/\t/g) || []).length,
  };
  return Object.entries(counts).sort((a, b) => b[1] - a[1])[0][0];
}

// Split respectueux des ="..." quoted d'Excel + guillemets standards
function splitRow(line, sep) {
  const out = [];
  let cur = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { inQuote = !inQuote; continue; }
    if (c === sep && !inQuote) { out.push(cur); cur = ''; continue; }
    cur += c;
  }
  out.push(cur);
  return out.map(s => s.trim());
}

// Nettoie une cellule : retire ="..." d'Excel, guillemets, espaces
function cleanCell(v) {
  if (v == null) return '';
  let s = String(v).trim();
  // ="..." → ...
  const m = s.match(/^=?"(.*)"$/);
  if (m) s = m[1];
  // Cas où splitRow a déjà retiré les guillemets internes → il reste "=03666..."
  if (s.startsWith('=')) s = s.slice(1);
  return s.trim();
}

// Détection des colonnes par mots-clés (case + accent insensible)
function normalizeHeader(h) {
  return String(h || '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '').trim();
}

function findCol(headerRow, patterns) {
  for (let i = 0; i < headerRow.length; i++) {
    const h = normalizeHeader(headerRow[i]);
    if (patterns.some(p => h.includes(p))) return i;
  }
  return -1;
}

// Parse une date au format "16.07.26" ou "16/07/26" ou "16-07-2026"
function parseDate(str) {
  if (!str) return null;
  const m = String(str).match(/(\d{1,2})[.\/\-](\d{1,2})[.\/\-](\d{2,4})/);
  if (!m) return null;
  let [, d, mo, y] = m;
  if (y.length === 2) y = '20' + y;
  const iso = `${y}-${mo.padStart(2, '0')}-${d.padStart(2, '0')}`;
  const t = Date.parse(iso);
  return isNaN(t) ? null : iso;
}

// ------------------------------------------------------------
// Parser principal
// ------------------------------------------------------------
function parseFournisseurCsv(csvText) {
  const sep = detectSeparator(csvText);
  const lines = csvText.split(/\r?\n/).map(l => splitRow(l, sep));
  // Trim vides en fin de fichier
  while (lines.length && lines[lines.length - 1].every(c => !cleanCell(c))) lines.pop();
  if (lines.length === 0) throw new Error('CSV vide');

  // 1. Nom fournisseur = 1ère cellule non-vide
  let fournisseurNom = '';
  for (const row of lines.slice(0, 5)) {
    const c = cleanCell(row[0]);
    if (c) { fournisseurNom = c; break; }
  }

  // 2. n° commande + date (cherche "Commande n°" dans les 15 premières lignes)
  let numeroCommande = null;
  let dateEnvoi = null;
  for (const row of lines.slice(0, 15)) {
    const joined = row.map(cleanCell).join(' ');
    const mCmd = joined.match(/Commande\s*n[°º]?\s*:?\s*([A-Z0-9_-]+)/i);
    if (mCmd && !numeroCommande) numeroCommande = mCmd[1];
    const mDate = joined.match(/du\s*:?\s*(\d{1,2}[.\/\-]\d{1,2}[.\/\-]\d{2,4})/i);
    if (mDate && !dateEnvoi) dateEnvoi = parseDate(mDate[1]);
  }

  // 3. Trouve la ligne header : contient "REF LABO" ou "SKU" ou "REFERENCE"
  let headerIdx = -1;
  for (let i = 0; i < lines.length; i++) {
    const normalized = lines[i].map(normalizeHeader);
    const hasSkuCol = normalized.some(h => h.includes('ref labo') || h === 'sku' || h.includes('reference') || h.includes('ref bandit'));
    const hasQteCol = normalized.some(h => h.includes('qte cde') || h.includes('qte commande') || h.includes('quantite') || h === 'qte');
    if (hasSkuCol && hasQteCol) { headerIdx = i; break; }
  }
  if (headerIdx === -1) {
    throw new Error('Ligne header introuvable — attendu au moins une colonne "RÉF LABO" (ou SKU / Référence) et une colonne "QTÉ CDÉE" (ou Quantité).');
  }

  const headerRow = lines[headerIdx];
  const colSku = findCol(headerRow, ['ref labo', 'sku', 'reference', 'ref bandit']);
  const colQte = findCol(headerRow, ['qte cde', 'qte commande', 'quantite', 'qte']);
  const colNom = findCol(headerRow, ['nom du produit', 'nom produit', 'designation', 'libelle']);
  const colEan = findCol(headerRow, ['gtin', 'ean']);
  const colCip = findCol(headerRow, ['code article', 'cip']);
  const colPa = findCol(headerRow, ['pa', 'prix', 'p.u.', 'pu ht']);

  // 4. Parse lignes data
  const rows = [];
  const warnings = [];
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const row = lines[i];
    const sku = cleanCell(row[colSku]);
    if (!sku) continue;
    const qte = Number(cleanCell(row[colQte]).replace(',', '.'));
    if (!Number.isFinite(qte) || qte <= 0) {
      warnings.push(`Ligne ${i + 1} SKU ${sku}: quantité invalide (${row[colQte]}), ignorée`);
      continue;
    }
    const ref = stockDb.getReferentielSku(sku);
    const pa = colPa >= 0 ? (Number(cleanCell(row[colPa]).replace(',', '.')) || null) : null;
    rows.push({
      sku,
      nom_csv: colNom >= 0 ? cleanCell(row[colNom]) : null,
      ean_csv: colEan >= 0 ? cleanCell(row[colEan]) : null,
      cip_csv: colCip >= 0 ? cleanCell(row[colCip]) : null,
      qte_commandee: qte,
      pa_unitaire: pa != null ? pa : (ref ? (ref.pa_vs != null ? ref.pa_vs : ref.pa_dernier) : null),
      in_referentiel: !!ref,
      nom_ref: ref ? ref.nom_court : null,
    });
    if (!ref) warnings.push(`Ligne ${i + 1} SKU ${sku}: SKU inconnu du référentiel — sera rejeté à la création du BDC`);
  }

  return {
    separator: sep,
    fournisseur_nom: fournisseurNom || null,
    numero_commande: numeroCommande || null,
    date_envoi: dateEnvoi || null,
    header_row_index: headerIdx + 1, // 1-indexed pour l'utilisateur
    detected_columns: {
      sku: colSku >= 0 ? headerRow[colSku] : null,
      qte: colQte >= 0 ? headerRow[colQte] : null,
      nom: colNom >= 0 ? headerRow[colNom] : null,
      ean: colEan >= 0 ? headerRow[colEan] : null,
      cip: colCip >= 0 ? headerRow[colCip] : null,
      pa: colPa >= 0 ? headerRow[colPa] : null,
    },
    total_qte: rows.reduce((s, r) => s + r.qte_commandee, 0),
    total_montant: rows.reduce((s, r) => s + r.qte_commandee * (r.pa_unitaire || 0), 0),
    rows_count: rows.length,
    matched_count: rows.filter(r => r.in_referentiel).length,
    unmatched_count: rows.filter(r => !r.in_referentiel).length,
    warnings,
    rows,
  };
}

module.exports = { parseFournisseurCsv };
