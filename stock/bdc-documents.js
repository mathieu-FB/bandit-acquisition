// ============================================================
// BDC DOCUMENTS — générateurs à joindre aux commandes fournisseur
//
// 1. BL xlsx : simple export SKU + qté (sans en-tête) pour tout le BDC.
//    Format attendu par Bandit pour le suivi logistique.
//
// 2. Carton marks .doc (par ligne) : document Word contenant les infos
//    à imprimer sur les cartons du fournisseur. Certains champs sont
//    pré-remplis (SKU, CIP, EAN + barcode) et d'autres à remplir par
//    le fournisseur (numéro carton, qté, poids brut, dimensions).
//    Généré en HTML+Word compatible (application/msword) pour éviter
//    d'ajouter une lib docx.
// ============================================================

const XLSX = require('xlsx');
const stockDb = require('./db');

// ------------------------------------------------------------
// EAN13 barcode → SVG inline (aucune dépendance externe).
// Standard encoding : L/G/R tables, parity pattern by first digit.
// ------------------------------------------------------------
const L_CODES = ['0001101', '0011001', '0010011', '0111101', '0100011', '0110001', '0101111', '0111011', '0110111', '0001011'];
const G_CODES = ['0100111', '0110011', '0011011', '0100001', '0011101', '0111001', '0000101', '0010001', '0001001', '0010111'];
const R_CODES = ['1110010', '1100110', '1101100', '1000010', '1011100', '1001110', '1010000', '1000100', '1001000', '1110100'];
const PARITY = ['LLLLLL', 'LLGLGG', 'LLGGLG', 'LLGGGL', 'LGLLGG', 'LGGLLG', 'LGGGLL', 'LGLGLG', 'LGLGGL', 'LGGLGL'];

function ean13Svg(code13Raw, opts = {}) {
  const moduleW = opts.moduleWidth || 2;
  const height = opts.height || 80;
  const code13 = String(code13Raw || '').trim().padStart(13, '0');
  if (!/^\d{13}$/.test(code13)) {
    // Fallback : simple text (barcode invalide)
    return `<svg xmlns="http://www.w3.org/2000/svg" width="200" height="30"><text x="0" y="20" font-family="monospace" font-size="14" fill="red">EAN invalide: ${code13Raw}</text></svg>`;
  }
  const first = code13[0];
  const leftDigits = code13.slice(1, 7);
  const rightDigits = code13.slice(7);
  const parity = PARITY[Number(first)];

  let bits = '101'; // start guard
  for (let i = 0; i < 6; i++) {
    const d = Number(leftDigits[i]);
    bits += parity[i] === 'L' ? L_CODES[d] : G_CODES[d];
  }
  bits += '01010'; // center guard
  for (let i = 0; i < 6; i++) {
    bits += R_CODES[Number(rightDigits[i])];
  }
  bits += '101'; // end guard

  const totalWidth = bits.length * moduleW;
  let rects = '';
  for (let i = 0; i < bits.length; i++) {
    if (bits[i] === '1') {
      rects += `<rect x="${i * moduleW}" y="0" width="${moduleW}" height="${height}" fill="black"/>`;
    }
  }
  // Texte lisible sous le barcode : premier digit décalé à gauche + 6+6
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${totalWidth + 10}" height="${height + 22}" style="background:white;">
    ${rects}
    <text x="${totalWidth / 2}" y="${height + 18}" text-anchor="middle" font-family="monospace" font-size="14">${code13}</text>
  </svg>`;
}

// ------------------------------------------------------------
// BL xlsx : 2 colonnes (SKU, qté) sans en-tête, une ligne par ligne BDC
// non annulée / non entièrement reçue.
// ------------------------------------------------------------
function generateBlXlsx(bdcId) {
  const bdc = stockDb.getBdc(bdcId);
  if (!bdc) throw new Error(`BDC introuvable: ${bdcId}`);
  const lignes = stockDb.getBdcLignes(bdcId);
  const rows = lignes.map(l => {
    const utile = Math.max(0, (l.qte_commandee || 0) - (l.qte_annulee_fournisseur || 0));
    return [l.sku, utile];
  });
  const ws = XLSX.utils.aoa_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, 'BL');
  return {
    buffer: XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' }),
    filename: `${bdc.numero}-BL.xlsx`,
  };
}

// ------------------------------------------------------------
// Carton marks .doc (HTML compatible Word) — un document par ligne BDC.
// Contient les infos pré-remplies (SKU, CIP, EAN + barcode SVG) et les
// champs à remplir par le fournisseur (n° carton, qté, poids, dims).
// ------------------------------------------------------------
function generateCartonMarksDoc(ligneId) {
  const ligne = stockDb.getBdcLigne(ligneId);
  if (!ligne) throw new Error(`Ligne BDC introuvable: ${ligneId}`);
  const ref = stockDb.getReferentielSku(ligne.sku);
  if (!ref) throw new Error(`SKU inconnu dans référentiel: ${ligne.sku}`);
  const bdc = stockDb.getBdc(ligne.bdc_id);
  const fournisseur = bdc && bdc.fournisseur_id ? stockDb.getFournisseurById(bdc.fournisseur_id) : null;
  const barcodeSvg = ref.ean_13 ? ean13Svg(ref.ean_13, { height: 80, moduleWidth: 2 }) : '<div style="color:red;">EAN 13 manquant dans le référentiel</div>';

  // Style spécifique msword : marges, tableau, labels alignés.
  const html = `<!DOCTYPE html>
<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40">
<head>
  <meta charset="UTF-8">
  <title>Carton marks — ${escapeHtml(ligne.sku)}</title>
  <!--[if gte mso 9]><xml><w:WordDocument><w:View>Print</w:View></w:WordDocument></xml><![endif]-->
  <style>
    body { font-family: Arial, sans-serif; font-size: 14pt; margin: 30px; }
    h1 { text-align: center; font-size: 22pt; margin: 0 0 8px; letter-spacing: 2px; }
    .subtitle { text-align: center; color: #555; margin-bottom: 22px; font-size: 12pt; }
    table.marks { width: 100%; border-collapse: collapse; margin-top: 8px; }
    table.marks td { padding: 12px 14px; border: 2px solid #000; vertical-align: middle; }
    table.marks td.lbl { width: 42%; font-weight: bold; background: #f0f0f0; font-size: 13pt; text-transform: uppercase; letter-spacing: 1px; }
    table.marks td.val { font-size: 15pt; }
    .fill { color: #888; font-style: italic; }
    .barcode-wrap { text-align: center; padding: 16px 0; }
    .footer { margin-top: 30px; font-size: 10pt; color: #888; text-align: center; border-top: 1px solid #ccc; padding-top: 10px; }
  </style>
</head>
<body>
  <h1>CARTON MARKS</h1>
  <div class="subtitle">${escapeHtml(fournisseur ? fournisseur.nom : '')} → SAS A&amp;M – Bandit${bdc ? ` · BDC ${escapeHtml(bdc.numero)}` : ''}</div>

  <table class="marks">
    <tr>
      <td class="lbl">Product</td>
      <td class="val">${escapeHtml(ref.nom_court || ref.nom_long || ref.sku)}</td>
    </tr>
    <tr>
      <td class="lbl">SKU</td>
      <td class="val" style="font-family:monospace;">${escapeHtml(ref.sku)}</td>
    </tr>
    <tr>
      <td class="lbl">CIP</td>
      <td class="val" style="font-family:monospace;">${escapeHtml(ref.cip || '—')}</td>
    </tr>
    <tr>
      <td class="lbl">EAN 13</td>
      <td class="val" style="font-family:monospace;">${escapeHtml(ref.ean_13 || '—')}</td>
    </tr>
    <tr>
      <td class="lbl">Barcode</td>
      <td class="val barcode-wrap">${barcodeSvg}</td>
    </tr>
    <tr>
      <td class="lbl">Carton n°</td>
      <td class="val fill">à remplir par le fournisseur</td>
    </tr>
    <tr>
      <td class="lbl">Qty per carton</td>
      <td class="val fill">à remplir par le fournisseur</td>
    </tr>
    <tr>
      <td class="lbl">Gross weight</td>
      <td class="val fill">à remplir par le fournisseur</td>
    </tr>
    <tr>
      <td class="lbl">CTN size</td>
      <td class="val fill">à remplir par le fournisseur</td>
    </tr>
  </table>

  <div class="footer">
    Généré le ${new Date().toLocaleDateString('fr-FR')} · Bandit · Merci d'imprimer et d'apposer sur chaque carton
  </div>
</body>
</html>`;

  const filename = `${bdc ? bdc.numero + '-' : ''}${ligne.sku}-carton-marks.doc`;
  return { html, filename };
}

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

module.exports = { generateBlXlsx, generateCartonMarksDoc, ean13Svg };
