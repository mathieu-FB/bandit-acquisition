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
const fs = require('fs');
const path = require('path');
const stockDb = require('./db');

// Logo Bandit — chargé une seule fois puis converti en data URI base64.
// PNG (pas SVG) : Word .doc HTML supporte les PNG en data URI depuis 2007
// alors qu'il traite les SVG comme des "images liées" impossibles à afficher.
// Cherche dans public/img/ (asset historique) avec fallback stock/assets/.
let LOGO_BANDIT_DATA_URI = null;
function getBanditLogoDataUri() {
  if (LOGO_BANDIT_DATA_URI !== null) return LOGO_BANDIT_DATA_URI;
  const candidates = [
    path.join(__dirname, '..', 'public', 'img', 'logo-bandit.png'),
    path.join(__dirname, 'assets', 'logo-bandit-black.png'),
  ];
  for (const p of candidates) {
    try {
      const bin = fs.readFileSync(p);
      const b64 = bin.toString('base64');
      LOGO_BANDIT_DATA_URI = `data:image/png;base64,${b64}`;
      return LOGO_BANDIT_DATA_URI;
    } catch (_) { /* try next */ }
  }
  console.warn('[BDC docs] Logo Bandit PNG introuvable dans public/img ni stock/assets — fallback texte.');
  LOGO_BANDIT_DATA_URI = '';
  return LOGO_BANDIT_DATA_URI;
}

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
// EAN13 barcode → tableau HTML de barres noires/blanches.
// Word .doc ne rend pas de manière fiable un SVG inline (surtout dans les
// cellules de tableau) — la version SVG affichait juste le texte dans certains
// contextes. Cette version utilise un <table> avec bgcolor par cellule :
// 100 % compatible avec le rendu HTML de Word.
// ------------------------------------------------------------
function ean13BarsHtml(code13Raw, opts = {}) {
  const moduleW = opts.moduleWidth || 2; // en pt (Word interprète mieux)
  const height = opts.height || 70;
  const fontSize = opts.fontSize || 14;
  const code13 = String(code13Raw || '').trim().padStart(13, '0');
  if (!/^\d{13}$/.test(code13)) {
    return `<div style="color:red;font-family:monospace;font-size:12pt;">EAN 13 invalide : ${escapeHtml(code13Raw)}</div>`;
  }
  const first = code13[0];
  const leftDigits = code13.slice(1, 7);
  const rightDigits = code13.slice(7);
  const parity = PARITY[Number(first)];

  let bits = '101';
  for (let i = 0; i < 6; i++) {
    const d = Number(leftDigits[i]);
    bits += parity[i] === 'L' ? L_CODES[d] : G_CODES[d];
  }
  bits += '01010';
  for (let i = 0; i < 6; i++) {
    bits += R_CODES[Number(rightDigits[i])];
  }
  bits += '101';

  // Groupe les modules consécutifs de même couleur → réduit le nb de cellules
  // (95 modules → typiquement 30-40 groupes). Word gère mieux et le rendu est
  // aussi net car les cellules ont border=0 et padding=0.
  const runs = [];
  let cur = { color: bits[0], w: 1 };
  for (let i = 1; i < bits.length; i++) {
    if (bits[i] === cur.color) cur.w++;
    else { runs.push(cur); cur = { color: bits[i], w: 1 }; }
  }
  runs.push(cur);
  const cells = runs.map(r => {
    const bg = r.color === '1' ? '#000000' : '#FFFFFF';
    const w = r.w * moduleW;
    return `<td width="${w}" height="${height}" bgcolor="${bg}" style="width:${w}pt;height:${height}pt;background-color:${bg};padding:0;margin:0;border:0;font-size:0;line-height:0;mso-line-height-rule:exactly;">&nbsp;</td>`;
  }).join('');
  return `
    <table cellspacing="0" cellpadding="0" border="0" style="border-collapse:collapse;margin:0 auto;border:0;">
      <tr>${cells}</tr>
    </table>
    <div style="font-family:'Courier New',monospace;font-size:${fontSize}pt;text-align:center;margin-top:3pt;letter-spacing:${moduleW}pt;font-weight:600;">${code13}</div>
  `;
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
// Carton marks .doc — un document par ligne BDC, calqué sur le template
// "Bandit" fourni par le fournisseur :
//   HEADER      : logo/label "Bandit" centré
//   COLONNE GAUCHE  : Nom produit (gros) · SKU · CIP · fields à remplir
//                    surlignés jaune (QTY/CTN, Net Weight, Gross Weight,
//                    CTN SIZE)
//   COLONNE DROITE  : image produit (via image_url Shopify) · label
//                    produit · barcode EAN13 · SKU · label Bandit
//   FOOTER      : "CARTON No. XX / XX" très gros centré (surligné jaune)
//
// Le layout à 2 colonnes est fait avec un <table> classique (Word HTML
// rendering fiable). Le barcode utilise ean13BarsHtml (cellules bg-color
// noir/blanc) — plus fiable qu'un SVG inline dans Word.
// ------------------------------------------------------------
function generateCartonMarksDoc(ligneId) {
  const ligne = stockDb.getBdcLigne(ligneId);
  if (!ligne) throw new Error(`Ligne BDC introuvable: ${ligneId}`);
  const ref = stockDb.getReferentielSku(ligne.sku);
  if (!ref) throw new Error(`SKU inconnu dans référentiel: ${ligne.sku}`);
  const bdc = stockDb.getBdc(ligne.bdc_id);

  const productName = ref.nom_court || ref.nom_long || ref.sku;
  const barcodeHtml = ref.ean_13
    ? ean13BarsHtml(ref.ean_13, { moduleWidth: 2, height: 55, fontSize: 10 })
    : '<div style="color:red;font-size:11pt;">EAN 13 manquant dans le référentiel</div>';
  const productImageUrl = ref.image_url || null;
  // Image produit : Word ignore width sans height quand l'image source a un
  // aspect ratio étiré → force width ET height explicitement (perte d'aspect
  // ratio assumée pour une vignette de référence). Attribut HTML + style CSS
  // pour couvrir tous les modes de rendu Word.
  const productImageHtml = productImageUrl
    ? `<img src="${escapeAttr(productImageUrl)}" width="200" height="200" style="width:200px;height:200px;display:block;margin:0 auto;" alt="${escapeAttr(productName)}">`
    : '<div style="width:200px;height:200px;background:#f3f3f3;color:#999;font-size:10pt;text-align:center;line-height:200px;margin:0 auto;">image non dispo</div>';

  // Logo Bandit (PNG en base64 data URI) — width fixe pour rendu Word.
  // Ratio original 1263×522 ≈ 2.42:1 → logo 180×74 pour le header.
  const logoDataUri = getBanditLogoDataUri();
  const logoHeaderHtml = logoDataUri
    ? `<img src="${escapeAttr(logoDataUri)}" width="180" height="74" style="width:180px;height:74px;display:block;margin:0 auto;" alt="Bandit">`
    : '<div style="font-family:cursive;font-size:28pt;font-weight:bold;">Bandit</div>';
  // Petit logo sous barcode — 80×33 (même ratio).
  const logoSmallHtml = logoDataUri
    ? `<img src="${escapeAttr(logoDataUri)}" width="80" height="33" style="width:80px;height:33px;vertical-align:middle;" alt="Bandit">`
    : '<span style="font-family:cursive;font-size:12pt;font-style:italic;">Bandit</span>';

  // Surlignage jaune façon Word (background sur span). Les valeurs "XX" sont
  // sélectionnables → fournisseur peut cliquer-remplacer dans Word.
  const fillMark = (txt) => `<span style="background:#FFEB3B;padding:1pt 4pt;font-weight:600;">${escapeHtml(txt)}</span>`;

  // Layout LANDSCAPE (A4 paysage : 29.7cm × 21cm) pour maximiser l'espace
  // horizontal et faire tenir le tout sur 1 page.
  //
  // Zone utile ≈ 27cm × 19cm avec marges 1.5cm.
  // À 96dpi cela fait ≈ 1020 × 720 pixels utiles.
  //
  // Layout :
  //   - <table> width=980px, table-layout:fixed
  //   - Colonne gauche 550 px : nom produit + SKU/CIP + fields à remplir
  //   - Colonne droite 430 px : image 200×200 + barcode + SKU/logo Bandit
  //   - Footer : "CARTON No. XX / XX" en 30pt centré
  //
  // Landscape via @page mso-page-orientation:landscape + wrapping div
  // WordSection1. Recette éprouvée pour Word .doc HTML.

  const html = `<!DOCTYPE html>
<html xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:w="urn:schemas-microsoft-com:office:word" xmlns="http://www.w3.org/TR/REC-html40">
<head>
  <meta charset="UTF-8">
  <title>Carton marks — ${escapeHtml(ligne.sku)}</title>
  <!--[if gte mso 9]><xml>
    <w:WordDocument>
      <w:View>Print</w:View>
      <w:Zoom>100</w:Zoom>
    </w:WordDocument>
  </xml><![endif]-->
  <style>
    @page WordSection1 {
      size: 29.7cm 21.0cm;
      mso-page-orientation: landscape;
      margin: 1.2cm 1.5cm 1.2cm 1.5cm;
      mso-header-margin: 1cm;
      mso-footer-margin: 1cm;
    }
    div.WordSection1 { page: WordSection1; }
    body { font-family: Arial, Helvetica, sans-serif; margin: 0; font-size: 13pt; }
    .brand-header { text-align: center; margin: 0 0 12px; }
    .prod-name { font-size: 22pt; font-weight: 500; text-align: center; margin: 10px 0 14px; letter-spacing: 1pt; line-height: 1.1; }
    .sku-line, .cip-line { font-size: 16pt; text-align: center; margin: 6px 0; letter-spacing: 1pt; white-space: nowrap; }
    .fields { margin-top: 20px; font-size: 13pt; text-align: center; }
    .fields .row { margin: 8px 0; white-space: nowrap; }
    .fields .lbl { font-weight: 600; }
    .prod-photo-wrap { text-align: center; padding-top: 2px; }
    .prod-photo-caption { font-size: 10pt; text-align: center; margin-top: 3px; margin-bottom: 8px; font-weight: 500; }
    .barcode-wrap { text-align: center; margin: 4px 0; }
    .carton-no { text-align: center; font-size: 28pt; font-weight: bold; margin: 25px 0 8px; letter-spacing: 2pt; }
    .footer { margin-top: 10px; font-size: 8.5pt; color: #888; text-align: center; border-top: 1px solid #ccc; padding-top: 5px; }
  </style>
</head>
<body>
<div class="WordSection1">

  <div class="brand-header">${logoHeaderHtml}</div>

  <table cellspacing="0" cellpadding="0" border="0" width="980" style="width:980px;table-layout:fixed;border-collapse:collapse;margin:0 auto;">
    <tr>
      <td width="550" valign="top" style="width:550px;vertical-align:top;padding:0 20px;">
        <div class="prod-name">${escapeHtml(productName).toUpperCase()}</div>
        <div class="sku-line">SKU : ${escapeHtml(ref.sku)}</div>
        <div class="cip-line">CIP : ${escapeHtml(ref.cip || '—')}</div>

        <div class="fields">
          <div class="row"><span class="lbl">QTY/<u>CTN:</u></span> ${fillMark('XX')}</div>
          <div class="row"><span class="lbl">Net Weight :</span> ${fillMark('XX kg')}</div>
          <div class="row"><span class="lbl">Gross Weight :</span> ${fillMark('XX kg')}</div>
          <div class="row"><span class="lbl">CTN <u>SIZE:</u></span> ${fillMark('XXcm')} / ${fillMark('XXcm')} / ${fillMark('XXcm')}</div>
        </div>
      </td>

      <td width="430" valign="top" style="width:430px;vertical-align:top;padding:0 20px;">
        <div class="prod-photo-wrap">${productImageHtml}</div>
        <div class="prod-photo-caption">${escapeHtml(productName)}</div>

        <div class="barcode-wrap">${barcodeHtml}</div>

        <table cellspacing="0" cellpadding="0" border="0" width="280" style="width:280px;margin:8px auto 0;">
          <tr>
            <td width="140" style="width:140px;font-family:'Courier New',monospace;font-size:10pt;text-align:left;vertical-align:middle;">${escapeHtml(ref.sku)}</td>
            <td width="140" style="width:140px;text-align:right;vertical-align:middle;">${logoSmallHtml}</td>
          </tr>
        </table>
      </td>
    </tr>
  </table>

  <div class="carton-no">CARTON No. ${fillMark('XX')} / ${fillMark('XX')}</div>

  <div class="footer">
    ${bdc ? `BDC ${escapeHtml(bdc.numero)} · ` : ''}Généré le ${new Date().toLocaleDateString('fr-FR')} · Merci d'imprimer et d'apposer sur chaque carton
  </div>

</div>
</body>
</html>`;

  const filename = `${bdc ? bdc.numero + '-' : ''}${ligne.sku}-carton-marks.doc`;
  return { html, filename };
}

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function escapeAttr(s) {
  return escapeHtml(s).replace(/"/g, '&quot;');
}

module.exports = { generateBlXlsx, generateCartonMarksDoc, ean13Svg, ean13BarsHtml };
