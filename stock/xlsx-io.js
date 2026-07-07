// ============================================================
// XLSX I/O — thin wrapper over SheetJS for read + write.
// ============================================================

const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

// Read a single sheet as an array of row-arrays (header row included at index 0).
// Empty trailing cells are preserved so column indices stay aligned.
function readSheetRaw(xlsxPath, sheetName) {
  if (!fs.existsSync(xlsxPath)) throw new Error(`Fichier xlsx introuvable: ${xlsxPath}`);
  const wb = XLSX.readFile(xlsxPath, { cellDates: false, cellFormula: false });
  const sheet = wb.Sheets[sheetName];
  if (!sheet) throw new Error(`Feuille introuvable: ${sheetName} (feuilles disponibles: ${wb.SheetNames.join(', ')})`);
  // header: 1 → array of arrays; defval: null keeps empty cells as null instead of skipping them
  return XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null, blankrows: false });
}

function listSheets(xlsxPath) {
  if (!fs.existsSync(xlsxPath)) throw new Error(`Fichier xlsx introuvable: ${xlsxPath}`);
  const wb = XLSX.readFile(xlsxPath, { bookSheets: true });
  return wb.SheetNames;
}

// Write an array of row objects to a new xlsx file. First key becomes column A, etc.
// `headers` is an ordered array of { key, label } — label appears in the header row,
// values are pulled from row[key] in the same order.
function writeRowsToXlsx(xlsxPath, sheetName, headers, rows) {
  const dir = path.dirname(xlsxPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const aoa = [headers.map(h => h.label)];
  rows.forEach(r => aoa.push(headers.map(h => r[h.key] ?? null)));

  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.aoa_to_sheet(aoa);

  // Column widths — heuristic based on label + a bit of padding
  sheet['!cols'] = headers.map(h => ({ wch: Math.min(60, Math.max(10, (h.label || '').length + 4)) }));

  XLSX.utils.book_append_sheet(wb, sheet, sheetName);
  XLSX.writeFile(wb, xlsxPath);
  return xlsxPath;
}

// Serialize an xlsx workbook to a Buffer (for HTTP download without touching disk).
function rowsToBuffer(sheetName, headers, rows) {
  const aoa = [headers.map(h => h.label)];
  rows.forEach(r => aoa.push(headers.map(h => r[h.key] ?? null)));
  const wb = XLSX.utils.book_new();
  const sheet = XLSX.utils.aoa_to_sheet(aoa);
  sheet['!cols'] = headers.map(h => ({ wch: Math.min(60, Math.max(10, (h.label || '').length + 4)) }));
  XLSX.utils.book_append_sheet(wb, sheet, sheetName);
  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
}

// Parse a buffer (from an uploaded xlsx) into raw row arrays for a given sheet.
function readBufferSheetRaw(buffer, sheetName) {
  const wb = XLSX.read(buffer, { type: 'buffer', cellDates: false, cellFormula: false });
  const targetSheet = sheetName || wb.SheetNames[0];
  const sheet = wb.Sheets[targetSheet];
  if (!sheet) throw new Error(`Feuille introuvable dans le buffer: ${targetSheet} (feuilles: ${wb.SheetNames.join(', ')})`);
  return {
    sheetName: targetSheet,
    availableSheets: wb.SheetNames,
    rows: XLSX.utils.sheet_to_json(sheet, { header: 1, defval: null, blankrows: false }),
  };
}

// Utility: cell coerce to number (returns null if empty / not numeric).
function toNumber(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  const s = String(v).trim().replace(/\s/g, '').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function toInt(v) {
  const n = toNumber(v);
  return n == null ? null : Math.round(n);
}

function toStr(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

module.exports = {
  readSheetRaw,
  listSheets,
  writeRowsToXlsx,
  rowsToBuffer,
  readBufferSheetRaw,
  toNumber,
  toInt,
  toStr,
};
