// ============================================================
// STOCK MODULE — SQLite schema, statements, and typed API.
// Shares the main bandit-cache.db connection via cache.getDb().
// Must be initialised AFTER cache.init(). See init() below.
// ============================================================

const cache = require('../cache');

let db = null;
let stmts = null;

// ------------------------------------------------------------
// Schema — 10 tables, all prefixed `stock_` for isolation.
// ------------------------------------------------------------
const SCHEMA = `
CREATE TABLE IF NOT EXISTS stock_fournisseurs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nom TEXT NOT NULL UNIQUE,
  email TEXT,
  adresse TEXT,
  contact TEXT,
  devise TEXT DEFAULT 'EUR',
  incoterm TEXT DEFAULT 'EXW',
  conditions_paiement TEXT,
  notes TEXT,
  actif INTEGER DEFAULT 1,
  created_at INTEGER,
  updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS stock_referentiel_sku (
  sku TEXT PRIMARY KEY,
  categorie TEXT,
  famille TEXT,
  animal TEXT,
  cip TEXT,
  ean_13 TEXT,
  ean_12 TEXT,
  nom_court TEXT,
  nom_long TEXT,
  nom_en TEXT,
  motif TEXT,
  taille TEXT,
  url_produit TEXT,
  pa_dernier REAL,
  pa_vs REAL,
  pvc_ttc REAL,
  pays_fabrication TEXT,
  hs_code_10 TEXT,
  hs_code_6 TEXT,
  poids_brut REAL,
  poids_net REAL,
  longueur REAL,
  largeur REAL,
  hauteur REAL,
  fournisseur_defaut_id INTEGER,
  moq INTEGER DEFAULT 1,
  colisage INTEGER DEFAULT 1,
  lead_time_jours INTEGER,
  couverture_visee_jours INTEGER,
  couverture_visee_source TEXT,
  actif INTEGER DEFAULT 1,
  shopify_product_id TEXT,
  shopify_variant_id TEXT,
  shopify_inventory_item_id TEXT,
  shopify_variant_title TEXT,
  image_url TEXT,
  matrice_bg_ref INTEGER,
  matrice_bh_ref REAL,
  seeded_at INTEGER,
  updated_at INTEGER,
  FOREIGN KEY (fournisseur_defaut_id) REFERENCES stock_fournisseurs(id)
);
CREATE INDEX IF NOT EXISTS idx_stock_ref_actif ON stock_referentiel_sku(actif);
CREATE INDEX IF NOT EXISTS idx_stock_ref_famille ON stock_referentiel_sku(famille, animal);
CREATE INDEX IF NOT EXISTS idx_stock_ref_fournisseur ON stock_referentiel_sku(fournisseur_defaut_id);
CREATE INDEX IF NOT EXISTS idx_stock_ref_variant ON stock_referentiel_sku(shopify_variant_id);
CREATE INDEX IF NOT EXISTS idx_stock_ref_inventory_item ON stock_referentiel_sku(shopify_inventory_item_id);

CREATE TABLE IF NOT EXISTS stock_parametres_famille (
  famille TEXT NOT NULL,
  animal TEXT NOT NULL,
  couverture_visee_jours INTEGER DEFAULT 90,
  coeff_securite REAL DEFAULT 1.1,
  coeff_saisonnalite_json TEXT,
  coeff_tendance REAL DEFAULT 1.0,
  updated_at INTEGER,
  PRIMARY KEY (famille, animal)
);

CREATE TABLE IF NOT EXISTS stock_previsions_mensuelles (
  sku TEXT NOT NULL,
  annee INTEGER NOT NULL,
  mois INTEGER NOT NULL,
  ventes_reelles INTEGER DEFAULT 0,
  is_estimated INTEGER DEFAULT 0,
  updated_at INTEGER,
  PRIMARY KEY (sku, annee, mois)
);
CREATE INDEX IF NOT EXISTS idx_stock_prev_sku ON stock_previsions_mensuelles(sku);

CREATE TABLE IF NOT EXISTS stock_actuel (
  sku TEXT PRIMARY KEY,
  stock_dispo INTEGER DEFAULT 0,
  location_id TEXT,
  source TEXT,
  updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS stock_bdc (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  numero TEXT UNIQUE NOT NULL,
  fournisseur_id INTEGER NOT NULL,
  date_creation INTEGER NOT NULL,
  date_envoi INTEGER,
  date_eta INTEGER,
  date_reception_prevue INTEGER,
  date_reception_reelle INTEGER,
  statut TEXT DEFAULT 'brouillon',
  montant_total REAL,
  devise TEXT DEFAULT 'EUR',
  notes TEXT,
  created_at INTEGER,
  updated_at INTEGER,
  FOREIGN KEY (fournisseur_id) REFERENCES stock_fournisseurs(id)
);
CREATE INDEX IF NOT EXISTS idx_stock_bdc_statut ON stock_bdc(statut);
CREATE INDEX IF NOT EXISTS idx_stock_bdc_fournisseur ON stock_bdc(fournisseur_id);
CREATE INDEX IF NOT EXISTS idx_stock_bdc_eta ON stock_bdc(date_eta);

CREATE TABLE IF NOT EXISTS stock_bdc_lignes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  bdc_id INTEGER NOT NULL,
  sku TEXT NOT NULL,
  qte_commandee INTEGER NOT NULL,
  qte_recue INTEGER DEFAULT 0,
  pa_unitaire REAL,
  devise TEXT DEFAULT 'EUR',
  FOREIGN KEY (bdc_id) REFERENCES stock_bdc(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_stock_bdc_lignes_bdc ON stock_bdc_lignes(bdc_id);
CREATE INDEX IF NOT EXISTS idx_stock_bdc_lignes_sku ON stock_bdc_lignes(sku);

CREATE TABLE IF NOT EXISTS stock_alertes_etat (
  sku TEXT PRIMARY KEY,
  niveau TEXT,
  niveau_precedent TEXT,
  date_rupture_estimee INTEGER,
  proposition_qte INTEGER,
  proposition_montant REAL,
  message TEXT,
  updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_stock_alertes_niveau ON stock_alertes_etat(niveau);

CREATE TABLE IF NOT EXISTS stock_parametres_globaux (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS stock_sync_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,
  status TEXT NOT NULL,
  message TEXT,
  duration_ms INTEGER,
  started_at INTEGER NOT NULL,
  finished_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_stock_sync_log_started ON stock_sync_log(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_stock_sync_log_type ON stock_sync_log(type, started_at DESC);
`;

// ------------------------------------------------------------
// Lifecycle
// ------------------------------------------------------------
function init() {
  db = cache.getDb();
  db.exec(SCHEMA);
  prepareStatements();
  seedDefaults();
  console.log('[Stock] Schema initialised (10 tables).');
}

function seedDefaults() {
  const now = Date.now();
  const defaults = [
    ['adresse_livraison_nom', 'VETO SANTE (Pour French Bandit)'],
    ['adresse_livraison_ligne1', '13 Rue Pierre Boulanger'],
    ['adresse_livraison_cp', '63370'],
    ['adresse_livraison_ville', 'LEMPDES'],
    ['adresse_livraison_pays', 'France'],
    ['contact_livraison_nom', 'Cédric SECRÉTIN'],
    ['contact_livraison_tel', '+33 6 80 99 29 74'],
    ['shopify_location_id', '82726682960'],
    ['alerte_email_destinataire', 'mathieu@french-bandit.com'],
    ['bdc_prefixe_numero', 'BDC'],
  ];
  const tx = db.transaction((rows) => {
    rows.forEach(([k, v]) => {
      stmts.upsertParametreGlobalIfAbsent.run(k, v, now);
    });
  });
  tx(defaults);

  // Seed familles avec couvertures visées + coeff sécurité par défaut.
  // On utilise INSERT OR IGNORE via l'upsert (avec la clé (famille, animal))
  // pour ne pas écraser des overrides existants.
  const familles = [
    // Fontaine → produit à forte tendance, couverture 60j pour absorber les pics
    { famille: 'Fontaine', animal: 'Chat', couverture_visee_jours: 60, coeff_securite: 1.15, coeff_tendance: 1.0 },
  ];
  familles.forEach(f => {
    const existing = stmts.selectParametreFamille.get(f.famille, f.animal);
    if (existing) return;
    stmts.upsertParametreFamille.run({
      famille: f.famille,
      animal: f.animal,
      couverture_visee_jours: f.couverture_visee_jours,
      coeff_securite: f.coeff_securite,
      coeff_saisonnalite_json: null,
      coeff_tendance: f.coeff_tendance,
      now,
    });
  });
}

function prepareStatements() {
  stmts = {
    // ---- fournisseurs ----
    insertFournisseur: db.prepare(`
      INSERT INTO stock_fournisseurs (nom, email, adresse, contact, devise, incoterm, conditions_paiement, notes, actif, created_at, updated_at)
      VALUES (@nom, @email, @adresse, @contact, @devise, @incoterm, @conditions_paiement, @notes, 1, @now, @now)
    `),
    updateFournisseur: db.prepare(`
      UPDATE stock_fournisseurs SET
        email = @email,
        adresse = @adresse,
        contact = @contact,
        devise = @devise,
        incoterm = @incoterm,
        conditions_paiement = @conditions_paiement,
        notes = @notes,
        actif = @actif,
        updated_at = @now
      WHERE id = @id
    `),
    selectFournisseurById: db.prepare(`SELECT * FROM stock_fournisseurs WHERE id = ?`),
    selectFournisseurByNom: db.prepare(`SELECT * FROM stock_fournisseurs WHERE nom = ?`),
    selectAllFournisseurs: db.prepare(`SELECT * FROM stock_fournisseurs ORDER BY nom COLLATE NOCASE`),

    // ---- referentiel_sku ----
    upsertReferentielSku: db.prepare(`
      INSERT INTO stock_referentiel_sku (
        sku, categorie, famille, animal, cip, ean_13, ean_12,
        nom_court, nom_long, nom_en, motif, taille, url_produit,
        pa_dernier, pa_vs, pvc_ttc, pays_fabrication, hs_code_10, hs_code_6,
        poids_brut, poids_net, longueur, largeur, hauteur,
        lead_time_jours, couverture_visee_jours, couverture_visee_source,
        matrice_bg_ref, matrice_bh_ref,
        actif, seeded_at, updated_at
      ) VALUES (
        @sku, @categorie, @famille, @animal, @cip, @ean_13, @ean_12,
        @nom_court, @nom_long, @nom_en, @motif, @taille, @url_produit,
        @pa_dernier, @pa_vs, @pvc_ttc, @pays_fabrication, @hs_code_10, @hs_code_6,
        @poids_brut, @poids_net, @longueur, @largeur, @hauteur,
        @lead_time_jours, @couverture_visee_jours, @couverture_visee_source,
        @matrice_bg_ref, @matrice_bh_ref,
        1, @now, @now
      )
      ON CONFLICT(sku) DO UPDATE SET
        categorie = excluded.categorie,
        famille = excluded.famille,
        animal = excluded.animal,
        cip = excluded.cip,
        ean_13 = excluded.ean_13,
        ean_12 = excluded.ean_12,
        nom_court = excluded.nom_court,
        nom_long = excluded.nom_long,
        nom_en = excluded.nom_en,
        motif = excluded.motif,
        taille = excluded.taille,
        url_produit = excluded.url_produit,
        pa_dernier = excluded.pa_dernier,
        pa_vs = excluded.pa_vs,
        pvc_ttc = excluded.pvc_ttc,
        pays_fabrication = excluded.pays_fabrication,
        hs_code_10 = excluded.hs_code_10,
        hs_code_6 = excluded.hs_code_6,
        poids_brut = excluded.poids_brut,
        poids_net = excluded.poids_net,
        longueur = excluded.longueur,
        largeur = excluded.largeur,
        hauteur = excluded.hauteur,
        lead_time_jours = COALESCE(excluded.lead_time_jours, stock_referentiel_sku.lead_time_jours),
        couverture_visee_jours = COALESCE(excluded.couverture_visee_jours, stock_referentiel_sku.couverture_visee_jours),
        couverture_visee_source = COALESCE(excluded.couverture_visee_source, stock_referentiel_sku.couverture_visee_source),
        matrice_bg_ref = excluded.matrice_bg_ref,
        matrice_bh_ref = excluded.matrice_bh_ref,
        seeded_at = excluded.seeded_at,
        updated_at = excluded.updated_at
    `),
    updateReferentielShopify: db.prepare(`
      UPDATE stock_referentiel_sku SET
        shopify_product_id = @shopify_product_id,
        shopify_variant_id = @shopify_variant_id,
        shopify_inventory_item_id = @shopify_inventory_item_id,
        shopify_variant_title = @shopify_variant_title,
        image_url = COALESCE(@image_url, image_url),
        updated_at = @now
      WHERE sku = @sku
    `),
    updateReferentielOverrides: db.prepare(`
      UPDATE stock_referentiel_sku SET
        fournisseur_defaut_id = @fournisseur_defaut_id,
        moq = @moq,
        colisage = @colisage,
        lead_time_jours = @lead_time_jours,
        couverture_visee_jours = @couverture_visee_jours,
        couverture_visee_source = @couverture_visee_source,
        actif = @actif,
        updated_at = @now
      WHERE sku = @sku
    `),
    selectReferentielSku: db.prepare(`SELECT * FROM stock_referentiel_sku WHERE sku = ?`),
    selectAllReferentiel: db.prepare(`SELECT * FROM stock_referentiel_sku ORDER BY famille, animal, sku`),
    selectActifReferentiel: db.prepare(`SELECT * FROM stock_referentiel_sku WHERE actif = 1 ORDER BY famille, animal, sku`),
    countReferentiel: db.prepare(`SELECT COUNT(*) AS n FROM stock_referentiel_sku`),
    selectByInventoryItemId: db.prepare(`SELECT * FROM stock_referentiel_sku WHERE shopify_inventory_item_id = ?`),

    // ---- parametres_famille ----
    upsertParametreFamille: db.prepare(`
      INSERT INTO stock_parametres_famille (famille, animal, couverture_visee_jours, coeff_securite, coeff_saisonnalite_json, coeff_tendance, updated_at)
      VALUES (@famille, @animal, @couverture_visee_jours, @coeff_securite, @coeff_saisonnalite_json, @coeff_tendance, @now)
      ON CONFLICT(famille, animal) DO UPDATE SET
        couverture_visee_jours = excluded.couverture_visee_jours,
        coeff_securite = excluded.coeff_securite,
        coeff_saisonnalite_json = excluded.coeff_saisonnalite_json,
        coeff_tendance = excluded.coeff_tendance,
        updated_at = excluded.updated_at
    `),
    selectParametreFamille: db.prepare(`SELECT * FROM stock_parametres_famille WHERE famille = ? AND animal = ?`),
    selectAllParametresFamille: db.prepare(`SELECT * FROM stock_parametres_famille ORDER BY famille, animal`),

    // ---- previsions_mensuelles ----
    upsertPrevisionMensuelle: db.prepare(`
      INSERT INTO stock_previsions_mensuelles (sku, annee, mois, ventes_reelles, is_estimated, updated_at)
      VALUES (@sku, @annee, @mois, @ventes_reelles, @is_estimated, @now)
      ON CONFLICT(sku, annee, mois) DO UPDATE SET
        ventes_reelles = excluded.ventes_reelles,
        is_estimated = excluded.is_estimated,
        updated_at = excluded.updated_at
    `),
    selectPrevisionsForSku: db.prepare(`SELECT annee, mois, ventes_reelles, is_estimated FROM stock_previsions_mensuelles WHERE sku = ? ORDER BY annee, mois`),
    selectAllPrevisions: db.prepare(`SELECT sku, annee, mois, ventes_reelles, is_estimated FROM stock_previsions_mensuelles`),
    countPrevisionsForSku: db.prepare(`SELECT COUNT(*) AS n FROM stock_previsions_mensuelles WHERE sku = ?`),

    // ---- stock_actuel ----
    upsertStockActuel: db.prepare(`
      INSERT INTO stock_actuel (sku, stock_dispo, location_id, source, updated_at)
      VALUES (@sku, @stock_dispo, @location_id, @source, @now)
      ON CONFLICT(sku) DO UPDATE SET
        stock_dispo = excluded.stock_dispo,
        location_id = excluded.location_id,
        source = excluded.source,
        updated_at = excluded.updated_at
    `),
    selectStockActuel: db.prepare(`SELECT * FROM stock_actuel WHERE sku = ?`),
    selectAllStockActuel: db.prepare(`SELECT * FROM stock_actuel`),

    // ---- bdc ----
    insertBdc: db.prepare(`
      INSERT INTO stock_bdc (numero, fournisseur_id, date_creation, date_envoi, date_eta, date_reception_prevue, date_reception_reelle, statut, montant_total, devise, notes, created_at, updated_at)
      VALUES (@numero, @fournisseur_id, @date_creation, @date_envoi, @date_eta, @date_reception_prevue, @date_reception_reelle, @statut, @montant_total, @devise, @notes, @now, @now)
    `),
    updateBdc: db.prepare(`
      UPDATE stock_bdc SET
        fournisseur_id = @fournisseur_id,
        date_envoi = @date_envoi,
        date_eta = @date_eta,
        date_reception_prevue = @date_reception_prevue,
        date_reception_reelle = @date_reception_reelle,
        statut = @statut,
        montant_total = @montant_total,
        devise = @devise,
        notes = @notes,
        updated_at = @now
      WHERE id = @id
    `),
    selectBdcById: db.prepare(`SELECT * FROM stock_bdc WHERE id = ?`),
    selectBdcByNumero: db.prepare(`SELECT * FROM stock_bdc WHERE numero = ?`),
    selectAllBdc: db.prepare(`SELECT * FROM stock_bdc ORDER BY date_creation DESC`),
    selectBdcByStatut: db.prepare(`SELECT * FROM stock_bdc WHERE statut IN (SELECT value FROM json_each(?)) ORDER BY date_creation DESC`),
    selectMaxBdcNumeroForYear: db.prepare(`SELECT MAX(numero) AS max_num FROM stock_bdc WHERE numero LIKE ?`),

    // ---- bdc_lignes ----
    insertBdcLigne: db.prepare(`
      INSERT INTO stock_bdc_lignes (bdc_id, sku, qte_commandee, qte_recue, pa_unitaire, devise)
      VALUES (@bdc_id, @sku, @qte_commandee, @qte_recue, @pa_unitaire, @devise)
    `),
    updateBdcLigne: db.prepare(`
      UPDATE stock_bdc_lignes SET
        qte_commandee = @qte_commandee,
        qte_recue = @qte_recue,
        pa_unitaire = @pa_unitaire,
        devise = @devise
      WHERE id = @id
    `),
    deleteBdcLignesForBdc: db.prepare(`DELETE FROM stock_bdc_lignes WHERE bdc_id = ?`),
    selectBdcLignesForBdc: db.prepare(`SELECT * FROM stock_bdc_lignes WHERE bdc_id = ? ORDER BY id`),
    selectBdcLignesForSku: db.prepare(`
      SELECT l.*, b.numero, b.statut, b.date_envoi, b.date_eta, b.date_reception_prevue, b.date_reception_reelle
      FROM stock_bdc_lignes l
      JOIN stock_bdc b ON b.id = l.bdc_id
      WHERE l.sku = ? AND b.statut IN ('envoye', 'confirme', 'expedie')
      ORDER BY b.date_eta
    `),

    // ---- alertes_etat ----
    upsertAlerteEtat: db.prepare(`
      INSERT INTO stock_alertes_etat (sku, niveau, niveau_precedent, date_rupture_estimee, proposition_qte, proposition_montant, message, updated_at)
      VALUES (@sku, @niveau, @niveau_precedent, @date_rupture_estimee, @proposition_qte, @proposition_montant, @message, @now)
      ON CONFLICT(sku) DO UPDATE SET
        niveau_precedent = stock_alertes_etat.niveau,
        niveau = excluded.niveau,
        date_rupture_estimee = excluded.date_rupture_estimee,
        proposition_qte = excluded.proposition_qte,
        proposition_montant = excluded.proposition_montant,
        message = excluded.message,
        updated_at = excluded.updated_at
    `),
    selectAllAlertes: db.prepare(`SELECT * FROM stock_alertes_etat`),
    selectAlertesByNiveau: db.prepare(`SELECT * FROM stock_alertes_etat WHERE niveau IN (SELECT value FROM json_each(?))`),

    // ---- parametres_globaux ----
    upsertParametreGlobal: db.prepare(`
      INSERT INTO stock_parametres_globaux (key, value, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    `),
    upsertParametreGlobalIfAbsent: db.prepare(`
      INSERT INTO stock_parametres_globaux (key, value, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(key) DO NOTHING
    `),
    selectParametreGlobal: db.prepare(`SELECT value FROM stock_parametres_globaux WHERE key = ?`),
    selectAllParametresGlobaux: db.prepare(`SELECT key, value, updated_at FROM stock_parametres_globaux ORDER BY key`),

    // ---- sync_log ----
    insertSyncLogStart: db.prepare(`
      INSERT INTO stock_sync_log (type, status, message, started_at) VALUES (?, 'running', NULL, ?)
    `),
    updateSyncLogFinish: db.prepare(`
      UPDATE stock_sync_log SET status = @status, message = @message, duration_ms = @duration_ms, finished_at = @finished_at WHERE id = @id
    `),
    selectRecentSyncLog: db.prepare(`SELECT * FROM stock_sync_log ORDER BY started_at DESC LIMIT ?`),
    selectRecentSyncLogByType: db.prepare(`SELECT * FROM stock_sync_log WHERE type = ? ORDER BY started_at DESC LIMIT ?`),
    selectSyncLogStartedAt: db.prepare(`SELECT started_at FROM stock_sync_log WHERE id = ?`),
  };
}

// ============================================================
// API — grouped by domain
// ============================================================

// ------------------------ Fournisseurs ------------------------
function upsertFournisseur({ nom, email = null, adresse = null, contact = null, devise = 'EUR', incoterm = 'EXW', conditions_paiement = null, notes = null, actif = 1 }) {
  const now = Date.now();
  const existing = stmts.selectFournisseurByNom.get(nom);
  if (existing) {
    stmts.updateFournisseur.run({
      id: existing.id, email, adresse, contact, devise, incoterm, conditions_paiement, notes, actif, now,
    });
    return existing.id;
  }
  const info = stmts.insertFournisseur.run({
    nom, email, adresse, contact, devise, incoterm, conditions_paiement, notes, now,
  });
  return info.lastInsertRowid;
}

function getFournisseurById(id) { return stmts.selectFournisseurById.get(id) || null; }
function getFournisseurByNom(nom) { return stmts.selectFournisseurByNom.get(nom) || null; }
function listFournisseurs() { return stmts.selectAllFournisseurs.all(); }

// ------------------------ Référentiel SKU ------------------------
function upsertReferentielSKUBulk(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const now = Date.now();
  let n = 0;
  const tx = db.transaction((items) => {
    items.forEach(r => {
      stmts.upsertReferentielSku.run({
        sku: r.sku,
        categorie: r.categorie ?? null,
        famille: r.famille ?? null,
        animal: r.animal ?? null,
        cip: r.cip ?? null,
        ean_13: r.ean_13 ?? null,
        ean_12: r.ean_12 ?? null,
        nom_court: r.nom_court ?? null,
        nom_long: r.nom_long ?? null,
        nom_en: r.nom_en ?? null,
        motif: r.motif ?? null,
        taille: r.taille ?? null,
        url_produit: r.url_produit ?? null,
        pa_dernier: r.pa_dernier ?? null,
        pa_vs: r.pa_vs ?? null,
        pvc_ttc: r.pvc_ttc ?? null,
        pays_fabrication: r.pays_fabrication ?? null,
        hs_code_10: r.hs_code_10 ?? null,
        hs_code_6: r.hs_code_6 ?? null,
        poids_brut: r.poids_brut ?? null,
        poids_net: r.poids_net ?? null,
        longueur: r.longueur ?? null,
        largeur: r.largeur ?? null,
        hauteur: r.hauteur ?? null,
        lead_time_jours: r.lead_time_jours ?? null,
        couverture_visee_jours: r.couverture_visee_jours ?? null,
        couverture_visee_source: r.couverture_visee_source ?? null,
        matrice_bg_ref: r.matrice_bg_ref ?? null,
        matrice_bh_ref: r.matrice_bh_ref ?? null,
        now,
      });
      n++;
    });
  });
  tx(rows);
  return n;
}

function updateReferentielShopify(sku, { shopify_product_id = null, shopify_variant_id = null, shopify_inventory_item_id = null, shopify_variant_title = null, image_url = null }) {
  const now = Date.now();
  return stmts.updateReferentielShopify.run({
    sku, shopify_product_id, shopify_variant_id, shopify_inventory_item_id, shopify_variant_title, image_url, now,
  });
}

function updateReferentielOverrides(sku, overrides) {
  const cur = stmts.selectReferentielSku.get(sku);
  if (!cur) throw new Error(`SKU inconnu: ${sku}`);
  const now = Date.now();
  return stmts.updateReferentielOverrides.run({
    sku,
    fournisseur_defaut_id: overrides.fournisseur_defaut_id ?? cur.fournisseur_defaut_id,
    moq: overrides.moq ?? cur.moq,
    colisage: overrides.colisage ?? cur.colisage,
    lead_time_jours: overrides.lead_time_jours ?? cur.lead_time_jours,
    couverture_visee_jours: overrides.couverture_visee_jours ?? cur.couverture_visee_jours,
    couverture_visee_source: overrides.couverture_visee_source ?? cur.couverture_visee_source,
    actif: overrides.actif ?? cur.actif,
    now,
  });
}

function getReferentielSku(sku) { return stmts.selectReferentielSku.get(sku) || null; }
function listReferentielAll() { return stmts.selectAllReferentiel.all(); }
function listReferentielActif() { return stmts.selectActifReferentiel.all(); }
function countReferentiel() { return stmts.countReferentiel.get().n; }
function getSkuByInventoryItemId(inventoryItemId) { return stmts.selectByInventoryItemId.get(String(inventoryItemId)) || null; }

// ------------------------ Paramètres famille ------------------------
function upsertParametreFamille({ famille, animal, couverture_visee_jours = 90, coeff_securite = 1.1, coeff_saisonnalite = null, coeff_tendance = 1.0 }) {
  const now = Date.now();
  stmts.upsertParametreFamille.run({
    famille, animal, couverture_visee_jours, coeff_securite,
    coeff_saisonnalite_json: coeff_saisonnalite ? JSON.stringify(coeff_saisonnalite) : null,
    coeff_tendance, now,
  });
}
function getParametreFamille(famille, animal) {
  const r = stmts.selectParametreFamille.get(famille, animal);
  if (!r) return null;
  return { ...r, coeff_saisonnalite: r.coeff_saisonnalite_json ? safeJson(r.coeff_saisonnalite_json) : null };
}
function listParametresFamille() {
  return stmts.selectAllParametresFamille.all().map(r => ({
    ...r,
    coeff_saisonnalite: r.coeff_saisonnalite_json ? safeJson(r.coeff_saisonnalite_json) : null,
  }));
}

// ------------------------ Prévisions mensuelles ------------------------
function upsertPrevisionsMensuellesBulk(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const now = Date.now();
  let n = 0;
  const tx = db.transaction((items) => {
    items.forEach(r => {
      stmts.upsertPrevisionMensuelle.run({
        sku: r.sku,
        annee: r.annee,
        mois: r.mois,
        ventes_reelles: r.ventes_reelles || 0,
        is_estimated: r.is_estimated ? 1 : 0,
        now,
      });
      n++;
    });
  });
  tx(rows);
  return n;
}

function getPrevisionsForSku(sku) { return stmts.selectPrevisionsForSku.all(sku); }
function countPrevisionsForSku(sku) { return stmts.countPrevisionsForSku.get(sku).n; }
function listAllPrevisions() { return stmts.selectAllPrevisions.all(); }

// ------------------------ Stock actuel ------------------------
function upsertStockActuelBulk(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const now = Date.now();
  let n = 0;
  const tx = db.transaction((items) => {
    items.forEach(r => {
      stmts.upsertStockActuel.run({
        sku: r.sku,
        stock_dispo: r.stock_dispo || 0,
        location_id: r.location_id || null,
        source: r.source || null,
        now,
      });
      n++;
    });
  });
  tx(rows);
  return n;
}

function getStockActuel(sku) { return stmts.selectStockActuel.get(sku) || null; }
function listStockActuel() { return stmts.selectAllStockActuel.all(); }

// ------------------------ BDC ------------------------
const BDC_STATUTS = ['brouillon', 'envoye', 'confirme', 'expedie', 'receptionne', 'annule'];
const BDC_STATUTS_EN_COURS = ['envoye', 'confirme', 'expedie']; // "en-cours utiles" — participent au stock projeté

function nextBdcNumero(prefix = 'BDC', year = new Date().getFullYear()) {
  const like = `${prefix}-${year}-%`;
  const row = stmts.selectMaxBdcNumeroForYear.get(like);
  const max = row && row.max_num ? row.max_num : null;
  let n = 1;
  if (max) {
    const m = max.match(/-(\d+)$/);
    if (m) n = parseInt(m[1], 10) + 1;
  }
  return `${prefix}-${year}-${String(n).padStart(3, '0')}`;
}

function createBdc({ fournisseur_id, statut = 'brouillon', date_envoi = null, date_eta = null, date_reception_prevue = null, notes = null, devise = 'EUR', lignes = [] }) {
  const now = Date.now();
  const prefixRow = stmts.selectParametreGlobal.get('bdc_prefixe_numero');
  const prefix = prefixRow && prefixRow.value ? prefixRow.value : 'BDC';
  const numero = nextBdcNumero(prefix);
  const montant_total = lignes.reduce((s, l) => s + (l.qte_commandee || 0) * (l.pa_unitaire || 0), 0);

  let bdcId;
  const tx = db.transaction(() => {
    const info = stmts.insertBdc.run({
      numero, fournisseur_id,
      date_creation: now,
      date_envoi, date_eta, date_reception_prevue,
      date_reception_reelle: null,
      statut, montant_total, devise, notes,
      now,
    });
    bdcId = info.lastInsertRowid;
    lignes.forEach(l => {
      stmts.insertBdcLigne.run({
        bdc_id: bdcId,
        sku: l.sku,
        qte_commandee: l.qte_commandee,
        qte_recue: l.qte_recue || 0,
        pa_unitaire: l.pa_unitaire ?? null,
        devise: l.devise || devise,
      });
    });
  });
  tx();
  return { id: bdcId, numero };
}

function updateBdcMeta(id, patch) {
  const cur = stmts.selectBdcById.get(id);
  if (!cur) throw new Error(`BDC introuvable: ${id}`);
  const now = Date.now();
  stmts.updateBdc.run({
    id,
    fournisseur_id: patch.fournisseur_id ?? cur.fournisseur_id,
    date_envoi: patch.date_envoi ?? cur.date_envoi,
    date_eta: patch.date_eta ?? cur.date_eta,
    date_reception_prevue: patch.date_reception_prevue ?? cur.date_reception_prevue,
    date_reception_reelle: patch.date_reception_reelle ?? cur.date_reception_reelle,
    statut: patch.statut ?? cur.statut,
    montant_total: patch.montant_total ?? cur.montant_total,
    devise: patch.devise ?? cur.devise,
    notes: patch.notes ?? cur.notes,
    now,
  });
}

function replaceBdcLignes(bdc_id, lignes = []) {
  const cur = stmts.selectBdcById.get(bdc_id);
  if (!cur) throw new Error(`BDC introuvable: ${bdc_id}`);
  const now = Date.now();
  const montant_total = lignes.reduce((s, l) => s + (l.qte_commandee || 0) * (l.pa_unitaire || 0), 0);
  const tx = db.transaction(() => {
    stmts.deleteBdcLignesForBdc.run(bdc_id);
    lignes.forEach(l => {
      stmts.insertBdcLigne.run({
        bdc_id, sku: l.sku,
        qte_commandee: l.qte_commandee,
        qte_recue: l.qte_recue || 0,
        pa_unitaire: l.pa_unitaire ?? null,
        devise: l.devise || cur.devise,
      });
    });
    stmts.updateBdc.run({
      id: bdc_id,
      fournisseur_id: cur.fournisseur_id,
      date_envoi: cur.date_envoi,
      date_eta: cur.date_eta,
      date_reception_prevue: cur.date_reception_prevue,
      date_reception_reelle: cur.date_reception_reelle,
      statut: cur.statut,
      montant_total,
      devise: cur.devise,
      notes: cur.notes,
      now,
    });
  });
  tx();
}

function getBdc(id) { return stmts.selectBdcById.get(id) || null; }
function getBdcByNumero(numero) { return stmts.selectBdcByNumero.get(numero) || null; }
function listBdc() { return stmts.selectAllBdc.all(); }
function listBdcByStatut(statuts) { return stmts.selectBdcByStatut.all(JSON.stringify(statuts)); }
function listBdcEnCours() { return listBdcByStatut(BDC_STATUTS_EN_COURS); }

function getBdcLignes(bdc_id) { return stmts.selectBdcLignesForBdc.all(bdc_id); }
function getEnCoursForSku(sku) { return stmts.selectBdcLignesForSku.all(sku); }

// ------------------------ Alertes ------------------------
function upsertAlertesEtatBulk(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const now = Date.now();
  let n = 0;
  const tx = db.transaction((items) => {
    items.forEach(r => {
      stmts.upsertAlerteEtat.run({
        sku: r.sku,
        niveau: r.niveau,
        niveau_precedent: r.niveau_precedent ?? null,
        date_rupture_estimee: r.date_rupture_estimee ?? null,
        proposition_qte: r.proposition_qte ?? null,
        proposition_montant: r.proposition_montant ?? null,
        message: r.message ?? null,
        now,
      });
      n++;
    });
  });
  tx(rows);
  return n;
}

function listAllAlertes() { return stmts.selectAllAlertes.all(); }
function listAlertesByNiveau(niveaux) { return stmts.selectAlertesByNiveau.all(JSON.stringify(niveaux)); }

// ------------------------ Paramètres globaux ------------------------
function setParametreGlobal(key, value) {
  stmts.upsertParametreGlobal.run(key, value, Date.now());
}
function getParametreGlobal(key) {
  const r = stmts.selectParametreGlobal.get(key);
  return r ? r.value : null;
}
function listParametresGlobaux() {
  return stmts.selectAllParametresGlobaux.all();
}

// ------------------------ Sync log ------------------------
function startSync(type) {
  const info = stmts.insertSyncLogStart.run(type, Date.now());
  return info.lastInsertRowid;
}
function finishSync(id, { status = 'ok', message = null } = {}) {
  const finished_at = Date.now();
  const row = stmts.selectSyncLogStartedAt.get(id);
  const started_at = row ? row.started_at : finished_at;
  stmts.updateSyncLogFinish.run({
    id, status, message, duration_ms: finished_at - started_at, finished_at,
  });
}
function listRecentSyncLog(limit = 50) { return stmts.selectRecentSyncLog.all(limit); }
function listRecentSyncLogByType(type, limit = 20) { return stmts.selectRecentSyncLogByType.all(type, limit); }

// ------------------------ Diagnostics ------------------------
function stats() {
  return {
    referentielSku: countReferentiel(),
    fournisseurs: db.prepare(`SELECT COUNT(*) AS n FROM stock_fournisseurs`).get().n,
    parametresFamille: db.prepare(`SELECT COUNT(*) AS n FROM stock_parametres_famille`).get().n,
    previsionsMensuelles: db.prepare(`SELECT COUNT(*) AS n FROM stock_previsions_mensuelles`).get().n,
    stockActuel: db.prepare(`SELECT COUNT(*) AS n FROM stock_actuel`).get().n,
    bdc: db.prepare(`SELECT COUNT(*) AS n FROM stock_bdc`).get().n,
    bdcEnCours: db.prepare(`SELECT COUNT(*) AS n FROM stock_bdc WHERE statut IN ('envoye','confirme','expedie')`).get().n,
    alertes: db.prepare(`SELECT COUNT(*) AS n FROM stock_alertes_etat`).get().n,
    parametresGlobaux: db.prepare(`SELECT COUNT(*) AS n FROM stock_parametres_globaux`).get().n,
    syncLog: db.prepare(`SELECT COUNT(*) AS n FROM stock_sync_log`).get().n,
  };
}

// ------------------------ Internals ------------------------
function safeJson(s) {
  try { return JSON.parse(s); } catch { return null; }
}

module.exports = {
  init,
  // fournisseurs
  upsertFournisseur, getFournisseurById, getFournisseurByNom, listFournisseurs,
  // referentiel
  upsertReferentielSKUBulk, updateReferentielShopify, updateReferentielOverrides,
  getReferentielSku, listReferentielAll, listReferentielActif, countReferentiel, getSkuByInventoryItemId,
  // parametres famille
  upsertParametreFamille, getParametreFamille, listParametresFamille,
  // previsions
  upsertPrevisionsMensuellesBulk, getPrevisionsForSku, countPrevisionsForSku, listAllPrevisions,
  // stock actuel
  upsertStockActuelBulk, getStockActuel, listStockActuel,
  // bdc
  BDC_STATUTS, BDC_STATUTS_EN_COURS,
  createBdc, updateBdcMeta, replaceBdcLignes, getBdc, getBdcByNumero, listBdc, listBdcByStatut, listBdcEnCours,
  getBdcLignes, getEnCoursForSku, nextBdcNumero,
  // alertes
  upsertAlertesEtatBulk, listAllAlertes, listAlertesByNiveau,
  // parametres globaux
  setParametreGlobal, getParametreGlobal, listParametresGlobaux,
  // sync log
  startSync, finishSync, listRecentSyncLog, listRecentSyncLogByType,
  // diagnostics
  stats,
};
