# Meta ad-level backfill — procédure de validation

Documente le lancement du backfill 2026-03-18 → hier et la validation d'écart demandée.

## Contexte

Persistance ad-level Meta introduite en 4 commits :
1. `cache.js` v2 : tables `meta_ad_daily` + `meta_creative`
2. `meta-sync.js` : fetch + upsert + backoff rate limit
3. `server.js` : cron 5h + endpoint backfill + npm script
4. `server.js` : `GET /api/meta/creatives`

**Aucune route existante n'a été modifiée.** `daily_metrics` et `/api/meta/analysis` continuent leur vie normale.

## Comment lancer le backfill demandé

```bash
cd www/bandit/bandit-acquisition
npm run meta:backfill -- --start=2026-03-18 --end=yesterday
```

Le script :
- init cache (migration v2 idempotente)
- appelle `metaSync.backfill('2026-03-18', 'YYYY-09-13')` — sériel jour par jour
- calcule automatiquement la validation croisée `sum(spend) meta_ad_daily` vs `sum(spend) daily_metrics.meta_json`
- tolérance ±2 % (exit 1 hors tolérance ou si erreurs)

Attendu : ~180 jours × ~5 pages insights + N creative endpoints. Sériel + backoff conservateur → **budgeter 30-60 min**.

## Test smoke local (2026-09-14)

Lancé sur 2 jours :

```
npm run meta:backfill -- --start=2026-09-12 --end=2026-09-13
```

Résultat :
- ✅ Migration v2 appliquée : `[Cache] Migrated schema to v2 (meta_ad_daily + meta_creative).`
- ✅ Init OK
- ✅ Boucle `syncDay` déclenchée pour chaque jour
- ✅ Reprise sur erreur : les 2 jours en erreur consignés dans `errors[]`, pas d'interruption
- ✅ Validation croisée s'exécute (0.00 € vs 0.00 € → tolérance OK par défaut)
- ❌ **Token META_ACCESS_TOKEN local expiré** (code 190, session expirée en juin 2026 — token utilisateur Meta 60j)

Le pipeline fonctionne. Blocage sur le token uniquement.

## Prochaine étape (côté user)

Deux options :

1. **Renouveler le token en local** puis relancer :
   ```bash
   npm run meta:backfill -- --start=2026-03-18 --end=yesterday
   ```

2. **Lancer sur staging/prod** (Railway) où le token système est déjà présent et rafraîchi. Deux façons :
   - Via SSH / one-off dyno : `npm run meta:backfill -- --start=2026-03-18 --end=yesterday`
   - Via endpoint admin (long-running, mode async) :
     ```
     POST /api/meta/sync/backfill?start=2026-03-18&end=2026-09-13&async=1
     Cookie admin session
     ```
     Le job tourne en arrière-plan, suivre les logs pour la progression.

## Ce que la validation doit vérifier

À reporter dans ce document après la première exécution complète :

- [ ] Spend total `meta_ad_daily` sur `2026-03-18 → 2026-09-13`
- [ ] Spend total `daily_metrics.meta_json` sur la même plage
- [ ] Écart absolu (€) et relatif (%)
- [ ] Écart ≤ ±2 % → OK
- [ ] Si écart > 2 %, documenter la raison probable :
  - Différence attribution (7d_click+1d_view explicite vs défauts compte pré-existant)
  - Fenêtre `daily_metrics` figée à J-2 vs re-sync ad-level qui capte les corrections tardives
  - `filtering: spend>5` dans /api/meta/analysis pour ad-level (existant) vs pas de filtre ici → **NOTE : le nouveau meta_ad_daily ne filtre PAS sur spend, donc il capte tout, y compris les micro-spends. Cela peut créer un léger surplus par rapport à daily_metrics qui agrège au niveau compte.**
  - Jours en erreur non retryés (checker `errors[]`)

## Points d'attention pour l'écart

**Ce qui peut créer un écart légitime** :

1. **Attribution windows explicites** — `meta_ad_daily` utilise `['7d_click', '1d_view']` forcé.
   `daily_metrics.meta_json` a été peuplé sans `action_attribution_windows` explicite → défauts du compte s'appliquent, qui peuvent être différents.

2. **Fenêtre de fraîcheur `daily_metrics`** — `ensureCached()` fige J-2 et antérieurs (voir `cache.js:528-540` + `server.js:766`). Un jour peuplé le 2026-04-02 avec les données d'un 2026-04-01 encore chaud n'a plus jamais été rafraîchi. Le backfill ad-level capte le spend "final" (avec ajustements tardifs).

3. **Filtrage `spend>5`** — `fetchMetaAdInsights` (server.js:3063) filtre `spend>5` au niveau ad. Le nouveau `fetchAdDailyInsights` (meta-sync.js) ne filtre PAS. Cependant, `daily_metrics.meta_json` est agrégé au niveau **compte** (`fetchMetaAdsData` avec `level: 'account'`) → pas de filtre spend, donc PAS d'écart de ce fait.

4. **Refunds / conversions post-attribution** — Meta peut ajuster les revenue quelques semaines après la conversion. daily_metrics figé ne reflète pas ces ajustements.

**Ce qui ne devrait PAS créer d'écart** :
- Doublons : PK `(day, ad_id)` garantit unicité. Un ad avec 2 rows du même jour est agrégé au niveau compte pareil.
- Timezone : `time_range` Meta est UTC. `daily_metrics.day` est calculé en Europe/Paris via `toParisDate()`. **⚠ ATTENTION** : cela peut créer un décalage de bord — un ad avec spend à 23h UTC = 01h Paris apparaît sous `day = D+1` dans `daily_metrics`, mais sous `day = D` dans `meta_ad_daily` (backfill utilise directement le `since/until` UTC). C'est probablement la source principale d'écart.

## À reporter ici après lancement

```
=== Résultats backfill 2026-03-18 → 2026-09-13 ===
Date lancement       : YYYY-MM-DD HH:MM
Environnement        : local | staging | prod
Durée totale         : XX min

Jours attendus       : 180
Jours OK             : ?
Jours en erreur      : ?
Rows insights total  : ?
Ads upsertés         : ?
Creatives fetched    : ?
Creatives cachés     : ?

Spend meta_ad_daily  : ? €
Spend daily_metrics  : ? €
Écart absolu         : ? €
Écart relatif        : ? %
Tolérance ±2 %       : OK | HORS TOL

Notes / raison écart :
- ...
```
