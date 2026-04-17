// Script pour obtenir un TikTok Ads access token via OAuth
// Usage:
//   1. Remplis TIKTOK_APP_ID et TIKTOK_APP_SECRET dans le .env
//   2. Lance: node tiktok-auth.js
//   3. Ouvre l'URL affichée dans ton navigateur
//   4. Autorise l'app, copie le "auth_code" depuis l'URL de redirection
//   5. Relance: node tiktok-auth.js <auth_code>

require('dotenv').config();
const fetch = require('node-fetch');

const APP_ID = process.env.TIKTOK_APP_ID;
const APP_SECRET = process.env.TIKTOK_APP_SECRET;
const REDIRECT_URI = 'https://web-production-1b6dc.up.railway.app/api/tiktok/callback';

const authCode = process.argv[2];

if (!APP_ID || !APP_SECRET) {
  console.log('Ajoute TIKTOK_APP_ID et TIKTOK_APP_SECRET dans le .env');
  process.exit(1);
}

if (!authCode) {
  // Step 1: Generate auth URL
  const authUrl = `https://business-api.tiktok.com/portal/auth?app_id=${APP_ID}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}&state=bandit`;
  console.log('\n=== Étape 1 ===');
  console.log('Ouvre cette URL dans ton navigateur :\n');
  console.log(authUrl);
  console.log('\nAutorise l\'app, puis copie le "auth_code" depuis l\'URL de redirection.');
  console.log('Relance ensuite : node tiktok-auth.js <auth_code>\n');
} else {
  // Step 2: Exchange auth code for access token
  console.log('\n=== Étape 2 : échange du code... ===\n');

  (async () => {
    const res = await fetch('https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        app_id: APP_ID,
        secret: APP_SECRET,
        auth_code: authCode,
      }),
    });

    const data = await res.json();

    if (data.code === 0 && data.data) {
      console.log('Access Token :', data.data.access_token);
      console.log('Advertiser IDs :', data.data.advertiser_ids);
      console.log('\n=> Ajoute ces valeurs dans ton .env et dans Railway :');
      console.log(`TIKTOK_ACCESS_TOKEN=${data.data.access_token}`);
      if (data.data.advertiser_ids && data.data.advertiser_ids.length > 0) {
        console.log(`TIKTOK_ADVERTISER_ID=${data.data.advertiser_ids[0]}`);
      }
      console.log('');
    } else {
      console.log('Erreur :', data.message || JSON.stringify(data));
    }
  })();
}
