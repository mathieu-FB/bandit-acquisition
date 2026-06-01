// ============================================================
// NOTORIETY CAMPAIGNS — shared config + helpers
// ============================================================
// Brand/notoriety campaigns inflate top-of-funnel spend without direct
// attribution. A flat daily amount is subtracted from the matching
// channel for every day a dashboard / report range overlaps a campaign.
//
// Used by both server.js (dashboard + objectives endpoints) and
// daily-report.js (morning email) so figures stay aligned.
//
// To add a campaign: push an entry with start, end (YYYY-MM-DD),
// dailyMeta, dailyGoogle and a label.

const NOTORIETY_CAMPAIGNS = [
  {
    label: 'Campagne notoriété',
    start: '2026-05-22',
    end: '2026-06-22',
    dailyMeta: 3370,
    dailyGoogle: 2000,
  },
];

// Returns { metaTotal, googleTotal, days, campaigns: [...] } for the range
function getNotorietyAdjustment(startStr, endStr) {
  let metaTotal = 0, googleTotal = 0, days = 0;
  const campaigns = [];
  NOTORIETY_CAMPAIGNS.forEach(c => {
    const overlapStart = startStr > c.start ? startStr : c.start;
    const overlapEnd = endStr < c.end ? endStr : c.end;
    if (overlapStart > overlapEnd) return;
    const overlapDays = Math.round(
      (new Date(overlapEnd + 'T12:00:00') - new Date(overlapStart + 'T12:00:00')) / 86400000
    ) + 1;
    const meta = c.dailyMeta * overlapDays;
    const google = c.dailyGoogle * overlapDays;
    metaTotal += meta;
    googleTotal += google;
    days += overlapDays;
    campaigns.push({
      label: c.label, start: c.start, end: c.end,
      overlapStart, overlapEnd, days: overlapDays,
      dailyMeta: c.dailyMeta, dailyGoogle: c.dailyGoogle,
      meta, google,
    });
  });
  return { metaTotal, googleTotal, days, campaigns };
}

// Returns { meta, google } daily deduction for a single day
function getNotorietyDailyDeduction(day) {
  let meta = 0, google = 0;
  NOTORIETY_CAMPAIGNS.forEach(c => {
    if (day >= c.start && day <= c.end) {
      meta += c.dailyMeta;
      google += c.dailyGoogle;
    }
  });
  return { meta, google };
}

module.exports = {
  NOTORIETY_CAMPAIGNS,
  getNotorietyAdjustment,
  getNotorietyDailyDeduction,
};
