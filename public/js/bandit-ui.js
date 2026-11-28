// Coquille Bandit : ouverture du rail sur mobile.
(function () {
  var rail = document.getElementById('rail'), btn = document.getElementById('railToggle');
  if (!rail || !btn) return;
  var set = function (open) { rail.classList.toggle('is-open', open); btn.setAttribute('aria-expanded', open); };
  btn.addEventListener('click', function () { set(!rail.classList.contains('is-open')); });
  rail.addEventListener('click', function (e) { if (e.target.closest('.tab-btn, .rail__sub button, .rail__sub a')) set(false); });
  document.addEventListener('keydown', function (e) { if (e.key === 'Escape') set(false); });
})();
