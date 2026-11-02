/* Q1 / Sprint / GP / deadline: clock + seven-day visual runway. */
(() => {
  "use strict";
  const elements = document.querySelectorAll("[data-countdown]");
  if (!elements.length) return;

  elements.forEach(node => {
    const bar = document.createElement("span");
    bar.className = "gp-race-progress";
    bar.setAttribute("aria-hidden", "true");
    const fill = document.createElement("i");
    bar.appendChild(fill);
    node.insertAdjacentElement("afterend", bar);
    node._raceBar = bar;
  });

  const update = () => {
    const now = Date.now();
    elements.forEach(node => {
      const target = Date.parse(node.dataset.countdown);
      const bar = node._raceBar;
      const wrapper = node.closest(".v3-race-stage,.v2-race-metric,.notice") || node.parentElement;
      if (!Number.isFinite(target)) {
        node.textContent = "Horaire indisponible";
        if (bar) bar.hidden = true;
        return;
      }
      const secs = Math.max(0, Math.floor((target - now) / 1000));
      if (wrapper) {
        wrapper.classList.toggle("gp-race-is-soon", secs > 3600 && secs <= 86400);
        wrapper.classList.toggle("gp-race-is-imminent", secs > 0 && secs <= 3600);
      }
      if (bar) {
        bar.hidden = secs === 0;
        bar.style.setProperty("--gp-time-progress", String(Math.min(1, secs / 604800)));
      }
      if (!secs) {
        node.textContent = node.dataset.countdownFinished || "Clôturé / Terminé";
        return;
      }
      const d = Math.floor(secs / 86400), h = Math.floor(secs % 86400 / 3600);
      const m = Math.floor(secs % 3600 / 60), s = secs % 60;
      node.textContent = (d ? d + "j " : "") + [h, m, s].map(v => String(v).padStart(2, "0")).join(":");
    });
  };
  update();
  window.setInterval(update, 1000);
})();
