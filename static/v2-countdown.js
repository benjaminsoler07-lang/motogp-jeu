(() => {
  const elements = document.querySelectorAll("[data-countdown]");
  if (!elements.length) return;
  const update = () => {
    const now = Date.now();
    elements.forEach((node) => {
      const target = Date.parse(node.dataset.countdown);
      if (!Number.isFinite(target)) { node.textContent = "Horaire indisponible"; return; }
      const secs = Math.max(0, Math.floor((target-now)/1000));
      if (!secs) { node.textContent = "Clôturé / Terminé"; return; }
      const d = Math.floor(secs/86400), h = Math.floor(secs%86400/3600);
      const m = Math.floor(secs%3600/60), s = secs%60;
      node.textContent = (d ? d+"j " : "") + [h,m,s].map(v => String(v).padStart(2,"0")).join(":");
    });
  };
  update(); setInterval(update,1000);
})();
