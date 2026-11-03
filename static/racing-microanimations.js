/* PronoGP: optional cosmetic effects; no mutations to predictions or server data. */
(() => {
  "use strict";
  const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  if (reduceMotion) return;

  // Start lights are a tiny decorative sequence at the top of the editable
  // prediction form. They do NOT block interaction or submit the form.
  const form = document.querySelector("form.prono-entry-form");
  if (form) {
    const success = Array.from(document.querySelectorAll("main .flash")).some(
      el => /Pronostic enregistré\s*✅/i.test(el.textContent || "")
    );
    if (success) {
      const save = form.querySelector('.sticky-save button[type="submit"]');
      if (save) {
        const original = save.textContent;
        save.classList.add("gp-prono-saved-confirmation");
        save.textContent = "✓ Pronostic enregistré";
        window.setTimeout(() => {
          save.textContent = original;
          save.classList.remove("gp-prono-saved-confirmation");
        }, 2200);
      }
    } else {
      const lights = document.createElement("div");
      lights.className = "gp-prono-lights";
      lights.setAttribute("aria-hidden", "true");
      for (let i = 0; i < 4; i++) {
        const bulb = document.createElement("span");
        bulb.className = "gp-prono-light";
        lights.appendChild(bulb);
      }
      form.insertBefore(lights, form.firstChild);
      window.setTimeout(() => lights.remove(), 2300);
    }
  }

  // Reveal actual result cards once; content stays visible without JS.
  const podiums = Array.from(document.querySelectorAll(".podium")).filter(
    podium => podium.querySelector(".podium-card:not(.podium-empty)")
  );
  const onVisible = typeof IntersectionObserver === "function"
    ? new IntersectionObserver((entries, obs) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            entry.target.classList.add("gp-podium-entrance");
            obs.unobserve(entry.target);
          }
        }
      }, { threshold: .12 })
    : null;
  podiums.forEach(podium => {
    if (onVisible) onVisible.observe(podium);
    else podium.classList.add("gp-podium-entrance");
  });

  // Numeric counting affects the presentation only, never the scoring model.
  const numbers = document.querySelectorAll("[data-gp-counter]");
  numbers.forEach(node => {
    const valueText = node.textContent.trim();
    const match = valueText.match(/^([+-]?\d+(?:[.,]\d+)?)(.*)$/s);
    if (!match) return;
    const target = Number(match[1].replace(",", "."));
    if (!Number.isFinite(target) || target < 0) return;
    const suffix = match[2];
    const decimalCount = (match[1].split(/[.,]/)[1] || "").length;
    const delimiter = match[1].includes(",") ? "," : ".";
    let started = false;
    const play = () => {
      if (started) return;
      started = true;
      const start = performance.now();
      const format = n => n.toFixed(decimalCount).replace(".", delimiter) + suffix;
      const tick = now => {
        if (!node.isConnected) return;
        const progress = Math.min(1, (now - start) / 750);
        const eased = 1 - Math.pow(1 - progress, 3);
        if (progress < 1) {
          node.textContent = format(target * eased);
          requestAnimationFrame(tick);
        } else node.textContent = valueText; // original exact scoring string
      };
      requestAnimationFrame(tick);
    };
    if (onVisible) {
      const observer = new IntersectionObserver((entries, obs) => {
        if (entries.some(entry => entry.isIntersecting)) {
          play();
          obs.disconnect();
        }
      }, { threshold: .15 });
      observer.observe(node);
    } else play();
  });
})();
