/* Partage PronoGP (PWA et navigateur) : partage natif, sinon copie du lien. */
(() => {
  "use strict";
  const button = document.querySelector("[data-share-pronogp]");
  if (!button) return;
  const feedback = document.querySelector("[data-share-feedback]");
  const url = button.dataset.shareUrl || (window.location.origin + "/");
  const title = "PronoGP";
  const text = "Rejoins PronoGP pour le championnat de pronostics MotoGP !";
  let feedbackTimer;

  function report(message) {
    if (!feedback) return;
    feedback.textContent = message;
    window.clearTimeout(feedbackTimer);
    feedbackTimer = window.setTimeout(() => { feedback.textContent = ""; }, 3500);
  }

  function legacyCopy(value) {
    const field = document.createElement("textarea");
    field.value = value;
    field.setAttribute("readonly", "");
    field.style.position = "fixed";
    field.style.left = "-9999px";
    document.body.appendChild(field);
    field.select();
    let copied = false;
    try { copied = document.execCommand("copy"); } catch (_) { /* no-op */ }
    field.remove();
    return copied;
  }

  async function copyLink() {
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(url);
        report("Lien PronoGP copié !");
        return;
      }
    } catch (_) { /* Continuer avec le repli */ }
    if (legacyCopy(url)) {
      report("Lien PronoGP copié !");
    } else {
      window.prompt("Copie le lien PronoGP :", url);
    }
  }

  button.addEventListener("click", async () => {
    if (typeof navigator.share === "function") {
      try {
        await navigator.share({ title, text, url });
        return;
      } catch (error) {
        if (error && error.name === "AbortError") return;
      }
    }
    await copyLink();
  });
})();
