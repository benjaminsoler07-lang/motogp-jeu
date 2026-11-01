/* Alerte de connexion et compteur des notifications non lues — PronoGP. */
(() => {
  "use strict";

  const bell = document.querySelector("[data-notification-bell]");
  if (!bell) return;

  const badge = bell.querySelector("[data-unread-count]");
  const dialog = document.querySelector("[data-notification-dialog]");
  let restoreFocus = null;

  function closeDialog() {
    if (!dialog || dialog.hidden) return;
    dialog.hidden = true;
    document.body.classList.remove("gp-notification-modal-open");
    (restoreFocus && restoreFocus.isConnected ? restoreFocus : bell).focus({preventScroll: true});
  }

  function openDialog(count, latestTitle) {
    if (!dialog || !count) return;
    const heading = dialog.querySelector("#gp-notification-title");
    const label = dialog.querySelector("[data-notification-unread-label]");
    const latest = dialog.querySelector("[data-notification-latest]");
    heading.textContent = count === 1 ? "Une notification t'attend !" : count + " notifications t'attendent !";
    label.textContent = count === 1 ? "1 notification non lue" : count + " notifications non lues";
    latest.hidden = !latestTitle;
    latest.textContent = latestTitle ? "Dernier message : « " + latestTitle + " »" : "";
    restoreFocus = document.activeElement;
    dialog.hidden = false;
    document.body.classList.add("gp-notification-modal-open");
    dialog.querySelector("[data-notification-close]").focus({preventScroll: true});
  }

  if (dialog) {
    dialog.querySelectorAll("[data-notification-close]").forEach(button => {
      button.addEventListener("click", closeDialog);
    });
    dialog.addEventListener("click", event => {
      if (event.target === dialog) closeDialog();
    });
    dialog.addEventListener("keydown", event => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeDialog();
      }
      if (event.key !== "Tab") return;
      const focusable = Array.from(dialog.querySelectorAll("button:not([disabled]), a[href]"));
      if (!focusable.length) return;
      const first = focusable[0], last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });
  }

  const endpoint = bell.dataset.unreadUrl;
  if (!endpoint || !badge) return;

  function showUnread(data) {
    if (!data || !Number.isSafeInteger(data.count) || data.count < 0) return;
    const count = data.count;
    badge.hidden = count === 0;
    badge.textContent = count > 99 ? "99+" : String(count);
    bell.classList.toggle("has-unread", count > 0);
    bell.setAttribute("aria-label", count ?
      "Mes notifications (" + count + " non lue" + (count > 1 ? "s" : "") + ")" :
      "Mes notifications");
    bell.title = count ? count + " notification" + (count > 1 ? "s" : "") + " non lue" + (count > 1 ? "s" : "") : "Notifications";
    if (count > 0 && bell.dataset.onLogin === "1" && dialog) {
      openDialog(count, typeof data.latest_title === "string" ? data.latest_title : "");
      bell.dataset.onLogin = "0";
    }
  }

  // Le navigateur reçoit uniquement le résumé de l'utilisateur authentifié.
  // Les erreurs réseau ne doivent jamais bloquer la navigation.
  async function refresh() {
    try {
      const response = await fetch(endpoint, {
        credentials: "same-origin",
        cache: "no-store",
        headers: {"Accept": "application/json"}
      });
      if (response.ok) showUnread(await response.json());
    } catch (_) {
      // Le site et sa navigation continuent à fonctionner sans ce service.
    }
  }

  refresh();
})();
