// Amélioration progressive : si ce script ne se charge pas, le formulaire HTML reste utilisable.
(() => {
  "use strict";

  const form = document.getElementById("profile-photo-form");
  const input = document.getElementById("photo");
  const feedback = document.getElementById("profile-photo-feedback");
  const preview = document.getElementById("profile-photo-preview");
  const button = document.getElementById("profile-photo-save");
  if (!form || !input || !feedback || !preview || !button) return;

  const MAX_SOURCE_BYTES = 50 * 1024 * 1024;
  const MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
  const MAX_DIMENSION = 1024;
  let previewUrl = null;
  let sending = false;

  function show(message, state = "") {
    feedback.textContent = message;
    feedback.dataset.state = state;
  }

  function clearPreview() {
    if (previewUrl) URL.revokeObjectURL(previewUrl);
    previewUrl = null;
    preview.removeAttribute("src");
    preview.hidden = true;
  }

  input.addEventListener("change", () => {
    clearPreview();
    const file = input.files && input.files[0];
    if (!file) {
      show("Sélectionne une photo, puis appuie sur Enregistrer.");
      return;
    }
    if (file.size > MAX_SOURCE_BYTES) {
      show("Cette photo dépasse 50 Mo. Réduis-la avant de l'envoyer.", "error");
      return;
    }
    previewUrl = URL.createObjectURL(file);
    preview.onload = () => {
      preview.hidden = false;
      show("Photo sélectionnée. Appuie sur Enregistrer ma photo.", "ready");
    };
    preview.onerror = () => {
      preview.hidden = true;
      show("Impossible de lire ce format. Utilise une image JPG, PNG ou WebP.", "error");
    };
    preview.src = previewUrl;
  });

  function decodeImage(file) {
    return new Promise((resolve, reject) => {
      const url = URL.createObjectURL(file);
      const image = new Image();
      image.onload = () => {
        URL.revokeObjectURL(url);
        resolve(image);
      };
      image.onerror = () => {
        URL.revokeObjectURL(url);
        reject(new Error("Format non reconnu. Essaie une photo JPG, PNG ou WebP."));
      };
      image.src = url;
    });
  }

  async function preparePhoto(file) {
    if (file.size > MAX_SOURCE_BYTES) {
      throw new Error("Photo trop volumineuse (50 Mo maximum avant réduction).");
    }
    const image = await decodeImage(file);
    if (!image.naturalWidth || !image.naturalHeight) {
      throw new Error("La photo est illisible.");
    }
    const ratio = Math.min(1, MAX_DIMENSION / Math.max(image.naturalWidth, image.naturalHeight));
    const canvas = document.createElement("canvas");
    canvas.width = Math.max(1, Math.round(image.naturalWidth * ratio));
    canvas.height = Math.max(1, Math.round(image.naturalHeight * ratio));
    const context = canvas.getContext("2d");
    if (!context) throw new Error("Impossible de préparer la photo sur cet appareil.");
    context.fillStyle = "#fff";
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const blob = await new Promise(resolve => canvas.toBlob(resolve, "image/jpeg", 0.84));
    canvas.width = 0;
    canvas.height = 0;
    if (!blob || blob.type !== "image/jpeg" || blob.size > MAX_UPLOAD_BYTES) {
      throw new Error("La photo n'a pas pu être préparée. Essaie une photo plus petite.");
    }
    return blob;
  }

  form.addEventListener("submit", async event => {
    event.preventDefault();
    if (sending) return;
    const file = input.files && input.files[0];
    if (!file) {
      show("Choisis d'abord une photo.", "error");
      return;
    }
    sending = true;
    button.disabled = true;
    button.textContent = "⏳ Envoi en cours…";
    show("Préparation et envoi de la photo…", "sending");

    try {
      const blob = await preparePhoto(file);
      const data = new FormData(form);
      data.set("photo", blob, "photo-profil.jpg");
      const response = await fetch(form.action, {
        method: "POST",
        credentials: "same-origin",
        body: data,
        cache: "no-store"
      });
      if (response.status === 413) throw new Error("Photo trop volumineuse pour le serveur.");
      if (!response.ok) throw new Error("Le serveur a refusé l'envoi (erreur " + response.status + ").");

      const documentResult = new DOMParser().parseFromString(await response.text(), "text/html");
      const returnedProfile = documentResult.getElementById("profile-photo-form");
      const message = documentResult.querySelector(".flash")?.textContent?.trim() || "";
      if (!returnedProfile) {
        throw new Error("Ta session a peut-être expiré. Reconnecte-toi, puis réessaie.");
      }
      if (!message.includes("Photo enregistrée.")) {
        throw new Error(message || "Enregistrement non confirmé. Réessaie.");
      }

      const currentAvatar = document.querySelector(".profile-v2 .avatar-v2 img");
      const updatedAvatar = documentResult.querySelector(".profile-v2 .avatar-v2 img");
      if (currentAvatar && updatedAvatar) {
        const url = new URL(updatedAvatar.getAttribute("src"), window.location.href);
        url.searchParams.set("refresh", String(Date.now()));
        currentAvatar.style.display = "";
        currentAvatar.src = url.toString();
      }
      input.value = "";
      show("✅ Photo enregistrée ! Elle sera visible sur les classements.", "success");
    } catch (error) {
      show(error instanceof Error ? error.message : "Erreur pendant l'envoi de la photo.", "error");
    } finally {
      sending = false;
      button.disabled = false;
      button.textContent = "📸 Enregistrer ma photo";
    }
  });
})();
