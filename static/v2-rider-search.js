(() => {
  const selects = [...document.querySelectorAll('select')].filter(select =>
    [...select.options].filter(o => o.value.startsWith('#')).length > 5);
  for (const select of selects) {
    const original = [...select.options].map(o => ({value:o.value, label:o.text, selected:o.selected}));
    const input = document.createElement('input');
    input.type = 'search';
    input.autocomplete = 'off';
    input.placeholder = '🔎 N° ou nom du pilote…';
    input.setAttribute('aria-label','Rechercher pour '+(select.id || select.name));
    input.className = 'v2-rider-search';
    select.parentNode.insertBefore(input, select);
    input.addEventListener('input', () => {
      const query = input.value.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim();
      const picked = select.value;
      select.replaceChildren();
      original.filter(o => !query || o.label.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').includes(query) || o.value===picked)
        .forEach(o => {
          const node = new Option(o.label,o.value);
          node.selected = o.value===picked;
          select.add(node);
        });
      select.value = picked;
    });
    select.addEventListener('change', () => { input.value = ''; input.dispatchEvent(new Event('input')); });
  }
})();
