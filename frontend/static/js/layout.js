// frontend/static/js/layout.js
//
// Controla el estado visual del layout principal.
// Actualmente solo maneja el colapso/expansión del sidebar y persiste
// la preferencia del usuario en localStorage.

document.addEventListener("DOMContentLoaded", () => {
  const layout = document.getElementById("appLayout");
  const btn = document.getElementById("sidebarToggle");
  const chev = btn?.querySelector(".chev");

  if (!layout || !btn || !chev) return;

  function setCollapsed(flag) {
    layout.classList.toggle("sidebar-collapsed", flag);
    chev.textContent = flag ? "›" : "‹";
    btn.setAttribute("aria-label", flag ? "Mostrar menú" : "Ocultar menú");
    localStorage.setItem("sidebarCollapsed", flag ? "1" : "0");
  }

  const saved = localStorage.getItem("sidebarCollapsed") === "1";
  setCollapsed(saved);

  btn.addEventListener("click", () => {
    setCollapsed(!layout.classList.contains("sidebar-collapsed"));
  });
});