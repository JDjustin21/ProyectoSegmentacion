document.addEventListener("DOMContentLoaded", () => {
  const layout = document.getElementById("appLayout");
  const btn = document.getElementById("sidebarToggle");
  if (!layout || !btn) return;

  function setCollapsed(flag) {
    layout.classList.toggle("sidebar-collapsed", flag);
    btn.querySelector(".chev").textContent = flag ? "›" : "‹";
    btn.setAttribute("aria-label", flag ? "Mostrar menú" : "Ocultar menú");
    localStorage.setItem("sidebarCollapsed", flag ? "1" : "0");
  }

  const saved = localStorage.getItem("sidebarCollapsed") === "1";
  setCollapsed(saved);

  btn.addEventListener("click", () => {
    setCollapsed(!layout.classList.contains("sidebar-collapsed"));
  });
});
