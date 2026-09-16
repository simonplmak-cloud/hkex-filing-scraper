/* Accessibility patch for Material for MkDocs.
 *
 * Material renders the header search control as a bare checkbox (`#__search`) with no
 * accessible name, which axe reports as a WCAG 2.2 violation (`label`, critical). The real
 * search text field is labelled already; this gives the toggle one too, and keeps it applied
 * across instant navigation.
 */
(function () {
  "use strict";

  function labelSearchToggle() {
    var toggle = document.getElementById("__search");
    if (toggle && !toggle.getAttribute("aria-label")) {
      toggle.setAttribute("aria-label", "Toggle search");
    }
  }

  function makeScrollWrappersFocusable() {
    // Material wraps horizontally scrollable tables in .md-typeset__scrollwrap. A scroll
    // container must be keyboard reachable (axe: scrollable-region-focusable).
    var wrappers = document.querySelectorAll(".md-typeset__scrollwrap");
    for (var i = 0; i < wrappers.length; i++) {
      var wrapper = wrappers[i];
      if (wrapper.getAttribute("tabindex") !== "0") {
        wrapper.setAttribute("tabindex", "0");
        wrapper.setAttribute("role", "region");
        wrapper.setAttribute("aria-label", "Scrollable table");
      }
    }
  }

  function enhance() {
    labelSearchToggle();
    makeScrollWrappersFocusable();
  }

  enhance();
  document.addEventListener("DOMContentLoaded", enhance);
  window.addEventListener("load", enhance);

  // Material's instant-loading observable re-renders content on navigation.
  if (typeof document$ !== "undefined" && typeof document$.subscribe === "function") {
    document$.subscribe(enhance);
  }
})();
