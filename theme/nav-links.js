// Reserved implementation links and indicator for the guides sidebar.
//
// mdBook's SUMMARY.md cannot express a sidebar entry that points at an external
// URL: a list item's link target must be a chapter file in `src`, and a raw URL
// makes the build fail ("failed to read chapter https://..."). The three
// implementation entries are therefore carried in SUMMARY.md as *draft prefix
// chapters* (bare `[Title]()` links, no chapter files) pinned above
// `[Overview](README.md)` in the same un-numbered prefix block. mdBook v0.5.4
// renders them as
// <li class="chapter-item expanded "><span class="chapter-link-wrapper"><span>...</span></span></li>
// (verified against this repo's own `mdbook build` output — the draft title
// sits in a plain <span> nested inside .chapter-link-wrapper; this differs
// from the bare <div> shape the ProcessKit-fSharp reference documents for its
// own mdBook version, so the selector below is this repo's own, not a copy of
// the reference's).
// This script upgrades the two external entries to live links and marks the
// local implementation as a non-clickable indicator:
//
//   * "Rust version"   -> a non-clickable indicator for this implementation.
//   * "Python wrapper" -> a live external link to the Python wrapper's docs site.
//   * ".NET version"   -> a live external link to the .NET implementation's site.
//
// Without JS the entries degrade to plain greyed draft items — never a broken or
// misdirected link.
(function () {
  "use strict";

  var ENTRIES = {
    "Rust version": { placeholder: "Current implementation" },
    "Python wrapper": { href: "https://zelanton.github.io/processkit-py/" },
    ".NET version": { href: "https://zelanton.github.io/ProcessKit-fSharp/" }
  };

  function apply() {
    // mdBook v0.5.4 renders draft prefix chapters as:
    // <li class="chapter-item expanded "><span class="chapter-link-wrapper"><span>Rust version</span></span></li>
    // (verified against this repo's own `mdbook build` output; no leading "N."
    // since prefix chapters are never numbered — the digit-stripping regex
    // below is just defensive)
    var drafts = document.querySelectorAll(
      ".sidebar .chapter .chapter-link-wrapper > span"
    );

    Array.prototype.forEach.call(drafts, function (spanEntry) {
      var textContent = spanEntry.textContent || "";
      var title = textContent.replace(/^\s*\d+\.\s*/, "").trim();
      var spec = ENTRIES[title];
      if (!spec) {
        return;
      }

      if (spec.href) {
        var link = document.createElement("a");
        link.href = spec.href;
        link.rel = "noopener";
        while (spanEntry.firstChild) {
          link.appendChild(spanEntry.firstChild);
        }
        spanEntry.replaceWith(link);
      } else if (spec.placeholder) {
        spanEntry.classList.add("current-implementation");
        spanEntry.title = spec.placeholder;
        spanEntry.setAttribute("aria-label", title + " — " + spec.placeholder);
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", apply);
  } else {
    apply();
  }
})();
