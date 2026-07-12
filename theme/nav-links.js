(() => {
    const implementations = new Map([
        ["Python wrapper", "https://zelanton.github.io/processkit-py/"],
        [".NET version", "https://zelanton.github.io/ProcessKit-fSharp/"],
    ]);
    const currentImplementation = "Rust version";

    const updateSwitcher = () => {
        let switcherItems = 0;

        for (const item of document.querySelectorAll(
            ".sidebar .chapter .chapter-link-wrapper > span",
        )) {
            const label = item.textContent.trim();

            if (label === currentImplementation) {
                item.classList.add("current-implementation");
                item.setAttribute("aria-current", "page");
                item.setAttribute(
                    "aria-label",
                    "Rust version (current implementation)",
                );
                switcherItems += 1;
            } else if (implementations.has(label)) {
                const link = document.createElement("a");
                link.href = implementations.get(label);
                link.textContent = label;
                item.replaceWith(link);
                switcherItems += 1;
            }
        }

        return switcherItems === implementations.size + 1;
    };

    const sidebar = document.getElementById("mdbook-sidebar");
    if (updateSwitcher() || sidebar === null) {
        return;
    }

    const observer = new MutationObserver(() => {
        if (updateSwitcher()) {
            observer.disconnect();
        }
    });
    observer.observe(sidebar, { childList: true, subtree: true });
})();