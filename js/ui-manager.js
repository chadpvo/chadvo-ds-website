/**
 * UI Manager - Centralizes the generation of global UI elements (Navbar, Footer)
 * Depends on: assets/js/page-config.js (defines SITE_CONFIG)
 */

class UIManager {
    constructor(config) {
        this.config = config;
    }

    // WIDGET 1: NAVIGATION BAR
    renderNavbar(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        // 1. Generate Navigation Links
        const linksHtml = this.config.navLinks.map(link => 
            `<a href="${link.url}" class="text-decoration-none text-dark ms-4 fw-medium">${link.name}</a>`
        ).join('');

        // 2. Build the Full Navbar HTML
        container.innerHTML = `
            <div class="container py-4">
                <div class="d-flex justify-content-between align-items-center">
                    
                    <a href="${this.config.navLinks.find(l => l.name === 'Home')?.url || '../../index.html'}">
                        <img src="${this.config.logoPath}" alt="${this.config.author.name}" class="site-logo">                    
                    </a>    
                    <div class="d-flex align-items-center">
                        <button id="themeToggle" class="theme-toggle me-3 bg-transparent border-0 p-2" aria-label="Toggle dark mode" style="color: inherit;">
                            <i class="fas fa-moon"></i>
                        </button>

                        <nav class="top-nav d-none d-md-block">
                            ${linksHtml}
                        </nav>
                        
                        </div>
                </div>
            </div>
        `;
        
        // 3. Initialize the Theme Toggle functionality
        this.initThemeToggle();
    }

    // WIDGET 2: FOOTER
    renderFooter(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        // 1. Generate Social Icons
        const socialHtml = this.config.socialLinks.map(link => 
            `<a href="${link.url}" target="_blank" class="mx-2 text-dark fs-4 text-decoration-none transition-hover">
                <i class="${link.icon}"></i>
             </a>`
        ).join('');

        // 2. Build the Footer HTML
        // Note: We inject the .container div inside the section
        container.innerHTML = `
            <div class="container">
                <h4 class="fw-bold mb-4">Let's Work Together!</h4>
                
                <div class="d-flex justify-content-center social-icons mb-4">
                    ${socialHtml}
                </div>
                
                <p class="footer-text small mb-1 text-muted">
                    &copy; Copyright <span id="year">${this.config.copyrightYear}</span> All rights reserved | Developed by ${this.config.author.name}
                </p>
            </div>
        `;
    }

    // UTILITY: THEME TOGGLE (Dark Mode)
    initThemeToggle() {
        const toggleBtn = document.getElementById('themeToggle');
        if (!toggleBtn) return;
        
        const icon = toggleBtn.querySelector('i');

        // 1. Helper to apply theme
        const applyTheme = (mode) => {
            document.body.classList.toggle('dark-mode', mode === 'dark');
            icon.className = mode === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
            
            // Mapbox hook: If a map exists on this page, try to update its style
            if (window.setTheme && typeof window.setTheme === 'function') {
                window.setTheme(mode);
            }
        };

        // 2. Load saved preference
        const savedTheme = localStorage.getItem('theme') || 'light';
        applyTheme(savedTheme);

        // 3. Handle Click
        toggleBtn.addEventListener('click', () => {
            const currentMode = document.body.classList.contains('dark-mode') ? 'dark' : 'light';
            const newMode = currentMode === 'dark' ? 'light' : 'dark';
            
            localStorage.setItem('theme', newMode);
            applyTheme(newMode);
        });
    }
}

// AUTO-INITIALIZATION
// This runs automatically when the script is loaded on any page
document.addEventListener('DOMContentLoaded', () => {
    // Ensure SITE_CONFIG is loaded first
    if (typeof SITE_CONFIG === 'undefined') {
        console.error('Error: page-config.js must be loaded before ui-manager.js');
        return;
    }

    const ui = new UIManager(SITE_CONFIG);

    // Try to render components if their mount points exist
    ui.renderNavbar('stickyHeader');
    ui.renderFooter('contact');
});