// ============================================
// SEO Meta Tag Injector
// Automatically injects SEO tags based on page config
// ============================================

function injectSEOTags(pageKey) {
  const page = PAGE_METADATA[pageKey];
  if (!page) {
    console.error(`Page metadata not found for key: ${pageKey}`);
    return;
  }

  const fullURL = SITE_CONFIG.domain + page.path;
  const fullImageURL = SITE_CONFIG.domain + page.image;

  // ============================================
  // Core Meta Tags
  // ============================================
  
  document.title = page.title;
  
  setMetaTag('description', page.description);
  setMetaTag('author', SITE_CONFIG.author.name);
  setMetaTag('keywords', page.keywords);
  
  // Canonical URL
  setLinkTag('canonical', fullURL);

  // ============================================
  // Open Graph Tags (LinkedIn, Facebook, etc.)
  // ============================================
  
  setMetaProperty('og:title', page.title);
  setMetaProperty('og:description', page.description);
  setMetaProperty('og:url', fullURL);
  setMetaProperty('og:type', page.type);
  setMetaProperty('og:image', fullImageURL);

  // ============================================
  // Twitter Card Tags
  // ============================================
  
  setMetaTag('twitter:card', 'summary_large_image', 'name');
  setMetaTag('twitter:title', page.title, 'name');
  setMetaTag('twitter:description', page.description, 'name');
  setMetaTag('twitter:image', fullImageURL, 'name');

  // ============================================
  // Structured Data (JSON-LD)
  // ============================================
  
  injectStructuredData(page, fullURL);
}

// ============================================
// Helper Functions
// ============================================

function setMetaTag(name, content, attribute = 'name') {
  let meta = document.querySelector(`meta[${attribute}="${name}"]`);
  if (!meta) {
    meta = document.createElement('meta');
    meta.setAttribute(attribute, name);
    document.head.appendChild(meta);
  }
  meta.setAttribute('content', content);
}

function setMetaProperty(property, content) {
  setMetaTag(property, content, 'property');
}

function setLinkTag(rel, href) {
  let link = document.querySelector(`link[rel="${rel}"]`);
  if (!link) {
    link = document.createElement('link');
    link.setAttribute('rel', rel);
    document.head.appendChild(link);
  }
  link.setAttribute('href', href);
}

function injectStructuredData(page, fullURL) {
  // Remove existing structured data if present
  const existing = document.querySelector('script[type="application/ld+json"]');
  if (existing) existing.remove();

  const script = document.createElement('script');
  script.type = 'application/ld+json';

  let structuredData;

  if (page.type === 'article') {
    // Article Schema
    structuredData = {
      "@context": "https://schema.org",
      "@type": "TechArticle",
      "headline": page.title.split('|')[0].trim(),
      "author": {
        "@type": "Person",
        "name": SITE_CONFIG.author.name,
        "url": SITE_CONFIG.domain,
        "jobTitle": SITE_CONFIG.author.jobTitle,
        "sameAs": [
          SITE_CONFIG.author.linkedin,
          SITE_CONFIG.author.github
        ]
      },
      "datePublished": page.datePublished,
      "description": page.description,
      "keywords": page.keywords,
      "articleSection": page.articleSection,
      "about": page.about,
      "url": fullURL,
      "image": SITE_CONFIG.domain + page.image
    };
  } else {
    // Website/Person Schema (for homepage)
    structuredData = {
      "@context": "https://schema.org",
      "@type": "Person",
      "name": SITE_CONFIG.author.name,
      "url": SITE_CONFIG.domain,
      "jobTitle": SITE_CONFIG.author.jobTitle,
      "sameAs": [
        SITE_CONFIG.author.linkedin,
        SITE_CONFIG.author.github
      ],
      "knowsAbout": page.about || [
        "Machine Learning",
        "Real Estate Analytics",
        "Python",
        "Data Engineering"
      ]
    };
  }

  script.textContent = JSON.stringify(structuredData, null, 2);
  document.head.appendChild(script);
}

// ============================================
// Auto-detect page and inject on load
// ============================================

document.addEventListener('DOMContentLoaded', function() {
  // Get page key from body data attribute
  const pageKey = document.body.getAttribute('data-page');
  
  if (pageKey) {
    injectSEOTags(pageKey);
  } else {
    console.warn('No data-page attribute found on <body>. SEO tags not injected.');
  }
});