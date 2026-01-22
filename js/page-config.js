// Automatically detects if we are on GitHub Pages or Localhost
const isGitHubPages = window.location.hostname.includes('github.io');
const repoName = '/chadvo-ds-website'; // Your repository name

// If on GitHub, use repo name. If local, use root (empty string)
const pathPrefix = isGitHubPages ? repoName : ''; 

const SITE_CONFIG = {
  domain: 'https://chadpvo.github.io/chadvo-ds-website/',
  
  // DYNAMIC LOGO PATH (Fixes the broken image!)
  logoPath: `${pathPrefix}/assets/img/logo.png`, 
  
  author: {
    name: 'Chad Vo',
    jobTitle: 'Data Scientist',
    linkedin: 'https://linkedin.com/in/chadvo',
    github: 'https://github.com/chadpvo',
    email: 'chadpvo@gmail.com'
  },

  // Dynamic Navigation Links (Fixes broken menu links locally)
  navLinks: [
    { name: "About", url: `${pathPrefix}/index.html#about` },
    { name: "Contact", url: `${pathPrefix}/index.html#contact` }
  ],

  // Footer Social Links
  socialLinks: [
    { icon: "fab fa-linkedin", url: "https://linkedin.com/in/chadvo" },
    { icon: "fab fa-github", url: "https://github.com/chadpvo" },
    { icon: "fas fa-envelope", url: "mailto:chadpvo@gmail.com" }
  ],

  copyrightYear: new Date().getFullYear()
};

// ============================================
// 3. PAGE METADATA
// ============================================

const PAGE_METADATA = {
  'home': {
    title: 'Chad Vo - Data Scientist',
    description: 'Data Scientist specializing in real estate analytics.',
    keywords: 'data science, machine learning, python',
    image: `${pathPrefix}/assets/img/og-home.jpg`,
    type: 'website',
    path: '/'
  },
  
  'flight_delay': {
    title: 'Flight Delay Predictions',
    description: 'Machine learning pipeline predicting airline delays.',
    image: `${pathPrefix}/projects/flight_delay/assets/flight_delay_cover.jpg`,
    type: 'article',
    path: '/projects/flight_delay/index.html'
  },

  'map_building_tool': {
    title: 'Economic Map Tool',
    description: 'Interactive map visualizing US Economic data.',
    image: `${pathPrefix}/projects/map_viz/assets/map_cover.jpg`,
    type: 'application',
    path: '/projects/map_viz/index.html'
  },
  
  'drone_flight_route_visualizer': {
    title: 'Drone Flight Route Visualizer',
    description: 'Interactive web app using D3.js to map drone paths.',
    image: `${pathPrefix}/projects/drone_flight_route_visualizer/assets/drone_flight_route_visualizer_cover.jpg`,
    type: 'article',
    path: 'projects/drone_flight_route_visualization/index.html'
  },

  'hotel_booking_cancellation_prediction': {
    title: 'Hotel Booking Cancellation Prediction',
    description: 'Predicting hotel booking cancellations using ML.',
    image: `${pathPrefix}/projects/hotel_booking_cancellation/assets/hotel_cancellation_cover.jpg`,
    type: 'article',
    path: 'projects/hotel_booking_cancellation/index.html'
  }
};