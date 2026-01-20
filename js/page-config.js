// ============================================
// Central Configuration for All Pages
// ============================================

const SITE_CONFIG = {
  domain: 'https://chadpvo.github.io/chadvo-ds-website/index.html#', // UPDATE THIS WITH YOUR ACTUAL DOMAIN
  author: {
    name: 'Chad Vo',
    jobTitle: 'Data Scientist',
    linkedin: 'https://linkedin.com/in/chadvo',
    github: 'https://github.com/chadpvo',
    email: 'chadpvo@gmail.com'
  }
};

// ============================================
// Page-Specific Metadata
// ============================================

const PAGE_METADATA = {
  // Homepage
  'home': {
    title: 'Chad Vo - Data Scientist',
    description: 'Data Scientist specializing in real estate analytics, machine learning, and data platforms.',
    keywords: 'data science, machine learning, real estate analytics, python, data engineering',
    image: '/assets/img/og-home.jpg',
    type: 'website',
    path: '/'
  },

  // Flight Delay Project
  'flight_delay': {
    title: 'Flight Delay Predictions | Chad Vo - Data Scientist',
    description: 'Machine learning pipeline predicting airline delays at scale using Apache Spark, XGBoost, and FT-Transformer on 30M flight records and weather data.',
    keywords: 'machine learning, Apache Spark, flight delay prediction, data engineering, XGBoost, PyTorch, distributed computing, time-series analysis',
    image: '/projects/flight_delay/assets/flight_delay_cover.jpg',
    type: 'article',
    path: '/projects/flight_delay/index.html',
    datePublished: '2026-1-12',
    articleSection: 'Data Science Projects',
    about: [
      'Machine Learning',
      'Data Engineering',
      'Apache Spark',
      'Distributed Computing',
      'Time Series Analysis'
    ]
  },

  // Drone Flight Route Visualizer Project
  'drone_flight_route_visualizer': {
    title: 'Drone Flight Route Visualizer | Chad Vo - Data Scientist',
    description: 'Drone Flight Route Visualizer: An interactive web app using D3.js to map and analyze drone flight paths with elevation profiles and geospatial data overlays.',
    keywords: 'visualization, D3.js, drone flight paths, geospatial data, elevation profiles, interactive web app',
    image: '/projects/drone_flight_route_visualizer/assets/drone_flight_route_visualizer_cover.jpg',
    type: 'article',
    path: 'projects/drone_flight_route_visualization/index.html',
    datePublished: '2026-1-17',
    articleSection: 'Data Visualization Projects',
    about: [
      'Data Visualization',
      'D3.js',
      'Geospatial Analysis',
      'Web Development',
      'Interactive Applications',
      'Data Visualization'
    ]
  },
    // Hotel Cancellation Prediction Project
  'hotel_cancellation_prediction': {
    title: 'Hotel Cancellation Prediction | Chad Vo - Data Scientist',
    description: 'Hotel Cancellation Prediction: A machine learning project predicting hotel booking cancellations using various algorithms and data preprocessing techniques.',
    keywords: 'machine learning, hotel cancellation, prediction, data preprocessing, algorithms',
    image: '/projects/hotel_cancellation/assets/hotel_cancellation_cover.jpg',
    type: 'article',
    path: 'projects/hotel_cancellation/index.html',
    datePublished: '2026-1-19',
    articleSection: 'Data Science Projects',
    about: [
      'Machine Learning',
      'Data Engineering',
      'Distributed Computing',
      'Time Series Analysis',
      'Feature Engineering',
      'TensorFlow'
    ]
  },
  // Template for adding new projects
  'project_template': {
    title: 'Project Name | Chad Vo - Data Scientist',
    description: 'Brief description of the project (140-160 characters)',
    keywords: 'keyword1, keyword2, keyword3',
    image: '/projects/project_name/assets/cover.jpg',
    type: 'article',
    path: '/projects/project_name/index.html',
    datePublished: '2025-01-01',
    articleSection: 'Data Science Projects',
    about: ['Topic 1', 'Topic 2', 'Topic 3']
  }
};