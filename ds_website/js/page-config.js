// ============================================
// Central Configuration for All Pages
// ============================================

const SITE_CONFIG = {
  domain: 'https://yourdomain.com', // UPDATE THIS WITH YOUR ACTUAL DOMAIN
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
    datePublished: '2024-11-15',
    articleSection: 'Data Science Projects',
    about: [
      'Machine Learning',
      'Data Engineering',
      'Apache Spark',
      'Distributed Computing',
      'Time Series Analysis'
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