# A Domain Adaptation Framework for Harmonized Representation Learning in Medical Datasets


├── preprocessing/           # ETL Pipeline for MIMIC-III [cite: 150, 151]
│   ├── partition_clean.ps1  # Custom PowerShell script for large CSV management 
│   └── schema_setup.sql     # PostgreSQL relational schema and indexing [cite: 357, 358]
├── Experiment_1/            # Preliminary Analysis (RF, LR, MLP baselines) [cite: 203, 207]
│   ├── experiment_1.ipynb   # Analysis and model benchmarking
│   ├── results/             # Data saved in .npz format
│   └── plots/               # ROC and PR benchmark curves [cite: 276]
├── Experiment_2/            # Performance Ceiling identification [cite: 224]
│   ├── experiment_2.ipynb
│   ├── results/
│   └── plots/               # Maximum predictive performance curves [cite: 227]
├── Experiment_3/            # Data Scarcity simulations [cite: 248]
│   ├── experiment_3.ipynb
│   ├── results/
│   └── plots/               # Robustness analysis under limited data [cite: 311]
└── Experiment_4/            # Scarcity and Statistical Shift tests [cite: 258]
    ├── experiment_4.ipynb
    ├── results/
    └── plots/               # Demographic and comorbidity filtering results [cite: 321]
