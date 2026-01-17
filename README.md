# A Domain Adaptation Framework for Harmonized Representation Learning in Medical Datasets
This repository contains the complete implementation, preprocessing, and experimental results for the Master's Thesis submitted to the Universitat de Barcelona.

## Abstract 
This Master’s Thesis addresses the critical challenge of clinical data fragmentation and the prohibitive costs of medical data acquisition by proposing a deep learning architecture for cross-dataset knowledge transfer. While the medical community possesses vast amounts of data, it remains largely trapped in isolated silos characterized by structural heterogeneity and measurement bias. To bridge these gaps, this research introduces a multi-branch neural framework that leverages a large-scale auxiliary dataset, MIMIC-III, to enrich the latent representations of smaller, specialized target datasets.

The methodology centers on a dual-encoding strategy where a shared encoder extracts robust statistical patterns from common clinical attributes across populations, while independent private encoders preserve domain-specific niche variables. Empirical validation in the context of ICU mortality prediction demonstrates that this harmonized representation learning consistently improves Precision-Recall and AUC-ROC metrics. By employing a rigorous methodology upon sequential experiments, the study confirms that these performance gains are statistically significant and directly attributable to the enhanced feature representation, rather than artifacts of stochasticity or overfitting. 

Ultimately, this work provides a scalable blueprint for clinical data codification, proving that common attributes can serve as a functional bridge to maximize the utility of existing medical records in data-constrained environments.


## Architecture


The proposed architecture is designed as a multi-input, multi-output deep neural network that facilitates knowledge transfer through a shared latent space. The core of the model consists of three distinct encoding blocks: a shared encoder ($E_c$) and two independent private encoders ($E_p^A$ and $E_p^B$), as illustrated in the next figure.

<img width="50%" alt="Architecture" src="https://github.com/user-attachments/assets/17e53262-fc95-4bf1-8e2f-3666075a5583" />


We define two datasets, $X^A$ and $X^B$, which possess different feature spaces and dimensionalities. Each dataset is partitioned into private and common feature sets: 
    -    $X_p^A$ and $X_p^B$ represent the private (dataset-specific) features.
    -    $X_c^A$ and $X_c^B$ represent the common features shared between both domains.


Let us consider $X^A$ is significantly smaller than $X^B$. Our objective is to enhance the prediction capability of the model on $X^A$ by improving the representation of its common attributes through the auxiliary information in $X^B$.

The shared encoder $E_c$ serves as the primary mechanism for harmonized representation learning. It processes $X_c^A$ and $X_c^B$ to produce the shared latent representations $h_c^A$ and $h_c^B$ respectively. By routing shared variables through common weights, the smaller dataset $X^A$ leverages the broader statistical patterns of the larger dataset $X^B$ to improve the representation of common variables.

The private encoders  $E_p^A(x_p^A)$ and $E_p^B(x_p^B)$ are designed to process features unique to each dataset, generating private latent vectors $h_p^A$ and $h_p^B$. For the specialized dataset $X^A$, the private encoder ensures that niche variables—which do not exist in the auxiliary dataset—are preserved during the harmonization process.

All encoding blocks are implemented as feed-forward neural networks consisting of fully connected dense layers that progressively decrease in size. This funnel structure acts as a dimensionality reduction mechanism, forcing the encoders to extract the most salient features into a compressed latent space. To ensure robust training, penalize complexity, and reduce the risk of overfitting, the architecture incorporates batch normalization, dropout, and L2 regularization.

The latent representations are then concatenated (e.g., $h_p^A \oplus h_c^A$), followed by a single dense neural layer designed to fuse the shared and private information. The model is optimized against two separate losses simultaneously: $\mathcal{L}_A$ and $\mathcal{L}_B$. Both branches utilize a Softmax activation and Categorical Cross-Entropy with label smoothing. 

This multi-task learning environment allows the shared encoder to receive gradients from both paths. The larger auxiliary dataset $X^B$ provides a stable anchor for $E_c$, while gradients from $X^A$ fine-tune the shared space and the private encoder $E_p^A$ to leverage specific features. This dual-gradient flow is the fundamental mechanism for transferring knowledge from the larger dataset to the smaller one.

### Notes
The notebooks assume the availability of preprocessed data which are not included, one needs to download MIMIC-III first and do all the preprocessing
python3.11.9
