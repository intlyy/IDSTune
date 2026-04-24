# Prompt Templates

This folder contains the prompt templates used in our paper.

## Contents

We provide three categories of prompt templates:

- **`Prompt_Feature_Selection`**  
  Prompt used for workload feature selection, enabling the system to identify relevant workload characteristics for downstream tuning.

- **`Prompt_Specialist_Agent`**  
  Prompt used by specialist agents, including:
  - Knob Tuner  
  - Index Recommender  
  - Materialized View Recommender  

- **`Prompt_Supervisor_Agent`**  
  Prompt used by the supervisor agent to coordinate decisions across specialists and reach a consensus configuration.

## PDF Versions

For better readability, we also provide corresponding PDF files for each prompt template:
- `Prompt_Feature_Selection.pdf`
- `Prompt_Specialist_Agent.pdf`
- `Prompt_Supervisor_Agent.pdf`

These PDFs present the prompts in a formatted and structured manner as referenced in the paper.

## Notes

- The templates are presented in a generalized form and may omit system-specific details.
- Minor variations may be applied during runtime (e.g., dynamic inputs, context injection).
- The prompts are designed to be **modular and reusable**, supporting different database systems and workloads.