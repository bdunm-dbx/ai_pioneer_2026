# Workshop: MLflow Evaluations for GenAI on Databricks

> **Audience**: Data Scientists, ML Engineers, and Platform Engineers
>
> **Goal**: Learn how to evaluate ML and GenAI models using **MLflow Evaluations** in Databricks, understand where results live in the UI, and how to operationalize evaluation workflows.

---

## 1. Workshop Overview

> **Audience**: GenAI Engineers, Applied AI Scientists, ML Engineers
>
> **Goal**: Learn how to evaluate **GenAI / LLM applications** using **MLflow Evaluations** in Databricks, understand how these evaluations differ from classic ML, and where to inspect results in the Databricks UI.

This workshop is **GenAI-first**. Classic ML evaluations are covered briefly for comparison.

---

## 2. What Are MLflow Evaluations?

MLflow Evaluations provide a **standardized framework** for evaluating:

- **GenAI / LLM applications** (text quality, relevance, safety)
- **Classic ML models** (classification, regression)

For GenAI, evaluations focus less on "accuracy" and more on **judgment-based metrics** that assess output quality.

Key GenAI benefits:

- Native support for **LLM-as-a-judge** evaluators
- Row-level qualitative and quantitative results
- Reproducible evaluation artifacts tied to prompts, models, and data

📌 *Talking point*: In GenAI, evaluations answer *“Is this response good?”* rather than *“Is this prediction correct?”*

---

## 3. Where MLflow Evaluations Fit in a GenAI Workflow

In GenAI applications, MLflow Evaluations typically happen:

1. After prompt, chain, or agent changes
2. Before deploying or updating a serving endpoint
3. Continuously after deployment for quality regression detection

They integrate with:

- MLflow Experiments (prompt + model iteration)
- Model Registry (governed promotion)
- Databricks Model Serving (endpoint-based evaluation)

📸 **Screenshot placeholder**: GenAI lifecycle with evaluation checkpoints

---

## 4. Prerequisites & Setup (UI)

Before starting:

1. Ensure you are in a Databricks workspace with MLflow enabled
2. Navigate to **Experiments** in the left-hand navigation
3. Create or select an existing MLflow Experiment

📸 **Screenshot placeholder**: Experiments UI

---

## 5. Running an Evaluation (High-Level Flow)

At a high level, running an evaluation involves:

- A trained model (or endpoint)
- An evaluation dataset
- An evaluation configuration (metrics, evaluators)

📌 *Talking point*: MLflow handles metric computation and logging automatically.

---

## 6. Evaluating a GenAI / LLM Application

### What Is Being Evaluated?

In GenAI, you are typically evaluating:

- Prompt templates
- Foundation models or fine-tuned LLMs
- Chains, agents, or tools
- End-to-end application outputs

### Inputs to a GenAI Evaluation

- Input prompts or questions
- Optional reference answers
- Model or endpoint outputs
- One or more evaluators (LLM, rule-based, or human)

📌 *Talking point*: The "model" is often just one component of what’s being evaluated.

📸 **Screenshot placeholder**: Notebook cell triggering a GenAI evaluation

---

## 7. Exploring Evaluation Results in the UI

After the evaluation completes:

1. Navigate to the **Experiment Runs** page
2. Select the run associated with the evaluation
3. Review:
   - Metrics tab
   - Artifacts tab

📸 **Screenshot placeholder**: Run details page

Key artifacts to highlight:

- `evaluation_results.json`
- Plots (confusion matrix, ROC, etc.)

---

## 8. Model Comparison Using Evaluations

Databricks allows you to compare evaluation results across runs.

### Steps in the UI:

1. Select multiple runs in the Experiment UI
2. Click **Compare**
3. Analyze metric differences side by side

📌 *Talking point*: This is especially powerful when evaluations are standardized.

📸 **Screenshot placeholder**: Run comparison view

---

## 9. GenAI Evaluators Explained

MLflow supports multiple evaluator types for GenAI:

### Automated LLM-as-a-Judge Evaluators

- Answer relevance
- Correctness (vs reference)
- Faithfulness / groundedness

### Safety & Policy Evaluators

- Toxicity
- Bias
- Harmful content

### Custom & Rule-Based Evaluators

- Regex or keyword checks
- Business logic validations

📌 *Talking point*: Evaluators themselves can be models — and should be versioned and tracked.

---

## 10. GenAI Evaluation Flow (UI-Oriented)

Typical flow:

1. Define prompts and expected outputs
2. Run the model (endpoint or notebook)
3. Trigger MLflow evaluation
4. Review results in the experiment run

📸 **Screenshot placeholder**: GenAI evaluation results table

---

## 11. Inspecting Per-Row Evaluation Results

For GenAI evaluations, MLflow logs **row-level results**, including:

- Model output
- Reference output
- Evaluation scores per row

Where to find them:

- Run Artifacts → Evaluation tables
- Downloadable JSON / Parquet artifacts

📸 **Screenshot placeholder**: Per-row evaluation artifact

---

## 12. Using GenAI Evaluations for Governance & Promotion

GenAI evaluations are critical for:

- Justifying prompt or model changes
- Preventing silent quality regressions
- Enabling governed promotion to Production

UI flow:

1. Register the model or GenAI pipeline from the run
2. View evaluation artifacts directly on the model version
3. Use metrics and qualitative examples during stage reviews

📌 *Talking point*: Evaluations provide **evidence**, not just scores.

📸 **Screenshot placeholder**: Model Registry with evaluation artifacts

---

## 13. Best Practices

**Standardize Evaluations**
- Use consistent datasets and metrics

**Log Early and Often**
- Treat evaluations as first-class artifacts

**Automate**
- Include evaluations in CI/CD or training pipelines

**Use Human Review Strategically**
- Especially for GenAI edge cases

---

## 14. Common GenAI Evaluation Pitfalls

- Relying on a single LLM judge
- Not versioning prompts or evaluation data
- Evaluating only averages instead of per-row failures
- Ignoring qualitative review for edge cases

📌 *Talking point*: GenAI failures are often rare but severe — averages can hide them.

---

## 15. GenAI vs Classic ML: Quick Comparison

| Dimension | Classic ML | GenAI / LLMs |
|--------|------------|--------------|
| Primary signal | Numerical accuracy | Judgment-based quality |
| Metrics | Accuracy, RMSE, AUC | Relevance, faithfulness, safety |
| Ground truth | Required | Optional or partial |
| Evaluation cost | Low | Higher (LLM calls) |
| Failure modes | Gradual | Discrete / semantic |

📌 *Talking point*: GenAI evaluation is closer to **code review** than test accuracy.

---

## 16. Wrap-Up & Next Steps

In this workshop, we covered:

- How MLflow Evaluations support GenAI workflows
- How GenAI evaluation differs from classic ML
- Where to inspect qualitative and quantitative results in the UI
- How to use evaluations for governance and deployment decisions

**Next steps**:

- Add GenAI evaluations to prompt iteration loops
- Combine automated and human review
- Use evaluation artifacts to gate production changes

---

## 16. Appendix (Optional)

- Links to Databricks documentation
- Example evaluation schemas
- Suggested live demo flow

📸 **Screenshot placeholder**: Final recap slide