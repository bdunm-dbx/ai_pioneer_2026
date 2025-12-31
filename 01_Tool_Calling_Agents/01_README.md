# Hands-On Lab: Building Agent Systems with Databricks

## Architect Your First Agent with UC Functions
Follow the notebook 01_Tool_Calling_Agents/01_Create_Tools to see how Unity Catalog functions can provide helpful, business-tuned "tools" to LLM. This is the simplest example of agentic tools that can be used to craft your agent systems. Follow instructions in the notebook to create functions and then put them to practice in the AI Playground.

---
This first agent scenario will support the workflow of a customer service representative to illustrate the various agentic capabilites that can be accomplished using your existing structured data. We'll focus on processing product returns for our Telecom company as this gives us a tangible (and largely deterministic) set of steps to follow. The objective is to demonstrate the simplest form of agent tools using functions; later lab activities will demonstrate other tools that we can provide to agents.

### 1.1 Build Simple Tools
- **SQL Functions**: Create queries that access data critical to steps in the customer service workflow for processing a return.
- **Simple Python Function**: Create and register a Python function to overcome some common limitations of language models.
- Write queries to access data critical for handling a customer service return workflow.
- **Expore Unity Catalog**  
  - We'll go into Unity Catalog to see where our functions landed
  - This is a common governance layer that we'll use for out Data, Functions, and Agents

### 1.2 Integrate with an LLM [AI Playground]
- Combine the tools you created with a Language Model (LLM) in the AI Playground.
- Use the tools you create in your schema or, if you have issues, let the proctors know and they can direct you to backup functions.

### 1.3 Test the Agent [AI Playground]
- Ask the agent questions and observe the response.
- Dive deeper into the agent’s performance by exploring MLflow traces. Inspect agent runs in MLflow to understand how each tool is being called.  