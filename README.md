KidsPlaza Google Ads Pipeline powers the internal ETL process at KidsPlaza, automating the ingestion, transformation, and loading of digital advertising data into our centralized Google BigQuery warehouse.

It integrates Google Ads Insights data, supporting both daily sync and historical backfills. The pipeline is structured around modular stages such as fetch, enrich, ingest, staging, and mart, making it easier to test, maintain, and extend.

This repository is maintained by the Digital Marketing Team at KidsPlaza. For questions, access requests, or contributions, please contact the team via email at quang.nn@kidsplaza.vn (internal) or nhatquang.nguyen.129@gmail.com (external), or reach out via the internal Slack channel #data-engineering.

⚠️ Disclaimer: This project is intended for internal use only. It contains custom business logic designed specifically for KidsPlaza’s digital marketing workflows, naming conventions, and budgeting structures. Do not reuse, replicate, or adapt this codebase outside of this context without prior approval.

📄 License: All content and source code in this repository is proprietary to KidsPlaza. Redistribution, publication, or open-sourcing of any part of this project is strictly prohibited without explicit written consent from the company.

🤖 AI-Assisted Development: This repository includes code, documentation, and architectural guidance that has been partially developed or enhanced using AI tools (e.g., GitHub Copilot, ChatGPT by OpenAI), under the supervision of the development team. All AI-generated output has been reviewed and adapted to meet KidsPlaza's internal production standards.